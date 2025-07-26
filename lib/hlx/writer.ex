defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  alias HLX.Writer.Rendition

  @type mode :: :live | :vod
  @type segment_type :: :mpeg_ts | :fmp4
  @type tracks :: [ExMP4.Track.t()]
  @type rendition_opts :: [
          {:type, :audio}
          | {:track, ExMP4.Track.t()}
          | {:group_id, String.t()}
          | {:default, boolean()}
          | {:auto_select, boolean()}
        ]

  @type variant_opts :: [{:tracks, [ExMP4.Track.t()]} | {:audio, String.t()}]
  @type new_opts :: [
          {:type, :master | :media}
          | {:mode, mode()}
          | {:segment_type, segment_type()}
          | {:max_segments, non_neg_integer()}
        ]

  @opaque t :: %__MODULE__{
            type: :master | :media,
            mode: mode(),
            segment_type: segment_type(),
            variants: %{String.t() => HLX.Writer.Rendition.t()},
            lead_variant: String.t() | nil,
            max_segments: non_neg_integer()
          }

  defstruct [:type, :mode, :segment_type, :variants, :lead_variant, :max_segments]

  @doc """
  Creates a new HLS writer.

  The following options can be provided:
    * `type` - The type of the playlist, either `:master` or `:media`. Defaults to `:media`.
    * `mode` - The mode of the playlist, either `:live` or `:vod`. Defaults to `:live`.
    * `segment_type` - The type of segments to write, either `:mpeg_ts` or `:fmp4`. Defaults to `:fmp4`.
    * `max_segments` - The maximum number of segments to keep in the playlist, ignore on `vod` mode. Defaults to 0 (no limit).
  """
  @spec new(Keyword.t()) :: {:ok, t()}
  def new(options) do
    with {:ok, options} <- validate_writer_opts(options) do
      {:ok, struct!(%__MODULE__{variants: %{}}, options)}
    end
  end

  @doc """
  Add a new rendition to the master playlist.

  This adds a new `EXT-X-MEDIA` entry to the master playlist.

  The `name` should be unique across variants and renditions and it's used as the name of
  the playlist (`name.m3u8`).

  The following parameter may be provided:
    * `type` - [Required] The type should always be `:audio` since it's the only supported media type.
    * `track` - [Required] The track that defines the media.
    * `group_id` - [Required] The group id of the rendition.
    * `default` - A boolean indicating if this is the default rendition.
    * `auto_select` - A boolean setting the auto select.
  """
  @spec add_rendition(t(), String.t(), rendition_opts()) :: {:ok, t()} | {:error, any()}
  def add_rendition(%{type: :media}, _name, _opts), do: {:error, :not_master_playlist}

  def add_rendition(writer, name, opts) do
    # Validate options
    rendition_opts =
      [
        type: :rendition,
        target_duration: 2000,
        segment_type: writer.segment_type,
        max_segments: writer.max_segments
      ] ++
        Keyword.take(opts, [:group_id, :default, :auto_select])

    rendition = do_add_rendition(name, [opts[:track]], rendition_opts)
    {:ok, %{writer | variants: Map.put(writer.variants, name, rendition)}}
  end

  @doc """
  Add a new variant to the master playlist.

  This adds a new `EXT-X-STREAM-INF` entry to the master playlist.

  The `name` should be unique across variants and renditions and it's used as the name of
  the playlist (`<name>.m3u8`).

  The following parameter may be provided:
    * `tracks` - [Required] One or more tracks definitions, all the nedia are muxed in the same segment.
    * `audio` - Reference to a `group_id` of a rendition.
  """
  @spec add_variant(t(), String.t(), variant_opts()) :: {:ok | t()} | {:error, any()}
  def add_variant(writer, _name, _options)
      when writer.type == :media and map_size(writer.variants) >= 1 do
    {:error, "Media playlist support only one variant"}
  end

  def add_variant(writer, name, options) do
    # TODO: validate options
    rendition_options = [
      target_duration: 2000,
      audio: options[:audio],
      segment_type: writer.segment_type,
      max_segments: writer.max_segments
    ]

    rendition = do_add_rendition(name, options[:tracks], rendition_options)

    lead_variant =
      cond do
        not is_nil(writer.lead_variant) -> writer.lead_variant
        not is_nil(rendition.lead_track) -> name
        true -> nil
      end

    writer = %{
      writer
      | variants: Map.put(writer.variants, name, rendition),
        lead_variant: lead_variant
    }

    {:ok, writer}
  end

  @doc """
  Writes a sample to the specified variant or rendition.
  """
  @spec write_sample(t(), String.t() | nil, ExMP4.Sample.t()) :: t()
  def write_sample(writer, variant_or_rendition \\ nil, sample) do
    variant = writer.variants[variant_or_rendition]
    flush? = Rendition.flush?(variant, sample)
    lead_variant? = writer.lead_variant == variant_or_rendition

    {variant, flushed?} =
      if (writer.lead_variant == nil or lead_variant? or variant.lead_track != nil) and flush? do
        variant =
          variant
          |> flush_and_write()
          |> Rendition.push_sample(sample)

        {variant, true}
      else
        {Rendition.push_sample(variant, sample), false}
      end

    variants = Map.delete(writer.variants, variant_or_rendition)

    variants =
      if flush? and lead_variant? do
        Map.new(variants, fn {name, variant} ->
          variant = if variant.lead_track, do: variant, else: flush_and_write(variant)
          {name, variant}
        end)
      else
        variants
      end

    variants =
      if flushed? and writer.type == :master do
        variants = Map.put(variants, variant_or_rendition, variant)
        serialize_master_playlist(variants)
        variants
      else
        Map.put(variants, variant_or_rendition, variant)
      end

    %{writer | variants: variants}
  end

  defp do_add_rendition(name, tracks, options) do
    :ok = File.mkdir_p(name)
    init_uri = Path.join(name, "init.mp4")

    {init_data, rendition} =
      name
      |> Rendition.new(tracks, options)
      |> Rendition.init_header(init_uri)

    if init_data != <<>> do
      :ok = File.write!(init_uri, init_data)
    end

    rendition
  end

  defp flush_and_write(variant) do
    uri = Path.join(variant.name, Rendition.generate_segment_name(variant))
    {data, discarded_segment, variant} = Rendition.flush(variant, uri)

    File.write!(uri, data)
    File.write!("#{variant.name}.m3u8", HLX.MediaPlaylist.serialize(variant.playlist))

    if discarded_segment do
      if discarded_segment.media_init, do: File.rm(discarded_segment.media_init)
      File.rm(discarded_segment.uri)
    end

    variant
  end

  defp serialize_master_playlist(variants) do
    variants = Map.values(variants)

    streams =
      Enum.map(variants, fn variant ->
        renditions = get_referenced_renditions(variant, variants)

        {avg_bitrates, max_bitrates} =
          renditions
          |> Map.values()
          |> Enum.map(fn renditions ->
            renditions
            |> Enum.map(&Rendition.bandwidth/1)
            |> Enum.unzip()
            |> then(fn {a, m} -> {Enum.max(a), Enum.max(m)} end)
          end)
          |> Enum.unzip()

        Rendition.to_hls_tag(variant, {max_bitrates, avg_bitrates})
      end)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: 7,
        independent_segments: true,
        items: streams
      })

    File.write!("master.m3u8", payload)
  end

  defp get_referenced_renditions(rendition, renditions) do
    case Rendition.referenced_renditions(rendition) do
      [] ->
        %{}

      group_ids ->
        renditions
        |> Enum.group_by(&Rendition.group_id/1)
        |> Map.take(group_ids)
    end
  end

  defp validate_writer_opts(options) do
    defaults = [type: :media, mode: :live, segment_type: :fmp4, max_segments: 0]

    with {:ok, validated_options} <- Keyword.validate(options, defaults),
         :ok <- do_validate_writer_option(validated_options) do
      {:ok, validated_options}
    end
  end

  defp do_validate_writer_option([]), do: :ok

  defp do_validate_writer_option([{:type, type} | rest]) when type in [:media, :master] do
    do_validate_writer_option(rest)
  end

  defp do_validate_writer_option([{:mode, mode} | rest]) when mode in [:vod, :live] do
    do_validate_writer_option(rest)
  end

  defp do_validate_writer_option([{:segment_type, type} | rest]) when type in [:mpeg_ts, :fmp4] do
    do_validate_writer_option(rest)
  end

  defp do_validate_writer_option([{:max_segments, max_segments} | rest])
       when max_segments == 0 or max_segments >= 5 do
    do_validate_writer_option(rest)
  end

  defp do_validate_writer_option([{key, value} | _rest]) do
    {:error, "Invalid value for #{to_string(key)}: #{inspect(value)}"}
  end

  defimpl Inspect do
    def inspect(writer, _opts) do
      "#HLX.Writer<type: #{writer.type}, variants: #{map_size(writer.variants)}, " <>
        "lead_variant: #{writer.lead_variant}, max_segments: #{writer.max_segments}>"
    end
  end
end
