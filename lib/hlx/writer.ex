defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  alias HLX.Writer.Rendition

  @type mode :: :live | :vod
  @type segment_type :: :mpeg_ts | :fmp4
  @type tracks :: [HLX.Track.t()]
  @type rendition_opts :: [
          {:type, :audio}
          | {:track, HLX.Track.t()}
          | {:group_id, String.t()}
          | {:default, boolean()}
          | {:auto_select, boolean()}
        ]

  @type variant_opts :: [{:tracks, [HLX.Track.t()]} | {:audio, String.t()}]
  @type new_opts :: [
          {:type, :master | :media}
          | {:mode, mode()}
          | {:segment_type, segment_type()}
          | {:max_segments, non_neg_integer()}
          | {:storage, struct()}
        ]

  @opaque t :: %__MODULE__{
            type: :master | :media,
            mode: mode(),
            version: non_neg_integer(),
            segment_type: segment_type(),
            storage: HLX.Storage.t(),
            max_segments: non_neg_integer(),
            lead_variant: String.t() | nil,
            variants: %{String.t() => HLX.Writer.Rendition.t()}
          }

  defstruct [
    :type,
    :mode,
    :segment_type,
    :version,
    :storage,
    :max_segments,
    :lead_variant,
    :variants
  ]

  @doc """
  Creates a new HLS writer.

  The following options can be provided:
    * `type` - The type of the playlist, either `:master` or `:media`. Defaults to `:media`.
    * `mode` - The mode of the playlist, either `:live` or `:vod`. Defaults to `:live`.
    * `segment_type` - The type of segments to write, either `:mpeg_ts` or `:fmp4`. Defaults to `:fmp4`.
    * `max_segments` - The maximum number of segments to keep in the playlist, ignore on `vod` mode. Defaults to 0 (no limit).
    * `storage` - Storage configuration, a struct implementing `HLX.Storage`
  """
  @spec new(Keyword.t()) :: {:ok, t()}
  def new(options) do
    with {:ok, options} <- validate_writer_opts(options) do
      {storage, options} = Keyword.pop!(options, :storage)
      {:ok, struct!(%__MODULE__{variants: %{}, storage: HLX.Storage.new(storage)}, options)}
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
    rendition_options =
      [
        type: :rendition,
        target_duration: 2000,
        segment_type: writer.segment_type,
        max_segments: writer.max_segments
      ] ++
        Keyword.take(opts, [:group_id, :default, :auto_select])

    with {:ok, rendition} <- Rendition.new(name, [opts[:track]], rendition_options) do
      {:ok, maybe_save_init_header(writer, rendition)}
    end
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
  @spec add_variant(t(), String.t(), variant_opts()) :: {:ok, t()} | {:error, any()}
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

    with {:ok, rendition} <- Rendition.new(name, options[:tracks], rendition_options) do
      writer = maybe_save_init_header(writer, rendition)

      lead_variant =
        cond do
          not is_nil(writer.lead_variant) -> writer.lead_variant
          not is_nil(rendition.lead_track) -> name
          true -> nil
        end

      {:ok, %{writer | lead_variant: lead_variant}}
    end
  end

  @doc """
  Writes a sample to the specified variant or rendition.
  """
  @spec write_sample(t(), String.t(), HLX.Sample.t()) :: t()
  def write_sample(writer, variant_or_rendition, sample) do
    variant = Map.fetch!(writer.variants, variant_or_rendition)

    if Rendition.ready?(variant) do
      do_push_sample(writer, variant, sample)
    else
      rendition = Rendition.push_sample(variant, sample, writer.storage, false)
      maybe_save_init_header(writer, rendition)
    end
  end

  @doc """
  Closes the writer.

  Closes the writer and flush any pending segments. if the `mode` is `vod` creates the final
  playlists.
  """
  @spec close(t()) :: :ok
  def close(writer) do
    {variants, writer} =
      Enum.map_reduce(writer.variants, writer, fn {name, variant}, writer ->
        {writer, variant} = flush_and_write(writer, variant)
        {{name, variant}, writer}
      end)

    serialize_playlists(%{writer | variants: Map.new(variants)}, true)
    :ok
  end

  defp do_push_sample(writer, variant, sample) do
    lead_variant? = writer.lead_variant == variant.name
    end_segment? = writer.lead_variant == nil or lead_variant? or variant.lead_track != nil

    {variant, storage, flushed?} =
      Rendition.push_sample(variant, sample, writer.storage, end_segment?)

    variants = Map.delete(writer.variants, variant.name)

    {variants, writer} =
      if flushed? and lead_variant? do
        Enum.map_reduce(variants, writer, fn {name, variant}, writer ->
          case variant.lead_track do
            nil ->
              {writer, variant} = flush_and_write(writer, variant)
              {{name, variant}, writer}

            _ ->
              {{name, variant}, writer}
          end
        end)
        |> then(fn {variants, writer} -> {Map.new(variants), writer} end)
      else
        {variants, writer}
      end

    writer = %{writer | variants: Map.put(variants, variant.name, variant), storage: storage}
    if flushed? and writer.mode == :live, do: serialize_playlists(writer), else: writer
  end

  defp maybe_save_init_header(writer, rendition) do
    if Rendition.ready?(rendition) do
      {rendition, storage} = Rendition.save_init_header(rendition, writer.storage)
      %{writer | storage: storage, variants: Map.put(writer.variants, rendition.name, rendition)}
    else
      %{writer | variants: Map.put(writer.variants, rendition.name, rendition)}
    end
  end

  defp flush_and_write(writer, variant) do
    {variant, storage} = Rendition.flush(variant, writer.storage)
    {%{writer | storage: storage}, variant}
  end

  defp serialize_playlists(writer, end_list? \\ false)

  defp serialize_playlists(%{mode: :vod} = writer, false), do: writer

  defp serialize_playlists(%{variants: variants} = writer, end_list?) do
    {variants, storage} =
      Enum.map_reduce(variants, writer.storage, fn {_key, variant}, storage ->
        playlist = HLX.MediaPlaylist.to_m3u8_playlist(variant.playlist)

        playlist = %{
          playlist
          | info: %{
              playlist.info
              | version: writer.version,
                playlist_type: if(writer.mode == :vod, do: :vod)
            }
        }

        playlist = ExM3U8.serialize(playlist)
        playlist = if end_list?, do: playlist <> "#EXT-X-ENDLIST\n", else: playlist

        {uri, storage} = HLX.Storage.store_playlist(variant.name, playlist, storage)
        {{uri, variant}, storage}
      end)

    if writer.type == :master do
      storage = serialize_master_playlist(writer, variants)
      %{writer | storage: storage}
    else
      %{writer | storage: storage}
    end
  end

  defp serialize_master_playlist(writer, variants) do
    streams =
      Enum.map(variants, fn {uri, variant} ->
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

        %{Rendition.to_hls_tag(variant, {max_bitrates, avg_bitrates}) | uri: uri}
      end)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: writer.version,
        independent_segments: true,
        items: streams
      })

    HLX.Storage.store_master_playlist(payload, writer.storage)
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
    defaults = [type: :media, mode: :live, segment_type: :fmp4, max_segments: 0, storage: nil]

    with {:ok, validated_options} <- Keyword.validate(options, defaults),
         :ok <- do_validate_writer_option(validated_options) do
      validated_options =
        if validated_options[:mode] == :vod,
          do: Keyword.replace!(validated_options, :max_segments, 0),
          else: validated_options

      version = if validated_options[:segment_type] == :mpeg_ts, do: 6, else: 7

      {:ok, Keyword.put(validated_options, :version, version)}
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
       when max_segments == 0 or max_segments >= 3 do
    do_validate_writer_option(rest)
  end

  defp do_validate_writer_option([{:storage, storage} | rest]) when is_struct(storage) do
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
