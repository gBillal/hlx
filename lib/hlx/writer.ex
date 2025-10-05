defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  alias HLX.SampleQueue
  alias HLX.Writer.{TracksMuxer, Variant}

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
            variants: %{String.t() => Variant.t()},
            state: :init | :muxing | :closed
          }

  defstruct [
    :type,
    :mode,
    :segment_type,
    :version,
    :storage,
    :max_segments,
    :lead_variant,
    variants: %{},
    state: :init
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
      {:ok, struct(%__MODULE__{storage: HLX.Storage.new(storage)}, options)}
    end
  end

  @doc """
  Same as `new/1`, but raises an error if the operation fails.
  """
  @spec new!(Keyword.t()) :: t()
  def new!(options) do
    case new(options) do
      {:ok, writer} -> writer
      {:error, reason} -> raise "Failed to create writer: #{inspect(reason)}"
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
    muxer_options = [segment_type: writer.segment_type, max_segments: writer.max_segments]
    rendition_options = opts ++ [type: :rendition, max_segments: writer.max_segments]

    with {:ok, rendition} <- TracksMuxer.new(name, [opts[:track]], muxer_options) do
      variant = Variant.new(name, rendition, rendition_options)
      {:ok, maybe_save_init_header(writer, variant)}
    end
  end

  @doc """
  Same as `add_rendition/3`, but raises an error if the operation fails.
  """
  @spec add_rendition!(t(), String.t(), rendition_opts()) :: t()
  def add_rendition!(writer, name, opts) do
    case add_rendition(writer, name, opts) do
      {:ok, writer} -> writer
      {:error, reason} -> raise "Failed to add rendition: #{inspect(reason)}"
    end
  end

  @doc """
  Add a new variant to the master playlist.

  This adds a new `EXT-X-STREAM-INF` entry to the master playlist.

  The `name` should be unique across variants and renditions and it's used as the name of
  the playlist (`<name>.m3u8`).

  The following parameter may be provided:
    * `tracks` - [Required] One or more tracks definitions, all the media are muxed in the same segment.
    * `audio` - Reference to a `group_id` of a rendition.
  """
  @spec add_variant(t(), String.t(), variant_opts()) :: {:ok, t()} | {:error, any()}
  def add_variant(writer, _name, _options)
      when writer.type == :media and map_size(writer.variants) >= 1 do
    {:error, "Media playlist support only one variant"}
  end

  def add_variant(writer, name, options) do
    # TODO: validate options
    muxer_options = [segment_type: writer.segment_type]

    rendition_options = [
      target_duration: 2000,
      segment_type: writer.segment_type,
      max_segments: writer.max_segments,
      audio: options[:audio],
      type: :variant
    ]

    with {:ok, rendition} <- TracksMuxer.new(name, options[:tracks], muxer_options) do
      variant = Variant.new(name, rendition, rendition_options)
      writer = maybe_save_init_header(writer, variant)

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
  Same as `add_variant/3`, but raises an error if the operation fails.
  """
  @spec add_variant!(t(), String.t(), variant_opts()) :: t()
  def add_variant!(writer, name, options) do
    case add_variant(writer, name, options) do
      {:ok, writer} -> writer
      {:error, reason} -> raise "Failed to add variant: #{inspect(reason)}"
    end
  end

  @doc """
  Writes a sample to the specified variant or rendition.
  """
  @spec write_sample(t(), String.t(), HLX.Sample.t()) :: t()
  def write_sample(%{state: :init} = writer, variant_id, sample) do
    writer =
      cond do
        writer.type == :media ->
          [{id, variant}] = Map.to_list(writer.variants)
          %{writer | variants: Map.put(writer.variants, id, Variant.create_sample_queue(variant))}

        is_nil(writer.lead_variant) ->
          variants =
            Map.new(writer.variants, fn {id, variant} ->
              {id, Variant.create_sample_queue(variant)}
            end)

          %{writer | variants: variants}

        true ->
          lead_variant_id = writer.lead_variant

          {dependant_variants, independant_variants} =
            writer.variants
            |> Map.values()
            |> Enum.split_with(
              &(&1.config.type == :rendition or is_nil(&1.tracks_muxer.lead_track))
            )

          variants =
            Enum.reduce(independant_variants, writer.variants, fn variant, variants ->
              extra_variants = if variant.id == lead_variant_id, do: dependant_variants, else: []

              Map.put(
                variants,
                variant.id,
                Variant.create_sample_queue(variant, extra_variants)
              )
            end)

          dependant_variants
          |> Enum.reduce(variants, fn variant, variants ->
            Map.update!(variants, variant.id, &%{&1 | depends_on: lead_variant_id})
          end)
          |> then(&%{writer | variants: &1})
      end

    write_sample(%{writer | state: :muxing}, variant_id, sample)
  end

  def write_sample(writer, variant_id, sample) do
    variant = Map.fetch!(writer.variants, variant_id)
    ready? = TracksMuxer.ready?(variant.tracks_muxer)

    {tracks_muxer, sample} = TracksMuxer.process_sample(variant.tracks_muxer, sample)
    variant = %{variant | tracks_muxer: tracks_muxer}

    writer =
      if not ready? and TracksMuxer.ready?(tracks_muxer),
        do: maybe_save_init_header(writer, variant),
        else: %{writer | variants: Map.put(writer.variants, variant.id, variant)}

    queue_variant =
      case variant.depends_on do
        nil -> variant
        id -> writer.variants[id]
      end

    id = {variant_id, sample.track_id}

    case SampleQueue.push_sample(queue_variant.queue, id, sample) do
      {true, samples, queue} ->
        writer = flush_and_write(writer, SampleQueue.track_ids(queue))

        variants =
          samples
          |> push_samples(writer.variants)
          |> Map.update!(queue_variant.id, &%{&1 | queue: queue})

        serialize_playlists(%{writer | variants: variants})

      {false, samples, queue} ->
        variants =
          samples
          |> push_samples(writer.variants)
          |> Map.update!(queue_variant.id, &%{&1 | queue: queue})

        %{writer | variants: variants}
    end
  end

  @doc """
  Closes the writer.

  Closes the writer and flush any pending segments. if the `mode` is `vod` creates the final
  playlists.
  """
  @spec close(t()) :: :ok
  def close(writer) do
    writer
    |> flush_and_write()
    |> serialize_playlists(true)

    :ok
  end

  defp maybe_save_init_header(writer, %{tracks_muxer: tracks_muxer} = variant) do
    if TracksMuxer.ready?(tracks_muxer) do
      {variant, storage} = Variant.save_init_header(variant, writer.storage)
      %{writer | storage: storage, variants: Map.put(writer.variants, variant.id, variant)}
    else
      %{writer | variants: Map.put(writer.variants, variant.id, variant)}
    end
  end

  defp push_samples(samples, variants) do
    Enum.reduce(samples, variants, fn {{name, _id}, sample}, variants ->
      Map.update!(variants, name, &Variant.push_sample(&1, sample))
    end)
  end

  defp flush_and_write(writer, variant_ids \\ nil) do
    writer.variants
    |> Enum.map_reduce(writer, fn {id, variant}, writer ->
      if variant_ids == nil or id in variant_ids do
        {variant, storage} = Variant.flush(variant, writer.storage)
        {{id, variant}, %{writer | storage: storage}}
      else
        {{id, variant}, writer}
      end
    end)
    |> then(fn {variants, writer} -> %{writer | variants: Map.new(variants)} end)
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

        {uri, storage} = HLX.Storage.store_playlist(variant.id, playlist, storage)
        {{uri, variant}, storage}
      end)

    if writer.type == :master do
      storage = serialize_master_playlist(%{writer | storage: storage}, variants)
      %{writer | storage: storage}
    else
      %{writer | storage: storage}
    end
  end

  defp serialize_master_playlist(writer, variants) do
    streams =
      Enum.map(variants, fn {uri, variant} ->
        renditions = get_referenced_renditions(variant, Keyword.values(variants))
        %{Variant.to_hls_tag(variant, renditions) | uri: uri}
      end)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: writer.version,
        independent_segments: true,
        items: streams
      })

    HLX.Storage.store_master_playlist(payload, writer.storage)
  end

  defp get_referenced_renditions(variant, renditions) do
    renditions
    |> Enum.group_by(&Variant.group_id/1)
    |> Map.take(Variant.referenced_renditions(variant))
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
