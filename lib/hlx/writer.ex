defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  alias HLX.{PartQueue, SampleQueue}
  alias HLX.Writer.{Config, TracksMuxer, Variant}

  @type mode :: :live | :vod
  @type segment_type :: :mpeg_ts | :fmp4 | :low_latency
  @type tracks :: [HLX.Track.t()]
  @type rendition_opts :: [
          {:type, :audio}
          | {:track, HLX.Track.t()}
          | {:group_id, String.t()}
          | {:default, boolean()}
          | {:auto_select, boolean()}
        ]

  @type variant_opts :: [{:tracks, [HLX.Track.t()]} | {:audio, String.t()}]

  @opaque t :: %__MODULE__{
            config: Config.t(),
            lead_variant: String.t() | nil,
            variants: %{String.t() => Variant.t()},
            queues: %{String.t() => {SampleQueue.t(), PartQueue.t() | nil}},
            state: :init | :muxing | :closed
          }

  @enforce_keys [:config]
  defstruct @enforce_keys ++ [:lead_variant, variants: %{}, queues: %{}, state: :init]

  @doc """
  Creates a new HLS writer.

  The following options can be provided:
    * `type` - The type of the playlist, either `:master` or `:media`. Defaults to `:media`.
    * `mode` - The mode of the playlist, either `:live` or `:vod`. Defaults to `:live`.
    * `segment_type` - The type of segments to generate, either `:mpeg_ts`, `:fmp4` or `:low_latency`. Defaults to `:fmp4`.
    * `max_segments` - The maximum number of segments to keep in the playlist, ignored on `vod` mode. Defaults to 0 (no limit).
    * `storage_dir` - The directory where to store the playlists and segments. This is required.
    * `segment_duration` - The target duration of each segment in milliseconds. Defaults to 2000.
    * `part_duration` - The target duration of each part in milliseconds (only for low-latency segments). Defaults to 300.
    * `server_control` - A keyword list with server control options:
      * `can_block_reload` - A boolean indicating if the server support blocking manifest reload until a certain segment/part is available.
      Defaults to `false`.

  You can provide callbacks for segment and part creation by adding the following options:
    * `on_segment_created` - A 2-arity function that will be called when a new segment is created.
      The fuction receives two arguments: the variant id and the segment info.
    * `on_part_created` - A 2-arity function that will be called when a new part is created.
      The fuction receives two arguments: the variant id and the part info.
  """
  @spec new(Keyword.t()) :: {:ok, t()}
  def new(options) do
    with {:ok, config} <- Config.new(options) do
      {:ok, %__MODULE__{config: Map.new(config)}}
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
  def add_rendition(writer, name, opts) do
    cond do
      writer.config[:type] == :media ->
        {:error, :not_master_playlist}

      writer.state != :init ->
        {:error, :bad_state}

      true ->
        do_add_rendition(writer, name, opts)
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
  def add_variant(writer, name, options) do
    cond do
      writer.config[:type] == :media and map_size(writer.variants) >= 1 ->
        {:error, "Media playlist support only one variant"}

      writer.state != :init ->
        {:error, :bad_state}

      true ->
        do_add_variant(writer, name, options)
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
        writer.config[:type] == :media ->
          [{id, variant}] = Map.to_list(writer.variants)
          %{writer | queues: Map.put(writer.queues, id, create_queues(writer, variant))}

        is_nil(writer.lead_variant) ->
          queues =
            Map.new(writer.variants, fn {id, variant} ->
              {id, create_queues(writer, variant)}
            end)

          %{writer | queues: queues}

        true ->
          lead_variant_id = writer.lead_variant

          {dependant_variants, independant_variants} =
            writer.variants
            |> Map.values()
            |> Enum.split_with(
              &(&1.config.type == :rendition or is_nil(&1.tracks_muxer.lead_track))
            )

          queues =
            Map.new(independant_variants, fn variant ->
              extra_variants = (variant.id == lead_variant_id && dependant_variants) || []
              {variant.id, create_queues(writer, variant, extra_variants)}
            end)

          variants =
            Enum.reduce(dependant_variants, writer.variants, fn variant, variants ->
              Map.update!(variants, variant.id, &%{&1 | depends_on: lead_variant_id})
            end)

          %{writer | variants: variants, queues: queues}
      end

    write_sample(%{writer | state: :muxing}, variant_id, sample)
  end

  def write_sample(writer, variant_id, sample) do
    writer = maybe_set_base_timestamp(writer, Map.fetch!(writer.variants, variant_id), sample)
    variant = Map.fetch!(writer.variants, variant_id)

    {sample, variant} = Variant.process_sample(variant, sample)
    writer = %{writer | variants: Map.put(writer.variants, variant_id, variant)}

    {sample_queue, part_queue} = get_queues(writer, variant)
    id = {variant_id, sample.track_id}

    if part_queue,
      do: handle_part_queue(writer, {sample_queue, part_queue}, id, sample),
      else: handle_sample_queue(writer, sample_queue, id, sample)
  end

  @doc """
  Adds a discontinuity to the playlist.

  This flushes any pending segments, so make sure all the samples are written before
  and adds a discontinuity to the specified variant or to all variants if no
  `variant_id` is provided.
  """
  @spec add_discontinuity(t(), String.t() | nil) :: t()
  def add_discontinuity(writer, variant_id \\ nil) do
    variants =
      if variant_id,
        do: [Map.fetch!(writer.variants, variant_id)],
        else: Map.values(writer.variants)

    writer =
      Enum.reduce(variants, writer, fn variant, writer ->
        do_add_discontinuity(writer, variant)
      end)

    variants =
      Map.new(writer.variants, fn {id, variant} ->
        {id, %{variant | base_timestamp: nil, base_dts: nil}}
      end)

    %{writer | variants: variants}
  end

  @doc """
  Closes the writer.

  Closes the writer and flush any pending segments. if the `mode` is `vod` creates the final
  playlists.
  """
  @spec close(t()) :: :ok
  def close(writer) do
    {new_segments, writer} = flush_and_write(writer)

    writer
    |> serialize_playlists(true)
    |> maybe_invoke_callbacks(new_segments)

    :ok
  end

  defp do_add_rendition(%{config: config} = writer, name, opts) do
    # Validate options
    muxer_options = [segment_type: config[:segment_type], max_segments: config[:max_segments]]

    rendition_options =
      opts ++
        [type: :rendition, max_segments: config[:max_segments], storage_dir: config[:storage_dir]]

    with {:ok, rendition} <- TracksMuxer.new(name, [opts[:track]], muxer_options) do
      variant = Variant.new(name, rendition, rendition_options)
      {:ok, %{writer | variants: Map.put(writer.variants, name, variant)}}
    end
  end

  defp do_add_variant(%{config: config} = writer, name, options) do
    # credo:disable-for-next-line
    # TODO: validate options
    muxer_options = [segment_type: config[:segment_type]]

    rendition_options = [
      target_duration: config[:segment_duration],
      segment_type: config[:segment_type],
      max_segments: config[:max_segments],
      audio: options[:audio],
      type: :variant,
      storage_dir: config[:storage_dir]
    ]

    with {:ok, tracks_muxer} <- TracksMuxer.new(name, options[:tracks], muxer_options) do
      variant = Variant.new(name, tracks_muxer, rendition_options)

      lead_variant =
        cond do
          not is_nil(writer.lead_variant) -> writer.lead_variant
          not is_nil(tracks_muxer.lead_track) -> name
          true -> nil
        end

      {:ok,
       %{writer | lead_variant: lead_variant, variants: Map.put(writer.variants, name, variant)}}
    end
  end

  defp push_samples(samples, variants) do
    Enum.reduce(samples, variants, fn {{name, _id}, sample}, variants ->
      Map.update!(variants, name, &Variant.push_sample(&1, sample))
    end)
  end

  defp handle_sample_queue(writer, sample_queue, id, sample) do
    {flush?, samples, queue} = SampleQueue.push_sample(sample_queue, id, sample)

    writer =
      case flush? do
        true ->
          {segments, writer} = flush_and_write(writer, SampleQueue.track_ids(queue))

          writer
          |> serialize_playlists()
          |> maybe_invoke_callbacks(segments)

        false ->
          writer
      end

    %{
      writer
      | variants: push_samples(samples, writer.variants),
        queues: Map.put(writer.queues, queue.id, {queue, nil})
    }
  end

  defp handle_part_queue(writer, {sample_queue, part_queue}, id, sample) do
    {flush?, samples, queue} = SampleQueue.push_sample(sample_queue, id, sample)

    {writer, part_queue, {segments, partial_segments1}} =
      if flush? do
        {parts, part_queue} = PartQueue.flush(part_queue)

        {partial_segments, writer} = push_parts(parts, writer)
        {segments, writer} = flush_and_write(writer, SampleQueue.track_ids(queue))

        {writer, part_queue, {segments, partial_segments}}
      else
        {writer, part_queue, {[], []}}
      end

    {parts, part_queue} = push_samples_to_part_queue(part_queue, samples)
    writer = %{writer | queues: Map.put(writer.queues, queue.id, {queue, part_queue})}
    {partial_segments2, writer} = push_parts(Enum.concat(parts), writer)

    writer =
      if flush? or parts != [],
        do: serialize_playlists(writer),
        else: writer

    maybe_invoke_callbacks(writer, segments, partial_segments1 ++ partial_segments2)
  end

  defp do_add_discontinuity(writer, variant) do
    {sample_queue, part_queue} = get_queues(writer, variant)
    {samples, sample_queue} = SampleQueue.flush(sample_queue)

    {writer, segments, partial_segments} =
      if part_queue do
        {parts1, part_queue} = push_samples_to_part_queue(part_queue, samples)
        {parts2, part_queue} = PartQueue.flush(part_queue)

        {partial_segments, writer} = push_parts(parts1 ++ parts2, writer)
        {segments, writer} = flush_and_write(writer, SampleQueue.track_ids(sample_queue))

        writer = %{
          writer
          | queues: Map.put(writer.queues, variant.id, {sample_queue, part_queue})
        }

        {writer, segments, partial_segments}
      else
        writer = %{
          writer
          | variants: push_samples(samples, writer.variants),
            queues: Map.put(writer.queues, variant.id, {sample_queue, nil})
        }

        {segments, writer} = flush_and_write(writer, SampleQueue.track_ids(sample_queue))
        {writer, segments, []}
      end

    variants = Map.update!(writer.variants, variant.id, &Variant.add_discontinuity(&1))

    %{writer | variants: variants}
    |> serialize_playlists()
    |> maybe_invoke_callbacks(segments, partial_segments)
  end

  defp push_parts(parts, writer) do
    parts
    |> Enum.group_by(
      fn {{variant_id, _track_id}, _samples} -> variant_id end,
      fn {{_variant_id, track_id}, samples} -> {track_id, samples} end
    )
    |> Enum.map_reduce(writer, fn {variant_id, parts}, writer ->
      {part, variant} = Variant.push_parts(writer.variants[variant_id], parts)
      writer = %{writer | variants: Map.put(writer.variants, variant_id, variant)}
      {{variant_id, part}, writer}
    end)
  end

  defp flush_and_write(%{variants: variants} = writer, variant_ids \\ nil) do
    {new_segments, variants} =
      variants
      |> Map.values()
      |> Stream.filter(&(variant_ids == nil or &1.id in variant_ids))
      |> Enum.reduce({[], variants}, fn variant, {segments, variants} ->
        case Variant.flush(variant) do
          {nil, variant} ->
            {segments, Map.put(variants, variant.id, variant)}

          {segment, variant} ->
            {[{variant.id, segment} | segments], Map.put(variants, variant.id, variant)}
        end
      end)

    {new_segments, %{writer | variants: Map.new(variants)}}
  end

  defp get_queues(writer, variant) do
    case variant.depends_on do
      nil -> writer.queues[variant.id]
      id -> writer.queues[id]
    end
  end

  defp push_samples_to_part_queue(part_queue, samples) do
    Enum.reduce(samples, {[], part_queue}, fn {id, sample}, {parts, queue} ->
      case PartQueue.push_sample(queue, id, sample) do
        {[], queue} -> {parts, queue}
        {new_parts, queue} -> {[new_parts | parts], queue}
      end
    end)
  end

  defp serialize_playlists(writer, end_list? \\ false)

  defp serialize_playlists(%{mode: :vod} = writer, false), do: writer

  defp serialize_playlists(%{variants: variants, config: config} = writer, end_list?) do
    preload_hint? = not end_list? and writer.config[:segment_type] == :low_latency

    {rendition_reports, part_target_duration} =
      if config[:type] == :master,
        do: serialize_master_playlist(writer),
        else: {[], 0}

    Enum.each(variants, fn {id, variant} ->
      preload_hint = if preload_hint?, do: {:part, Variant.next_part_name(variant)}

      rendition_reports =
        if preload_hint? do
          rendition_reports
          |> Enum.reject(&(&1.uri == id))
          |> Enum.map(&%{&1 | uri: &1.uri <> ".m3u8"})
        else
          []
        end

      playlist =
        HLX.MediaPlaylist.to_m3u8(variant.playlist,
          version: config[:version],
          playlist_type: if(config[:mode] == :vod, do: :vod),
          preload_hint: preload_hint,
          can_block_reload?: config[:server_control][:can_block_reload],
          rendition_reports: rendition_reports,
          part_hold_back: part_target_duration * 3
        )

      playlist = ExM3U8.serialize(playlist)
      playlist = if end_list?, do: playlist <> "#EXT-X-ENDLIST\n", else: playlist

      File.write!(Path.join(config[:storage_dir], "#{variant.id}.m3u8"), playlist)
    end)

    writer
  end

  defp serialize_master_playlist(%{config: config} = writer) do
    {streams, rendition_reports, part_target_duration} =
      Enum.reduce(writer.variants, {[], [], 0}, fn {uri, variant},
                                                   {streams, reports, part_target_duration} ->
        renditions = get_referenced_renditions(variant, Map.values(writer.variants))

        part_target_duration =
          max(part_target_duration, variant.playlist.part_target_duration || 0)

        stream = %{Variant.to_hls_tag(variant, renditions) | uri: uri <> ".m3u8"}
        {[stream | streams], [Variant.rendition_report(variant) | reports], part_target_duration}
      end)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: config[:version],
        independent_segments: true,
        items: Enum.reverse(streams)
      })

    File.write!(Path.join(config[:storage_dir], "master.m3u8"), payload)
    {rendition_reports, part_target_duration}
  end

  defp create_queues(writer, variant, dependant_variants \\ []) do
    tracks = TracksMuxer.tracks(variant.tracks_muxer)
    lead_track = variant.tracks_muxer.lead_track || hd(tracks).id

    sample_queue =
      Enum.reduce(
        tracks,
        SampleQueue.new(variant.id, writer.config[:segment_duration]),
        &SampleQueue.add_track(&2, {variant.id, &1.id}, &1.id == lead_track, &1.timescale)
      )

    sample_queue =
      Enum.reduce(dependant_variants, sample_queue, fn variant, queue ->
        variant.tracks_muxer
        |> TracksMuxer.tracks()
        |> Enum.reduce(
          queue,
          &SampleQueue.add_track(&2, {variant.id, &1.id}, false, &1.timescale)
        )
      end)

    part_queue =
      if writer.config[:segment_type] == :low_latency do
        [variant | dependant_variants]
        |> Enum.flat_map(
          &Enum.map(TracksMuxer.tracks(&1.tracks_muxer), fn track -> {&1.id, track} end)
        )
        |> Enum.reduce(PartQueue.new(writer.config[:part_duration]), fn {id, track}, queue ->
          PartQueue.add_track(queue, id, track)
        end)
      end

    {sample_queue, part_queue}
  end

  defp get_referenced_renditions(variant, renditions) do
    renditions
    |> Enum.group_by(&Variant.group_id/1)
    |> Map.take(Variant.referenced_renditions(variant))
  end

  defp maybe_invoke_callbacks(%{config: config} = writer, new_segments, new_parts \\ []) do
    if config[:on_part_created] do
      Enum.each(new_parts, fn {variant_id, part} ->
        config[:on_part_created].(variant_id, part)
      end)
    end

    if config[:on_segment_created] do
      Enum.each(new_segments, fn {variant_id, segment} ->
        config[:on_segment_created].(variant_id, segment)
      end)
    end

    writer
  end

  defp maybe_set_base_timestamp(%{mode: :vod} = writer, _variant, _sample), do: writer

  defp maybe_set_base_timestamp(writer, %{base_timestamp: base}, _sample) when not is_nil(base),
    do: writer

  defp maybe_set_base_timestamp(%{variants: variants} = writer, variant, sample) do
    timestamp = DateTime.to_unix(sample.timestamp || DateTime.utc_now(), :millisecond)
    base_dts = {sample.dts || sample.pts, Variant.timescale(variant, sample.track_id)}

    variants =
      Enum.reduce(variants, %{}, fn {id, variant}, acc ->
        Map.put(acc, id, %{variant | base_timestamp: timestamp, base_dts: base_dts})
      end)

    %{writer | variants: variants}
  end

  defimpl Inspect do
    def inspect(writer, _opts) do
      "#HLX.Writer<type: #{writer.config[:type]}, mode: #{writer.config[:mode]}, segment_type: #{writer.config[:segment_type]}, " <>
        "variants: #{map_size(writer.variants)}, lead_variant: #{writer.lead_variant}, " <>
        "max_segments: #{writer.config[:max_segments]}>"
    end
  end
end
