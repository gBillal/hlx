defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.{MediaPlaylist, Storage, Track}
  alias HLX.Writer.{StreamInfo, TracksMuxer}

  @type t :: %__MODULE__{
          id: String.t(),
          playlist: MediaPlaylist.t(),
          tracks_muxer: TracksMuxer.t(),
          storage: Storage.Segment.t(),
          depends_on: String.t() | nil,
          config: StreamInfo.t(),
          ready?: boolean(),
          first_dts: %{Track.id() => non_neg_integer()},
          base_timestamp: non_neg_integer() | nil,
          base_dts: {dts :: non_neg_integer(), timescale :: non_neg_integer()} | nil
        }

  defstruct [
    :id,
    :tracks_muxer,
    :playlist,
    :storage,
    :depends_on,
    :config,
    :base_timestamp,
    :base_dts,
    first_dts: %{},
    ready?: false
  ]

  @spec new(String.t(), TracksMuxer.t(), keyword()) :: t()
  def new(id, tracks_muxer, config) do
    playlist = MediaPlaylist.new(config)

    stream_config = %StreamInfo{
      name: id,
      type: config[:type],
      audio: config[:audio],
      auto_select?: config[:auto_select],
      default?: config[:default],
      group_id: config[:group_id],
      language: config[:language],
      subtitles: config[:subtitles]
    }

    extension =
      case tracks_muxer.muxer_mod do
        HLX.Muxer.TS -> ".ts"
        HLX.Muxer.CMAF -> ".m4s"
      end

    variant = %__MODULE__{
      id: id,
      tracks_muxer: tracks_muxer,
      playlist: playlist,
      storage: Storage.Segment.new(config[:storage_dir], id, extension: extension),
      config: stream_config,
      ready?: TracksMuxer.ready?(tracks_muxer)
    }

    save_init_header(variant)
  end

  @spec process_sample(t(), HLX.Sample.t()) :: {HLX.Sample.t(), t()}
  def process_sample(variant, sample) do
    {tracks_muxer, sample} = TracksMuxer.process_sample(variant.tracks_muxer, sample)

    if not variant.ready? and TracksMuxer.ready?(tracks_muxer) do
      {sample, save_init_header(%{variant | ready?: true, tracks_muxer: tracks_muxer})}
    else
      {sample, %{variant | tracks_muxer: tracks_muxer}}
    end
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(%{ready?: true} = variant, sample) do
    %{
      variant
      | tracks_muxer: TracksMuxer.push_sample(variant.tracks_muxer, sample),
        first_dts: Map.put_new(variant.first_dts, sample.track_id, sample.dts)
    }
  end

  def push_sample(variant, _sample), do: variant

  @spec push_parts(t(), TracksMuxer.parts()) :: t()
  def push_parts(variant, parts) do
    {data, duration, tracks_muxer} = TracksMuxer.push_parts(variant.tracks_muxer, parts)
    {uri, storage} = Storage.Segment.store_part(data, variant.storage)
    {part, playlist} = MediaPlaylist.add_part(variant.playlist, uri, duration)

    {part,
     %{
       variant
       | tracks_muxer: tracks_muxer,
         playlist: playlist,
         storage: storage
     }}
  end

  @spec flush(t()) :: {HLX.Segment.t(), t()}
  def flush(variant) do
    case TracksMuxer.flush(variant.tracks_muxer) do
      {_data, dur, _tracks_muxer} when dur == 0 ->
        {nil, variant}

      {data, duration, tracks_muxer} ->
        {uri, storage} = Storage.Segment.store_segment(data, variant.storage)

        segment =
          %HLX.Segment{
            index: MediaPlaylist.segment_count(variant.playlist),
            uri: uri,
            size: IO.iodata_length(data),
            duration: duration,
            timestamp: calculate_timestamp(variant)
          }

        {playlist, storage} =
          case MediaPlaylist.add_segment(variant.playlist, segment) do
            {playlist, nil, parts} ->
              {playlist, Storage.Segment.delete_parts(parts, storage)}

            {playlist, discarded, parts} ->
              storage = Storage.Segment.delete_segment(discarded, storage)
              storage = Storage.Segment.delete_parts(parts, storage)
              {playlist, storage}
          end

        {segment,
         %{
           variant
           | tracks_muxer: tracks_muxer,
             playlist: playlist,
             storage: storage,
             first_dts: %{}
         }}
    end
  end

  @spec add_discontinuity(t()) :: t()
  def add_discontinuity(variant) do
    %{variant | playlist: MediaPlaylist.add_discontinuity(variant.playlist), ready?: false}
  end

  @spec referenced_renditions(t()) :: [String.t()]
  def referenced_renditions(%{config: config}) do
    Enum.reject([config.audio, config.subtitles], &is_nil/1)
  end

  @spec group_id(t()) :: String.t() | nil
  def group_id(%{config: config}), do: config.group_id

  @spec rendition_report(t()) :: {String.t(), {non_neg_integer(), non_neg_integer()}}
  def rendition_report(variant) do
    {last_msn, last_part} = MediaPlaylist.last_part(variant.playlist)

    %ExM3U8.Tags.RenditionReport{
      uri: variant.id,
      last_msn: last_msn,
      last_part: last_part
    }
  end

  @spec to_hls_tag(t(), %{String.t() => t()}) :: struct()
  def to_hls_tag(variant, referenced_renditions) do
    case variant.config.type do
      :rendition ->
        StreamInfo.to_media(variant.config)

      _ ->
        referenced_renditions = Map.values(referenced_renditions)
        {codecs, resolution} = codecs_resolution(variant, referenced_renditions)
        {avg_bitrates, max_bitrates} = avg_max_bandwidths(referenced_renditions)
        {avg_band, max_band} = bandwidth(variant)

        %{
          StreamInfo.to_stream(variant.config)
          | bandwidth: max_band + Enum.sum(max_bitrates),
            average_bandwidth: avg_band + Enum.sum(avg_bitrates),
            codecs: codecs,
            resolution: resolution
        }
    end
  end

  @spec next_part_name(t()) :: String.t()
  def next_part_name(%{storage: storage}), do: Storage.Segment.next_part_uri(storage)

  @spec timescale(t(), Track.id()) :: non_neg_integer()
  def timescale(variant, track_id) do
    variant.tracks_muxer.tracks[track_id].timescale
  end

  defp save_init_header(%{tracks_muxer: muxer} = variant) when muxer.muxer_mod == HLX.Muxer.TS,
    do: variant

  defp save_init_header(%{ready?: false} = variant), do: variant

  defp save_init_header(variant) do
    data = TracksMuxer.save_init_header(variant.tracks_muxer)
    {uri, storage} = Storage.Segment.store_init_header(data, variant.storage)

    %{
      variant
      | playlist: MediaPlaylist.add_init_header(variant.playlist, uri),
        storage: storage
    }
  end

  defp avg_max_bandwidths(renditions) do
    Stream.map(renditions, fn variants ->
      variants
      |> Stream.map(&bandwidth/1)
      |> Enum.unzip()
      |> then(fn {a, m} -> {Enum.max(a), Enum.max(m)} end)
    end)
    |> Enum.unzip()
  end

  defp bandwidth(%{playlist: playlist}), do: MediaPlaylist.bandwidth(playlist)

  defp calculate_timestamp(%{base_timestamp: nil}), do: nil

  defp calculate_timestamp(%{tracks_muxer: muxer} = variant) do
    {base_dts, timescale} = variant.base_dts

    first_dts =
      Enum.reduce(variant.first_dts, 0, fn {track_id, dts}, max_dts ->
        new_dts = div(dts * timescale, muxer.tracks[track_id].timescale)
        if new_dts > max_dts, do: new_dts, else: max_dts
      end)

    duration = div((first_dts - base_dts) * 1000, timescale)
    DateTime.from_unix!(variant.base_timestamp + duration, :millisecond)
  end

  defp codecs_resolution(variant, referenced_renditions) do
    referenced_codecs =
      referenced_renditions
      |> List.flatten()
      |> Stream.flat_map(&TracksMuxer.tracks(&1.tracks_muxer))
      |> Enum.map(& &1.mime)

    tracks = TracksMuxer.tracks(variant.tracks_muxer)

    codecs =
      tracks
      |> Stream.map(& &1.mime)
      |> Stream.concat(referenced_codecs)
      |> Stream.uniq()
      |> Enum.join(",")

    resolution =
      Enum.find_value(tracks, fn
        %{width: nil} -> nil
        %{width: width, height: height} -> {width, height}
      end)

    {codecs, resolution}
  end
end
