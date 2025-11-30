defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.MediaPlaylist
  alias HLX.Storage
  alias HLX.Writer.{StreamInfo, TracksMuxer}

  @type t :: %__MODULE__{
          id: String.t(),
          playlist: MediaPlaylist.t(),
          tracks_muxer: TracksMuxer.t(),
          storage: Storage.Segment.t(),
          depends_on: String.t() | nil,
          config: StreamInfo.t(),
          ready?: boolean()
        }

  defstruct [:id, :tracks_muxer, :playlist, :storage, :depends_on, :config, ready?: false]

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
    %{variant | tracks_muxer: TracksMuxer.push_sample(variant.tracks_muxer, sample)}
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
    {data, duration, tracks_muxer} = TracksMuxer.flush(variant.tracks_muxer)
    {uri, storage} = Storage.Segment.store_segment(data, variant.storage)

    segment =
      %HLX.Segment{
        index: MediaPlaylist.segment_count(variant.playlist),
        uri: uri,
        size: IO.iodata_length(data),
        duration: duration
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

    {segment, %{variant | tracks_muxer: tracks_muxer, playlist: playlist, storage: storage}}
  end

  @spec referenced_renditions(t()) :: [String.t()]
  def referenced_renditions(%{config: config}) do
    Enum.reject([config.audio, config.subtitles], &is_nil/1)
  end

  @spec group_id(t()) :: String.t() | nil
  def group_id(%{config: config}), do: config.group_id

  @spec to_hls_tag(t(), %{String.t() => t()}) :: struct()
  def to_hls_tag(variant, referenced_renditions) do
    case variant.config.type do
      :rendition ->
        StreamInfo.to_media(variant.config)

      _ ->
        referenced_codecs =
          referenced_renditions
          |> Map.values()
          |> List.flatten()
          |> Enum.flat_map(&TracksMuxer.tracks(&1.tracks_muxer))
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

        {avg_bitrates, max_bitrates} =
          referenced_renditions
          |> Map.values()
          |> Enum.map(fn variants ->
            variants
            |> Enum.map(&bandwidth/1)
            |> Enum.unzip()
            |> then(fn {a, m} -> {Enum.max(a), Enum.max(m)} end)
          end)
          |> Enum.unzip()

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

  defp bandwidth(%{playlist: playlist}), do: MediaPlaylist.bandwidth(playlist)
end
