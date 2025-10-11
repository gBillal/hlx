defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.MediaPlaylist
  alias HLX.Writer.{StreamInfo, TracksMuxer}

  @type t :: %__MODULE__{
          id: String.t(),
          playlist: MediaPlaylist.t(),
          tracks_muxer: TracksMuxer.t(),
          depends_on: String.t() | nil,
          config: StreamInfo.t()
        }

  defstruct [:id, :tracks_muxer, :playlist, :depends_on, :config]

  @spec new(String.t(), TracksMuxer.t(), keyword()) :: t()
  def new(id, tracks_muxer, config) do
    playlist = MediaPlaylist.new(config)

    config = %StreamInfo{
      name: id,
      type: config[:type],
      audio: config[:audio],
      auto_select?: config[:auto_select],
      default?: config[:default],
      group_id: config[:group_id],
      language: config[:language],
      subtitles: config[:subtitles]
    }

    %__MODULE__{id: id, tracks_muxer: tracks_muxer, playlist: playlist, config: config}
  end

  @spec save_init_header(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def save_init_header(variant, storage) do
    data = TracksMuxer.save_init_header(variant.tracks_muxer)
    {uri, storage} = HLX.Storage.store_init_header(variant.id, "init.mp4", data, storage)

    variant = %{
      variant
      | playlist: MediaPlaylist.add_init_header(variant.playlist, uri)
    }

    {variant, storage}
  end

  @spec process_sample(t(), HLX.Sample.t()) :: {HLX.Sample.t(), t()}
  def process_sample(variant, sample) do
    {tracks_muxer, sample} = TracksMuxer.process_sample(variant.tracks_muxer, sample)
    {sample, %{variant | tracks_muxer: tracks_muxer}}
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(variant, sample) do
    tracks_muxer = TracksMuxer.push_sample(variant.tracks_muxer, sample)
    %{variant | tracks_muxer: tracks_muxer}
  end

  @spec push_parts(t(), TracksMuxer.parts(), HLX.Storage.t()) :: t()
  def push_parts(variant, parts, storage) do
    part_name = generate_part_name(variant.playlist)

    {data, duration, tracks_muxer} = TracksMuxer.push_parts(variant.tracks_muxer, parts)
    {uri, storage} = HLX.Storage.store_part(variant.id, part_name, data, storage)
    playlist = MediaPlaylist.add_part(variant.playlist, uri, duration)

    {%{variant | tracks_muxer: tracks_muxer, playlist: playlist}, storage}
  end

  @spec flush(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def flush(variant, storage) do
    name = generate_segment_name(variant)
    {data, duration, tracks_muxer} = TracksMuxer.flush(variant.tracks_muxer)
    {uri, storage} = HLX.Storage.store_segment(variant.id, name, data, storage)

    segment =
      %HLX.Segment{
        uri: uri,
        size: IO.iodata_length(data),
        duration: duration
      }

    {playlist, storage} =
      case MediaPlaylist.add_segment(variant.playlist, segment) do
        {playlist, nil} ->
          {playlist, storage}

        {playlist, discarded} ->
          {playlist, HLX.Storage.delete_segment(variant.id, discarded, storage)}
      end

    {%{variant | tracks_muxer: tracks_muxer, playlist: playlist}, storage}
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
          |> Enum.map(& &1.mime)
          |> Enum.concat(referenced_codecs)
          |> Enum.uniq()
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
  def next_part_name(%{playlist: playlist}) do
    seg_count = MediaPlaylist.segment_count(playlist)

    case playlist.pending_segment do
      nil -> part_name(seg_count, 0)
      _ -> part_name(seg_count, length(playlist.pending_segment.parts))
    end
  end

  defp generate_segment_name(variant) do
    extension =
      case variant.tracks_muxer.muxer_mod do
        HLX.Muxer.TS -> "ts"
        HLX.Muxer.CMAF -> "m4s"
      end

    "segment_#{MediaPlaylist.segment_count(variant.playlist)}.#{extension}"
  end

  defp generate_part_name(playlist) do
    part_index = if playlist.pending_segment, do: length(playlist.pending_segment.parts), else: 0
    part_name(MediaPlaylist.segment_count(playlist), part_index)
  end

  defp part_name(segment, part), do: "segment_#{segment}_part_#{part}.m4s"

  defp bandwidth(%{playlist: playlist}), do: MediaPlaylist.bandwidth(playlist)
end
