defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.{MediaPlaylist, SampleQueue}
  alias HLX.Writer.{StreamInfo, TracksMuxer}

  @type t :: %__MODULE__{
          id: String.t(),
          playlist: MediaPlaylist.t(),
          tracks_muxer: TracksMuxer.t(),
          queue: SampleQueue.t(),
          depends_on: String.t(),
          config: StreamInfo.t()
        }

  defstruct [:id, :tracks_muxer, :playlist, :queue, :depends_on, :config]

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

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(variant, sample) do
    tracks_muxer = TracksMuxer.push_sample(variant.tracks_muxer, sample)
    %{variant | tracks_muxer: tracks_muxer}
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

  @spec create_sample_queue(t()) :: t()
  @spec create_sample_queue(t(), [t()]) :: t()
  def create_sample_queue(%{tracks_muxer: tracks_muxer} = variant, dependant_variants \\ []) do
    tracks = TracksMuxer.tracks(tracks_muxer)
    lead_track = tracks_muxer.lead_track || hd(tracks).id

    sample_queue =
      Enum.reduce(
        tracks,
        SampleQueue.new(2000),
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

    %{variant | queue: sample_queue}
  end

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

  defp generate_segment_name(variant) do
    extension =
      case variant.tracks_muxer.muxer_mod do
        HLX.Muxer.TS -> "ts"
        HLX.Muxer.CMAF -> "m4s"
      end

    "segment_#{MediaPlaylist.segment_count(variant.playlist)}.#{extension}"
  end

  defp bandwidth(%{playlist: playlist}), do: MediaPlaylist.bandwidth(playlist)
end
