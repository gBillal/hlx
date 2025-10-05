defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.{MediaPlaylist, SampleQueue}
  alias HLX.Writer.{Rendition, StreamInfo}

  @type t :: %__MODULE__{
          id: String.t(),
          playlist: MediaPlaylist.t(),
          rendition: Rendition.t(),
          queue: SampleQueue.t(),
          depends_on: String.t(),
          config: StreamInfo.t()
        }

  defstruct [:id, :rendition, :playlist, :queue, :depends_on, :config]

  @spec new(String.t(), Rendition.t(), keyword()) :: t()
  def new(id, rendition, config) do
    playlist = MediaPlaylist.new(config)

    config = %StreamInfo{
      name: id,
      audio: config[:audio],
      auto_select?: config[:auto_select],
      default?: config[:default],
      group_id: config[:group_id],
      language: config[:language],
      subtitles: config[:subtitles]
    }

    %__MODULE__{id: id, rendition: rendition, playlist: playlist, config: config}
  end

  @spec save_init_header(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def save_init_header(variant, storage) do
    data = Rendition.save_init_header(variant.rendition)
    {uri, storage} = HLX.Storage.store_init_header(variant.id, "init.mp4", data, storage)

    variant = %{
      variant
      | playlist: MediaPlaylist.add_init_header(variant.playlist, uri)
    }

    {variant, storage}
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(variant, sample) do
    rendition = Rendition.push_sample(variant.rendition, sample)
    %{variant | rendition: rendition}
  end

  @spec flush(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def flush(variant, storage) do
    name = generate_segment_name(variant)
    {data, duration, rendition} = Rendition.flush(variant.rendition)
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

    {%{variant | rendition: rendition, playlist: playlist}, storage}
  end

  @spec referenced_renditions(t()) :: [String.t()]
  def referenced_renditions(%{config: config}) do
    Enum.reject([config.audio, config.subtitles], &is_nil/1)
  end

  @spec group_id(t()) :: String.t() | nil
  def group_id(%{config: config}), do: config.group_id

  @spec create_sample_queue(t()) :: t()
  @spec create_sample_queue(t(), [t()]) :: t()
  def create_sample_queue(%{rendition: rendition} = variant, dependant_variants \\ []) do
    tracks = Rendition.tracks(rendition)
    lead_track = rendition.lead_track || hd(tracks).id

    sample_queue =
      Enum.reduce(
        tracks,
        SampleQueue.new(2000),
        &SampleQueue.add_track(&2, {variant.id, &1.id}, &1.id == lead_track, &1.timescale)
      )

    sample_queue =
      Enum.reduce(dependant_variants, sample_queue, fn variant, queue ->
        variant.rendition
        |> Rendition.tracks()
        |> Enum.reduce(
          queue,
          &SampleQueue.add_track(&2, {variant.id, &1.id}, false, &1.timescale)
        )
      end)

    %{variant | queue: sample_queue}
  end

  @spec to_hls_tag(t(), %{String.t() => t()}) :: struct()
  def to_hls_tag(variant, referenced_renditions) do
    case variant.rendition.type do
      :rendition ->
        StreamInfo.to_media(variant.config)

      _ ->
        referenced_codecs =
          referenced_renditions
          |> Map.values()
          |> List.flatten()
          |> Enum.flat_map(&Rendition.tracks(&1.rendition))
          |> Enum.map(& &1.mime)

        tracks = Rendition.tracks(variant.rendition)

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
      case variant.rendition.muxer_mod do
        HLX.Muxer.TS -> "ts"
        HLX.Muxer.CMAF -> "m4s"
      end

    "segment_#{MediaPlaylist.segment_count(variant.playlist)}.#{extension}"
  end

  defp bandwidth(%{playlist: playlist}), do: MediaPlaylist.bandwidth(playlist)
end
