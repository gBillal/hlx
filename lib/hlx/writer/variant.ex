defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.SampleQueue
  alias HLX.Writer.{Rendition, StreamInfo}

  @type t :: %__MODULE__{
          id: String.t(),
          rendition: Rendition.t(),
          queue: SampleQueue.t(),
          depends_on: String.t(),
          config: StreamInfo.t()
        }

  defstruct [:id, :rendition, :queue, :depends_on, :config]

  @spec new(String.t(), Rendition.t(), keyword()) :: t()
  def new(id, rendition, config) do
    config = %StreamInfo{
      name: id,
      audio: config[:audio],
      auto_select?: config[:auto_select],
      default?: config[:default],
      group_id: config[:group_id],
      language: config[:language],
      subtitles: config[:subtitles]
    }

    %__MODULE__{id: id, rendition: rendition, config: config}
  end

  @spec save_init_header(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def save_init_header(variant, storage) do
    {rendition, storage} = Rendition.save_init_header(variant.rendition, storage)
    {%{variant | rendition: rendition}, storage}
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(variant, sample) do
    rendition = Rendition.push_sample(variant.rendition, sample)
    %{variant | rendition: rendition}
  end

  @spec flush(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def flush(variant, storage) do
    {rendition, storage} = Rendition.flush(variant.rendition, storage)
    {%{variant | rendition: rendition}, storage}
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

        codecs =
          Rendition.tracks(variant.rendition)
          |> Enum.map(& &1.mime)
          |> Enum.concat(referenced_codecs)
          |> Enum.uniq()
          |> Enum.join(",")

        {avg_bitrates, max_bitrates} =
          referenced_renditions
          |> Map.values()
          |> Enum.map(fn variants ->
            variants
            |> Enum.map(&Rendition.bandwidth(&1.rendition))
            |> Enum.unzip()
            |> then(fn {a, m} -> {Enum.max(a), Enum.max(m)} end)
          end)
          |> Enum.unzip()

        {avg_band, max_band} = HLX.MediaPlaylist.bandwidth(variant.rendition.playlist)

        %{
          StreamInfo.to_stream(variant.config)
          | bandwidth: max_band + Enum.sum(max_bitrates),
            average_bandwidth: avg_band + Enum.sum(avg_bitrates),
            codecs: codecs
        }
    end
  end
end
