defmodule HLX.Writer.Variant do
  @moduledoc false

  alias HLX.SampleQueue
  alias HLX.Writer.Rendition

  @type t :: %__MODULE__{
          id: String.t(),
          rendition: Rendition.t(),
          queue: SampleQueue.t(),
          depends_on: String.t()
        }

  defstruct [:id, :rendition, :queue, :depends_on]

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
end
