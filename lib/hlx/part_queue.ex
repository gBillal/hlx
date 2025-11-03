defmodule HLX.PartQueue do
  @moduledoc false

  alias HLX.SampleQueue

  @type part_data :: [HLX.Sample.t()]

  @type part_ctx :: %{
          samples: [part_data()],
          duration: non_neg_integer(),
          queue: Qex.t([part_data()]),
          queue_size: non_neg_integer(),
          target_duration: non_neg_integer(),
          timescale: non_neg_integer()
        }

  @type t() :: %__MODULE__{
          target_duration: non_neg_integer(),
          parts: %{SampleQueue.track_id() => part_ctx()}
        }

  defstruct [:target_duration, parts: %{}]

  @spec new(non_neg_integer()) :: t()
  def new(target_duration) do
    %__MODULE__{target_duration: target_duration}
  end

  @spec add_track(t(), String.t(), HLX.Track.t()) :: t()
  def add_track(part_queue, variant, track) do
    target_duration = div(part_queue.target_duration * track.timescale, 1000)

    part_ctx = %{
      samples: [],
      duration: 0,
      queue: Qex.new(),
      queue_size: 0,
      target_duration: target_duration,
      timescale: track.timescale
    }

    %{part_queue | parts: Map.put(part_queue.parts, {variant, track.id}, part_ctx)}
  end

  @spec push_sample(t(), SampleQueue.track_id(), HLX.Sample.t()) ::
          {[{SampleQueue.track_id(), part_data()}], t()}
  def push_sample(part_queue, track_id, sample) do
    part_ctx = Map.fetch!(part_queue.parts, track_id)

    if part_ctx.duration + sample.duration > part_ctx.target_duration do
      part_ctx = %{
        part_ctx
        | samples: [sample],
          duration: sample.duration,
          queue: Qex.push(part_ctx.queue, Enum.reverse(part_ctx.samples)),
          queue_size: part_ctx.queue_size + 1
      }

      part_queue = %{part_queue | parts: Map.put(part_queue.parts, track_id, part_ctx)}
      maybe_flush_parts(part_queue)
    else
      part_ctx = %{
        part_ctx
        | samples: [sample | part_ctx.samples],
          duration: part_ctx.duration + sample.duration
      }

      {[], %{part_queue | parts: Map.put(part_queue.parts, track_id, part_ctx)}}
    end
  end

  @spec flush(t()) :: {[{SampleQueue.track_id(), part_data()}], t()}
  def flush(part_queue) do
    {parts_data, parts} =
      Enum.map_reduce(part_queue.parts, part_queue.parts, fn {track_id, part_ctx}, acc ->
        {part_data, queue} =
          case part_ctx.queue_size do
            0 -> {Enum.reverse(part_ctx.samples), part_ctx.queue}
            _ -> Qex.pop!(part_ctx.queue)
          end

        part_ctx = %{part_ctx | duration: 0, samples: [], queue: queue, queue_size: 0}
        {{track_id, part_data}, Map.put(acc, track_id, part_ctx)}
      end)

    {parts_data, %{part_queue | parts: parts}}
  end

  defp maybe_flush_parts(part_queue) do
    if Enum.all?(part_queue.parts, fn {_id, part_ctx} -> part_ctx.queue_size > 0 end) do
      {parts_data, parts} =
        Enum.map_reduce(part_queue.parts, part_queue.parts, fn {track_id, part_ctx}, acc ->
          {part, queue} = Qex.pop!(part_ctx.queue)
          part_ctx = %{part_ctx | queue: queue, queue_size: part_ctx.queue_size - 1}
          {{track_id, part}, Map.put(acc, track_id, part_ctx)}
        end)

      {parts_data, %{part_queue | parts: parts}}
    else
      {[], part_queue}
    end
  end
end
