defmodule HLX.Writer.BandwidthCalculator do
  @moduledoc false

  @type t :: %__MODULE__{
          total_bytes: non_neg_integer(),
          total_duration: non_neg_integer(),
          max_bitrate: non_neg_integer(),
          max_segments: non_neg_integer(),
          segments: :queue.queue() | nil
        }

  defstruct [:total_bytes, :total_duration, :max_bitrate, :max_segments, :segments]

  @spec new(non_neg_integer()) :: t()
  def new(max_segments) do
    %__MODULE__{
      total_bytes: 0,
      total_duration: 0,
      max_bitrate: 0,
      max_segments: max_segments,
      segments: if(max_segments != 0, do: :queue.new())
    }
  end

  @spec add_segment(t(), non_neg_integer(), non_neg_integer()) :: t()
  def add_segment(state, seg_size, seg_duration) do
    new_state = %__MODULE__{
      state
      | total_bytes: state.total_bytes + seg_size,
        total_duration: state.total_duration + seg_duration,
        max_bitrate: max(state.max_bitrate, trunc(seg_size * 8 / seg_duration))
    }

    cond do
      state.max_segments == 0 ->
        new_state

      :queue.len(state.segments) < state.max_segments ->
        %{new_state | segments: :queue.in({seg_size, seg_duration}, state.segments)}

      true ->
        {{:value, {old_seg_size, old_seg_duration}}, segments} =
          {seg_size, seg_duration}
          |> :queue.in(state.segments)
          |> :queue.out()

        old_bitrate = trunc(old_seg_size * 8 / old_seg_duration)

        max_bitrate =
          if old_bitrate == state.max_bitrate do
            :queue.to_list(segments)
            |> Enum.map(fn {size, duration} -> trunc(size * 8 / duration) end)
            |> Enum.max()
          else
            state.max_bitrate
          end

        %{
          new_state
          | segments: segments,
            total_bytes: new_state.total_bytes - old_seg_size,
            total_duration: new_state.total_duration - old_seg_duration,
            max_bitrate: max_bitrate
        }
    end
  end

  @spec avg_bitrate(t()) :: non_neg_integer()
  def avg_bitrate(state), do: trunc(state.total_bytes * 8 / state.total_duration)

  @spec max_bitrate(t()) :: non_neg_integer()
  def max_bitrate(state), do: state.max_bitrate
end
