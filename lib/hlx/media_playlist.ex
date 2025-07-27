defmodule HLX.MediaPlaylist do
  @moduledoc false

  alias HLX.Segment

  @type t :: %__MODULE__{
          segments: :queue.queue(Segment.t()),
          max_segments: non_neg_integer(),
          segment_count: non_neg_integer(),
          temp_init: String.t() | nil,
          sequence_number: non_neg_integer(),
          discontinuity_number: non_neg_integer()
        }

  defstruct [
    :segments,
    :max_segments,
    :segment_count,
    :temp_init,
    :sequence_number,
    :discontinuity_number
  ]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    %__MODULE__{
      segments: :queue.new(),
      max_segments: Keyword.get(opts, :max_segments, 0),
      segment_count: 0,
      discontinuity_number: 0,
      sequence_number: 0
    }
  end

  @spec add_init_header(t(), String.t()) :: t()
  def add_init_header(state, uri), do: %{state | temp_init: uri}

  @spec add_segment(t(), Segment.t()) :: {t(), Segment.t() | nil}
  def add_segment(%__MODULE__{} = state, segment) do
    state
    |> do_add_segment(segment)
    |> delete_old_segment()
  end

  @spec add_discontinuity(t()) :: t()
  def add_discontinuity(%__MODULE__{} = state) do
    # new_playlist = %MediaPlaylist{
    #   playlist
    #   | timeline: [%Tags.Discontinuity{} | playlist.timeline]
    # }

    state
  end

  @spec to_m3u8_playlist(t()) :: ExM3U8.MediaPlaylist.t()
  def to_m3u8_playlist(%__MODULE__{segments: segments} = state) do
    {timeline, target_duration} =
      :queue.fold(
        fn segment, {acc, target_duration} ->
          acc = [Segment.hls_tag(segment) | acc]
          {acc, max(target_duration, round(segment.duration))}
        end,
        {[], 0},
        segments
      )

    %ExM3U8.MediaPlaylist{
      timeline: Enum.reverse(timeline) |> List.flatten(),
      info: %ExM3U8.MediaPlaylist.Info{
        version: 7,
        independent_segments: true,
        media_sequence: state.sequence_number,
        discontinuity_sequence: state.discontinuity_number,
        target_duration: target_duration
      }
    }
  end

  @spec bandwidth(t()) :: {non_neg_integer(), non_neg_integer()}
  def bandwidth(%{segment_count: 0}), do: {0, 0}

  def bandwidth(%{segments: segments}) do
    {size, duration, max_bitrate} =
      :queue.fold(
        fn segment, {size, duration, max_bitrate} ->
          {size + segment.size, duration + segment.duration,
           max(max_bitrate, Segment.bitrate(segment))}
        end,
        {0, 0, 0},
        segments
      )

    {trunc(size * 8 / duration), max_bitrate}
  end

  @spec segment_count(t()) :: non_neg_integer()
  def segment_count(state), do: state.segment_count + state.sequence_number

  defp do_add_segment(%{segments: segments} = state, segment) do
    {segment, state} =
      if state.temp_init do
        {%{segment | media_init: state.temp_init}, %{state | temp_init: nil}}
      else
        {segment, state}
      end

    %{state | segments: :queue.in(segment, segments), segment_count: state.segment_count + 1}
  end

  defp delete_old_segment(%{max_segments: 0} = state), do: {state, nil}

  defp delete_old_segment(state) when state.segment_count <= state.max_segments, do: {state, nil}

  defp delete_old_segment(%{segments: segments} = state) do
    {{:value, discarded_segment}, segments} = :queue.out(segments)
    {{:value, oldest_segment}, segments} = :queue.out(segments)

    discontinuity_number =
      state.discontinuity_number + if oldest_segment.discontinuity?, do: 1, else: 0

    {discarded_segment, oldest_segment} =
      if is_nil(oldest_segment.media_init) do
        oldest_segment = %{
          oldest_segment
          | media_init: discarded_segment.media_init,
            discontinuity?: false
        }

        {%{discarded_segment | media_init: nil}, oldest_segment}
      else
        {discarded_segment, %{oldest_segment | discontinuity?: false}}
      end

    {%{
       state
       | segments: :queue.in_r(oldest_segment, segments),
         segment_count: state.segment_count - 1,
         sequence_number: state.sequence_number + 1,
         discontinuity_number: discontinuity_number
     }, discarded_segment}
  end
end
