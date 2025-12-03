defmodule HLX.MediaPlaylist do
  @moduledoc false

  alias HLX.{Part, Segment}

  @type t :: %__MODULE__{
          segments: Qex.t(Segment.t()),
          max_segments: non_neg_integer(),
          segment_count: non_neg_integer(),
          temp_init: String.t() | nil,
          sequence_number: non_neg_integer(),
          discontinuity_number: non_neg_integer(),
          pending_segment: Segment.t() | nil,
          target_duration: non_neg_integer(),
          part_target_duration: number() | nil,
          part_index: non_neg_integer()
        }

  defstruct [
    :segments,
    :max_segments,
    :segment_count,
    :temp_init,
    :sequence_number,
    :discontinuity_number,
    :pending_segment,
    :target_duration,
    :part_target_duration,
    :part_index
  ]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    %__MODULE__{
      segments: Qex.new(),
      max_segments: Keyword.get(opts, :max_segments, 0),
      segment_count: 0,
      discontinuity_number: 0,
      sequence_number: 0,
      target_duration: 0,
      part_index: 0
    }
  end

  @spec add_init_header(t(), String.t()) :: t()
  def add_init_header(state, uri), do: %{state | temp_init: uri}

  @spec add_segment(t(), Segment.t()) :: {t(), Segment.t() | nil, [Part.t()]}
  def add_segment(%__MODULE__{pending_segment: nil} = state, segment) do
    {state, parts} = delete_old_parts(state)
    {state, segment} = state |> do_add_segment(segment) |> delete_old_segment()
    {state, segment, parts}
  end

  def add_segment(%__MODULE__{pending_segment: pending_segment} = state, segment) do
    pending_segment = %{
      pending_segment
      | uri: segment.uri,
        size: segment.size,
        duration: segment.duration
    }

    add_segment(%{state | pending_segment: nil}, pending_segment)
  end

  @spec add_part(t(), String.t(), number()) :: {Part.t(), t()}
  def add_part(%{pending_segment: nil} = playlist, part_uri, part_duration) do
    add_part(%{playlist | pending_segment: %Segment{}, part_index: 0}, part_uri, part_duration)
  end

  def add_part(%{pending_segment: segment} = playlist, part_uri, part_duration) do
    part = %Part{
      uri: part_uri,
      duration: part_duration,
      index: playlist.part_index,
      segment_index: segment_count(playlist)
    }

    {part,
     %{
       playlist
       | pending_segment: %{segment | parts: [part | segment.parts]},
         part_target_duration: max(playlist.part_target_duration || 0, part_duration),
         part_index: playlist.part_index + 1
     }}
  end

  @spec add_discontinuity(t()) :: t()
  def add_discontinuity(%__MODULE__{} = state) do
    # new_playlist = %MediaPlaylist{
    #   playlist
    #   | timeline: [%Tags.Discontinuity{} | playlist.timeline]
    # }

    state
  end

  @spec to_m3u8(t(), keyword()) :: ExM3U8.MediaPlaylist.t()
  def to_m3u8(%__MODULE__{segments: segments} = state, opts \\ []) do
    timeline = Enum.reduce(segments, [], &[Segment.hls_tag(&1) | &2])

    timeline =
      if state.pending_segment,
        do: [Segment.hls_tag(state.pending_segment) | timeline],
        else: timeline

    timeline =
      case opts[:preload_hint] do
        {type, uri} -> [%ExM3U8.Tags.PreloadHint{type: type, uri: uri} | timeline]
        _ -> timeline
      end

    server_control =
      if state.part_target_duration do
        %ExM3U8.MediaPlaylist.ServerControl{
          can_block_reload?: Keyword.get(opts, :can_block_reload?, false),
          hold_back: state.target_duration * 3,
          part_hold_back: state.part_target_duration * 3
        }
      end

    %ExM3U8.MediaPlaylist{
      timeline: timeline |> Enum.reverse() |> List.flatten(),
      info: %ExM3U8.MediaPlaylist.Info{
        version: Keyword.get(opts, :version, 7),
        playlist_type: Keyword.get(opts, :playlist_type),
        independent_segments: true,
        media_sequence: state.sequence_number,
        discontinuity_sequence: state.discontinuity_number,
        target_duration: state.target_duration,
        part_inf: state.part_target_duration,
        server_control: server_control
      }
    }
  end

  @spec bandwidth(t()) :: {non_neg_integer(), non_neg_integer()}
  def bandwidth(%{segment_count: 0}), do: {0, 0}

  def bandwidth(%{segments: segments}) do
    {size, duration, max_bitrate} =
      Enum.reduce(segments, {0, 0, 0}, fn segment, {size, duration, max_bitrate} ->
        {size + segment.size, duration + segment.duration,
         max(max_bitrate, Segment.bitrate(segment))}
      end)

    {trunc(size * 8 / duration), max_bitrate}
  end

  @spec segment_count(t()) :: non_neg_integer()
  def segment_count(state), do: state.segment_count + state.sequence_number

  defp delete_old_parts(state) do
    if not is_nil(state.part_target_duration) and state.segment_count > 2 do
      {seg_1, segments} = Qex.pop_back!(state.segments)
      {seg_2, segments} = Qex.pop_back!(segments)
      {seg_3, segments} = Qex.pop_back!(segments)

      state =
        segments
        |> Qex.push(%{seg_3 | parts: []})
        |> Qex.push(seg_2)
        |> Qex.push(seg_1)
        |> then(&%{state | segments: &1})

      {state, seg_3.parts}
    else
      {state, []}
    end
  end

  defp do_add_segment(%{segments: segments} = state, segment) do
    {segment, state} =
      if state.temp_init do
        {%{segment | media_init: state.temp_init}, %{state | temp_init: nil}}
      else
        {segment, state}
      end

    %{
      state
      | segments: Qex.push(segments, segment),
        segment_count: state.segment_count + 1,
        target_duration: max(state.target_duration, ceil(segment.duration))
    }
  end

  defp delete_old_segment(%{max_segments: 0} = state), do: {state, nil}

  defp delete_old_segment(state) when state.segment_count <= state.max_segments, do: {state, nil}

  defp delete_old_segment(%{segments: segments} = state) do
    {discarded_segment, segments} = Qex.pop!(segments)
    {oldest_segment, segments} = Qex.pop!(segments)

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
       | segments: Qex.push_front(segments, oldest_segment),
         segment_count: state.segment_count - 1,
         sequence_number: state.sequence_number + 1,
         discontinuity_number: discontinuity_number
     }, discarded_segment}
  end
end
