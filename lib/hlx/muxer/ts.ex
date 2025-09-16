defmodule HLX.Muxer.TS do
  @moduledoc """
  Muxer for MPEG-TS streams.
  """
  @behaviour HLX.Muxer

  import ExMP4.Helper, only: [timescalify: 3]

  alias MPEG.TS.{Marshaler, Muxer, PMT}

  @ts_clock 90_000

  defstruct [:muxer, :tracks, :track_to_stream, :packets]

  @impl true
  def init(tracks) do
    {track_to_stream, muxer} =
      Enum.reduce(tracks, {%{}, Muxer.new()}, fn track, {track_to_stream, muxer} ->
        {pid, muxer} = Muxer.add_elementary_stream(muxer, stream_type_id(track))

        track_to_stream =
          Map.put(track_to_stream, track.id, %{
            timescale: track.timescale,
            pid: pid,
            media: track.codec
          })

        {track_to_stream, muxer}
      end)

    {pat_packet, muxer} = Muxer.mux_pat(muxer)
    {pmt_packet, muxer} = Muxer.mux_pmt(muxer)

    %__MODULE__{
      track_to_stream: track_to_stream,
      tracks: tracks,
      muxer: muxer,
      packets: [pmt_packet, pat_packet]
    }
  end

  @impl true
  def push(sample, state) do
    stream_info = Map.fetch!(state.track_to_stream, sample.track_id)

    dts = timescalify(sample.dts, stream_info.timescale, @ts_clock)
    pts = timescalify(sample.pts, stream_info.timescale, @ts_clock)

    {packets, muxer} =
      Muxer.mux_sample(
        state.muxer,
        stream_info.pid,
        sample.payload,
        pts,
        dts: dts,
        sync?: sample.sync?
      )

    %{state | packets: [packets | state.packets], muxer: muxer}
  end

  @impl true
  def flush_segment(%{muxer: muxer} = state) do
    data =
      state.packets
      |> Enum.reverse()
      |> Marshaler.marshal()

    {pat_packet, muxer} = Muxer.mux_pat(muxer)
    {pmt_packet, muxer} = Muxer.mux_pmt(muxer)

    {data, %{state | packets: [pmt_packet, pat_packet], muxer: muxer}}
  end

  defp stream_type_id(%{codec: :h264}), do: PMT.encode_stream_type(:H264)
  defp stream_type_id(%{codec: :aac}), do: PMT.encode_stream_type(:AAC)
  defp stream_type_id(%{codec: :h265}), do: PMT.encode_stream_type(:HEVC)
  defp stream_type_id(%{codec: :hevc}), do: PMT.encode_stream_type(:HEVC)
  defp stream_type_id(%{codec: media}), do: raise("Unsupported media: #{inspect(media)}")
end
