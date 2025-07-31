defmodule HLX.Muxer.TS do
  @moduledoc """
  Muxer for MPEG-TS streams.
  """
  @behaviour HLX.Muxer

  import ExMP4.Helper, only: [timescalify: 3]

  alias MPEG.TS.{Marshaler, Packet, PMT}

  @ts_clock 90_000
  @ts_payload_size 184
  @max_counter 16

  defstruct [:psi, :tracks, :track_to_stream, :packets, continuity_counter: 0]

  @impl true
  def init(tracks) do
    pat = %{1 => 0x1000}

    track_to_stream =
      tracks
      |> Enum.with_index(0x100)
      |> Map.new(fn {track, pid} ->
        {track.id,
         %{
           timescale: track.timescale,
           pid: pid,
           media: track.codec,
           stream_type_id: stream_type_id(track),
           stream_id: stream_id(track.type)
         }}
      end)

    pmt = %MPEG.TS.PMT{
      pcr_pid: 0x100,
      program_info: [],
      streams:
        Map.new(Map.values(track_to_stream), &{&1.pid, %{stream_type_id: &1.stream_type_id}})
    }

    psi = [psi_packet(0, pat), psi_packet(2, pmt)]

    %__MODULE__{
      track_to_stream: track_to_stream,
      tracks: tracks,
      psi: psi,
      packets: Enum.reverse(psi)
    }
  end

  @impl true
  def push(sample, state) do
    stream_info = Map.fetch!(state.track_to_stream, sample.track_id)

    pes =
      MPEG.TS.PES.new(sample.payload,
        stream_id: stream_info.stream_id,
        dts: timescalify(sample.dts, stream_info.timescale, @ts_clock),
        pts: timescalify(sample.pts, stream_info.timescale, @ts_clock)
      )

    packets = generate(pes, stream_info.pid, sample.sync?, state.continuity_counter)

    continuity_counter = rem(state.continuity_counter + length(packets), @max_counter)
    %{state | continuity_counter: continuity_counter, packets: [packets | state.packets]}
  end

  @impl true
  def flush_segment(state) do
    state.packets
    |> Enum.reverse()
    |> Marshaler.marshal()
    |> then(&{&1, %{state | packets: Enum.reverse(state.psi)}})
  end

  defp psi_packet(table_id, table) do
    %MPEG.TS.PSI{
      header: %{
        table_id: table_id,
        section_syntax_indicator: true,
        transport_stream_id: 1,
        version_number: 0,
        current_next_indicator: true,
        section_number: 0,
        last_section_number: 0
      },
      table: Marshaler.marshal(table)
    }
    |> Marshaler.marshal()
    |> MPEG.TS.Packet.new(
      pid: if(table_id == 0, do: 0x0000, else: 0x1000),
      pusi: true,
      continuity_counter: 0,
      random_access_indicator: false
    )
  end

  defp stream_type_id(%{codec: :h264}), do: PMT.encode_stream_type(:H264)
  defp stream_type_id(%{codec: :aac}), do: PMT.encode_stream_type(:AAC)
  defp stream_type_id(%{codec: :h265}), do: PMT.encode_stream_type(:HEVC)
  defp stream_type_id(%{codec: :hevc}), do: PMT.encode_stream_type(:HEVC)
  defp stream_type_id(%{codec: media}), do: raise("Unsupported media: #{inspect(media)}")

  defp stream_id(:video), do: 0xE0
  defp stream_id(:audio), do: 0xC0

  defp generate(pes, pid, sync?, continuity_counter) do
    pes_data = Marshaler.marshal(pes)
    header_size = 8

    chunks =
      {0, @ts_payload_size - header_size}
      |> chunk(byte_size(pes_data))
      |> Enum.map(fn {offset, size} -> :binary.part(pes_data, offset, size) end)

    first_packet =
      Packet.new(hd(chunks),
        pusi: true,
        random_access_indicator: sync?,
        pcr: pes.dts * 300,
        pid: pid,
        continuity_counter: continuity_counter
      )

    tl(chunks)
    |> Enum.with_index(continuity_counter + 1)
    |> Enum.map(fn {chunk, index} ->
      Packet.new(chunk, pid: pid, continuity_counter: rem(index, @max_counter))
    end)
    |> then(&[first_packet | &1])
  end

  defp chunk({offset, size}, remaining) when remaining <= size, do: [{offset, remaining}]

  defp chunk({offset, size}, remaining) do
    [{offset, size} | chunk({offset + size, @ts_payload_size}, remaining - size)]
  end
end
