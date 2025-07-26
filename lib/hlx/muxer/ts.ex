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
  @h264_aud <<0x01::32, 0x09, 0xF0>>
  @h265_aud <<0x01::32, 0x46, 0x01, 0x60>>

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
           media: track.media,
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
    sample = maybe_add_aud(sample, stream_info.media)

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

  defp stream_type_id(%{media: :h264}), do: PMT.encode_stream_type(:H264)
  defp stream_type_id(%{media: :h265}), do: PMT.encode_stream_type(:HEVC)
  defp stream_type_id(%{media: :aac}), do: PMT.encode_stream_type(:AAC)
  defp stream_type_id(%{media: media}), do: raise("Unsupported media: #{inspect(media)}")

  defp stream_id(:video), do: 0xE0
  defp stream_id(:audio), do: 0xC0

  defp maybe_add_aud(sample, :h264) do
    %{sample | payload: @h264_aud <> sample.payload}
  end

  defp maybe_add_aud(sample, :h265) do
    %{sample | payload: @h265_aud <> sample.payload}
  end

  defp maybe_add_aud(sample, _media), do: sample

  def generate(pes, pid, sync?, continuity_counter) do
    header_size = 8

    chunks = chunk_sample_payload(Marshaler.marshal(pes), @ts_payload_size - header_size)

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

  defp chunk_sample_payload(<<>>, _size), do: []

  defp chunk_sample_payload(payload, size) do
    case payload do
      <<part::binary-size(size), rest::binary>> ->
        [part | chunk_sample_payload(rest, @ts_payload_size)]

      last_chunk ->
        [last_chunk]
    end
  end
end
