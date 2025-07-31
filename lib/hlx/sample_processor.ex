defmodule HLX.SampleProcessor do
  @moduledoc false

  alias HLX.Track
  alias MediaCodecs.{H264, H265}

  @type container :: :mpeg_ts | :fmp4

  @h264_aud <<0x09, 0xF0>>
  @h265_aud <<0x46, 0x01, 0x60>>

  @spec process_sample(Track.t(), iodata(), container()) :: {Track.t(), iodata()}
  def process_sample(%{codec: :h264} = track, payload, container) do
    {{sps, pps}, nalus} = H264.pop_parameter_sets(payload)
    keyframe? = Enum.any?(nalus, &H264.NALU.keyframe?/1)

    track =
      if keyframe? and sps != [] and pps != [],
        do: %{track | priv_data: {List.first(sps), pps}},
        else: track

    cond do
      container == :fmp4 ->
        {track, H264.annexb_to_elementary_stream(nalus)}

      container == :mpeg_ts and H264.NALU.type(List.first(nalus)) != :aud ->
        {track, to_annexb(@h264_aud, payload)}

      true ->
        {track, to_annexb(@h264_aud, payload)}
    end
  end

  def process_sample(%{codec: codec} = track, payload, container) when codec in [:h265, :hevc] do
    {{vps, sps, pps}, nalus} = H265.pop_parameter_sets(payload)
    keyframe? = Enum.any?(nalus, &H265.NALU.keyframe?/1)

    track =
      if keyframe? and sps != [] and pps != [],
        do: %{track | priv_data: {List.first(vps), List.first(sps), pps}},
        else: track

    cond do
      container == :fmp4 ->
        {track, H265.annexb_to_elementary_stream(nalus)}

      container == :mpeg_ts and H265.NALU.type(List.first(nalus)) != :aud ->
        {track, to_annexb(@h265_aud, payload)}

      true ->
        {track, to_annexb(@h265_aud, payload)}
    end
  end

  def process_sample(track, payload, _), do: {track, payload}

  defp to_annexb(aud, nalus) when is_list(nalus) do
    for nalu <- [aud | nalus], into: <<>>, do: <<1::32>> <> nalu
  end

  defp to_annexb(aud, payload), do: <<1::32>> <> aud <> payload
end
