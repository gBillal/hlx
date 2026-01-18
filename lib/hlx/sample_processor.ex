defmodule HLX.SampleProcessor do
  @moduledoc false

  alias HLX.Track
  alias MediaCodecs.{AV1, H264, H265, MPEG4}

  @type container :: :mpeg_ts | :fmp4

  @h264_aud <<0x09, 0xF0>>
  @h265_aud <<0x46, 0x01, 0x60>>

  @spec process_sample(Track.t(), HLX.Sample.t(), container()) :: {Track.t(), HLX.Sample.t()}
  def process_sample(%{codec: :h264} = track, sample, container) do
    {{sps, pps}, nalus} = H264.pop_parameter_sets(sample.payload)
    keyframe? = Enum.any?(nalus, &H264.NALU.keyframe?/1)

    track =
      if keyframe? and sps != [] and pps != [],
        do: Track.update_priv_data(track, {List.first(sps), pps}),
        else: track

    payload =
      cond do
        container == :fmp4 ->
          Enum.map(nalus, &[<<byte_size(&1)::32>>, &1])

        container == :mpeg_ts and H264.NALU.type(List.first(nalus)) != :aud ->
          to_annexb(@h264_aud, sample.payload)

        true ->
          to_annexb(sample.payload)
      end

    {track, %{sample | payload: payload, sync?: keyframe?}}
  end

  def process_sample(%{codec: :av1, priv_data: nil} = track, sample, :fmp4) do
    obus = if is_list(track.payload), do: track.payload, else: AV1.obus(track.payload)
    track = %{track | priv_data: Enum.find(obus, &(AV1.OBU.type(&1) == :sequence_header))}
    {track, sample}
  end

  def process_sample(%{codec: :av1} = track, sample, :fmp4), do: {track, sample}

  def process_sample(%{codec: :av1}, _sample, container) do
    raise "Unsupported container #{inspect(container)} for AV1 samples"
  end

  def process_sample(%{codec: :aac} = track, sample, container) do
    cond do
      container == :fmp4 and adts?(sample.payload) ->
        {:ok, %{frames: frames}, <<>>} = MPEG4.ADTS.parse(sample.payload)
        {track, %{sample | payload: frames, sync?: true}}

      container == :mpeg_ts and not adts?(sample.payload) ->
        adts = %MPEG4.ADTS{
          audio_object_type: track.priv_data.object_type,
          channels: track.priv_data.channels,
          sampling_frequency: track.priv_data.sampling_frequency,
          frames_count: 1,
          frames: sample.payload
        }

        {track, %{sample | payload: MPEG4.ADTS.serialize(adts), sync?: true}}

      true ->
        {track, %{sample | sync?: true}}
    end
  end

  def process_sample(%{codec: codec} = track, sample, container) when codec in [:h265, :hevc] do
    {{vps, sps, pps}, nalus} = H265.pop_parameter_sets(sample.payload)
    keyframe? = Enum.any?(nalus, &H265.NALU.keyframe?/1)

    track =
      if keyframe? and sps != [] and pps != [],
        do: Track.update_priv_data(track, {List.first(vps), List.first(sps), pps}),
        else: track

    payload =
      cond do
        container == :fmp4 ->
          Enum.map(nalus, &[<<byte_size(&1)::32>>, &1])

        container == :mpeg_ts and H265.NALU.type(List.first(nalus)) != :aud ->
          to_annexb(@h265_aud, sample.payload)

        true ->
          to_annexb(sample.payload)
      end

    {track, %{sample | payload: payload, sync?: keyframe?}}
  end

  def process_sample(track, payload, _), do: {track, payload}

  defp to_annexb(nalus) when is_list(nalus) do
    nalus
    |> Enum.map(&[<<1::32>>, &1])
    |> IO.iodata_to_binary()
  end

  defp to_annexb(payload), do: payload

  defp to_annexb(aud, nalus) when is_list(nalus), do: to_annexb([aud | nalus])
  defp to_annexb(aud, payload), do: <<1::32>> <> aud <> payload

  defp adts?(<<0xFFF::12, _::bitstring>>), do: true
  defp adts?(_), do: false
end
