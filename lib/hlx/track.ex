defmodule HLX.Track do
  @moduledoc """
  Module describing a media track.
  """

  alias ExMP4.Box
  alias MediaCodecs.{H264, H265}

  @codecs [:h264, :h265, :hevc, :aac]

  @type codec :: :h264 | :h265 | :hevc | :aac | :unknown

  @type t :: %__MODULE__{
          id: non_neg_integer(),
          type: :video | :audio,
          codec: codec(),
          timescale: non_neg_integer(),
          priv_data: any()
        }

  defstruct [:id, :type, :codec, :timescale, :priv_data]

  @doc """
  Creates a new track struct with the given options.
  """
  @spec new(keyword()) :: t()
  def new(opts), do: struct(__MODULE__, opts)

  @spec from(ExMP4.Track.t()) :: t()
  def from(%ExMP4.Track{} = track) do
    %__MODULE__{
      id: track.id,
      type: track.type,
      codec: track.media,
      timescale: track.timescale,
      priv_data: priv_data(track.media, track.priv_data)
    }
  end

  def to_mp4_track(%{codec: :h264} = track) do
    {sps, pps} = track.priv_data
    parsed_sps = H264.NALU.SPS.parse(sps)

    %ExMP4.Track{
      id: track.id,
      type: track.type,
      media: track.codec,
      timescale: track.timescale,
      width: H264.NALU.SPS.width(parsed_sps),
      height: H264.NALU.SPS.height(parsed_sps),
      priv_data: Box.Avcc.new([sps], List.wrap(pps)),
      sample_table: %Box.Stbl{stsz: %Box.Stsz{}, stco: %Box.Stco{}},
      trex: %Box.Trex{
        track_id: track.id,
        default_sample_flags: if(track.type == :video, do: 0x10000, else: 0)
      }
    }
  end

  def to_mp4_track(%{codec: codec} = track) when codec in [:hevc, :h265] do
    {vps, sps, pps} = track.priv_data
    parsed_sps = H265.NALU.SPS.parse(List.first(sps))

    %ExMP4.Track{
      id: track.id,
      type: track.type,
      media: :h265,
      timescale: track.timescale,
      width: H265.NALU.SPS.width(parsed_sps),
      height: H265.NALU.SPS.height(parsed_sps),
      priv_data: Box.Hvcc.new(vps, sps, List.wrap(pps)),
      sample_table: %Box.Stbl{stsz: %Box.Stsz{}, stco: %Box.Stco{}},
      trex: %Box.Trex{
        track_id: track.id,
        default_sample_flags: if(track.type == :video, do: 0x10000, else: 0)
      }
    }
  end

  def to_mp4_track(%{codec: :aac, priv_data: priv_data} = track) do
    audio_sepecific_config =
      <<priv_data.object_type::5,
        MediaCodecs.MPEG4.Utils.sampling_frequency_index(track.priv_data.sampling_frequency)::4,
        priv_data.channels::4, priv_data.aot_specific_config::bitstring>>

    %ExMP4.Track{
      id: track.id,
      type: track.type,
      media: :aac,
      media_tag: :esds,
      timescale: track.timescale,
      channels: priv_data.channels,
      sample_rate: priv_data.sampling_frequency,
      priv_data: Box.Esds.new(audio_sepecific_config),
      sample_table: %Box.Stbl{stsz: %Box.Stsz{}, stco: %Box.Stco{}},
      trex: %Box.Trex{
        track_id: track.id,
        default_sample_flags: if(track.type == :video, do: 0x10000, else: 0)
      }
    }
  end

  @spec validate(t()) :: {:ok, t()} | {:error, any()}
  def validate(%{codec: codec}) when codec not in @codecs do
    {:error, "Unsupported codec: #{inspect(codec)}"}
  end

  def validate(%{codec: :aac, priv_data: nil}) do
    {:error, "Missign audio specific config for AAC track"}
  end

  def validate(%{codec: :aac, priv_data: data} = track) when is_binary(data) do
    {:ok, %{track | priv_data: MediaCodecs.MPEG4.AudioSpecificConfig.parse(data)}}
  end

  def validate(track), do: {:ok, track}

  defp priv_data(:h264, avcc) do
    case {avcc.sps, avcc.pps} do
      {[], []} -> nil
      {sps, pps} -> {List.first(sps), pps}
    end
  end

  defp priv_data(:h265, hvcc) do
    case {hvcc.vps, hvcc.sps, hvcc.pps} do
      {[], [], []} -> nil
      {vps, sps, pps} -> {List.first(vps), List.first(sps), pps}
    end
  end

  defp priv_data(:aac, esds) do
    descriptor = MediaCodecs.MPEG4.ESDescriptor.parse(esds.es_descriptor)
    descriptor.dec_config_descr.decoder_specific_info
  end

  defp priv_data(_media, _priv_data), do: nil
end
