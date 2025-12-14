defmodule HLX.Track do
  @moduledoc """
  Module describing a media track.

  The following fields are required: `id`, `type`, `codec`, `timescale`.

  ## Private Data
  `priv_data` contains codec specific initialization data. The format of the field is:
    * `h264` - For H.264 it's a tuple of sps and a list of pps
    * `h265` - For H.265 or HEVC it's a tuple of vps, sps and a list of pps.
    * `av1` - For AV1 it's a list of config OBUs (usually sequence header) as a bitstream.
    * `aac` - For AAC it's a binary describing the audio specific configuration.

  The `priv_data` is not mandatory if:
    * H.264 samples have in-band parameter sets.
    * H.265 samples have in-band parameter sets.
    * AV1 samples have in-band sequence header OBU.
  """

  alias ExMP4.Box
  alias MediaCodecs.{MPEG4, H264, H265}
  alias MediaCodecs.AV1.OBU

  @codecs [:h264, :h265, :hevc, :aac, :av1]

  @type id :: non_neg_integer()
  @type codec :: :h264 | :h265 | :hevc | :aac | :unknown

  @type t :: %__MODULE__{
          id: id(),
          type: :video | :audio,
          codec: codec(),
          timescale: non_neg_integer(),
          priv_data: any(),
          width: non_neg_integer() | nil,
          height: non_neg_integer() | nil,
          mime: String.t() | nil
        }

  defstruct [:id, :type, :codec, :timescale, :priv_data, :mime, :width, :height]

  @doc """
  Creates a new track struct with the given options.
  """
  @spec new(keyword()) :: t()
  def new(opts), do: struct(__MODULE__, opts) |> update_fields()

  @spec update_priv_data(t(), any()) :: t()
  def update_priv_data(%{priv_data: priv_data} = track, priv_data), do: track

  def update_priv_data(track, priv_data) do
    update_fields(%{track | priv_data: priv_data})
  end

  @doc """
  Creates a new track from an `ex_mp4` track.
  """
  @spec from(ExMP4.Track.t()) :: t()
  def from(%ExMP4.Track{} = track) do
    %__MODULE__{
      id: track.id,
      type: track.type,
      codec: track.media,
      timescale: track.timescale,
      priv_data: priv_data(track.media, track.priv_data)
    }
    |> update_fields()
  end

  @doc """
  Converts a track to an `ex_mp4` track.
  """
  @spec to_mp4_track(t()) :: ExMP4.Track.t()
  def to_mp4_track(%{codec: :h264} = track) do
    {sps, pps} = track.priv_data
    %{set_common_fields(track) | priv_data: Box.Avcc.new([sps], List.wrap(pps))}
  end

  def to_mp4_track(%{codec: codec} = track) when codec in [:hevc, :h265] do
    {vps, sps, pps} = track.priv_data

    %{
      set_common_fields(track)
      | media: :h265,
        priv_data: Box.Hvcc.new([vps], [sps], List.wrap(pps))
    }
  end

  def to_mp4_track(%{codec: :av1} = track) do
    %{set_common_fields(track) | priv_data: Box.Av1c.new(track.priv_data)}
  end

  def to_mp4_track(%{codec: :aac, priv_data: priv_data} = track) do
    audio_sepecific_config = MPEG4.AudioSpecificConfig.serialize(priv_data)

    %{
      set_common_fields(track)
      | channels: priv_data.channels,
        sample_rate: priv_data.sampling_frequency,
        priv_data: Box.Esds.new(audio_sepecific_config)
    }
  end

  @doc false
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

  defp priv_data(:av1, av1c), do: av1c.config_obus

  defp priv_data(:aac, esds) do
    [descriptor] = MediaCodecs.MPEG4.parse_descriptors(esds.es_descriptor)
    descriptor.dec_config_descr.decoder_specific_info
  end

  defp priv_data(_media, _priv_data), do: nil

  defp update_fields(%{priv_data: nil} = track), do: track

  defp update_fields(%{codec: :h264, priv_data: {sps, _pps}} = track) do
    sps = H264.NALU.SPS.parse(sps)

    %{
      track
      | mime: H264.NALU.SPS.mime_type(sps, "avc1"),
        width: H264.NALU.SPS.width(sps),
        height: H264.NALU.SPS.height(sps)
    }
  end

  defp update_fields(%{codec: codec, priv_data: {_vps, sps, _pps}} = track)
       when codec in [:h265, :hevc] do
    sps = H265.NALU.SPS.parse(sps)

    %{
      track
      | mime: H265.NALU.SPS.mime_type(sps, "hvc1"),
        width: H265.NALU.SPS.width(sps),
        height: H265.NALU.SPS.height(sps)
    }
  end

  defp update_fields(%{codec: :av1, priv_data: obu} = track) do
    %{payload: sequence_header} = OBU.parse!(obu)

    %{
      track
      | mime: OBU.SequenceHeader.mime_type(sequence_header),
        width: OBU.SequenceHeader.width(sequence_header),
        height: OBU.SequenceHeader.height(sequence_header)
    }
  end

  defp update_fields(%{codec: :aac, priv_data: audio_specific_config} = track) do
    audio_specific_config =
      if is_binary(audio_specific_config),
        do: MPEG4.AudioSpecificConfig.parse(audio_specific_config),
        else: audio_specific_config

    %{
      track
      | priv_data: audio_specific_config,
        mime: "mp4a.40.#{audio_specific_config.object_type}"
    }
  end

  defp set_common_fields(track) do
    %ExMP4.Track{
      id: track.id,
      type: track.type,
      media: track.codec,
      timescale: track.timescale,
      width: track.width,
      height: track.height,
      sample_table: %Box.Stbl{stsz: %Box.Stsz{}, stco: %Box.Stco{}},
      trex: %Box.Trex{
        track_id: track.id,
        default_sample_flags: if(track.type == :video, do: 0x10000, else: 0)
      }
    }
  end
end
