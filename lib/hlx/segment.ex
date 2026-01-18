defmodule HLX.Segment do
  @moduledoc """
  Module describing a media segment in an HLS playlist.
  """

  alias ExM3U8.Tags

  @type t :: %__MODULE__{
          index: non_neg_integer(),
          uri: String.t(),
          size: non_neg_integer(),
          duration: number(),
          timestamp: DateTime.t() | nil,
          media_init: String.t() | nil,
          discontinuity?: boolean(),
          parts: [HLX.Part.t()]
        }

  defstruct [
    :uri,
    :size,
    :duration,
    :timestamp,
    :media_init,
    index: 0,
    discontinuity?: false,
    parts: []
  ]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    struct(__MODULE__, opts)
  end

  @spec bitrate(t()) :: non_neg_integer()
  def bitrate(%{size: size, duration: duration}), do: trunc(size * 8 / duration)

  @spec hls_tag(t()) :: [struct()]
  def hls_tag(segment) do
    [
      [media_init_tag(segment.media_init)],
      Enum.reverse(segment.parts),
      [
        program_date_time_tag(segment.timestamp),
        segment_tag(segment.uri, segment.duration),
        discontinuity_tag(segment.discontinuity?)
      ]
    ]
    |> Enum.concat()
    |> Enum.reject(&is_nil/1)
  end

  defp media_init_tag(nil), do: nil
  defp media_init_tag(uri), do: %Tags.MediaInit{uri: uri}

  defp program_date_time_tag(nil), do: nil
  defp program_date_time_tag(date), do: %Tags.ProgramDateTime{date: date}

  defp segment_tag(nil, _duration), do: nil
  defp segment_tag(uri, duration), do: %Tags.Segment{uri: uri, duration: duration}

  defp discontinuity_tag(false), do: nil
  defp discontinuity_tag(true), do: %Tags.Discontinuity{}
end
