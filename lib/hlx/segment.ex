defmodule HLX.Segment do
  @moduledoc false

  alias ExM3U8.Tags

  @type t :: %__MODULE__{
          uri: String.t(),
          size: non_neg_integer(),
          duration: number(),
          timestamp: DateTime.t() | nil,
          media_init: String.t() | nil,
          discontinuity?: boolean()
        }

  defstruct [:uri, :size, :duration, :timestamp, :media_init, discontinuity?: false]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    struct!(__MODULE__, opts)
  end

  @spec bitrate(t()) :: non_neg_integer()
  def bitrate(%{size: size, duration: duration}), do: trunc(size * 8 / duration)

  @spec hls_tag(t()) :: [struct()]
  def hls_tag(segment) do
    acc = if segment.discontinuity?, do: [%Tags.Discontinuity{}], else: []
    acc = [%Tags.Segment{uri: segment.uri, duration: segment.duration} | acc]

    acc =
      if segment.timestamp, do: [%Tags.ProgramDateTime{date: segment.timestamp} | acc], else: acc

    if segment.media_init, do: [%Tags.MediaInit{uri: segment.media_init} | acc], else: acc
  end
end
