defmodule HLX.Segment do
  @moduledoc false

  alias ExM3U8.Tags

  @type t :: %__MODULE__{
          uri: String.t(),
          size: non_neg_integer(),
          duration: number(),
          timestamp: DateTime.t() | nil,
          media_init: String.t() | nil,
          discontinuity?: boolean(),
          parts: [ExM3U8.Tags.Part.t()]
        }

  defstruct [
    :uri,
    :size,
    :duration,
    :timestamp,
    :media_init,
    discontinuity?: false,
    parts: []
  ]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    struct(__MODULE__, opts)
  end

  @spec bitrate(t()) :: non_neg_integer()
  def bitrate(%{size: size, duration: duration}), do: trunc(size * 8 / duration)

  defimpl ExM3U8.Serializer do
    alias ExM3U8.{Serializer, Tags}

    def serialize(segment) do
      [
        serialize_init_tag(segment.media_init),
        serialize_program_date_time(segment.timestamp),
        Enum.reverse(segment.parts),
        serialize_segment(segment.uri, segment.duration),
        serialize_discontinuity(segment.discontinuity?)
      ]
      |> List.flatten()
      |> Enum.map_intersperse("\n", &Serializer.serialize/1)
    end

    defp serialize_init_tag(nil), do: []
    defp serialize_init_tag(uri), do: %Tags.MediaInit{uri: uri}

    defp serialize_program_date_time(nil), do: []
    defp serialize_program_date_time(date), do: %Tags.ProgramDateTime{date: date}

    defp serialize_segment(nil, _duration), do: []
    defp serialize_segment(uri, duration), do: %Tags.Segment{uri: uri, duration: duration}

    defp serialize_discontinuity(false), do: []
    defp serialize_discontinuity(true), do: %Tags.Discontinuity{}
  end
end
