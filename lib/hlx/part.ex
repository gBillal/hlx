defmodule HLX.Part do
  @moduledoc false

  alias ExM3U8.Tags

  @type t :: %__MODULE__{
          uri: String.t(),
          size: non_neg_integer(),
          duration: number(),
          index: non_neg_integer(),
          segment_index: non_neg_integer()
        }

  defstruct [:uri, :size, :duration, :index, :segment_index]

  @spec new(Keyword.t()) :: t()
  def new(opts) do
    struct(__MODULE__, opts)
  end

  defimpl ExM3U8.Serializer do
    alias ExM3U8.{Serializer, Tags}

    def serialize(part) do
      Serializer.serialize(%Tags.Part{
        uri: part.uri,
        duration: part.duration,
        independent?: part.index == 0
      })
    end
  end
end
