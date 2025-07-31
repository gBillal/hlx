defmodule HLX.Track do
  @moduledoc """
  Module describing a media track.
  """

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
  def new(opts), do: struct!(__MODULE__, opts)
end
