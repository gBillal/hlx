defmodule HLX.Sample do
  @moduledoc """
  Module describing a media sample.
  """

  @type t :: %__MODULE__{
          track_id: non_neg_integer(),
          dts: non_neg_integer(),
          pts: non_neg_integer(),
          duration: non_neg_integer(),
          sync?: boolean(),
          payload: iodata(),
          timestamp: DateTime.t() | nil
        }

  defstruct [:track_id, :dts, :pts, :duration, :sync?, :payload, :timestamp]

  @doc """
  Creates a new sample.
  """
  @spec new(iodata(), keyword()) :: t()
  def new(payload, opts \\ []), do: struct(%__MODULE__{payload: payload}, opts)
end
