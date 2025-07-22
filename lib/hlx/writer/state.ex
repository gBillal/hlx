defmodule HLX.Writer.State do
  @moduledoc false

  @type t :: %__MODULE__{
          type: :master | :media,
          variants: %{String.t() => HLX.Writer.Rendition.t()},
          lead_variant: String.t(),
          max_segments: non_neg_integer()
        }

  defstruct [:variants, :lead_variant, :max_segments, type: :media]
end
