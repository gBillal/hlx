defmodule HLX.Writer.State do
  @moduledoc false

  @type t :: %__MODULE__{
          type: :master | :media,
          variants: %{String.t() => HLX.Writer.Rendition.t()},
          lead_variant: String.t()
        }

  defstruct [:variants, :lead_variant, type: :media]
end
