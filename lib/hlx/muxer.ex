defmodule HLX.Muxer do
  @moduledoc """
  Behaviour describing muxing media samples to HLS segments.
  """

  @type state :: any()

  @callback init([HLX.Track.t()]) :: state()

  @callback get_init_header(state()) :: binary()

  @callback push(sample :: HLX.Sample.t(), state()) :: state()

  @callback flush_segment(state()) :: {iodata(), state()}

  @optional_callbacks get_init_header: 1
end
