defmodule HLX.Muxer do
  @moduledoc """
  Behaviour describing muxing media samples to HLS segments.
  """

  @type state :: any()

  @callback init([ExMP4.Track.t()]) :: state()

  @callback get_init_header(state()) :: binary()

  @callback push(sample :: ExMP4.Sample.t(), state()) :: state()

  @callback flush_segment(state()) :: {binary(), state()}

  @optional_callbacks get_init_header: 1
end
