defmodule HLX.Storage do
  @moduledoc """
  Behaviour for implementing storage backend for manifest and segments.
  """

  @type state :: any()
  @type playlist_name :: String.t()
  @type resource_name :: binary()
  @type uri :: binary()
  @type payload :: iodata()

  @doc """
  Callback to initialize the storage backend.
  """
  @callback init(any()) :: {:ok, state()} | {:error, any()}

  @doc """
  Callback invoked to store the master playlist.
  """
  @callback store_master_playlist(payload(), state()) :: state()

  @doc """
  Callback invoked to store an M3U8 playlist.
  """
  @callback store_playlist(playlist_name(), payload(), state()) :: {uri(), state()}

  @doc """
  Callback invoked to store init header.

  Note that this function is only called if the muxer supports init headers.
  """
  @callback store_init_header(playlist_name(), resource_name(), payload(), state()) ::
              {uri(), state()}

  @doc """
  Callback invoked to store a segment.
  """
  @callback store_segment(playlist_name(), resource_name(), payload(), state()) ::
              {uri(), state()}

  @doc """
  Callback invoked to delete a segment.
  """
  @callback delete_segment(playlist_name(), HLX.Segment.t(), state()) :: state()

  @optional_callbacks store_init_header: 4
end
