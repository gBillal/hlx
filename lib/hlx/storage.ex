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

  @opaque t :: %__MODULE__{
            mod: module(),
            state: state()
          }

  defstruct [:mod, :state]

  @doc false
  @spec new(struct()) :: t
  def new(%struct{} = state) do
    %__MODULE__{mod: struct, state: state}
  end

  @doc false
  def store_master_playlist(payload, storage) do
    %{storage | state: storage.mod.store_master_playlist(payload, storage.state)}
  end

  @doc false
  def store_playlist(playlist_name, payload, storage) do
    {uri, state} = storage.mod.store_playlist(playlist_name, payload, storage.state)
    {uri, %{storage | state: state}}
  end

  @doc false
  def store_init_header(playlist_name, resource_name, payload, storage) do
    {uri, state} =
      storage.mod.store_init_header(playlist_name, resource_name, payload, storage.state)

    {uri, %{storage | state: state}}
  end

  @doc false
  @spec store_segment(playlist_name(), resource_name(), payload(), t()) :: {uri(), t()}
  def store_segment(playlist_name, resource_name, payload, storage) do
    {uri, state} = storage.mod.store_segment(playlist_name, resource_name, payload, storage.state)
    {uri, %{storage | state: state}}
  end

  @doc false
  @spec delete_segment(playlist_name(), HLX.Segment.t(), t()) :: t()
  def delete_segment(playlist_name, segment, storage) do
    new_state = storage.mod.delete_segment(playlist_name, segment, storage.state)
    %{storage | state: new_state}
  end
end
