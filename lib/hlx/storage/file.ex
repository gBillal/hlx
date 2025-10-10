defmodule HLX.Storage.File do
  @moduledoc """
  Module implementing the `HLX.Storage` behaviour that stores manifests and segments in
  the local file system.
  """

  @behaviour HLX.Storage

  defstruct [:dir]

  @impl true
  def store_master_playlist(payload, state) do
    File.write!(path(state, "master.m3u8"), payload)
    state
  end

  @impl true
  def store_playlist(playlist_name, payload, state) do
    name = "#{playlist_name}.m3u8"
    store_data(path(state, name), payload)
    {name, state}
  end

  @impl true
  def store_init_header(playlist_name, resource_name, payload, state) do
    uri = Path.join([playlist_name, resource_name])
    store_data(path(state, uri), payload)
    {uri, state}
  end

  @impl true
  def store_segment(playlist_name, resource_name, payload, state) do
    uri = Path.join([playlist_name, resource_name])
    store_data(path(state, uri), payload)
    {uri, state}
  end

  @impl true
  def store_part(playlist_name, resource_name, payload, state) do
    uri = Path.join([playlist_name, resource_name])
    store_data(path(state, uri), payload)
    {uri, state}
  end

  @impl true
  def delete_segment(_playlist_name, segment, state) do
    File.rm(path(state, segment.uri))
    if segment.media_init, do: File.rm(path(state, segment.media_init))
    state
  end

  @impl true
  def path(playlist_name, resource_name, state), do: Path.join([playlist_name, resource_name])

  defp store_data(path, data) do
    File.mkdir_p!(Path.dirname(path))
    File.write!(path, data)
  end

  defp path(state, name), do: Path.join(state.dir, name)
end
