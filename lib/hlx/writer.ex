defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  use GenServer

  alias ExM3U8.Tags.Stream
  alias HLX.Writer.Rendition
  alias __MODULE__.State

  @type tracks :: [ExMP4.Track.t()]

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @spec add_tracks(pid() | GenServer.name(), tracks()) :: :ok | {:error, term()}
  def add_tracks(writer, tracks) do
    GenServer.call(writer, {:add_tracks, tracks})
  end

  def add_rendition(writer, name, type, track, group_id, opts \\ []) do
    GenServer.call(writer, {:add_rendition, {name, type, track, group_id, opts}})
  end

  def add_variant(writer, name, tracks) do
    GenServer.call(writer, {:add_variant, name, tracks})
  end

  def write_sample(writer, variant \\ nil, sample) do
    GenServer.cast(writer, {:write, variant, sample})
  end

  @impl true
  def init(options) do
    {:ok, %State{variants: %{}, type: options[:type] || :media}}
  end

  @impl true
  def handle_call({:add_tracks, tracks}, _from, state) do
    {tracks, _acc} = Enum.map_reduce(tracks, 1, &{%{&1 | id: &2}, &2 + 1})

    {init_data, variant} =
      "index"
      |> Rendition.new(tracks, target_duration: 2000)
      |> Rendition.init_header("init.mp4")

    :ok = File.write!("init.mp4", init_data)
    {:reply, {:ok, tracks}, %{state | variant: variant}}
  end

  @impl true
  def handle_call({:add_rendition, _msg}, _from, %{type: :media} = state) do
    {:reply, {:error, :not_master_playlist}, state}
  end

  @impl true
  def handle_call({:add_rendition, {name, :audio, track, _group_id, _opts}}, _from, state) do
    {init_data, variant} =
      name
      |> Rendition.new([track], type: :rendition, target_duration: 2000)
      |> Rendition.init_header("#{name}_init.mp4")

    :ok = File.write!("#{name}_init.mp4", init_data)

    {:reply, :ok, %{state | variants: Map.put(state.variants, name, variant)}}
  end

  @impl true
  def handle_call({:add_variant, _name, _tracks}, _from, %{type: :media} = state) do
    {:reply, {:error, :not_master_playlist}, state}
  end

  @impl true
  def handle_call({:add_variant, name, tracks}, _from, state) do
    {init_data, variant} =
      name
      |> Rendition.new(tracks, target_duration: 2000)
      |> Rendition.init_header("#{name}_init.mp4")

    lead_variant =
      cond do
        not is_nil(state.lead_variant) -> state.lead_variant
        not is_nil(variant.lead_track) -> name
        true -> nil
      end

    :ok = File.write!("#{name}_init.mp4", init_data)

    state = %{
      state
      | variants: Map.put(state.variants, name, variant),
        lead_variant: lead_variant
    }

    {:reply, :ok, state}
  end

  @impl true
  def handle_cast({:write, variant_name, sample}, state) do
    variant = state.variants[variant_name]
    flush? = Rendition.flush?(variant, sample)
    lead_variant? = state.lead_variant == variant_name

    {variant, flushed?} =
      if (state.lead_variant == nil or lead_variant? or variant.lead_track != nil) and flush? do
        variant =
          variant
          |> flush_and_write(variant_name)
          |> Rendition.push_sample(sample)

        {variant, true}
      else
        {Rendition.push_sample(variant, sample), false}
      end

    variants =
      if flush? and lead_variant? do
        state.variants
        |> Map.delete(variant_name)
        |> Map.new(fn {name, variant} ->
          variant = if variant.lead_track, do: variant, else: flush_and_write(variant, name)
          {name, variant}
        end)
      else
        state.variants
      end

    if flushed? and state.type == :master, do: serialize_master_playlist(state.variants)

    {:noreply, %{state | variants: Map.put(variants, variant_name, variant)}}
  end

  defp flush_and_write(variant, name) do
    uri = "#{name}_#{variant.segment_count}.m4s"
    {data, variant} = Rendition.flush(variant, uri)
    File.write!(uri, data)
    File.write!("#{name}.m3u8", HLX.MediaPlaylist.serialize(variant.playlist))
    variant
  end

  defp serialize_master_playlist(variants) do
    {renditions, variants} =
      variants
      |> Map.values()
      |> Enum.reduce({[], []}, fn variant, {renditions, variants} ->
        if variant.type == :rendition do
          rendition = %ExM3U8.Tags.Media{
            type: :audio,
            name: variant.name,
            uri: "#{variant.name}.m3u8",
            group_id: "group",
            default?: true,
            language: ""
          }

          {[rendition | renditions], variants}
        else
          variant = %Stream{uri: "#{variant.name}.m3u8", bandwidth: 0, codecs: ""}
          {renditions, [variant | variants]}
        end
      end)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: 7,
        independent_segments: true,
        items: renditions ++ variants
      })

    File.write!("master.m3u8", payload)
  end
end
