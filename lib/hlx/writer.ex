defmodule HLX.Writer do
  @moduledoc """
  Module for writing HLS master and media playlists.
  """

  use GenServer

  alias HLX.Writer.Rendition
  alias __MODULE__.State

  @type tracks :: [ExMP4.Track.t()]
  @type writer :: pid() | GenServer.name()
  @type rendition_opts :: [
          {:type, :audio}
          | {:track, ExMP4.Track.t()}
          | {:group_id, String.t()}
          | {:default, boolean()}
          | {:auto_select, boolean()}
        ]

  @type variant_opts :: [{:tracks, [ExMP4.Track.t()]} | {:audio, String.t()}]

  @spec start_link(Keyword.t()) :: GenServer.on_start()
  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: opts[:name])
  end

  @doc """
  Add a new rendition to the master playlist.

  This adds a new `EXT-X-MEDIA` entry to the master playlist.

  The `name` should be unique across variants and renditions and it's used as the name of
  the playlist (`name.m3u8`).

  The following parameter may be provided:
    * `type` - [Required] The type should always be `:audio` since it's the only supported media type.
    * `track` - [Required] The track that defines the media.
    * `group_id` - [Required] The group id of the rendition.
    * `default` - A boolean indicating if this is the default rendition.
    * `auto_select` - A boolean setting the auto select.
  """
  @spec add_rendition(writer(), String.t(), rendition_opts()) :: :ok | {:error, any()}
  def add_rendition(writer, name, opts) do
    GenServer.call(writer, {:add_rendition, Keyword.put(opts, :name, name)})
  end

  @doc """
  Add a new variant to the master playlist.

  This adds a new `EXT-X-STREAM-INF` entry to the master playlist.

  The `name` should be unique across variants and renditions and it's used as the name of
  the playlist (`<name>.m3u8`).

  The following parameter may be provided:
    * `tracks` - [Required] One or more tracks definitions, all the nedia are muxed in the same segment.
    * `audio` - Reference to a `group_id` of a rendition.
  """
  @spec add_variant(writer(), String.t(), variant_opts()) :: :ok | {:error, any()}
  def add_variant(writer, name, options) do
    GenServer.call(writer, {:add_variant, Keyword.put(options, :name, name)})
  end

  @doc """
  Writes a sample to the specified variant or rendition.
  """
  @spec write_sample(writer(), String.t() | nil, ExMP4.Sample.t()) :: :ok
  def write_sample(writer, variant_or_rendition \\ nil, sample) do
    GenServer.cast(writer, {:write, variant_or_rendition, sample})
  end

  @impl true
  def init(options) do
    {:ok, %State{variants: %{}, type: options[:type] || :media}}
  end

  @impl true
  def handle_call({:add_rendition, _options}, _from, %{type: :media} = state) do
    {:reply, {:error, :not_master_playlist}, state}
  end

  @impl true
  def handle_call({:add_rendition, opts}, _from, state) do
    # Validate options
    name = opts[:name]

    variant_opts =
      [type: :rendition, target_duration: 2000] ++
        Keyword.take(opts, [:group_id, :default, :auto_select])

    {init_data, variant} =
      name
      |> Rendition.new([opts[:track]], variant_opts)
      |> Rendition.init_header("#{name}_init.mp4")

    :ok = File.write!("#{name}_init.mp4", init_data)

    {:reply, :ok, %{state | variants: Map.put(state.variants, name, variant)}}
  end

  @impl true
  def handle_call({:add_variant, _options}, _from, state)
      when state.type == :media and map_size(state.variants) >= 1 do
    {:reply, {:error, "Media playlist support only one variant"}, state}
  end

  @impl true
  def handle_call({:add_variant, options}, _from, state) do
    # TODO: validate options
    name = options[:name]
    rendition_options = [target_duration: 2000, audio: options[:audio]]

    {init_data, variant} =
      name
      |> Rendition.new(options[:tracks], rendition_options)
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
  def handle_cast({:write, variant_or_rendition, sample}, state) do
    variant = state.variants[variant_or_rendition]
    flush? = Rendition.flush?(variant, sample)
    lead_variant? = state.lead_variant == variant_or_rendition

    {variant, flushed?} =
      if (state.lead_variant == nil or lead_variant? or variant.lead_track != nil) and flush? do
        variant =
          variant
          |> flush_and_write(variant_or_rendition)
          |> Rendition.push_sample(sample)

        {variant, true}
      else
        {Rendition.push_sample(variant, sample), false}
      end

    variants =
      if flush? and lead_variant? do
        state.variants
        |> Map.delete(variant_or_rendition)
        |> Map.new(fn {name, variant} ->
          variant = if variant.lead_track, do: variant, else: flush_and_write(variant, name)
          {name, variant}
        end)
      else
        state.variants
      end

    variants =
      if flushed? and state.type == :master do
        variants =
          variant
          |> update_bandwidth(get_referenced_renditions(variant, Map.values(variants)))
          |> then(&Map.put(variants, variant_or_rendition, &1))

        serialize_master_playlist(variants)
        variants
      else
        Map.put(variants, variant_or_rendition, variant)
      end

    {:noreply, %{state | variants: variants}}
  end

  defp flush_and_write(variant, name) do
    uri = "#{name}_#{variant.segment_count}.m4s"
    {data, variant} = Rendition.flush(variant, uri)
    File.write!(uri, data)
    File.write!("#{name}.m3u8", HLX.MediaPlaylist.serialize(variant.playlist))
    variant
  end

  defp serialize_master_playlist(variants) do
    streams = variants |> Map.values() |> Enum.map(& &1.hls_tag)

    payload =
      ExM3U8.serialize(%ExM3U8.MultivariantPlaylist{
        version: 7,
        independent_segments: true,
        items: streams
      })

    File.write!("master.m3u8", payload)
  end

  defp get_referenced_renditions(rendition, renditions) do
    case Rendition.referenced_renditions(rendition) do
      [] ->
        []

      group_ids ->
        renditions
        |> Enum.group_by(&Rendition.group_id/1)
        |> Map.take(group_ids)
    end
  end

  # check for referenced rendition and update the bandwidth
  defp update_bandwidth(rendition, renditions) do
    Enum.reduce(renditions, rendition, fn {_group_id, renditions}, rendition ->
      avg_bandwidth =
        renditions
        |> Enum.map(&Rendition.avg_bandwidth/1)
        |> Enum.max()

      max_bandwidth =
        renditions
        |> Enum.map(&Rendition.max_bandwidth/1)
        |> Enum.max()

      rendition
      |> Rendition.add_avg_bandwidth(avg_bandwidth)
      |> Rendition.add_max_bandwidth(max_bandwidth)
    end)
  end
end
