defmodule HLX.Writer.Rendition do
  @moduledoc false

  import HLX.SampleProcessor

  alias __MODULE__.Config
  alias ExM3U8.Tags.{Stream, Media}
  alias HLX.Muxer.{CMAF, TS}

  @type t :: %__MODULE__{
          type: :rendition | :variant | nil,
          name: binary(),
          playlist: HLX.MediaPlaylist.t(),
          tracks: %{non_neg_integer() => HLX.Track.t()},
          muxer_mod: module(),
          muxer_state: any() | nil,
          track_durations: %{non_neg_integer() => {non_neg_integer(), non_neg_integer()}},
          lead_track: non_neg_integer() | nil,
          target_duration: non_neg_integer(),
          config: Config.t()
        }

  defstruct [
    :type,
    :name,
    :playlist,
    :tracks,
    :muxer_mod,
    :muxer_state,
    :track_durations,
    :lead_track,
    :target_duration,
    :config
  ]

  @spec new(binary(), [HLX.Track.t()], keyword()) :: {:ok, t()} | {:error, any()}
  def new(name, tracks, opts) do
    target_duration = Keyword.get(opts, :target_duration, 2_000)

    validation =
      Enum.reduce_while(tracks, {:ok, %{}}, fn track, {:ok, acc} ->
        case HLX.Track.validate(track) do
          {:ok, track} -> {:cont, {:ok, Map.put(acc, track.id, track)}}
          error -> {:halt, error}
        end
      end)

    with {:ok, tracks} <- validation do
      muxer_mod = if opts[:segment_type] == :mpeg_ts, do: TS, else: CMAF
      muxer_state = if all_tracks_ready?(tracks), do: muxer_mod.init(Map.values(tracks))

      lead_track =
        Enum.find_value(tracks, fn
          {id, %{type: :video}} -> id
          {_id, _track} -> nil
        end)

      config = %Config{
        name: name,
        audio: opts[:audio],
        auto_select?: opts[:auto_select],
        default?: opts[:default],
        group_id: opts[:group_id],
        language: opts[:language],
        subtitles: opts[:subtitles]
      }

      {:ok,
       %__MODULE__{
         type: opts[:type],
         name: name,
         playlist: HLX.MediaPlaylist.new(opts),
         tracks: tracks,
         muxer_mod: muxer_mod,
         muxer_state: muxer_state,
         track_durations: init_track_durations(tracks, target_duration),
         lead_track: lead_track,
         target_duration: target_duration,
         config: config
       }}
    end
  end

  # check if all the tracks have initialization data
  @spec ready?(t()) :: boolean()
  def ready?(%{muxer_state: state}), do: not is_nil(state)

  @spec tracks(t()) :: [HLX.Track.t()]
  def tracks(%{tracks: tracks}), do: Map.values(tracks)

  @spec save_init_header(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def save_init_header(%{muxer_mod: TS} = rendition, storage), do: {rendition, storage}

  def save_init_header(rendition, storage) do
    data = rendition.muxer_mod.get_init_header(rendition.muxer_state)
    {uri, storage} = HLX.Storage.store_init_header(rendition.name, "init.mp4", data, storage)

    rendition = %{
      rendition
      | playlist: HLX.MediaPlaylist.add_init_header(rendition.playlist, uri)
    }

    {rendition, storage}
  end

  @spec process_sample(t(), HLX.Sample.t()) :: {t(), HLX.Sample.t()}
  def process_sample(%{muxer_state: nil} = rendition, sample) do
    track = rendition.tracks[sample.track_id]
    sample = %{sample | dts: sample.dts || sample.pts}
    {track, sample} = process_sample(track, sample, container(rendition.muxer_mod))
    tracks = Map.put(rendition.tracks, track.id, track)

    if all_tracks_ready?(tracks) do
      muxer_state = rendition.muxer_mod.init(Map.values(tracks))
      rendition = %{rendition | muxer_state: muxer_state, tracks: tracks}
      {rendition, sample}
    else
      {rendition, sample}
    end
  end

  def process_sample(rendition, sample) do
    track = rendition.tracks[sample.track_id]
    sample = %{sample | dts: sample.dts || sample.pts}
    {_track, sample} = process_sample(track, sample, container(rendition.muxer_mod))
    {rendition, sample}
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(rendition, sample) do
    track_id = sample.track_id
    muxer_state = rendition.muxer_mod.push(sample, rendition.muxer_state)

    track_durations =
      Map.update!(rendition.track_durations, track_id, fn {duration, target_duration} ->
        {duration + sample.duration, target_duration}
      end)

    %{rendition | muxer_state: muxer_state, track_durations: track_durations}
  end

  @spec flush(t(), HLX.Storage.t()) :: {t(), HLX.Storage.t()}
  def flush(rendition, storage) do
    name = generate_segment_name(rendition)
    {data, muxer_state} = rendition.muxer_mod.flush_segment(rendition.muxer_state)
    {uri, storage} = HLX.Storage.store_segment(rendition.name, name, data, storage)

    segment =
      %HLX.Segment{
        uri: uri,
        size: IO.iodata_length(data),
        duration: segment_duration(rendition)
      }

    {playlist, storage} =
      case HLX.MediaPlaylist.add_segment(rendition.playlist, segment) do
        {playlist, nil} ->
          {playlist, storage}

        {playlist, discarded} ->
          {playlist, HLX.Storage.delete_segment(rendition.name, discarded, storage)}
      end

    rendition = %{
      rendition
      | muxer_state: muxer_state,
        playlist: playlist,
        track_durations: init_track_durations(rendition.tracks, rendition.target_duration)
    }

    {rendition, storage}
  end

  @spec referenced_renditions(t()) :: [String.t()]
  def referenced_renditions(%{hls_tag: %Stream{} = stream}) do
    Enum.reject([stream.audio, stream.subtitles], &is_nil/1)
  end

  def referenced_renditions(_rendition), do: []

  @spec group_id(t()) :: String.t() | nil
  def group_id(%{hls_tag: %Media{group_id: group_id}}), do: group_id
  def group_id(_rendition), do: nil

  @spec bandwidth(t()) :: {non_neg_integer(), non_neg_integer()}
  def bandwidth(%{playlist: playlist}), do: HLX.MediaPlaylist.bandwidth(playlist)

  @spec generate_segment_name(t()) :: binary()
  def generate_segment_name(rendition) do
    extension =
      case rendition.muxer_mod do
        TS -> "ts"
        CMAF -> "m4s"
      end

    "segment_#{HLX.MediaPlaylist.segment_count(rendition.playlist)}.#{extension}"
  end

  @spec to_hls_tag(t(), {list(), list()}) :: struct()
  @spec to_hls_tag(t()) :: struct()
  def to_hls_tag(state, bandwidths \\ {[], []})

  def to_hls_tag(%{type: :rendition, config: config}, {_max, _avg}), do: Config.to_media(config)

  def to_hls_tag(state, {max_bandwidths, avg_bandwidths}) do
    {avg_band, max_band} = HLX.MediaPlaylist.bandwidth(state.playlist)

    %{
      Config.to_stream(state.config)
      | bandwidth: max_band + Enum.sum(max_bandwidths),
        average_bandwidth: avg_band + Enum.sum(avg_bandwidths)
    }
  end

  defp init_track_durations(tracks, target_duration) do
    Map.new(tracks, fn {id, track} ->
      {id, {0, ExMP4.Helper.timescalify(target_duration, :millisecond, track.timescale)}}
    end)
  end

  defp segment_duration(%{lead_track: nil} = rendition) do
    rendition.track_durations
    |> Enum.map(fn {track_id, {duration, _target}} ->
      duration / rendition.tracks[track_id].timescale
    end)
    |> Enum.max()
  end

  defp segment_duration(%{lead_track: track_id} = rendition) do
    duration = rendition.track_durations[track_id] |> elem(0)
    timescale = rendition.tracks[track_id].timescale
    duration / timescale
  end

  defp container(TS), do: :mpeg_ts
  defp container(CMAF), do: :fmp4

  defp all_tracks_ready?(tracks) do
    tracks
    |> Map.values()
    |> Enum.all?(&(&1.priv_data != nil))
  end
end
