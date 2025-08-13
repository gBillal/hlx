defmodule HLX.Writer.Rendition do
  @moduledoc false

  import HLX.SampleProcessor

  alias ExM3U8.Tags.{Stream, Media}
  alias HLX.Muxer.{CMAF, TS}

  @type t :: %__MODULE__{
          type: :rendition | :variant | nil,
          name: binary(),
          playlist: HLX.MediaPlaylist.t(),
          tracks: %{non_neg_integer() => HLX.Track.t()},
          muxer_mod: module(),
          muxer_state: any() | nil,
          track_durations: %{non_neg_integer() => non_neg_integer()},
          lead_track: non_neg_integer() | nil,
          target_duration: non_neg_integer()
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
    :target_duration
  ]

  @spec new(binary(), [HLX.Track.t()], keyword()) :: {:ok, t()} | {:error, any()}
  def new(name, tracks, opts) do
    with {:ok, tracks} <- validate_tracks(tracks) do
      target_duration = Keyword.get(opts, :target_duration, 2_000)

      rendition = %__MODULE__{
        type: opts[:type],
        name: name,
        playlist: HLX.MediaPlaylist.new(opts),
        tracks: tracks,
        track_durations: init_track_durations(tracks),
        target_duration: target_duration
      }

      rendition
      |> assign_muxer(opts[:segment_type])
      |> maybe_init_muxer()
      |> assign_lead_track()
      |> then(&{:ok, &1})
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
    rendition = %{rendition | tracks: tracks}

    if all_tracks_ready?(tracks) do
      rendition
      |> maybe_init_muxer()
      |> then(&{&1, sample})
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
    track_durations = Map.update!(rendition.track_durations, track_id, &(&1 + sample.duration))

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
        track_durations: init_track_durations(rendition.tracks)
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

  defp validate_tracks(tracks) do
    Enum.reduce_while(tracks, {:ok, %{}}, fn track, {:ok, acc} ->
      case HLX.Track.validate(track) do
        {:ok, track} -> {:cont, {:ok, Map.put(acc, track.id, track)}}
        error -> {:halt, error}
      end
    end)
  end

  defp assign_muxer(rendition, :mpeg_ts), do: %{rendition | muxer_mod: TS}
  defp assign_muxer(rendition, :fmp4), do: %{rendition | muxer_mod: CMAF}

  defp maybe_init_muxer(%{tracks: tracks} = rendition) do
    if all_tracks_ready?(tracks) do
      %{rendition | muxer_state: rendition.muxer_mod.init(Map.values(tracks))}
    else
      rendition
    end
  end

  defp assign_lead_track(rendition) do
    rendition.tracks
    |> Enum.find_value(fn
      {id, %{type: :video}} -> id
      _other -> nil
    end)
    |> then(&%{rendition | lead_track: &1})
  end

  defp init_track_durations(tracks) do
    Map.new(tracks, fn {id, _track} -> {id, 0} end)
  end

  defp segment_duration(rendition) do
    rendition.track_durations
    |> Enum.map(fn {track_id, duration} ->
      duration / rendition.tracks[track_id].timescale
    end)
    |> Enum.max()
  end

  defp container(TS), do: :mpeg_ts
  defp container(CMAF), do: :fmp4

  defp all_tracks_ready?(tracks) do
    tracks
    |> Map.values()
    |> Enum.all?(&(&1.priv_data != nil))
  end
end
