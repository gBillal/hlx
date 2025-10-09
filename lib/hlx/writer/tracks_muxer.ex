defmodule HLX.Writer.TracksMuxer do
  @moduledoc false

  import HLX.SampleProcessor

  alias HLX.Muxer.{CMAF, TS}

  @type t :: %__MODULE__{
          name: binary(),
          tracks: %{non_neg_integer() => HLX.Track.t()},
          muxer_mod: module(),
          muxer_state: any() | nil,
          track_durations: %{non_neg_integer() => non_neg_integer()},
          lead_track: non_neg_integer() | nil
        }

  defstruct [
    :name,
    :tracks,
    :muxer_mod,
    :muxer_state,
    :track_durations,
    :lead_track
  ]

  @spec new(binary(), [HLX.Track.t()], keyword()) :: {:ok, t()} | {:error, any()}
  def new(name, tracks, opts) do
    with {:ok, tracks} <- validate_tracks(tracks) do
      tracks_muxer = %__MODULE__{
        name: name,
        tracks: tracks,
        track_durations: init_track_durations(tracks)
      }

      tracks_muxer
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

  @spec save_init_header(t()) :: iodata()
  def save_init_header(%{muxer_mod: TS}), do: <<>>

  def save_init_header(tracks_muxer) do
    tracks_muxer.muxer_mod.get_init_header(tracks_muxer.muxer_state)
  end

  @spec process_sample(t(), HLX.Sample.t()) :: {t(), HLX.Sample.t()}
  def process_sample(%{muxer_state: nil} = tracks_muxer, sample) do
    {track, sample} =
      tracks_muxer.tracks
      |> Map.fetch!(sample.track_id)
      |> process_sample(
        %{sample | dts: sample.dts || sample.pts},
        container(tracks_muxer.muxer_mod)
      )

    tracks_muxer = %{tracks_muxer | tracks: Map.put(tracks_muxer.tracks, track.id, track)}

    if all_tracks_ready?(tracks_muxer.tracks) do
      tracks_muxer
      |> maybe_init_muxer()
      |> then(&{&1, sample})
    else
      {tracks_muxer, sample}
    end
  end

  def process_sample(tracks_muxer, sample) do
    {_track, sample} =
      tracks_muxer.tracks
      |> Map.fetch!(sample.track_id)
      |> process_sample(
        %{sample | dts: sample.dts || sample.pts},
        container(tracks_muxer.muxer_mod)
      )

    {tracks_muxer, sample}
  end

  @spec push_sample(t(), HLX.Sample.t()) :: t()
  def push_sample(tracks_muxer, sample) do
    track_id = sample.track_id
    muxer_state = tracks_muxer.muxer_mod.push(sample, tracks_muxer.muxer_state)
    track_durations = Map.update!(tracks_muxer.track_durations, track_id, &(&1 + sample.duration))
    %{tracks_muxer | muxer_state: muxer_state, track_durations: track_durations}
  end

  @spec push_part(t(), [{non_neg_integer(), [HLX.Sample.t()]}]) :: {iodata(), number(), t()}
  def push_part(tracks_muxer, parts) do
    {data, part_duration, muxer_state} =
      tracks_muxer.muxer_mod.push_part(parts, tracks_muxer.muxer_state)

    tracks_duration =
      Map.new(parts, fn {track_id, samples} ->
        part_duration = Enum.reduce(samples, 0, &(&1.duration + &2))
        {track_id, tracks_muxer.track_durations[track_id] + part_duration}
      end)

    tracks_muxer = %{
      tracks_muxer
      | muxer_state: muxer_state,
        track_durations: tracks_duration
    }

    {data, part_duration, tracks_muxer}
  end

  @spec flush(t()) :: {iodata(), non_neg_integer(), t()}
  def flush(tracks_muxer) do
    {data, muxer_state} = tracks_muxer.muxer_mod.flush_segment(tracks_muxer.muxer_state)

    {data, segment_duration(tracks_muxer),
     %{
       tracks_muxer
       | muxer_state: muxer_state,
         track_durations: init_track_durations(tracks_muxer.tracks)
     }}
  end

  defp validate_tracks(tracks) do
    Enum.reduce_while(tracks, {:ok, %{}}, fn track, {:ok, acc} ->
      case HLX.Track.validate(track) do
        {:ok, track} -> {:cont, {:ok, Map.put(acc, track.id, track)}}
        error -> {:halt, error}
      end
    end)
  end

  defp assign_muxer(tracks_muxer, :mpeg_ts), do: %{tracks_muxer | muxer_mod: TS}
  defp assign_muxer(tracks_muxer, :fmp4), do: %{tracks_muxer | muxer_mod: CMAF}
  defp assign_muxer(tracks_muxer, :low_latency), do: %{tracks_muxer | muxer_mod: CMAF}

  defp maybe_init_muxer(%{tracks: tracks} = tracks_muxer) do
    if all_tracks_ready?(tracks) do
      %{tracks_muxer | muxer_state: tracks_muxer.muxer_mod.init(Map.values(tracks))}
    else
      tracks_muxer
    end
  end

  defp assign_lead_track(tracks_muxer) do
    tracks_muxer.tracks
    |> Enum.find_value(fn
      {id, %{type: :video}} -> id
      _other -> nil
    end)
    |> then(&%{tracks_muxer | lead_track: &1})
  end

  defp init_track_durations(tracks) do
    Map.new(tracks, fn {id, _track} -> {id, 0} end)
  end

  defp segment_duration(tracks_muxer) do
    tracks_muxer.track_durations
    |> Enum.map(fn {track_id, duration} ->
      duration / tracks_muxer.tracks[track_id].timescale
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
