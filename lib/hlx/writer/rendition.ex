defmodule HLX.Writer.Rendition do
  @moduledoc false

  alias HLX.Muxer.CMAF

  @type t :: %__MODULE__{
          name: String.t(),
          type: :rendition | :variant,
          playlist: HLX.MediaPlaylist.t(),
          tracks: %{non_neg_integer() => ExMP4.Track.t()},
          muxer: any(),
          track_durations: %{non_neg_integer() => non_neg_integer()},
          lead_track: non_neg_integer() | nil,
          segment_count: non_neg_integer(),
          target_duration: non_neg_integer()
        }

  defstruct [
    :name,
    :type,
    :playlist,
    :tracks,
    :muxer,
    :track_durations,
    :lead_track,
    :segment_count,
    :target_duration
  ]

  @spec new(String.t(), [ExMP4.Track.t()], Keyword.t()) :: t()
  def new(name, tracks, opts) do
    target_duration = Keyword.get(opts, :target_duration, 2_000)
    muxer = CMAF.init(tracks)

    lead_track =
      Enum.find_value(tracks, fn
        %{type: :video, id: id} -> id
        _other -> nil
      end)

    tracks = Map.new(tracks, &{&1.id, &1})

    %__MODULE__{
      name: name,
      type: opts[:type] || :variant,
      playlist: HLX.MediaPlaylist.new([]),
      tracks: tracks,
      muxer: muxer,
      track_durations: init_track_durations(Map.values(tracks), target_duration),
      lead_track: lead_track,
      segment_count: 0,
      target_duration: Keyword.get(opts, :target_duration, 2_000)
    }
  end

  @spec init_header(t(), String.t()) :: {binary(), t()}
  def init_header(variant, init_uri) do
    init_header = CMAF.get_init_header(variant.muxer)
    playlist = HLX.MediaPlaylist.add_init_header(variant.playlist, init_uri)
    {init_header, %{variant | playlist: playlist}}
  end

  @spec push_sample(t(), ExMP4.Sample.t()) :: t()
  def push_sample(variant, sample) do
    muxer = CMAF.push(sample, variant.muxer)

    track_durations =
      Map.update!(variant.track_durations, sample.track_id, fn {duration, target_duration} ->
        {duration + sample.duration, target_duration}
      end)

    %{variant | muxer: muxer, track_durations: track_durations}
  end

  @spec flush?(t(), ExMP4.Sample.t()) :: boolean()
  def flush?(variant, sample) do
    {duration, target_duration} = variant.track_durations[sample.track_id]

    (is_nil(variant.lead_track) or variant.lead_track == sample.track_id) and sample.sync? and
      duration >= target_duration
  end

  @spec flush(t(), String.t()) :: {binary(), t()}
  def flush(variant, segment_uri) do
    {data, muxer} = CMAF.flush_segment(variant.muxer)

    {playlist, _} =
      HLX.MediaPlaylist.add_segment(
        variant.playlist,
        %{uri: segment_uri, duration: segment_duration(variant)}
      )

    {data,
     %{
       variant
       | muxer: muxer,
         playlist: playlist,
         track_durations:
           init_track_durations(Map.values(variant.tracks), variant.target_duration),
         segment_count: variant.segment_count + 1
     }}
  end

  defp init_track_durations(tracks, target_duration) do
    Map.new(tracks, fn track ->
      {track.id, {0, ExMP4.Helper.timescalify(target_duration, :millisecond, track.timescale)}}
    end)
  end

  defp segment_duration(%{lead_track: nil} = variant) do
    variant.track_durations
    |> Enum.map(fn {track_id, {duration, _target}} ->
      duration / variant.tracks[track_id].timescale
    end)
    |> Enum.max()
  end

  defp segment_duration(%{lead_track: track_id} = variant) do
    duration = variant.track_durations[track_id] |> elem(0)
    timescale = variant.tracks[track_id].timescale
    duration / timescale
  end
end
