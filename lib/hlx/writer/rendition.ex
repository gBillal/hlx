defmodule HLX.Writer.Rendition do
  @moduledoc false

  alias ExM3U8.Tags.{Stream, Media}
  alias HLX.Muxer.CMAF

  @type t :: %__MODULE__{
          name: String.t(),
          playlist: HLX.MediaPlaylist.t(),
          tracks: %{non_neg_integer() => ExMP4.Track.t()},
          muxer_mod: module(),
          muxer_state: any(),
          track_durations: %{non_neg_integer() => non_neg_integer()},
          lead_track: non_neg_integer() | nil,
          target_duration: non_neg_integer(),
          hls_tag: Stream.t() | Media.t() | nil
        }

  defstruct [
    :name,
    :playlist,
    :tracks,
    :muxer_mod,
    :muxer_state,
    :track_durations,
    :lead_track,
    :target_duration,
    :hls_tag
  ]

  @spec new(String.t(), [ExMP4.Track.t()], Keyword.t()) :: t()
  def new(name, tracks, opts) do
    target_duration = Keyword.get(opts, :target_duration, 2_000)
    muxer_state = CMAF.init(tracks)

    lead_track =
      Enum.find_value(tracks, fn
        %{type: :video, id: id} -> id
        _other -> nil
      end)

    tracks = Map.new(tracks, &{&1.id, &1})

    %__MODULE__{
      name: name,
      playlist: HLX.MediaPlaylist.new(opts[:max_segments]),
      tracks: tracks,
      muxer_mod: CMAF,
      muxer_state: muxer_state,
      track_durations: init_track_durations(Map.values(tracks), target_duration),
      lead_track: lead_track,
      target_duration: Keyword.get(opts, :target_duration, 2_000),
      hls_tag: hls_tag(name, opts)
    }
  end

  @spec init_header(t(), String.t()) :: {binary(), t()}
  def init_header(rendition, init_uri) do
    init_header = rendition.muxer_mod.get_init_header(rendition.muxer_state)
    playlist = HLX.MediaPlaylist.add_init_header(rendition.playlist, init_uri)
    {init_header, %{rendition | playlist: playlist}}
  end

  @spec push_sample(t(), ExMP4.Sample.t()) :: t()
  def push_sample(rendition, sample) do
    muxer_state = rendition.muxer_mod.push(sample, rendition.muxer_state)

    track_durations =
      Map.update!(rendition.track_durations, sample.track_id, fn {duration, target_duration} ->
        {duration + sample.duration, target_duration}
      end)

    %{rendition | muxer_state: muxer_state, track_durations: track_durations}
  end

  @spec flush?(t(), ExMP4.Sample.t()) :: boolean()
  def flush?(rendition, sample) do
    {duration, target_duration} = rendition.track_durations[sample.track_id]

    (is_nil(rendition.lead_track) or rendition.lead_track == sample.track_id) and sample.sync? and
      duration >= target_duration
  end

  @spec flush(t(), String.t()) :: {binary(), t()}
  def flush(rendition, segment_uri) do
    {data, muxer_state} = rendition.muxer_mod.flush_segment(rendition.muxer_state)
    seg_duration = segment_duration(rendition)

    {playlist, _discarded} =
      HLX.MediaPlaylist.add_segment(
        rendition.playlist,
        HLX.Segment.new(segment_uri, size: IO.iodata_length(data), duration: seg_duration)
      )

    {data,
     %{
       rendition
       | muxer_state: muxer_state,
         playlist: playlist,
         track_durations:
           init_track_durations(Map.values(rendition.tracks), rendition.target_duration)
     }}
  end

  @spec referenced_renditions(t()) :: [String.t()]
  def referenced_renditions(%{hls_tag: %Stream{} = stream}) do
    Enum.reject([stream.audio, stream.subtitles], &is_nil/1)
  end

  def referenced_renditions(_rendition), do: []

  @spec group_id(t()) :: String.t() | nil
  def group_id(%{hls_tag: %Media{group_id: group_id}}), do: group_id
  def group_id(_rendition), do: nil

  @spec segment_count(t()) :: non_neg_integer()
  def segment_count(%{playlist: playlist}), do: HLX.MediaPlaylist.segment_count(playlist)

  @spec bandwidth(t()) :: {non_neg_integer(), non_neg_integer()}
  def bandwidth(%{playlist: playlist}), do: HLX.MediaPlaylist.bandwidth(playlist)

  @spec to_hls_tag(t(), list(), list()) :: struct() | nil
  def to_hls_tag(state, max_bandwidths \\ [], avg_bandwidths \\ [])

  def to_hls_tag(%{hls_tag: %Stream{} = stream} = state, max_bandwidths, avg_bandwidths) do
    {avg_band, max_band} = HLX.MediaPlaylist.bandwidth(state.playlist)

    %{
      stream
      | bandwidth: max_band + Enum.sum(max_bandwidths),
        average_bandwidth: avg_band + Enum.sum(avg_bandwidths)
    }
  end

  def to_hls_tag(%{hls_tag: tag}, _max, _avg), do: tag

  defp init_track_durations(tracks, target_duration) do
    Map.new(tracks, fn track ->
      {track.id, {0, ExMP4.Helper.timescalify(target_duration, :millisecond, track.timescale)}}
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

  defp hls_tag(name, opts) do
    case opts[:type] do
      :rendition ->
        %Media{
          name: name,
          uri: "#{name}.m3u8",
          type: :audio,
          group_id: opts[:group_id],
          default?: opts[:default] == true,
          language: opts[:language],
          auto_select?: opts[:auto_select] == true
        }

      _rendition ->
        %Stream{
          uri: "#{name}.m3u8",
          bandwidth: 0,
          audio: opts[:audio],
          subtitles: opts[:subtitles],
          codecs: ""
        }
    end
  end
end
