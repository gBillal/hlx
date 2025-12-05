defmodule HLX.SampleQueue do
  @moduledoc false

  import ExMP4.Helper, only: [timescalify: 3]

  @type track_id :: {String.t(), non_neg_integer()}
  @type track :: %{
          id: track_id(),
          queue: Qex.t(),
          queue_size: non_neg_integer(),
          buffer?: boolean(),
          timescale: non_neg_integer(),
          duration: non_neg_integer()
        }

  @type t() :: %__MODULE__{
          id: String.t(),
          target_duration: non_neg_integer(),
          lead_track: track_id() | nil,
          tracks: %{track_id() => track()},
          last_sample_timestamp: non_neg_integer()
        }

  defstruct id: nil, lead_track: nil, tracks: %{}, target_duration: 0, last_sample_timestamp: 0

  @spec new(String.t(), non_neg_integer()) :: t()
  def new(id, target_duration) do
    %__MODULE__{id: id, target_duration: target_duration}
  end

  @spec track_ids(t()) :: [track_id()]
  def track_ids(%{tracks: tracks}) do
    tracks
    |> Map.keys()
    |> Enum.map(&elem(&1, 0))
    |> Enum.uniq()
  end

  @spec add_track(t(), track_id(), boolean(), non_neg_integer()) :: t()
  def add_track(sample_queue, id, lead?, timescale) do
    track = %{
      id: id,
      queue: Qex.new(),
      queue_size: 0,
      buffer?: false,
      timescale: timescale,
      duration: 0
    }

    if lead? do
      target_duration = timescalify(sample_queue.target_duration, :millisecond, timescale)

      %{
        sample_queue
        | lead_track: id,
          tracks: Map.put(sample_queue.tracks, id, track),
          target_duration: target_duration
      }
    else
      %{sample_queue | tracks: Map.put(sample_queue.tracks, id, track)}
    end
  end

  def push_sample(%{lead_track: id} = sample_queue, id, sample) do
    track = sample_queue.tracks[id]
    new_segment? = sample.sync? and track.duration >= sample_queue.target_duration

    should_buffer? =
      new_segment? and not flush_queue?(sample_queue) and map_size(sample_queue.tracks) > 1

    cond do
      track.buffer? or should_buffer? ->
        track = %{push(track, sample) | buffer?: true, duration: 0}
        {false, [], %{sample_queue | tracks: Map.put(sample_queue.tracks, id, track)}}

      new_segment? ->
        tracks = Map.put(sample_queue.tracks, id, %{track | duration: sample.duration})
        {sample_queue, samples} = drain_queues(sample_queue)
        {true, [{id, sample} | samples], %{sample_queue | tracks: tracks}}

      true ->
        tracks =
          Map.put(sample_queue.tracks, id, %{track | duration: track.duration + sample.duration})

        sample_queue = %{
          sample_queue
          | last_sample_timestamp: sample.dts,
            tracks: tracks
        }

        {sample_queue, samples} = drain_queues(sample_queue)
        {false, [{id, sample} | samples], sample_queue}
    end
  end

  def push_sample(sample_queue, id, sample) do
    track = sample_queue.tracks[id]
    lead_track = sample_queue.tracks[sample_queue.lead_track]
    sample_timestamp = timescalify(sample.dts, track.timescale, lead_track.timescale)

    cond do
      sample_timestamp <= sample_queue.last_sample_timestamp ->
        {false, [{id, sample}], sample_queue}

      lead_track.buffer? ->
        track = push(track, sample)
        sample_queue = %{sample_queue | tracks: Map.put(sample_queue.tracks, id, track)}

        if flush_queue?(sample_queue) do
          {sample_queue, lead_sample} = drain_lead_track(sample_queue)
          {sample_queue, samples} = drain_queues(sample_queue)
          {true, Enum.concat(lead_sample, samples), sample_queue}
        else
          {false, [], sample_queue}
        end

      true ->
        track = push(track, sample)
        {false, [], %{sample_queue | tracks: Map.put(sample_queue.tracks, id, track)}}
    end
  end

  def flush(sample_queue) do
    {sample_queue, lead_sample} = drain_lead_track(sample_queue)
    {sample_queue, samples} = drain_queues(sample_queue)

    tracks =
      Map.new(sample_queue.tracks, fn {id, track} ->
        {id, %{track | buffer?: false, duration: 0, queue_size: 0, queue: Qex.new()}}
      end)

    {Enum.concat(lead_sample, samples),
     %{sample_queue | tracks: tracks, last_sample_timestamp: 0}}
  end

  defp push(track, sample, where \\ :back)

  defp push(track, sample, :back) do
    queue = Qex.push(track.queue, sample)
    %{track | queue: queue, queue_size: track.queue_size + 1}
  end

  defp push(track, sample, :front) do
    queue = Qex.push_front(track.queue, sample)
    %{track | queue: queue, queue_size: track.queue_size + 1}
  end

  defp pop_sample(track) do
    {sample, queue} = Qex.pop!(track.queue)
    {sample, %{track | queue: queue, queue_size: track.queue_size - 1}}
  end

  defp flush_queue?(sample_queue) do
    Enum.all?(sample_queue.tracks, fn {id, track} ->
      track.queue_size > 0 or id == sample_queue.lead_track
    end)
  end

  defp drain_lead_track(sample_queue) do
    track = sample_queue.tracks[sample_queue.lead_track]
    last_sample_dts = sample_queue.last_sample_timestamp

    callback = fn sample ->
      not sample.sync? or sample.dts - last_sample_dts < sample_queue.target_duration
    end

    {samples, track} = drain(track, callback, [])

    {last_sample_timestamp, duration, samples} =
      reverse_samples(samples, track.id, last_sample_dts)

    track = %{track | buffer?: track.queue_size > 0, duration: duration}

    {%{
       sample_queue
       | last_sample_timestamp: last_sample_timestamp,
         tracks: Map.put(sample_queue.tracks, sample_queue.lead_track, track)
     }, samples}
  end

  defp drain_queues(sample_queue) do
    lead_track = sample_queue.tracks[sample_queue.lead_track]

    {samples, tracks} =
      Enum.map_reduce(sample_queue.tracks, %{}, fn {id, track}, tracks ->
        if id == sample_queue.lead_track do
          {[], Map.put(tracks, id, track)}
        else
          timestamp =
            timescalify(sample_queue.last_sample_timestamp, lead_track.timescale, track.timescale)

          {samples, track} = drain(track, &(&1.dts <= timestamp), [])
          {Enum.reduce(samples, [], &[{id, &1} | &2]), Map.put(tracks, id, track)}
        end
      end)

    {%{sample_queue | tracks: tracks}, Enum.concat(samples)}
  end

  defp drain(%{queue_size: 0} = track, _callback, samples), do: {samples, track}

  defp drain(track, callback, samples) do
    {sample, track} = pop_sample(track)

    if callback.(sample),
      do: drain(track, callback, [sample | samples]),
      else: {samples, push(track, sample, :front)}
  end

  defp reverse_samples([], _id, last_timestamp), do: {last_timestamp, 0, []}

  defp reverse_samples(samples, id, _last_timestamp) do
    last_sample_timestamp = hd(samples).dts

    samples
    |> Enum.reduce({0, []}, fn sample, {duration, samples} ->
      {duration + sample.duration, [{id, sample} | samples]}
    end)
    |> then(&Tuple.insert_at(&1, 0, last_sample_timestamp))
  end
end
