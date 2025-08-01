defmodule HLX.Muxer.CMAF do
  @moduledoc """
  Module implementing `HLX.Muxer` that mux media data into fmp4 fragments.
  """

  @behaviour HLX.Muxer

  alias ExMP4.{Box, Track}

  @ftyp %Box.Ftyp{major_brand: "iso5", minor_version: 512, compatible_brands: ["iso6", "mp41"]}
  @mdat_header_size 8

  @type t :: %__MODULE__{
          tracks: %{non_neg_integer() => ExMP4.Track.t()},
          header: ExMP4.Box.t(),
          segments: map(),
          fragments: map(),
          seq_no: non_neg_integer()
        }

  defstruct [:tracks, :header, :segments, :fragments, :seq_no]

  @impl true
  def init(tracks) do
    tracks = Map.new(tracks, &{&1.id, HLX.Track.to_mp4_track(&1)})

    %__MODULE__{
      tracks: tracks,
      header: build_header(Map.values(tracks)),
      segments: new_segments(tracks),
      fragments: new_fragments(tracks),
      seq_no: 1
    }
  end

  @impl true
  def get_init_header(state) do
    Box.serialize([@ftyp, state.header])
  end

  @impl true
  def push(sample, state) do
    fragments =
      Map.update!(state.fragments, sample.track_id, fn {traf, data} ->
        {Box.Traf.store_sample(traf, sample), [sample.payload | data]}
      end)

    %{state | fragments: fragments}
  end

  @impl true
  def flush_segment(state) do
    {moof, mdat} = build_moof_and_mdat(state)
    segments = finalize_segments(state.segments, moof, mdat)

    base_data_offset = Box.size(moof) + @mdat_header_size

    moof = Box.Moof.update_base_offsets(moof, base_data_offset, true)

    tracks =
      Enum.reduce(moof.traf, state.tracks, fn traf, tracks ->
        Map.update!(
          tracks,
          traf.tfhd.track_id,
          &%{&1 | duration: &1.duration + Box.Traf.duration(traf)}
        )
      end)

    segment_data = Box.serialize([segments, moof, mdat])

    state = %{
      state
      | tracks: tracks,
        fragments: new_fragments(tracks),
        segments: new_segments(tracks)
    }

    {segment_data, %{state | seq_no: state.seq_no + 1}}
  end

  defp build_header(tracks) do
    %Box.Moov{
      mvhd: %Box.Mvhd{
        creation_time: DateTime.utc_now(),
        modification_time: DateTime.utc_now(),
        next_track_id: length(tracks) + 1
      },
      trak: Enum.map(tracks, &Track.to_trak(&1, ExMP4.movie_timescale())),
      mvex: %Box.Mvex{
        trex: Enum.map(tracks, & &1.trex)
      }
    }
  end

  defp new_segments(tracks) do
    Map.new(tracks, fn {track_id, track} ->
      sidx = %Box.Sidx{
        reference_id: track_id,
        timescale: track.timescale,
        earliest_presentation_time: track.duration,
        first_offset: 0,
        entries: []
      }

      {track_id, sidx}
    end)
  end

  defp new_fragments(tracks) do
    Map.new(tracks, fn {id, track} ->
      traf = %Box.Traf{
        tfhd: %Box.Tfhd{track_id: id},
        tfdt: %Box.Tfdt{base_media_decode_time: track.duration},
        trun: [%Box.Trun{}]
      }

      {id, {traf, []}}
    end)
  end

  defp build_moof_and_mdat(state) do
    moof = %Box.Moof{mfhd: %Box.Mfhd{sequence_number: state.seq_no}}
    mdat = %Box.Mdat{content: []}

    {moof, mdat} =
      Enum.reduce(state.fragments, {moof, mdat}, fn {_track_id, {traf, data}}, {moof, mdat} ->
        traf = Box.Traf.finalize(traf, true)
        data = Enum.reverse(data)

        moof = %Box.Moof{moof | traf: [traf | moof.traf]}
        mdat = %Box.Mdat{mdat | content: [data | mdat.content]}

        {moof, mdat}
      end)

    moof = %Box.Moof{moof | traf: Enum.reverse(moof.traf)}
    mdat = %Box.Mdat{mdat | content: Enum.reverse(mdat.content)}

    {moof, mdat}
  end

  defp finalize_segments(segments, moof, mdat) do
    {segments, _size} =
      Enum.map_reduce(moof.traf, 0, fn traf, acc ->
        segment = segments[traf.tfhd.track_id]

        segment = %Box.Sidx{
          segment
          | first_offset: acc,
            entries: [
              %{
                reference_type: 0,
                referenced_size: Box.size(moof) + Box.size(mdat),
                subsegment_duration: Box.Traf.duration(traf),
                starts_with_sap: 1,
                sap_type: 0,
                sap_delta_time: 0
              }
            ]
        }

        {segment, acc + Box.size(segment)}
      end)

    Enum.reverse(segments)
  end
end
