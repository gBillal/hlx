defmodule HLX.Writer.BandwdithCalculatorTest do
  use ExUnit.Case, async: true

  alias HLX.{MediaPlaylist, Segment}

  setup do
    segments = [
      Segment.new(uri: "seg_1", size: 20_000, duration: 1.96),
      Segment.new(uri: "seg_0", size: 17_000, duration: 2.02),
      Segment.new(uri: "seg_2", size: 15_000, duration: 2.14),
      Segment.new(uri: "seg_3", size: 16_000, duration: 2)
    ]

    %{segments: segments}
  end

  test "Media playlist with max segments", %{segments: segments} do
    playlist = MediaPlaylist.new(max_segments: 3)

    playlist =
      Enum.reduce(0..2, playlist, fn idx, acc ->
        assert {acc, nil} = MediaPlaylist.add_segment(acc, Enum.at(segments, idx))
        acc
      end)

    assert {67_973, 81_632} = MediaPlaylist.bandwidth(playlist)
    assert MediaPlaylist.segment_count(playlist) == 3

    [deleted_segment | _] = segments

    assert {playlist, ^deleted_segment} =
             MediaPlaylist.add_segment(playlist, Enum.at(segments, 3))

    assert {62_337, 67_326} = MediaPlaylist.bandwidth(playlist)
    assert MediaPlaylist.segment_count(playlist) == 4
  end

  test "Calculate avg and max bandwidth", %{segments: segments} do
    playlist = MediaPlaylist.new([])

    playlist =
      Enum.reduce(segments, playlist, fn segment, acc ->
        assert {acc, nil} = MediaPlaylist.add_segment(acc, segment)
        acc
      end)

    assert {66_995, 81_632} = MediaPlaylist.bandwidth(playlist)
  end
end
