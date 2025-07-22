defmodule HLX.Writer.BandwdithCalculatorTest do
  use ExUnit.Case, async: true

  alias HLX.{MediaPlaylist, Segment}

  test "Calculate avg and max bandwidth" do
    playlist = MediaPlaylist.new([])

    {playlist, nil} =
      MediaPlaylist.add_segment(playlist, Segment.new("seg_0", size: 17_000, duration: 2.02))

    {playlist, nil} =
      MediaPlaylist.add_segment(playlist, Segment.new("seg_1", size: 20_000, duration: 1.96))

    {playlist, nil} =
      MediaPlaylist.add_segment(playlist, Segment.new("seg_2", size: 15_000, duration: 2.14))

    {playlist, nil} =
      MediaPlaylist.add_segment(playlist, Segment.new("seg_3", size: 16_000, duration: 2))

    assert {66_995, 81_632} = MediaPlaylist.bandwidth(playlist)
  end

  # test "Calculate avg and max bandwidth width limited segments" do
  #   calculator = BandwidthCalculator.new(3)

  #   calculator = BandwidthCalculator.add_segment(calculator, 20_000, 1.96)
  #   calculator = BandwidthCalculator.add_segment(calculator, 17_000, 2.02)
  #   calculator = BandwidthCalculator.add_segment(calculator, 15_000, 2.14)

  #   assert BandwidthCalculator.avg_bitrate(calculator) == 67_973
  #   assert BandwidthCalculator.max_bitrate(calculator) == 81_632

  #   calculator = BandwidthCalculator.add_segment(calculator, 16_000, 2)

  #   assert BandwidthCalculator.avg_bitrate(calculator) == 62_337
  #   assert BandwidthCalculator.max_bitrate(calculator) == 67_326
  # end
end
