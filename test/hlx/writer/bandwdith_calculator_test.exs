defmodule HLX.Writer.BandwdithCalculatorTest do
  use ExUnit.Case, async: true

  alias HLX.Writer.BandwidthCalculator

  test "Calculate avg and max bandwidth" do
    calculator = BandwidthCalculator.new(0)

    calculator = BandwidthCalculator.add_segment(calculator, 17_000, 2.02)
    calculator = BandwidthCalculator.add_segment(calculator, 20_000, 1.96)
    calculator = BandwidthCalculator.add_segment(calculator, 15_000, 2.14)
    calculator = BandwidthCalculator.add_segment(calculator, 16_000, 2)

    assert BandwidthCalculator.avg_bitrate(calculator) == 66_995
    assert BandwidthCalculator.max_bitrate(calculator) == 81_632
  end

  test "Calculate avg and max bandwidth width limited segments" do
    calculator = BandwidthCalculator.new(3)

    calculator = BandwidthCalculator.add_segment(calculator, 20_000, 1.96)
    calculator = BandwidthCalculator.add_segment(calculator, 17_000, 2.02)
    calculator = BandwidthCalculator.add_segment(calculator, 15_000, 2.14)

    assert BandwidthCalculator.avg_bitrate(calculator) == 67_973
    assert BandwidthCalculator.max_bitrate(calculator) == 81_632

    calculator = BandwidthCalculator.add_segment(calculator, 16_000, 2)

    assert BandwidthCalculator.avg_bitrate(calculator) == 62_337
    assert BandwidthCalculator.max_bitrate(calculator) == 67_326
  end
end
