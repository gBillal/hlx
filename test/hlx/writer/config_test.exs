defmodule HLX.Writer.ConfigTest do
  use ExUnit.Case, async: true

  alias HLX.Writer.Config

  describe "validate/1" do
    test "returns :ok for valid config" do
      config = [
        segment_duration: 6000,
        max_segments: 5,
        storage_dir: "/tmp/hlx_test"
      ]

      assert {:ok, config} = Config.new(config)
      assert config[:segment_duration] == 6000
      assert config[:max_segments] == 5
      assert config[:storage_dir] == "/tmp/hlx_test"
      assert config[:type] == :media
    end

    test "returns {:error, reason} for invalid config" do
      config = [
        version: -1,
        target_duration: 0,
        max_segments: -5,
        storage_dir: ""
      ]

      assert {:error, _reason} = Config.new(config)
    end

    test "validate server control config" do
      assert {:ok, config} = Config.new(server_control: [], storage_dir: "/tmp/hlx_test")
      assert config[:server_control] == [can_block_reload: false]

      assert {:ok, config} =
               Config.new(server_control: [can_block_reload: true], storage_dir: "/tmp/hlx_test")

      assert config[:server_control] == [can_block_reload: true]

      assert {:error, _} =
               Config.new(server_control: [can_block_reload: 90], storage_dir: "/tmp/hlx_test")
    end
  end
end
