defmodule HLX.WriterTest do
  use ExUnit.Case, async: true

  alias HLX.Writer
  alias HLX.Storage.File

  @moduletag :tmp_dir

  @default_track %ExMP4.Track{
    id: 1,
    type: :video,
    media: :h264,
    timescale: 90_000,
    width: 1280,
    height: 720
  }

  @audio_track %ExMP4.Track{
    id: 2,
    type: :audio,
    media: :aac,
    timescale: 44100,
    channels: 2,
    sample_rate: 44100
  }

  describe "new writer" do
    test "create a new writer", %{tmp_dir: dir} do
      assert {:ok, _writer} = Writer.new(storage: %File{dir: dir})
    end

    test "new writer with invalid params", %{tmp_dir: dir} do
      assert {:error, _} = Writer.new(type: :master_playlist, storage: %File{dir: dir})
      assert {:error, _} = Writer.new(max_segments: -1, storage: %File{dir: dir})
    end

    test "create more than one variant for media playlist should fail", %{tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(type: :media, storage: %File{dir: dir})
      assert {:ok, writer} = Writer.add_variant(writer, "variant1", tracks: [@default_track])

      assert {:error, "Media playlist support only one variant"} =
               Writer.add_variant(writer, "variant2", tracks: [@default_track])
    end

    test "create a rendition for media playlist should fail", %{tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(type: :media, storage: %File{dir: dir})

      assert {:error, :not_master_playlist} =
               Writer.add_rendition(writer, "video", type: :audio, track: @audio_track)
    end
  end
end
