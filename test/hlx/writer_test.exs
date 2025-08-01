defmodule HLX.WriterTest do
  use ExUnit.Case, async: true

  alias HLX.{Storage, Writer}
  alias MediaCodecs.MPEG4

  @moduletag :tmp_dir

  @default_track %HLX.Track{
    id: 1,
    type: :video,
    codec: :h264,
    timescale: 90_000
  }

  @audio_track %HLX.Track{
    id: 2,
    type: :audio,
    codec: :aac,
    timescale: 44100
  }

  describe "new writer" do
    test "create a new writer", %{tmp_dir: dir} do
      assert {:ok, _writer} = Writer.new(storage: %Storage.File{dir: dir})
    end

    test "new writer with invalid params", %{tmp_dir: dir} do
      assert {:error, _} = Writer.new(type: :master_playlist, storage: %Storage.File{dir: dir})
      assert {:error, _} = Writer.new(max_segments: -1, storage: %Storage.File{dir: dir})
    end

    test "create more than one variant for media playlist should fail", %{tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(type: :media, storage: %Storage.File{dir: dir})
      assert {:ok, writer} = Writer.add_variant(writer, "variant1", tracks: [@default_track])

      assert {:error, "Media playlist support only one variant"} =
               Writer.add_variant(writer, "variant2", tracks: [@default_track])
    end

    test "create a rendition for media playlist should fail", %{tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(type: :media, storage: %Storage.File{dir: dir})

      assert {:error, :not_master_playlist} =
               Writer.add_rendition(writer, "video", type: :audio, track: @audio_track)
    end
  end

  describe "Media playlist" do
    setup do
      audio_track =
        HLX.Track.new(
          id: 1,
          type: :audio,
          codec: :aac,
          timescale: 48_000,
          priv_data: <<17, 144>>
        )

      %{audio_track: audio_track}
    end

    for segment_type <- [:mpeg_ts, :fmp4] do
      test "#{segment_type}: audio only", %{audio_track: track, tmp_dir: dir} do
        assert {:ok, writer} =
                 Writer.new(
                   storage: %Storage.File{dir: dir},
                   mode: :vod,
                   segment_type: unquote(segment_type)
                 )

        assert {:ok, writer} = Writer.add_variant(writer, "audio", tracks: [track])

        assert :ok =
                 "test/fixtures/audio.aac"
                 |> File.stream!(1024)
                 |> Stream.transform(<<>>, &MPEG4.parse_adts_stream!(&2 <> &1))
                 |> Stream.transform(0, fn adts_packet, pts ->
                   sample =
                     HLX.Sample.new(adts_packet.frames, pts: pts, duration: 1024, track_id: 1)

                   {[sample], pts + 1024}
                 end)
                 |> Enum.reduce(writer, &Writer.write_sample(&2, "audio", &1))
                 |> Writer.close()

        playlist = Path.join(dir, "audio.m3u8")
        assert File.exists?(playlist)

        assert {:ok, media_playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

        if unquote(segment_type) == :fmp4 do
          assert %{
                   timeline: [%ExM3U8.Tags.MediaInit{uri: "audio/init.mp4"} | _rest],
                   info: %{target_duration: 2, media_sequence: 0}
                 } = media_playlist

          assert File.exists?(Path.join(dir, "audio/init.mp4"))
        end

        segments = Enum.filter(media_playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
        assert length(segments) == 5

        extension = if unquote(segment_type) == :mpeg_ts, do: ".ts", else: ".m4s"

        for segment <- segments do
          assert Path.join(dir, segment.uri) |> File.exists?()
          assert String.ends_with?(segment.uri, extension)
        end

        # TODO: check the actual segment data
      end
    end

    test "live media playlist", %{audio_track: track, tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(storage: %Storage.File{dir: dir}, max_segments: 3)
      assert {:ok, writer} = Writer.add_variant(writer, "audio", tracks: [track])

      assert :ok = write_audio_samples(writer)

      playlist = Path.join(dir, "audio.m3u8")
      assert File.exists?(playlist)

      assert {:ok, media_playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

      assert %{
               timeline: [%ExM3U8.Tags.MediaInit{uri: "audio/init.mp4"} | _rest],
               info: %{target_duration: 2, media_sequence: 2}
             } = media_playlist

      assert File.exists?(Path.join(dir, "audio/init.mp4"))

      segments = Enum.filter(media_playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
      assert length(segments) == 3

      for segment <- segments do
        assert Path.join(dir, segment.uri) |> File.exists?()
        assert String.ends_with?(segment.uri, ".m4s")
      end
    end

    defp write_audio_samples(writer) do
      "test/fixtures/audio.aac"
      |> File.stream!(1024)
      |> Stream.transform(<<>>, &MPEG4.parse_adts_stream!(&2 <> &1))
      |> Stream.transform(0, fn adts_packet, pts ->
        sample =
          HLX.Sample.new(adts_packet.frames, pts: pts, duration: 1024, track_id: 1)

        {[sample], pts + 1024}
      end)
      |> Enum.reduce(writer, &Writer.write_sample(&2, "audio", &1))
      |> Writer.close()
    end
  end
end
