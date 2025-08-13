defmodule HLX.WriterTest do
  use ExUnit.Case, async: true

  alias HLX.{Storage, Writer}
  alias MediaCodecs.{H264, H265, MPEG4}

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
    timescale: 44800
  }

  setup do
    audio_track = HLX.Track.update_priv_data(@audio_track, <<17, 144>>)
    %{audio_track: audio_track, video_track: @default_track}
  end

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
    for segment_type <- [:mpeg_ts, :fmp4] do
      test "#{segment_type}: audio only", %{audio_track: track, tmp_dir: dir} do
        assert {:ok, writer} =
                 Writer.new(
                   storage: %Storage.File{dir: dir},
                   mode: :vod,
                   segment_type: unquote(segment_type)
                 )

        assert {:ok, writer} = Writer.add_variant(writer, "audio", tracks: [track])

        assert :ok = writer |> write_audio_samples("audio") |> Writer.close()

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
        assert length(segments) == 6

        extension = if unquote(segment_type) == :mpeg_ts, do: ".ts", else: ".m4s"

        for segment <- segments do
          assert Path.join(dir, segment.uri) |> File.exists?()
          assert String.ends_with?(segment.uri, extension)
        end

        # TODO: check the actual segment data
      end

      test "#{segment_type}: audio video", %{
        audio_track: audio_track,
        video_track: video_track,
        tmp_dir: dir
      } do
        assert {:ok, writer} =
                 Writer.new(
                   storage: %Storage.File{dir: dir},
                   mode: :vod,
                   segment_type: unquote(segment_type)
                 )

        assert {:ok, writer} =
                 Writer.add_variant(writer, "audio_video", tracks: [video_track, audio_track])

        assert :ok =
                 writer
                 |> write_video_samples("audio_video")
                 |> write_audio_samples("audio_video")
                 |> Writer.close()

        playlist = Path.join(dir, "audio_video.m3u8")
        assert File.exists?(playlist)

        assert {:ok, media_playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

        if unquote(segment_type) == :fmp4 do
          assert %{
                   timeline: [%ExM3U8.Tags.MediaInit{uri: "audio_video/init.mp4"} | _rest],
                   info: %{target_duration: 2, media_sequence: 0}
                 } = media_playlist

          assert File.exists?(Path.join(dir, "audio_video/init.mp4"))
        end

        segments = Enum.filter(media_playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
        assert length(segments) == 5

        extension = if unquote(segment_type) == :mpeg_ts, do: ".ts", else: ".m4s"

        for segment <- segments do
          assert Path.join(dir, segment.uri) |> File.exists?()
          assert String.ends_with?(segment.uri, extension)
        end
      end
    end

    test "live media playlist", %{audio_track: track, tmp_dir: dir} do
      assert {:ok, writer} = Writer.new(storage: %Storage.File{dir: dir}, max_segments: 3)
      assert {:ok, writer} = Writer.add_variant(writer, "audio", tracks: [track])

      assert :ok = writer |> write_audio_samples("audio") |> Writer.close()

      playlist = Path.join(dir, "audio.m3u8")
      assert File.exists?(playlist)

      assert {:ok, media_playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

      assert %{
               timeline: [%ExM3U8.Tags.MediaInit{uri: "audio/init.mp4"} | _rest],
               info: %{target_duration: 2, media_sequence: 3}
             } = media_playlist

      assert File.exists?(Path.join(dir, "audio/init.mp4"))

      segments = Enum.filter(media_playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
      assert length(segments) == 3

      for segment <- segments do
        assert Path.join(dir, segment.uri) |> File.exists?()
        assert String.ends_with?(segment.uri, ".m4s")
      end
    end
  end

  describe "Master playlist" do
    test "audio video", %{audio_track: audio_track, video_track: video_track, tmp_dir: dir} do
      assert {:ok, writer} =
               Writer.new(
                 type: :master,
                 storage: %Storage.File{dir: dir},
                 mode: :vod,
                 segment_type: :fmp4
               )

      assert {:ok, writer} =
               Writer.add_rendition(writer, "audio", track: audio_track, group_id: "audio-group")

      assert {:ok, writer} =
               Writer.add_variant(writer, "video", tracks: [video_track], audio: "audio-group")

      assert :ok =
               writer
               |> write_video_samples("video")
               |> write_audio_samples("audio")
               |> Writer.close()

      master_playlist = Path.join(dir, "master.m3u8")
      video_playlist = Path.join(dir, "video.m3u8")
      audio_playlist = Path.join(dir, "audio.m3u8")

      assert File.exists?(master_playlist)
      assert File.exists?(video_playlist)
      assert File.exists?(audio_playlist)

      assert {:ok, media_playlist} =
               ExM3U8.deserialize_multivariant_playlist(File.read!(master_playlist))

      assert %ExM3U8.MultivariantPlaylist{
               version: 7,
               independent_segments: true,
               items: [
                 %ExM3U8.Tags.Media{
                   uri: "audio.m3u8",
                   group_id: "audio-group",
                   type: :audio
                 },
                 %ExM3U8.Tags.Stream{
                   uri: "video.m3u8",
                   audio: "audio-group",
                   codecs: "avc1.64001F,mp4a.40.2"
                 }
               ]
             } = media_playlist

      for {name, playlist} <- [{"video", video_playlist}, {"audio", audio_playlist}] do
        uri = "#{name}/init.mp4"

        assert {:ok, playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

        assert %{
                 timeline: [%ExM3U8.Tags.MediaInit{uri: ^uri} | _rest],
                 info: %{target_duration: 2, media_sequence: 0}
               } = playlist

        assert File.exists?(Path.join(dir, uri))

        segments = Enum.filter(playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
        assert length(segments) == 5

        for segment <- segments do
          assert Path.join(dir, segment.uri) |> File.exists?()
          assert String.ends_with?(segment.uri, ".m4s")
        end
      end
    end

    test "2 audio & 2 videos", %{audio_track: audio_track, video_track: video_track, tmp_dir: dir} do
      assert {:ok, writer} =
               Writer.new(
                 type: :master,
                 storage: %Storage.File{dir: dir},
                 mode: :vod,
                 segment_type: :fmp4
               )

      assert {:ok, writer} =
               Writer.add_rendition(writer, "audio", track: audio_track, group_id: "audio-group")

      assert {:ok, writer} =
               Writer.add_variant(writer, "video", tracks: [video_track], audio: "audio-group")

      assert {:ok, writer} =
               Writer.add_variant(writer, "video2",
                 tracks: [%{video_track | codec: :h265}, audio_track]
               )

      assert :ok =
               writer
               |> write_video_samples("video")
               |> write_audio_samples("audio")
               |> write_video_samples("video2", :h265)
               |> write_audio_samples("video2")
               |> Writer.close()

      master_playlist = Path.join(dir, "master.m3u8")
      video_playlist = Path.join(dir, "video.m3u8")
      video2_playlist = Path.join(dir, "video2.m3u8")
      audio_playlist = Path.join(dir, "audio.m3u8")

      assert File.exists?(master_playlist)
      assert File.exists?(video_playlist)
      assert File.exists?(video2_playlist)
      assert File.exists?(audio_playlist)

      assert {:ok, media_playlist} =
               ExM3U8.deserialize_multivariant_playlist(File.read!(master_playlist))

      assert %ExM3U8.MultivariantPlaylist{
               version: 7,
               independent_segments: true,
               items: [
                 %ExM3U8.Tags.Media{
                   uri: "audio.m3u8",
                   group_id: "audio-group",
                   type: :audio
                 },
                 %ExM3U8.Tags.Stream{
                   uri: "video.m3u8",
                   audio: "audio-group",
                   codecs: "avc1.64001F,mp4a.40.2"
                 },
                 %ExM3U8.Tags.Stream{
                   uri: "video2.m3u8",
                   audio: nil,
                   codecs: "hvc1.1.6.L93.B0,mp4a.40.2"
                 }
               ]
             } = media_playlist

      for {name, playlist} <- [
            {"video", video_playlist},
            {"audio", audio_playlist},
            {"video2", video2_playlist}
          ] do
        uri = "#{name}/init.mp4"

        assert {:ok, playlist} = ExM3U8.deserialize_media_playlist(File.read!(playlist))

        assert %{
                 timeline: [%ExM3U8.Tags.MediaInit{uri: ^uri} | _rest],
                 info: %{target_duration: 2, media_sequence: 0}
               } = playlist

        assert File.exists?(Path.join(dir, uri))

        segments = Enum.filter(playlist.timeline, &is_struct(&1, ExM3U8.Tags.Segment))
        assert length(segments) == 5

        for segment <- segments do
          assert Path.join(dir, segment.uri) |> File.exists?()
          assert String.ends_with?(segment.uri, ".m4s")
        end
      end
    end
  end

  defp write_audio_samples(writer, variant) do
    "test/fixtures/audio.aac"
    |> File.stream!(1024)
    |> Stream.transform(<<>>, &MPEG4.parse_adts_stream!(&2 <> &1))
    |> Stream.transform(0, fn adts_packet, pts ->
      sample =
        HLX.Sample.new(adts_packet.frames, pts: pts, duration: 1024, track_id: 2)

      {[sample], pts + 1024}
    end)
    |> Enum.reduce(writer, &Writer.write_sample(&2, variant, &1))
  end

  defp write_video_samples(writer, variant, codec \\ :h264) do
    {mod, splitter_mod} =
      if codec == :h264,
        do: {H264, H264.AccessUnitSplitter},
        else: {H265, H265.AccessUnitSplitter}

    "test/fixtures/video.#{codec}"
    |> File.read!()
    |> mod.nalus()
    |> Stream.transform(
      fn -> splitter_mod.new() end,
      fn sample, splitter ->
        case splitter_mod.process(sample, splitter) do
          {nil, splitter} -> {[], splitter}
          {au, splitter} -> {[au], splitter}
        end
      end,
      &{splitter_mod.flush(&1), &1},
      &Function.identity/1
    )
    |> Stream.transform(0, fn au, pts ->
      sample = HLX.Sample.new(au, pts: pts, duration: 1500, track_id: 1)
      {[sample], pts + 1500}
    end)
    |> Enum.reduce(writer, &Writer.write_sample(&2, variant, &1))
  end
end
