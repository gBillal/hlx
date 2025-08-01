# HLX

HLS writer and reader (Planned).

Features:
  * Writer/Muxer
    * Generate streams in mpeg-ts and fMP4.
    * Generate media playlist and/or multivariant playlists.
    * Generate playlists with one or multiple tracks/streams.
    * Support H.264, H.265 and AAC codecs.
    * Save playlists/segments on disk or anywhere else by implementing `HLX.Storage` behaviour.

## Usage
To create a simple media playlists with target duration of 2 seconds (default) and store the manifest and 
segments on the current directory:

```elixir
{:ok, writer} = HLX.Writer.new(storage: %HLX.Storage.File{dir: "."})
track = HLX.Track.new(id: 1, type: :video, codec: :h264, timescale: 90_000)
{:ok, writer} = HLX.Writer.add_variant(writer, "video", tracks: [track])

# send samples
samples = ...
Enum.reduce(samples, writer, &HLX.Writer.write_sample(&2, &1))
``` 

## Installation

The package can be installed by adding `hlx` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:hlx, "~> 0.1.0"}
  ]
end
```

