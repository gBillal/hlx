# HLX

[![Hex.pm](https://img.shields.io/hexpm/v/hlx.svg)](https://hex.pm/packages/hlx)
[![API Docs](https://img.shields.io/badge/api-docs-yellow.svg?style=flat)](https://hexdocs.pm/hlx)

HLS writer and reader (Planned).

Features:
  * Writer/Muxer
    * Generate streams in mpeg-ts and fMP4.
    * Support low-latency HLS (LL-HLS).
    * Generate media playlist and/or multivariant playlists.
    * Generate playlists with one or multiple tracks/streams.
    * Support H.264, H.265, AV1 and AAC codecs.

## Usage
To create a simple media playlists with target duration of 2 seconds (default) and store the manifest and
segments on the current directory:

```elixir
{:ok, writer} = HLX.Writer.new(storage_dir: ".")
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
    {:hlx, "~> 0.5.0"}
  ]
end
```
