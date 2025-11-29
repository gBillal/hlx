defmodule HLX.Storage.Segment do
  @moduledoc false

  @type t :: %__MODULE__{
          dir: Path.t(),
          playlist_name: String.t(),
          segment_idx: non_neg_integer(),
          part_idx: non_neg_integer(),
          init_header_idx: non_neg_integer(),
          extension: String.t()
        }

  @enforce_keys [:dir, :playlist_name]
  defstruct @enforce_keys ++
              [
                segment_idx: 0,
                part_idx: 0,
                init_header_idx: 0,
                extension: ".m4s"
              ]

  @spec new(Path.t(), String.t(), Keyword.t()) :: t()
  def new(dir, playlist_name, opts \\ []) do
    File.mkdir_p!(Path.join(dir, playlist_name))
    struct(%__MODULE__{dir: dir, playlist_name: playlist_name}, opts)
  end

  @spec store_init_header(binary(), t()) :: {String.t(), t()}
  def store_init_header(payload, state) do
    uri = Path.join(state.playlist_name, "init_#{state.init_header_idx}.mp4")
    File.write!(Path.join(state.dir, uri), payload)
    {uri, %{state | init_header_idx: state.init_header_idx + 1}}
  end

  @spec store_segment(binary(), t()) :: {String.t(), t()}
  def store_segment(payload, state) do
    uri = Path.join(state.playlist_name, "seg_#{state.segment_idx}#{state.extension}")
    File.write!(Path.join(state.dir, uri), payload, [:binary])
    {uri, %{state | segment_idx: state.segment_idx + 1, part_idx: 0}}
  end

  @spec store_part(binary(), t()) :: {String.t(), t()}
  def store_part(payload, state) do
    resource_name = "seg_#{state.segment_idx}_part_#{state.part_idx}#{state.extension}"
    uri = Path.join(state.playlist_name, resource_name)
    File.write!(Path.join(state.dir, uri), payload)
    {uri, %{state | part_idx: state.part_idx + 1}}
  end

  @spec delete_segment(HLX.Segment.t(), t()) :: t()
  def delete_segment(segment, state) do
    File.rm(Path.join(state.dir, segment.uri))
    if segment.media_init, do: File.rm(Path.join(state.dir, segment.media_init))
    state
  end

  @spec delete_parts([HLX.Part.t()], t()) :: t()
  def delete_parts(parts, state) do
    Enum.each(parts, fn part ->
      File.rm!(Path.join(state.dir, part.uri))
    end)

    state
  end

  @spec next_part_uri(t()) :: String.t()
  def next_part_uri(state) do
    resource_name = "seg_#{state.segment_idx}_part_#{state.part_idx}#{state.extension}"
    Path.join(state.playlist_name, resource_name)
  end
end
