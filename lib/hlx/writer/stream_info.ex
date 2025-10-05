defmodule HLX.Writer.StreamInfo do
  @moduledoc false

  @type t :: %__MODULE__{
          type: :rendition | :variant,
          name: String.t(),
          group_id: String.t(),
          default?: boolean(),
          language: String.t(),
          auto_select?: boolean(),
          audio: String.t(),
          subtitles: String.t()
        }

  defstruct [
    :type,
    :name,
    :group_id,
    :default?,
    :language,
    :auto_select?,
    :audio,
    :subtitles
  ]

  @spec to_stream(t()) :: ExM3U8.Tags.Stream.t()
  def to_stream(config) do
    %ExM3U8.Tags.Stream{
      uri: "#{config.name}.m3u8",
      bandwidth: 0,
      audio: config.audio,
      subtitles: config.subtitles,
      codecs: nil
    }
  end

  def to_media(config) do
    %ExM3U8.Tags.Media{
      name: config.name,
      uri: "#{config.name}.m3u8",
      type: :audio,
      group_id: config.group_id,
      default?: config.default? == true,
      language: config.language,
      auto_select?: config.auto_select? == true
    }
  end
end
