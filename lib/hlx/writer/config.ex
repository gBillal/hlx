defmodule HLX.Writer.Config do
  @moduledoc """
  Module describing writer config.
  """

  @type server_control :: [
          can_block_reload: boolean()
        ]

  @type t :: [
          type: :media | :master,
          mode: :vod | :live,
          segment_type: :mpeg_ts | :fmp4 | :low_latency,
          segment_duration: non_neg_integer(),
          part_duration: non_neg_integer(),
          max_segments: non_neg_integer(),
          storage_dir: String.t() | nil,
          server_control: server_control(),
          on_segment_created: (String.t(), HLX.Segment.t() -> any()) | nil,
          on_part_created: (String.t(), HLX.Part.t() -> any()) | nil
        ]

  @default_config [
    type: :media,
    mode: :live,
    segment_type: :fmp4,
    segment_duration: 2000,
    part_duration: 300,
    max_segments: 6,
    storage_dir: nil,
    on_segment_created: nil,
    on_part_created: nil
  ]

  @default_server_control [
    can_block_reload: false
  ]

  @spec new(Keyword.t()) :: {:ok, t()} | {:error, String.t()}
  def new(opts) do
    options = Keyword.merge(@default_config, opts)
    {server_control, options} = Keyword.pop(options, :server_control)
    server_control = Keyword.merge(@default_server_control, server_control || [])

    with :ok <- validate(options),
         :ok <- validate_server_control(server_control) do
      options =
        if options[:mode] == :vod,
          do: Keyword.replace!(options, :max_segments, 0),
          else: options

      version =
        case options[:segment_type] do
          :mpeg_ts -> 6
          :fmp4 -> 7
          :low_latency -> 9
        end

      config = Keyword.merge(options, server_control: server_control, version: version)
      {:ok, config}
    end
  end

  defp validate([]), do: :ok

  defp validate([{:type, type} | rest]) when type in [:media, :master] do
    validate(rest)
  end

  defp validate([{:mode, mode} | rest]) when mode in [:vod, :live] do
    validate(rest)
  end

  defp validate([{:segment_type, type} | rest]) when type in [:mpeg_ts, :fmp4, :low_latency] do
    validate(rest)
  end

  defp validate([{:max_segments, max_segments} | rest])
       when max_segments == 0 or max_segments >= 3 do
    validate(rest)
  end

  defp validate([{:storage_dir, dir} | rest]) when not is_nil(dir) do
    validate(rest)
  end

  defp validate([{:segment_duration, duration} | rest])
       when is_integer(duration) and duration >= 1000 do
    validate(rest)
  end

  defp validate([{:part_duration, duration} | rest])
       when is_integer(duration) and duration >= 100 do
    validate(rest)
  end

  defp validate([{:on_segment_created, callback} | rest])
       when is_function(callback, 2) or is_nil(callback) do
    validate(rest)
  end

  defp validate([{:on_part_created, callback} | rest])
       when is_function(callback, 2) or is_nil(callback) do
    validate(rest)
  end

  defp validate([{key, value} | _rest]) do
    {:error, "Invalid value for #{to_string(key)}: #{inspect(value)}"}
  end

  defp validate_server_control([]), do: :ok

  defp validate_server_control([{:can_block_reload, bool} | rest])
       when is_boolean(bool) do
    validate_server_control(rest)
  end

  defp validate_server_control([{key, value} | _rest]) do
    {:error, "Invalid value for server_control #{to_string(key)}: #{inspect(value)}"}
  end
end
