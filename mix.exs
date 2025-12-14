defmodule HLX.MixProject do
  use Mix.Project

  @version "0.4.0"
  @github_url "https://github.com/gBillal/hlx"

  def project do
    [
      app: :hlx,
      version: @version,
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps(),

      # hex
      description: "HLS reader and writer",
      package: package(),

      # docs
      name: "HLS Reader and Writer",
      source_url: @github_url,
      docs: docs()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:media_codecs, "~> 0.10.0"},
      {:ex_mp4, "~> 0.14.0"},
      {:ex_m3u8, "~> 0.15.0"},
      {:mpeg_ts, "~> 3.3.5"},
      {:qex, "~> 0.5.1"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end

  defp package do
    [
      maintainers: ["Billal Ghilas"],
      licenses: ["Apache-2.0"],
      links: %{
        "GitHub" => @github_url
      }
    ]
  end

  defp docs do
    [
      main: "readme",
      extras: ["README.md", "LICENSE"],
      formatters: ["html"],
      source_ref: "v#{@version}",
      nest_modules_by_prefix: [
        HLX.Muxer,
        HLX.Writer,
        HLX
      ]
    ]
  end
end
