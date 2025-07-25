defmodule HLX.MixProject do
  use Mix.Project

  def project do
    [
      app: :hlx,
      version: "0.1.0",
      elixir: "~> 1.15",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      {:media_codecs, "~> 0.7.0"},
      {:ex_mp4, "~> 0.11.0"},
      {:ex_m3u8, "~> 0.15.0"},
      {:ex_doc, ">= 0.0.0", only: :dev, runtime: false}
    ]
  end
end
