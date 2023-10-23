# Arterial

**TODO: Add description**

## Installation

If [available in Hex](https://hex.pm/docs/publish), the package can be installed
by adding `arterial` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:arterial, "~> 0.1.0"}
  ]
end
```

## Architecture

```mermaid
flowchart TD
    subgraph arterial [Arterial]
    A[fa:fa-person-military-pointing Arterial Sup] <--->|supervises| P(fa:fa-layer-group Pool)
    A[fa:fa-person-military-pointing Arterial Sup] <--->|supervises| CS[fa:fa-person-military-pointing Connection Mgr Sup]
    B>fa:fa-arrow-down-9-1 Backlog] -.-> P
    style B stroke-width:1px,color:#666,stroke-dasharray: 5 5
    CS <-->|supervises| C1(fa:fa-link Connection1)
    CS <-->|supervises| C2(fa:fa-link Connection2)
    CS <-->|supervises| CN(fa:fa-link ConnectionN)
    P -.-|monitors| C1
    P -.-|monitors| C2
    P -.-|monitors| CN
    end
    C1 o-.-o S([Server])
    C2 o-.-o S
    CN o-.-o S
```

Documentation can be generated with [ExDoc](https://github.com/elixir-lang/ex_doc)
and published on [HexDocs](https://hexdocs.pm). Once published, the docs can
be found at <https://hexdocs.pm/arterial>.

