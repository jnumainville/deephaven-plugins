# Unified Plotting Library — Design Plan

Status: design proposal (iterate on specifics; core decisions are committed)
Scope: a single, delta-aware, high-performance, column-oriented plotting
library exposing a shared Python API over a declarative JSON model, rendered on
WebGPU (with a WebGL2 fallback), and optimized for — but not bound to — the
Deephaven ticking-table engine.

This document specifies the architecture, the chart model, the resolution and
delta pipelines, and a committed set of design decisions. It is intentionally
lower-level than a napkin vision but higher-level than a spec: each numbered
component is expected to graduate into its own detailed design doc. Where the
original brainstorm left questions open, this revision **resolves them** and
records the rationale in the decision log (§13).

---

## 1. Objectives and Non-Goals

### 1.1 Objectives

1. **Delta-aware.** Every data and configuration change propagates as a minimal
   patch. The renderer never re-materializes a chart it can incrementally update.
2. **High performance.** GPU-resident columnar buffers; downsampling and
   level-of-detail are first-class, not add-ons. Target: interactive frame rates
   on tens of millions of points, bounded by pixels and GPU memory, not row count.
3. **Flexible and extensible.** A permissive low-level model that gatekeeps
   nothing, plus opinionated high-level APIs. New geoms, scales, stats, engines,
   and renderers are registrable without forking the core.
4. **Column-based.** Data is columnar end-to-end (Arrow-compatible). The engine
   performs aggregation, binning, and downsampling; the client renders results.
5. **Legible to agents and humans.** The JSON model is declarative, addressable
   by stable IDs, inheritable via classes, and **compilable** to a fully resolved,
   provenance-annotated form for debugging and machine reasoning.

### 1.2 Non-Goals

- Not a general 2D scene/illustration tool; shapes exist to serve charts.
- Not a dashboarding/layout framework beyond what charts and subcharts require.
- Not committed to reproducing every exotic chart type; breadth is bounded by
  what the columnar + GPU + delta model can serve performantly (§14 gate).

### 1.3 The performance / flexibility / extensibility gate

Every feature in this plan must pass three tests, or it is cut or deferred:

- **Performant:** it must not force per-row work on the main thread, must be
  expressible as columnar/GPU operations or engine-side ops, and must support
  incremental (delta) updates.
- **Flexible:** it must be expressible in the low-level JSON model without a
  bespoke code path, and composable with inheritance, scales, and layout.
- **Extensible:** it must be registrable through a documented extension point
  (geom, stat, scale, coord, engine op, or renderer backend) rather than hard-coded.

---

## 2. System Architecture

```mermaid
flowchart TD
    A[High-level Python API<br/>opinionated, fixes geoms/typing] --> B[Low-level Python API<br/>1:1 with the JSON model]
    B --> C[Chart Document<br/>classes, encodings, geoms, scales, coords, layout]
    C --> D[Resolver / Compiler<br/>class linearization, encoding->geom, provenance]
    D --> E[Query Planner<br/>server/client op negotiation, budgets]
    E --> F[Engine Adapter<br/>Deephaven default; pluggable]
    F --> G[Delta Producer<br/>table ticks -> model+data patches]
    G --> H[Scene Compiler<br/>resolved doc -> GPU scene graph]
    H --> I[Renderer Backend<br/>WebGPU primary / WebGL2 fallback / SVG export]
    I --> J[Interaction & Event Bus<br/>hover, select, zoom, callbacks]
    J --> C
```

Component responsibilities and the contracts between them:

| Component        | Input                      | Output                         | Contract                                                            |
| ---------------- | -------------------------- | ------------------------------ | ------------------------------------------------------------------- |
| Low-level API    | Python calls               | Chart Document (JSON)          | Everything expressible in JSON; no hidden capabilities.             |
| Resolver         | Document                   | Resolved Document + provenance | Deterministic; pure function of the document + registry.            |
| Query Planner    | Resolved Document          | Op plan (server vs. client)    | Cost-based; respects capability flags and budgets (§8).             |
| Engine Adapter   | Op plan + table refs       | Columnar result handles        | Abstract ops (bin, aggregate, downsample); Deephaven default.       |
| Delta Producer   | Engine updates             | Model patch + data patch       | RFC 6902-style model patches; columnar row-range data patches (§7). |
| Scene Compiler   | Resolved Document + data   | GPU scene graph                | Backend-agnostic; no DOM/GPU calls itself.                          |
| Renderer Backend | Scene graph + data buffers | Pixels + hit-test structures   | Swappable (WebGPU/WebGL2/SVG); identical scene-graph interface.     |
| Event Bus        | Pointer/keyboard, engine   | Model patches + user callbacks | Interactions are model mutations; round-trip through the document.  |

**Threading.** The resolver, planner, delta producer, and scene compiler run in
a worker; the renderer owns the GPU device. The main thread only marshals
pointer events and applies compiled draw commands. This keeps authoring and
delta work off the UI thread (satisfies the "warn if slow, never block" goal).

---

## 3. The Chart Document

The Chart Document is the single source of truth. It is a JSON object with a
versioned schema (published as JSON Schema for validation, editor generation,
and agent tool-use). Its top-level shape:

```
Document
├── version            // schema version, for migration
├── data               // named data sources (engine table refs, inline, derived)
├── classes            // reusable, inheritable property bundles ("red_scatter")
├── theme              // top-level class applied to the whole document
├── scales             // named scales/guides (color, size, x, y, ...)
├── coords             // coordinate systems (cartesian, polar, geo, 3d, none)
├── layout             // constraint-based box layout of frames
└── frames[]           // panels; each holds marks and/or nested subcharts
     └── marks[]       // series and shapes (the unit of rendering)
```

### 3.1 Nodes, IDs, and addressing

Every node (frame, mark, scale, axis, class) carries a **stable ID**. IDs are
JSON-Pointer-addressable (`/frames/0/marks/2`) and also carry an optional
author-assigned `id` used for patches, events, and editor targeting. Stable
addressing is what makes deltas, provenance, and linked selection tractable.

### 3.2 Marks: series and shapes are one vocabulary

A **mark** is the unit of rendering. There are two kinds, sharing one schema:

- **Series** — data-bound marks. A series is a _geom template repeated over the
  rows of a data source_ through encoding channels (§3.3).
- **Shapes** — singleton marks (reference line, region, annotation, custom path)
  positioned in data or paper coordinates.

Because a series is formally "a shape templated over data," annotations and data
marks share positioning, styling, and layout. Shapes may also be _data-bound_
(e.g. one reference band per group), which unifies annotations with faceting.

### 3.3 Encoding channels (the core model)

The core mental model is a **grammar of graphics**: a mark binds _channels_ to
_data fields or constants_, and a _geom_ interprets those channels. Channels:

`x`, `y`, `z`, `x2`, `y2` (ranged geoms), `color`, `fill`, `size`, `shape`,
`opacity`, `angle`, `text`, `detail` (grouping without visual effect), `order`,
`key` (identity for delta/animation).

A channel binds to `{ field, scale?, stat?, value? }`. `stat` requests a
transform (§3.6); `scale` names the scale that maps data to visual units.

**Geom from arity (resolved).** The low-level model _accepts_ an explicit
`geom`. If omitted, the resolver infers one from the bound channels using a
fixed, documented table, and **writes the inferred geom back into the compiled
document** so nothing is hidden:

| Bound channels           | Inferred geom      | Coord     |
| ------------------------ | ------------------ | --------- |
| `x` only (quantitative)  | strip (horizontal) | cartesian |
| `y` only (quantitative)  | strip (vertical)   | cartesian |
| `x` + `y`                | point (scatter)    | cartesian |
| `x` + `y` + `z`          | point (3D)         | 3d        |
| `x` + `y2`/`x2` (ranged) | bar / interval     | cartesian |
| categorical `x` + `y`    | bar                | cartesian |
| `theta` (+ `radius`)     | arc (pie/donut)    | polar     |

Inference is a convenience of the _default_ high-level layer; it never blocks
the low-level model, and it is deterministic. Changing scatter→line is a
one-field change (`geom: "line"`) because both consume `x`/`y`. The high-level
API may pin the geom (e.g. `scatter()` forbids inference).

### 3.4 Coordinate systems

`coords` are named and referenced by frames. Built-ins: `cartesian` (linear/log/
symlog/time/categorical per axis), `polar`, `ternary`, `geo`, `3d`, and `none`
(paper-space only, for indicators/pie/treemap that need no axes). Coordinate
systems are an extension point: a new coord registers projection + axis
generators. Marks are written once against channels and _reused_ across coords
where geometrically meaningful.

### 3.5 Axes and the layout system (resolved)

**Decision:** layout is a **constraint-based box model** inspired by CSS
fl/grid, not Plotly's domain/anchor arithmetic. It is both declaratively
authorable and mechanically computable, and it reflows to its container.

- The document is a tree of **frames** (boxes). A frame has a computed content
  box and holds a plot area plus edge-attached guides (axes, legends, colorbars).
- Sizing units: `fr` (fractional/flex), `px`, `%` (of parent), `auto`
  (content-driven, e.g. an axis sized to its tick labels), plus `min`/`max`/
  `aspect` constraints. Gutters/margins are explicit.
- Placement is by **edge attachment** (`top`/`right`/`bottom`/`left`) and
  **fractional grid** cells for facets/subcharts — no absolute pixel math
  required, but absolute is allowed for hand-tuning.
- The solver is a single pass over a DAG of constraints (no iterative
  relaxation in the common case), so layout is fast and its results are
  inspectable (the compiled document contains the resolved boxes).

Multiple axes are first-class: an axis is a guide node bound to a scale and
attached to a frame edge; several axes per edge are allowed (e.g. dual-y).

### 3.6 Stats (declarative transforms)

A `stat` is an engine-abstracted transform requested by a channel or mark:
`bin`, `bin2d`, `count`, `sum`/`mean`/aggregation, `density`/`kde`, `quantile`
(box/violin), `smooth`/`regression`, `ecdf`, `hexbin`, `contour`. Stats keep
histogram/violin/regression _uniform_ — a histogram is `geom: bar` +
`stat: bin`, not a special chart type. The planner (§8) decides whether a stat
runs server-side (default for Deephaven) or client-side.

### 3.7 Scales and guides as first-class nodes

Scales (`x`, `y`, `color`, `size`, `shape`, `opacity`, …) are named document
nodes with type (`linear`, `log`, `symlog`, `time`, `ordinal`, `quantize`,
`threshold`, `sequential`, `diverging`), domain (explicit or data-derived),
range, and guide config (axis/legend/colorbar). Scales can be **shared** across
marks and frames (linked axes, shared color legend) or **independent** per facet.
Because scales are nodes, they inherit and compile like everything else.

### 3.8 Classes and inheritance (resolved)

**Decision:** a **flat class registry** with **linear `extends` chains** and
**explicit multi-class application** — no deeply nested class trees.

- `classes` is a flat map: `{ "red_scatter": { extends: "scatter", ... } }`.
- A node applies classes via `class: ["a", "b"]`. Precedence is resolved by
  **C3 linearization** (the Python MRO algorithm) over the combined `extends`
  graph, producing a deterministic, unambiguous order.
- **Merge algebra** (also resolved):
  - Objects **deep-merge** (later/nearer wins per key).
  - Scalars **replace**.
  - Arrays **replace by default**; arrays whose elements carry an `id`
    **merge by `id`**. Explicit patch operators (`$append`, `$prepend`,
    `$remove`, `$replace`) are available for the rare cases that need them.
  - A node's own inline properties always win over its classes.
- **Data is inheritable**: a class may carry `data`/encoding defaults, not just
  style, so "make everything read from table T" is a one-class change.
- **Theme** is just the document-level class; replacing it re-propagates.

This deliberately avoids the deep-vs-shallow ambiguity: nesting of _definitions_
is disallowed; _composition_ is achieved by listing multiple flat classes. The
merge rules are total and deterministic, which is what makes "compile + annotate
provenance" trivial to implement and to reason about.

### 3.9 Compilation and provenance

The resolver produces a **compiled document**: inheritance flattened, encodings
resolved to explicit geoms, scale domains materialized, layout boxes computed.
With provenance enabled, every leaf value is annotated with its origin (which
class, theme, override, or default). This is the primary debugging tool and the
"explain this chart" surface for agents. See the example in §12.

---

## 4. Rendering (resolved: WebGPU primary + WebGL2 fallback)

**Decision:** WebGPU is the primary backend; a WebGL2 backend implements the
same scene-graph interface for browsers/GPUs without WebGPU; an SVG/Canvas2D
backend serves static export and tiny charts. Backends are registrable, which is
also how alternate charting backends (§9) plug in.

- The **scene compiler** turns the resolved document + columnar buffers into a
  backend-agnostic scene graph: draw batches keyed by geom, referencing GPU
  buffers, scale uniforms, and hit-test metadata.
- Geoms are **pluggable render primitives** (point, line, area, bar/interval,
  arc, rect/heatmap cell, mesh/surface, text). Each geom provides a shader/
  pipeline for each backend plus a CPU hit-tester. New geoms register here.
- Data lives in **GPU-resident columnar buffers**; scale changes update uniforms
  (cheap), data changes update buffer sub-ranges (delta), and only geometry
  affected by a patch is re-encoded.

---

## 5. Downsampling and Level-of-Detail

Downsampling is a first-class primitive and an engine-abstracted op:

- **Series reduction:** LTTB and min/max-per-pixel-bucket for lines/areas,
  preserving visual envelope. Configured as `downsample: { mode, target, strategy }`
  where `mode ∈ { off, count, auto }` and `auto` is viewport- and DPI-aware.
- **Density reduction:** point-heavy scatters/lines degrade to a binned density
  representation (heatmap/hexbin) past a configurable point budget — the same
  mechanism as `stat: bin2d`, so there is one code path.
- **Semantic zoom / LOD:** the compiled document may declare LOD tiers (points →
  hexbin → heatmap) selected by zoom level and point budget. Because tiers are
  just marks with visibility bound to a zoom signal, LOD needs no special engine.
- **Placement:** the planner prefers **server-side** reduction on Deephaven so
  only the pixels-worth of data crosses the wire, with client-side reduction as
  the fallback for engines that cannot do it.

---

## 6. Data Model and Engine Integration

- **Data sources** are named: engine table refs (Deephaven default), inline
  columnar arrays, or **derived sources** (a source + a chain of stats/filters).
- Data is columnar and Arrow-compatible throughout; the client holds only what
  it renders (a viewport- and LOD-bounded window).
- **Deephaven optimization:** ticking tables drive the delta producer directly;
  server-side stats/downsampling minimize traffic; `plot_by` and business-time
  calendars are native (carried forward from the current stack).

---

## 7. Delta Protocol (resolved)

**Decision:** two coordinated patch channels, coalesced per animation frame.

1. **Model patches** — RFC 6902-style JSON Patch operations (`add`/`remove`/
   `replace`/`move`) against the document, addressed by JSON Pointer / node ID.
   Used for config changes, interactions, and structural updates.
2. **Data patches** — columnar, table-oriented: `{ source, added: rowRanges,
modified: rowRanges/cells, removed: rowRanges, shifts }`, mirroring Deephaven
   update semantics. Only affected buffer sub-ranges are re-uploaded to the GPU.

**Granularity decision:** the default unit is the **column/row-range**, not the
individual point (point-level diffs are supported but opt-in for small marks).
Patches are **coalesced** within a frame and applied atomically so the renderer
never shows a half-applied state. `key`/`order` channels drive stable
enter/update/exit for animated transitions.

---

## 8. Query Planner and Op Negotiation (resolved)

**Decision:** a **capability- and cost-based planner** assigns each op (stat,
downsample, aggregate, filter) to server or client.

- Each engine adapter publishes **capability flags** (which ops it supports) and
  cost hints. Each renderer/backend publishes its own capabilities.
- The planner assigns an op **server-side** when the engine supports it and the
  data volume exceeds a budget (the common Deephaven case), otherwise
  **client-side**. Budgets are configurable (row count, bytes, latency target).
- If a required capability is unavailable anywhere, the document **fails loudly
  and early** (with a precise diagnostic), never silently degrading — this is the
  antidote to the Plots.jl-style feature-parity trap.

---

## 9. Extensibility and Alternate Backends

Registrable extension points, each with a stable interface:

- **Geoms** (render primitive + hit-tester), **stats** (transform + engine
  mapping), **scales**, **coordinate systems**, **engine adapters**, and
  **renderer backends**.
- Alternate **charting/rendering backends** (e.g. an SVG/vector backend, or a
  third-party GPU renderer) implement the scene-graph interface and share the
  same Python API and JSON document. Capability flags (§8) keep unsupported
  features honest.

---

## 10. Interactivity, Events, and Selections

- **Interactions as model mutations:** hover, click, box/lasso select, pan/zoom,
  legend toggles, and drag all emit model patches and/or fire server-side
  callbacks. This mirrors and generalizes the existing plotly-events design
  (see `plans/DH-22030-plotly-events.md`), including preventable-default events.
- **Selections are model state:** a selection is a named node holding a predicate
  or key set. Selections can drive other marks/frames (linked brushing) and can
  push down to the engine as filters (crossfilter), all through the same patch
  channel.
- **Signals:** lightweight reactive values (zoom level, selection, hovered key)
  that marks and LOD tiers bind to, evaluated in the worker.

---

## 11. Tooling: Editor, Provenance, Diagnostics

- **Schema-driven editor:** the published JSON Schema generates an editor UI, so
  the editor tracks capabilities automatically.
- **Provenance / "explain this chart":** the compiled, annotated document (§3.9)
  is both a human debugging aid and a machine-readable explanation for agents.
- **Authoring diagnostics:** resolution and data-fetch run in a worker and
  **warn when composition is slow** (large class graphs, expensive derived
  sources) rather than blocking, with timings attributed per node.

---

## 12. Example Chart Spec

A compact but representative document: a Deephaven-backed price chart with a
themed class, a shared color scale, server-side downsampling, a subchart for
volume, a dual axis, an annotation shape, and a selection. Illustrative field
names; not a frozen schema.

```jsonc
{
  "version": "1.0",

  "data": {
    "trades": { "engine": "deephaven", "table": "TradesTicking" },
    "daily": {
      "source": "trades",
      "stat": { "op": "bin", "field": "ts", "unit": "1d" }
    }
  },

  "theme": "corp_dark",

  "classes": {
    "corp_dark": {
      "palette": "viridis",
      "font": "Inter",
      "grid": { "color": "#333" }
    },
    "price_line": {
      "extends": "corp_dark",
      "geom": "line",
      "encode": {
        "x": { "field": "ts", "scale": "x" },
        "y": { "field": "price", "scale": "y" }
      },
      "downsample": { "mode": "auto", "strategy": "lttb" }
    },
    "red_scatter": {
      "extends": "corp_dark",
      "geom": "point",
      "style": { "color": "#e34" }
    }
  },

  "scales": {
    "x": { "type": "time", "domain": "auto" },
    "y": {
      "type": "log",
      "domain": "auto",
      "guide": { "axis": { "side": "left" } }
    },
    "y2": {
      "type": "linear",
      "domain": "auto",
      "guide": { "axis": { "side": "right" } }
    },
    "col": {
      "type": "ordinal",
      "field": "symbol",
      "range": "palette",
      "guide": { "legend": { "side": "right" } }
    }
  },

  "coords": { "main": { "type": "cartesian" } },

  "layout": {
    "type": "grid",
    "rows": ["3fr", "1fr"],
    "gap": "8px",
    "areas": { "price": [0, 0], "volume": [1, 0] }
  },

  "frames": [
    {
      "id": "price",
      "coord": "main",
      "area": "price",
      "marks": [
        {
          "id": "px",
          "class": ["price_line"],
          "encode": { "color": { "field": "symbol", "scale": "col" } }
        },

        {
          "id": "vwap",
          "class": ["price_line"],
          "data": "daily",
          "encode": { "y": { "field": "vwap", "scale": "y" } },
          "style": { "dash": "4 2" }
        },

        {
          "id": "settle",
          "kind": "shape",
          "shape": "rule",
          "encode": { "y": { "value": 100.0, "scale": "y" } },
          "style": { "color": "#888" },
          "label": "settlement"
        }
      ],
      "interactions": {
        "on_select": { "selection": "brush", "channel": "x" },
        "on_click": { "callback": "handle_point_click" }
      }
    },
    {
      "id": "volume",
      "coord": "main",
      "area": "volume",
      "marks": [
        {
          "id": "vol",
          "geom": "bar",
          "data": "daily",
          "encode": {
            "x": { "field": "ts", "scale": "x" },
            "y": { "field": "volume", "scale": "y2" },
            "color": { "field": "symbol", "scale": "col" }
          },
          "downsample": { "mode": "count", "target": 2000 }
        }
      ]
    }
  ],

  "selections": {
    "brush": { "type": "interval", "channel": "x", "pushdown": true }
  }
}
```

The **compiled + provenance-annotated** form of the `px` mark (excerpt) makes
inheritance and inference explicit:

```jsonc
{
  "id": "px",
  "geom": "line", // @from class:price_line
  "coord": "main",
  "encode": {
    "x": { "field": "ts", "scale": "x" }, // @from class:price_line
    "y": { "field": "price", "scale": "y" }, // @from class:price_line
    "color": { "field": "symbol", "scale": "col" } // @from node:inline
  },
  "style": { "font": "Inter" }, // @from theme:corp_dark
  "grid": { "color": "#333" }, // @from theme:corp_dark
  "downsample": { "mode": "auto", "strategy": "lttb", "target": 1440 },
  // mode/strategy @from class:price_line; target @from planner:auto(viewport)
  "plan": { "downsample": "server", "estimated_rows_out": 1440 } // @from query-planner
}
```

---

## 13. Decision Log (resolved questions & conflicts)

| #   | Question / conflict                                 | Decision                                                                                                                            |
| --- | --------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| 1   | Grammar vs. trace as the core model                 | **Grammar core** (channels + geoms + stats + scales). Traces are a _compiled artifact_.                                             |
| 2   | Deep vs. shallow inheritance; is nesting allowed?   | **Flat class registry**, linear `extends`, C3 linearization; **no nested class trees**. Explicit merge algebra (§3.8).              |
| 3   | Server-side vs. client-side ops                     | **Capability- + cost-based planner** (§8); server-side default on Deephaven; fail loudly if impossible.                             |
| 4   | WebGPU-only vs. fallback                            | **WebGPU primary + WebGL2 fallback + SVG export**, one scene-graph interface (§4).                                                  |
| 5   | How much typing inference is "magic"                | **Deterministic arity table** (§3.3); inference writes the geom back into the compiled doc; high-level API may pin it.              |
| 6   | Delta granularity                                   | **Column/row-range default**, point-level opt-in; two channels (model + data); frame-coalesced, atomic (§7).                        |
| 7   | Layout language ("CSS-like" without CSS complexity) | **Single-pass constraint box model** with `fr`/`px`/`%`/`auto` + edge attachment + grid; boxes materialized in compiled doc (§3.5). |
| 8   | Series vs. shapes as separate concepts              | **Unified marks** vocabulary; a series is a shape templated over data (§3.2).                                                       |
| 9   | Backend feature-parity leakage (Plots.jl trap)      | **Capability flags** + early hard failure with diagnostics; never silent degradation (§8/§9).                                       |
| 10  | Model versioning/migration                          | **Versioned schema** with forward migrations run by the resolver; documents declare `version`.                                      |

---

## 14. Feature Set (comprehensive, each gated by §1.3)

Only features that pass the performant/flexible/extensible gate are listed. Each
is expressible in the JSON model and reducible to columnar/GPU or engine ops.

**Core data-visual**

- Encoding channels; geom-from-arity; explicit geom override.
- Geoms: point, line, area/band, step, bar/interval, arc, rect/heatmap cell,
  hexbin, boxplot, violin, candlestick/OHLC, mesh/surface, text/label.
- Stats: bin, bin2d, count, aggregate, density/kde, quantile, regression/smooth,
  ecdf, contour, hexbin — uniform across geoms.
- Scales: linear, log, symlog, time/business-time, ordinal, quantize, threshold,
  sequential, diverging; shared or independent; colorblind-safe default palettes.
- Guides: axes (multi-axis, dual-y), legends, colorbars — all interactive.

**Performance**

- Server-side and client-side downsampling (LTTB, min/max, density).
- Level-of-detail / semantic zoom tiers bound to zoom signals.
- GPU-resident columnar buffers; sub-range buffer updates on delta.
- Viewport-bounded data windows; worker-side resolve/plan/diff.

**Composition & layout**

- Flat inheritable classes + themes; provenance-annotated compilation.
- Constraint box layout; arbitrarily deep subcharts; faceting as layout
  (`facet_wrap`/`facet_grid`) via the same grid + shared/independent scales.
- Multiple coordinate systems (cartesian, polar, ternary, geo, 3d, none).

**Interactivity**

- Hover/tooltip templating, crosshair, pan/zoom with history, box/lasso select.
- Preventable-default server callbacks; legend interactions.
- Selections as model state; linked brushing; engine filter push-down (crossfilter).
- Streaming-native enter/update/exit transitions keyed by `key`/`order`.

**Tooling & integration**

- Published JSON Schema; schema-driven auto-editor.
- "Explain this chart" provenance for agents; per-node authoring timings/warnings.
- Deterministic export (SVG/PNG/PDF + the source JSON) for reproducibility and
  visual-regression testing.
- Accessibility layer (keyboard nav, ARIA, generated text descriptions).
- Deephaven `plot_by` and business-time calendars as native features.

**Deferred (not yet gate-passing; revisit)**

- Network/graph and Sankey layouts (force/edge-routing are not naturally
  columnar/incremental — need a bounded, server-side layout strategy first).
- Fully arbitrary 2D vector illustration (out of scope by §1.2).

---

## 15. Supported-Plots Matrix (validation target)

Deliverable: a matrix mapping **each plot → geom(s), coord, required scales,
engine stat/op, and downsampling strategy** to prove the model covers breadth
without special cases. Seed rows:

| Plot             | Geom(s)      | Coord     | Stat/op         | Downsample      |
| ---------------- | ------------ | --------- | --------------- | --------------- |
| Scatter          | point        | cartesian | —               | density/hexbin  |
| Line/area        | line/area    | cartesian | —               | LTTB / min-max  |
| Bar (grouped)    | bar/interval | cartesian | aggregate       | count           |
| Histogram        | bar          | cartesian | bin             | server-side bin |
| 2D density       | rect/hexbin  | cartesian | bin2d/hexbin    | server-side bin |
| Box / violin     | box/violin   | cartesian | quantile/kde    | server-side     |
| OHLC/candlestick | candlestick  | cartesian | (agg)           | count           |
| Heatmap          | rect         | cartesian | bin2d/aggregate | server-side bin |
| Pie/donut        | arc          | polar     | sum             | —               |
| Sunburst/treemap | arc/rect     | none      | hierarchy       | —               |
| Indicator/KPI    | text/gauge   | none      | aggregate       | —               |
| 3D scatter/surf  | point/mesh   | 3d        | —/grid          | LOD             |
| Choropleth       | rect/path    | geo       | aggregate       | tile LOD        |

---

## 16. Roadmap (sequenced, non-binding)

1. **Document + resolver spike:** schema, node IDs, class linearization + merge
   algebra, encoding→geom inference, provenance compilation, published JSON Schema.
2. **Engine adapter + delta pipeline:** Deephaven ticking → columnar → dual-channel
   patches; one server-side op (downsampling) end-to-end.
3. **WebGPU core (x/y):** point + line geoms, downsampling primitive, one
   cartesian coord, the constraint layout solver; WebGL2 fallback stub.
4. **Interactivity + events:** hover/zoom/select, preventable callbacks, selections
   as model state with engine push-down.
5. **High-level Python API:** arity sugar, pinned-geom constructors, `plot_by`,
   themes-as-classes.
6. **Breadth:** stats (bin/quantile/regression), more geoms/coords, faceting,
   multi-axis, subcharts.
7. **Tooling:** schema-driven editor, provenance/"explain", authoring diagnostics,
   deterministic export.
8. **Alternate backends:** validate the scene-graph interface with a second
   renderer; formalize capability flags.

---

## 17. Cross-Ecosystem Rationale (why these decisions)

Condensed justification for the committed decisions, drawn from prior art:

- **Grammar core (ggplot2, Vega-Lite, Observable Plot):** most composable and
  agent-legible; avoids the scale/stat duplication of trace models (Plotly,
  ECharts). We keep Plotly/ECharts' JSON-serializability by emitting traces as a
  _compiled artifact_, and reject Plotly's domain/anchor layout math.
- **Flat marks + flat classes (Observable Plot):** sidesteps ggplot's deep-layer
  inheritance ambiguity; C3 linearization gives deterministic precedence.
- **Columnar + GPU + delta (deck.gl, Perspective, Makie):** proven at scale;
  Makie validates one API across multiple backends, while Plots.jl warns us to
  make parity gaps explicit via capability flags rather than silent degradation.
- **Reactive selections/signals (Vega signals, Bokeh, HoloViews):** gives linked
  brushing and LOD without a bespoke reactive engine, kept debuggable by the
  provenance/compile tooling.
- **Constraint layout (CSS fl/grid):** authorable and mechanically solvable, with
  materialized boxes for inspection — the readability Plotly lacks.
