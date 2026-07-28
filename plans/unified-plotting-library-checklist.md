# Unified Plotting Library — Implementation Checklist

Companion to [unified-plotting-library-vision.md](unified-plotting-library-vision.md).
Use this checklist to drive and track implementation. Section numbers in
parentheses (e.g. §3.8) reference the design plan.

## How to use this checklist

- Each **task** is a self-contained, independently testable chunk of work. It is
  sized so that it can be completed, tested, and merged on its own.
- Related tasks are grouped under a **phase** that mirrors the roadmap (plan §16).
- Every task lists an explicit **Done when** (acceptance / test) condition. A task
  is not complete until that condition is demonstrably met by an automated test,
  a benchmark, or a reproducible manual check.
- Prefer implementing tasks top-to-bottom; cross-phase dependencies are called
  out as **Depends on**. Tasks within a phase are mostly parallelizable unless a
  dependency is noted.
- Checkbox legend: `[ ]` not started · `[~]` in progress · `[x]` done.

---

## Phase 0 — Foundations & Scaffolding

- [ ] **0.1 Repo scaffold & package boundaries.** Create packages for
      `model` (schema/types), `resolver`, `engine-adapter`, `renderer`,
      `python-api`, and `tooling`, with a shared build and lint config.
      **Done when:** each package builds, an empty test runs in CI, and a
      dependency-graph lint forbids illegal cross-package imports.

- [ ] **0.2 Columnar data types & Arrow interop.** Define the internal columnar
      buffer types (typed arrays / Arrow vectors) and conversion helpers used by
      every layer. (plan §6)
      **Done when:** round-trip tests convert Arrow ↔ internal buffers for all
      supported dtypes (numeric, temporal, categorical, boolean, null) with no
      precision loss.

- [ ] **0.3 Worker harness.** Stand up the worker used by the resolver, planner,
      delta producer, and scene compiler, with a typed message channel. (plan §2)
      **Done when:** a round-trip echo test passes across the worker boundary and
      transferable buffers are moved (not copied), verified by a benchmark.

- [ ] **0.4 Golden-test & benchmark infrastructure.** Establish snapshot testing
      for documents/compiled output and a microbenchmark runner with recorded
      baselines.
      **Done when:** a sample snapshot test and a sample benchmark run in CI and
      fail on regression beyond a configured threshold.

---

## Phase 1 — Document Model & Resolver

- [ ] **1.1 Document schema v1 (JSON Schema).** Author the top-level document
      schema: `version`, `data`, `classes`, `theme`, `scales`, `coords`,
      `layout`, `frames[]`, `marks[]`. Publish as JSON Schema. (plan §3)
      **Done when:** valid fixtures pass and malformed fixtures fail validation
      with precise error paths; the schema is emitted as a build artifact.

- [ ] **1.2 Node IDs & JSON-Pointer addressing.** Implement stable node IDs and
      pointer resolution/lookup for every node type. (plan §3.1)
      **Done when:** given a document, any node is resolvable by pointer and by
      author `id`; duplicate `id`s are rejected with a diagnostic.

- [ ] **1.3 Class registry & C3 linearization.** Implement the flat class
      registry and C3 linearization over `extends` chains. (plan §3.8)
      **Done when:** unit tests cover linear chains, diamond inheritance, and
      ambiguous/cyclic graphs (the latter reported as errors), matching a table
      of expected precedence orders.

- [ ] **1.4 Merge algebra.** Implement the merge rules: objects deep-merge,
      scalars replace, arrays replace by default, arrays with `id` merge by `id`,
      and patch operators (`$append`, `$prepend`, `$remove`, `$replace`).
      (plan §3.8)
      **Done when:** a property-based test suite confirms determinism and
      associativity where required, plus explicit cases for each rule and operator.

- [ ] **1.5 Encoding channels & geom-from-arity resolver.** Bind channels to
      fields/constants and infer geoms from the fixed arity table, writing the
      inferred geom back into the compiled document. (plan §3.3)
      **Done when:** each row of the arity table has a test asserting the inferred
      geom; an explicit `geom` always overrides inference.

- [ ] **1.6 Compiled document + provenance.** Produce the fully resolved
      document (flattened classes, resolved encodings, materialized scale domains
      are deferred to 3.x) with per-leaf provenance annotations. (plan §3.9)
      **Done when:** the compiled form of the §12 example matches a golden
      snapshot, and every leaf reports a correct origin (class/theme/override/default).

- [ ] **1.7 Schema versioning & migration.** Implement version detection and a
      forward-migration pass run by the resolver. (plan §13 #10)
      **Done when:** a v0 fixture migrates to v1 and validates; an unknown future
      version fails with a clear message.

- [ ] **1.8 Authoring diagnostics & timing.** Instrument the resolver to attribute
      time per node and warn when composition exceeds a budget. (plan §11)
      **Done when:** a deliberately expensive fixture emits a warning with
      per-node timings; a cheap fixture emits none.

---

## Phase 2 — Engine Adapter & Delta Pipeline

- [ ] **2.1 Engine adapter interface & capability flags.** Define the adapter
      interface (data sources, ops, capability flags, cost hints). (plan §8, §9)
      **Done when:** a mock adapter implements the interface and reports
      capabilities; a contract test validates required methods and flag semantics.

- [ ] **2.2 Deephaven adapter — static tables.** Implement source resolution and
      columnar fetch for non-ticking Deephaven tables. **Depends on:** 0.2, 2.1.
      **Done when:** an integration test fetches a known table into internal
      buffers and matches expected values/dtypes.

- [ ] **2.3 Deephaven adapter — ticking & delta producer.** Subscribe to ticking
      tables and emit **data patches** (`added`/`modified`/`removed`/`shifts`).
      (plan §7)
      **Done when:** an integration test drives a ticking table and asserts the
      produced data patches exactly reflect the applied updates.

- [ ] **2.4 Model patch channel (RFC 6902 subset).** Implement apply/produce for
      model patches with JSON-Pointer targets. (plan §7)
      **Done when:** fuzz tests confirm `apply(base, diff(base, next)) == next`
      for representative document mutations.

- [ ] **2.5 Patch coalescing & atomic frame application.** Coalesce model + data
      patches within an animation frame and apply atomically. (plan §7)
      **Done when:** a test bursts N patches within one frame and asserts a single
      coalesced application with no intermediate observable state.

- [ ] **2.6 Query planner (server/client op placement).** Cost- and
      capability-based assignment of ops with configurable budgets; fail loudly
      when a required capability is missing. (plan §8)
      **Done when:** table-driven tests assign a set of (op, data-size,
      capabilities) inputs to the expected side, and an unsupported op fails with
      a precise diagnostic.

---

## Phase 3 — WebGPU Core (x/y)

- [ ] **3.1 Renderer backend interface & scene graph.** Define the
      backend-agnostic scene graph (draw batches, buffers, uniforms, hit-test
      metadata) and the backend interface. (plan §2, §4)
      **Done when:** a headless mock backend consumes a scene graph and records
      the expected draw batches for a fixture.

- [ ] **3.2 GPU device & columnar buffer management.** Initialize the WebGPU
      device and implement GPU-resident columnar buffers with sub-range updates.
      (plan §4)
      **Done when:** a buffer sub-range update test uploads only the changed range
      (verified via instrumentation) and renders the correct result.

- [ ] **3.3 Cartesian coordinate system & scales (linear/log/time).** Implement
      the cartesian coord and the first scale types with domain materialization.
      (plan §3.4, §3.7)
      **Done when:** scale unit tests map domain→range for linear, log, symlog,
      time, and ordinal, including edge cases (empty/degenerate domains).

- [ ] **3.4 Point geom + hit-tester.** Implement the scatter/point geom pipeline
      for WebGPU and its CPU hit-tester. **Depends on:** 3.1–3.3.
      **Done when:** a rendered fixture matches a pixel-golden within tolerance,
      and hit-testing returns the correct nearest point for sampled coordinates.

- [ ] **3.5 Line/area geom + hit-tester.** Implement line/area pipelines and
      hit-testing.
      **Done when:** pixel-golden and hit-test tests pass for single- and
      multi-series line/area fixtures.

- [ ] **3.6 Constraint box layout solver.** Implement the single-pass constraint
      layout (`fr`/`px`/`%`/`auto`, edge attachment, grid) and materialize boxes
      into the compiled document. (plan §3.5)
      **Done when:** layout fixtures produce expected boxes; `auto` axis sizing
      reacts to tick-label extents; a resize reflows deterministically.

- [ ] **3.7 Axes & gridlines rendering.** Render axes/gridlines/ticks bound to
      scales and attached to frame edges, including multi-axis (dual-y). (plan §3.5)
      **Done when:** a dual-y fixture renders both axes correctly and a
      pixel-golden passes.

- [ ] **3.8 End-to-end delta render.** Wire ticking data patches (2.3) through to
      GPU buffer sub-range updates and redraw. **Depends on:** 2.3, 2.5, 3.2.
      **Done when:** a ticking fixture updates on screen via sub-range uploads
      only (no full re-upload), verified by instrumentation and a golden sequence.

- [ ] **3.9 WebGL2 fallback backend (points + lines).** Implement the same scene
      graph on WebGL2 for the two core geoms. (plan §4, §13 #4)
      **Done when:** the WebGL2 backend passes the same point/line pixel-goldens
      (within tolerance) as WebGPU on shared fixtures.

---

## Phase 4 — Downsampling & Level-of-Detail

- [ ] **4.1 Series downsampling (LTTB + min/max).** Implement client-side LTTB
      and min/max-per-pixel-bucket reduction with `downsample` config. (plan §5)
      **Done when:** unit tests assert output point count matches the target and
      the visual envelope (min/max per bucket) is preserved for known inputs.

- [ ] **4.2 Server-side downsampling via engine op.** Expose downsampling as an
      engine op so the planner can push it to Deephaven. **Depends on:** 2.6, 4.1.
      **Done when:** the planner routes a large-series fixture to server-side
      reduction and only the reduced data crosses the adapter boundary.

- [ ] **4.3 Density reduction (bin2d/hexbin).** Implement density binning as the
      degrade path for point-heavy marks, sharing the `stat: bin2d` code path.
      (plan §5, §3.6)
      **Done when:** a scatter above the point budget renders as a density
      heatmap and the bin counts match a reference computation.

- [ ] **4.4 LOD tiers & zoom signal.** Implement LOD tiers (points → hexbin →
      heatmap) selected by a zoom signal and point budget. (plan §5, §10)
      **Done when:** zooming a fixture switches tiers at the configured thresholds
      and the transition is driven purely by the signal (no re-fetch when data is
      already resident).

---

## Phase 5 — Interactivity, Events & Selections

- [ ] **5.1 Pointer event marshaling & hit-test dispatch.** Route pointer/keyboard
      events from the main thread to the worker and dispatch to hit-testers.
      (plan §2, §10)
      **Done when:** synthetic pointer events resolve to the correct mark/point
      and emit the expected internal event, verified without a real GPU.

- [ ] **5.2 Hover/tooltip + crosshair.** Implement hover detection, tooltip
      templating, and crosshair. (plan §10, §14)
      **Done when:** a hover over a known point yields the templated tooltip
      content and crosshair position asserted by test.

- [ ] **5.3 Pan/zoom with history.** Implement pan/zoom as scale-domain model
      patches with an undo/redo history. (plan §10)
      **Done when:** a pan/zoom sequence produces the expected domain patches and
      history navigation restores prior domains exactly.

- [ ] **5.4 Box/lasso selection as model state.** Implement selection nodes
      (interval/point/predicate) updated by box/lasso interactions. (plan §10)
      **Done when:** a box selection populates the selection node with the correct
      key set/predicate, asserted by test.

- [ ] **5.5 Linked brushing & engine filter push-down.** Drive other marks/frames
      from a selection and push selections down as engine filters (crossfilter).
      **Depends on:** 2.6, 5.4.
      **Done when:** selecting in one frame filters a linked frame; with pushdown
      enabled, the engine receives the corresponding filter.

- [ ] **5.6 Server-side callbacks with preventable defaults.** Implement the
      event-callback bridge (click/select/legend) with preventable-default
      semantics, reusing the plotly-events model. (plan §10)
      **Done when:** a callback fires server-side with the correct payload, and
      returning `False` suppresses the default behavior (asserted end-to-end).

- [ ] **5.7 Streaming transitions (enter/update/exit).** Implement keyed
      enter/update/exit transitions driven by `key`/`order`. (plan §7, §14)
      **Done when:** an update fixture animates only changed elements and keys map
      old→new correctly across a data patch.

---

## Phase 6 — High-Level Python API

- [ ] **6.1 Low-level Python bindings (1:1 with the model).** Expose the full
      document model through Python with typed builders. (plan §2)
      **Done when:** a Python-built document serializes to JSON identical to a
      hand-written fixture, byte-for-byte after normalization.

- [ ] **6.2 Arity-sugar high-level constructors.** Implement ergonomic
      constructors that rely on geom-from-arity, plus pinned-geom variants
      (e.g. `scatter()`). (plan §3.3, §1)
      **Done when:** high-level calls produce the same compiled document as the
      equivalent low-level document for a matrix of inputs.

- [ ] **6.3 Themes-as-classes & `plot_by`.** Wire theming through the class system
      and implement `plot_by` grouping. (plan §3.8, §6)
      **Done when:** applying a theme changes only inherited style leaves (checked
      via provenance) and `plot_by` produces the expected per-group marks.

- [ ] **6.4 Business-time calendars & temporal axes.** Support business-time
      scales/axes carried forward from the current stack. (plan §6, §14)
      **Done when:** a business-time axis omits non-business periods and ticks at
      the expected positions for a known calendar.

---

## Phase 7 — Breadth (Geoms, Stats, Coords, Faceting)

- [ ] **7.1 Statistical geoms: bar/interval, histogram, box, violin.** Implement
      geoms + their stats (`bin`, `quantile`). (plan §3.6, §14)
      **Done when:** each geom renders a pixel-golden and its stat output matches a
      reference computation.

- [ ] **7.2 Regression/smoothing & ECDF stats.** Implement `regression`/`smooth`
      and `ecdf`. (plan §3.6)
      **Done when:** fitted values match a reference within tolerance for known
      datasets.

- [ ] **7.3 Financial geoms: OHLC/candlestick.** Implement candlestick/OHLC geom.
      (plan §14, §15)
      **Done when:** a candlestick fixture renders correctly and hit-testing
      returns the correct bar.

- [ ] **7.4 Rect/heatmap & hexbin geoms.** Implement rect/heatmap-cell and hexbin
      rendering. (plan §14, §15)
      **Done when:** heatmap and hexbin fixtures match pixel-goldens and bin counts.

- [ ] **7.5 Polar coord + arc geom (pie/donut).** Implement the polar coord and
      arc geom. (plan §3.4, §15)
      **Done when:** a pie/donut fixture renders with correct angular extents from
      summed values.

- [ ] **7.6 `none` coord + indicators (KPI/gauge).** Implement paper-space
      indicators requiring no axes. (plan §3.4, §14)
      **Done when:** a KPI/gauge fixture renders from an aggregate value with no
      axis system instantiated.

- [ ] **7.7 Hierarchical geoms: sunburst/treemap/icicle.** Implement hierarchy
      stat + arc/rect layout. (plan §15)
      **Done when:** a hierarchy fixture produces correct nested extents and
      drill-down interaction works.

- [ ] **7.8 Faceting as layout (`facet_wrap`/`facet_grid`).** Implement faceting
      via the grid layout with shared/independent scales. **Depends on:** 3.6.
      **Done when:** a facet fixture produces the expected grid of frames and
      shared scales share a domain while independent scales do not.

- [ ] **7.9 Arbitrarily-deep subcharts.** Support nested frames/subcharts through
      the layout system. (plan §3.5)
      **Done when:** a 3-level nested subchart fixture lays out and renders with
      correct boxes at every level.

- [ ] **7.10 3D coord + 3D scatter/surface.** Implement the 3D coord and
      point/mesh geoms. (plan §3.4, §15)
      **Done when:** a 3D scatter and a surface fixture render and camera
      interaction updates the view via model patches.

---

## Phase 8 — Tooling: Editor, Provenance, Export

- [ ] **8.1 Schema-driven editor generation.** Generate an editor UI from the
      published JSON Schema. (plan §11)
      **Done when:** editing a value in the generated UI produces the correct
      model patch, and the UI covers all schema-declared fields for a sample doc.

- [ ] **8.2 "Explain this chart" provenance view.** Surface the compiled,
      annotated document as a human/agent-readable explanation. (plan §11, §3.9)
      **Done when:** the view for the §12 example lists each resolved trait with
      its origin, matching the compiled snapshot.

- [ ] **8.3 Deterministic export (SVG/PNG/PDF + JSON).** Implement export via the
      SVG/Canvas2D backend plus source-JSON emission. (plan §4, §14)
      **Done when:** exporting a fixture twice yields byte-identical SVG and the
      emitted JSON re-renders to the same chart.

- [ ] **8.4 Accessibility layer.** Add keyboard navigation, ARIA roles, and
      generated text descriptions from the model. (plan §14)
      **Done when:** an automated a11y audit passes for a sample chart and keyboard
      navigation reaches every interactive element.

- [ ] **8.5 Visual-regression harness.** Wire deterministic export into CI as a
      visual-regression gate. **Depends on:** 8.3, 0.4.
      **Done when:** an intentional visual change fails the gate and an approved
      baseline update clears it.

---

## Phase 9 — Alternate Backends & Hardening

- [ ] **9.1 Second renderer backend via the scene-graph interface.** Validate the
      backend seam with an additional renderer (e.g. pure SVG/vector). (plan §9)
      **Done when:** the second backend passes the shared scene-graph conformance
      suite for the implemented geoms.

- [ ] **9.2 Capability-flag conformance suite.** Formalize capability negotiation
      across engines and backends with a shared conformance test. (plan §8, §9)
      **Done when:** a backend/engine missing a declared capability causes the
      documented early, precise failure — never silent degradation.

- [ ] **9.3 Second engine adapter (non-Deephaven).** Implement an additional
      engine adapter (e.g. an in-process columnar engine) to prove abstraction.
      (plan §6, §9)
      **Done when:** an existing chart fixture renders unchanged against the new
      adapter, with ops placed per the planner.

- [ ] **9.4 Performance benchmark gates.** Add benchmark gates for the headline
      target (interactive frame rates at tens of millions of points). (plan §1.1)
      **Done when:** the large-dataset benchmark meets the frame-time budget in CI
      and regressions beyond threshold fail the build.

- [ ] **9.5 Fuzz & soak testing for the delta pipeline.** Fuzz model/data patches
      and soak-test long ticking sessions for leaks/drift. (plan §7)
      **Done when:** extended fuzz/soak runs show no buffer leaks, no state drift
      (final render equals a from-scratch render), and no unhandled errors.

---

## Phase 10 — Precision, Nulls & Determinism (robustness)

- [ ] **10.1 Offset-encoded f32 precision.** Keep f64/i64 canonical on the CPU;
      upload per-trace/axis `offset+scale` relative f32; fold offset into the view
      transform in f64. (plan §4, §20)
      **Done when:** a 1-second span inside a 10-year ms-timestamp series renders
      without visible quantization, and a heap check confirms a 4-byte GPU footprint.

- [ ] **10.2 Deep-zoom offset re-centering.** Re-center the offset from zone maps
      before f32 granularity shows; pin offset 0 on log/symlog axes; hysteresis at
      the threshold. **Depends on:** 10.1, 10.5. (plan §20)
      **Done when:** deep zoom into a large-magnitude domain stays sub-pixel and
      zooming back out recovers the original point spread exactly.

- [ ] **10.3 Ticks/hover in f64/i64.** Compute tick positions, labels, and hover
      readouts CPU-side from canonical columns, never through f32. (plan §20)
      **Done when:** tick and hover values are exact at a zoom where geometry is
      f32-quantized, asserted by test.

- [ ] **10.4 Validity bitmaps & null-as-gap.** Carry Arrow validity bitmaps end to
      end; keep NaN out of vertex buffers; segment lines at nulls; skip nulls in
      aggregations with `count_valid` vs `count`. (plan §20)
      **Done when:** a series with interior nulls renders gaps (no invented
      segments), no NaN reaches a buffer, and aggregate null-handling matches a
      reference.

- [ ] **10.5 Zone maps (chunk statistics).** Compute per-chunk
      `min/max/count/null_count/sum/sum_sq` at ingest. (plan §7.2)
      **Done when:** autorange is O(chunks) (verified by instrumentation) and
      viewport chunk pruning skips non-intersecting chunks.

- [ ] **10.6 CPU reference rasterizer + perceptual-diff CI.** Ship a deterministic
      software rasterizer as the oracle; perceptual-diff every backend against it;
      assert reduced buffers bit-identical across backends. (plan §20)
      **Depends on:** 3.9.
      **Done when:** WebGPU and WebGL2 outputs pass perceptual diff vs the CPU
      reference, and aggregate/decimated buffers are asserted bit-identical.

- [ ] **10.7 GPU/WebGL context governor.** Keep a page under a live-context budget
      with LRU eviction of off-screen charts and rebuild-on-scroll; recover from
      device/context loss by rebuilding from the scene graph. (plan §20)
      **Done when:** a 30-chart dashboard keeps all visible charts live and no
      chart permanently blanks; a simulated context loss recovers via reupload.

---

## Phase 11 — Multi-tier LOD & Latency

- [ ] **11.1 Data-space tile pyramid (aggregated tier).** Build power-of-two
      density tiles in data coordinates; compose visible tiles per frame; re-bin
      only below the finest level and only the visible window. **Depends on:** 4.3.
      (plan §5.1)
      **Done when:** pan is tile reuse (0 re-bin) and per-frame cost is O(visible
      tiles), verified by instrumentation; bin counts match a reference.

- [ ] **11.2 Fill-rate-aware tier selection & buffer chunking.** Select tiers on
      count _and_ `mark_pixel_area × overdraw`; chunk large vertex buffers into
      multi-buffer draws. (plan §4, §5.1)
      **Done when:** a dense large-marker scatter trips aggregation below the
      vertex-count ceiling, and a >1 GB dataset allocates without a single
      oversized buffer.

- [ ] **11.3 Out-of-core tiling.** Page chunked columns by viewport with
      pre-aggregated overview tiles; keep resident memory screen-bounded.
      **Depends on:** 11.1. (plan §5.1)
      **Done when:** a larger-than-RAM fixture renders interactively with bounded
      resident memory (asserted by a memory ceiling in the benchmark).

- [ ] **11.4 Latency model: SWR + progressive refinement.** Keep drawing the old
      tier under the new view during rebuild; bin a sample first then refine.
      (plan §5.2)
      **Done when:** pan/zoom never blocks on recompute (same-frame uniform
      update) and a coarse density appears within one frame of a large re-bin.

- [ ] **11.5 Async GPU picking.** Render integer IDs to an offscreen target with
      async readback; exact row at direct/decimated tiers, bin-summary + drill at
      aggregated tiers. (plan §20, §5.2)
      **Done when:** hover resolves the correct target within ≤2 frames regardless
      of point count, and aggregated-tier hover reports a bin summary + top-k drill.

---

## Phase 12 — Transfer Cache & Filtering

- [ ] **12.1 Content-addressed, generation-keyed cache.** Give every transferable
      unit an immutable ID `(source, tier, tile|chunk, data_generation,
  filter_hash)`; LRU-evict under a byte budget. (plan §7.1)
      **Done when:** a changed tile produces a new ID (never an overwrite) and a
      re-request of a held ID transfers 0 bytes.

- [ ] **12.2 Manifest handshake.** On state change, send the needed ID list; the
      client requests only what it lacks; ship only those. **Depends on:** 12.1.
      (plan §7.1)
      **Done when:** pan within cached tiles transfers 0 bytes, and a fresh client
      (empty cache) recovers via the same manifest path with no special case.

- [ ] **12.3 Filter Tier A — indexed range predicates.** Resolve range filters by
      zone-map tile pruning + boundary re-bin. **Depends on:** 10.5. (plan §19)
      **Done when:** a range filter recomputes only boundary tiles (verified) and
      matches a full-recompute reference.

- [ ] **12.4 Filter Tier B — visible-window re-bin.** Serve arbitrary predicates
      by re-binning the visible window server-side under SWR. **Depends on:** 11.4.
      (plan §19)
      **Done when:** an arbitrary-predicate filter updates the visible view
      correctly without a full-dataset scan.

- [ ] **12.5 Selection bitmask.** Per-row 1-bit selection driving styled
      selected/unselected rendering at every tier; aggregated tiers carry a
      selected-count channel. (plan §19)
      **Done when:** a selection dims unselected direct marks and lights up
      selected density on an aggregated tier.

- [ ] **12.6 Filter Tier C — Falcon summed-area cube (linked brushing).** Build a
      cumulative-sum index on the active dimension; resolve brushes as cumsum
      differences across passive views; push down to Deephaven. **Depends on:**
      12.5, 5.5. (plan §19)
      **Done when:** a brush updates ≥5 linked views at interactive frame rates on
      a large fixture, index size scales with bins not rows, and pushdown filters
      the engine.

---

## Phase 13 — External Styling & Theming

- [ ] **13.1 CSS-native chrome.** Render axes/labels/legend/tooltips/container as
      DOM/SVG styleable by plain CSS/Tailwind with full cascade. (plan §18)
      **Done when:** a Tailwind/utility-class stylesheet restyles all chrome and a
      `@media (prefers-color-scheme)` rule flips chrome theming.

- [ ] **13.2 `--chart-*` custom-property token bridge.** Read documented tokens at
      mount, map to GPU uniforms/LUTs; normalize export-unsafe colors at the
      boundary. (plan §18)
      **Done when:** setting `--chart-*` variables (incl. via a cascading parent)
      restyles marks, and a probe resolves `oklch()`/`color-mix()` to fixed channels.

- [ ] **13.3 Live re-resolution (0-byte theme change).** Watch `matchMedia` +
      `MutationObserver`; apply theme changes as uniform/LUT updates, not data
      re-uploads. **Depends on:** 13.2, 7.1. (plan §18)
      **Done when:** a dark-mode toggle repaints via uniforms only (no buffer
      re-upload, 0 wire bytes) at frame rate on a large chart.

- [ ] **13.4 web-client-ui theme binding + Python parity.** Bind tokens to the app
      theme variables; expose `fig.theme(...)`; snapshot resolved tokens to the
      server for export parity. (plan §18)
      **Done when:** a chart inherits the app theme with no per-chart config, and a
      kernel-side export matches the on-screen CSS theme.

---

## Phase 14 — Visualization IR, Compiler Passes & Backends

This phase makes the "visualization is a data structure" thesis real. Much of it
underpins Phase 1; sequence it alongside the model spike.

- [ ] **14.1 VIR canonical schema + dual encodings.** Define the IR abstractly and
      provide JSON (human) and a compact binary (transport) encoding that both
      decode to the identical structure. (plan §2.4, §2.8)
      **Done when:** a corpus of documents round-trips JSON↔IR↔binary with
      structural equality, and the JSON is human-readable in a diff.

- [ ] **14.2 Immutable transformation API.** Implement `VIR → VIR` transforms
      (`add_axis`, `set_encoding`, `facet`, `aggregate`, …) that never mutate the
      input; pair with a structural diff. (plan §1.1, §2.2)
      **Done when:** transforms return new documents (input unchanged, asserted),
      and diffing two documents yields a minimal, human-readable patch.

- [ ] **14.3 Semantic validation pass.** Validate schema + cross-node rules and
      emit precise, user-facing diagnostics before any rendering. (plan §2.1)
      **Done when:** invalid documents fail with node-addressed messages and a
      valid corpus passes; validation runs independently of the renderer.

- [ ] **14.4 Optimization-pass framework + named passes.** A pass manager running
      ordered, individually testable `VIR → VIR` passes: LOD, transform fusion,
      constant folding, viewport pruning, batching, CSE. (plan §2.3)
      **Done when:** each pass has a before/after golden test, passes compose
      deterministically, and disabling a pass changes only its expected output.

- [ ] **14.5 Backend-lowering interface.** Define the "consume lowered VIR →
      output + hit-testing" contract shared by all backends. **Depends on:** 3.1.
      (plan §2.6)
      **Done when:** the WebGPU backend and one non-GPU backend implement the same
      interface and pass a shared conformance suite.

- [ ] **14.6 Library-as-backend adapter (Plotly or Vega).** Lower the VIR onto an
      existing library to prove backends can be third-party. **Depends on:** 14.5.
      (plan §2.6)
      **Done when:** a representative document renders through the adapter and
      preserves semantics (a scatter stays a scatter in the target's model).

- [ ] **14.7 Second frontend conformance.** A second frontend (e.g. TypeScript)
      emits VIR for a shared set of visualizations. (plan §2.7)
      **Done when:** the second frontend produces documents structurally identical
      to the Python frontend for the shared corpus.

- [ ] **14.8 Data-reference model (no inline arrays).** Marks carry column
      _references_ (source + column + derivation), and the planner materializes
      only viewport/LOD-bounded windows. (plan §2.5, §6)
      **Done when:** a 100M-row source renders with the serialized document under a
      fixed small byte size and no inline data, verified by test.

- [ ] **14.9 Plugin domain bundle (proof).** Ship one domain plugin (e.g.
      financial or graph/network) registering geoms/coords/marks without core
      changes. (plan §2.7)
      **Done when:** the plugin adds a working visualization type via public
      extension points only, with no edits to core packages.

---

## Cross-cutting definition of done

Every task above must also satisfy:

- [ ] Automated tests (unit/integration/golden/benchmark as appropriate) run in CI.
- [ ] Public surfaces are typed and documented; JSON Schema updated if the model changed.
- [ ] No main-thread per-row work introduced (resolve/plan/diff stay in the worker).
- [ ] Delta path preserved: the feature updates incrementally, never via full re-render.
- [ ] Passes the plan's performance / flexibility / extensibility gate (plan §1.3).
