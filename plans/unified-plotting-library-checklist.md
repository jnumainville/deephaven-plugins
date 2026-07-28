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

## Cross-cutting definition of done

Every task above must also satisfy:

- [ ] Automated tests (unit/integration/golden/benchmark as appropriate) run in CI.
- [ ] Public surfaces are typed and documented; JSON Schema updated if the model changed.
- [ ] No main-thread per-row work introduced (resolve/plan/diff stay in the worker).
- [ ] Delta path preserved: the feature updates incrementally, never via full re-render.
- [ ] Passes the plan's performance / flexibility / extensibility gate (plan §1.3).
