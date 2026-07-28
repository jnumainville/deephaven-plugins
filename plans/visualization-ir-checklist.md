# Visualization IR — Implementation Checklist

Companion to [visualization-ir-design.md](visualization-ir-design.md). Every task
is a self-contained, independently testable chunk with an explicit **Done when**
acceptance condition. Section references (e.g. §4.8) point at the design document.

Checkbox legend: `[ ]` not started · `[~]` in progress · `[x]` done.
**Depends on** notes call out cross-phase ordering; tasks within a phase are
otherwise parallelizable.

---

## Phase 0 — Foundations

- [ ] **0.1 Package boundaries.** Create packages: `ir` (schema/types/encodings),
      `compiler` (validation/resolve/passes), `planner`, `engine-adapter`,
      `scene`, `renderer`, `frontend`, `kernel`, `tooling`, with shared build/lint.
      **Done when:** each package builds, an empty test runs in CI, and a
      dependency-graph lint forbids illegal cross-package imports.

- [ ] **0.2 Columnar buffer types & Arrow interop.** Internal columnar buffer
      types with Arrow ↔ internal conversion for all supported dtypes. (§6)
      **Done when:** round-trip tests convert Arrow ↔ internal for numeric,
      temporal, categorical, boolean, and null with no precision loss.

- [ ] **0.3 Worker harness.** Typed message channel to the worker that hosts the
      resolver, planner, delta producer, and scene compiler, with generation IDs,
      bounded queues, cancellation/coalescing, and stale-result rejection. (§3)
      **Done when:** a round-trip echo passes across the worker boundary and
      transferable buffers are moved (not copied), verified by a benchmark; a
      superseding viewport command cancels or discards older work without growing
      the queue beyond its configured bound.

- [ ] **0.4 Golden + benchmark infrastructure.** Snapshot testing for
      documents/compiled output and a microbenchmark runner with baselines.
      **Done when:** a sample snapshot test and a sample benchmark run in CI and
      fail on regression beyond a configured threshold.

---

## Phase 1 — The VIR Core

- [ ] **1.1 Canonical schema (JSON Schema).** Author the top-level document schema:
      `version`, `data`, `classes`, `theme`, `scales`, `coords`, `layout`,
      `frames[]`, `marks[]`, `selections`. (§4)
      **Done when:** valid fixtures pass and malformed fixtures fail with
      node-addressed errors; the schema is emitted as a build artifact.

- [ ] **1.2 Node IDs & JSON-Pointer addressing.** Stable IDs and pointer
      resolution for every node type; author `id` uniqueness. (§4.1)
      **Done when:** any node is resolvable by pointer and by author `id`;
      duplicate IDs are rejected with a diagnostic.

- [ ] **1.3 Dual encodings (JSON + binary).** Abstract IR with a human-readable
      JSON encoding and a compact binary encoding that both decode identically. (§4.12)
      **Done when:** the v1 binary codec is selected/frozen; a cross-language corpus
      round-trips JSON ↔ IR ↔ binary with structural equality; canonical JSON is
      byte-stable with deterministic ordering and exact tagged/non-JSON types.

- [ ] **1.4 Immutable transformation API.** `VIR → VIR` transforms
      (`add_axis`, `set_encoding`, `facet`, `aggregate`, …) that never mutate input;
      paired structural diff. (§2 invariants)
      **Done when:** transforms return new documents (input unchanged, asserted)
      and diffing two documents yields a minimal, readable patch.

- [ ] **1.5 Versioning & forward migration.** Version detection + a forward-migration
      pass run by the resolver. (§4.11)
      **Done when:** chained older-version fixtures migrate deterministically with
      provenance, namespaced extension fields survive, incompatible changes require a
      major version, and an unknown future version fails before data access.

---

## Phase 2 — Semantic Model

- [ ] **2.1 Marks: series & shapes.** One mark schema covering data-bound series and
      singleton/data-bound shapes. (§4.2)
      **Done when:** series and shape fixtures resolve through one code path;
      a data-bound shape produces one instance per group.

- [ ] **2.2 Encoding channels.** Channel set and the `{ field?, value?, scale?, stat? }`
      binding, with `field`/`value` mutual exclusion. (§4.3)
      **Done when:** each channel binds and validates; a `field`+`value` conflict is
      a validation error.

- [ ] **2.3 Geom-from-arity resolver.** Infer geom from bound channels per the fixed
      table; write the inferred geom back into the resolved document; explicit geom
      overrides. (§4.4)
      **Done when:** each table row has a test asserting the inferred geom, and an
      explicit `geom` always wins.

- [ ] **2.4 Coordinate systems.** `cartesian` (linear/log/symlog/time/categorical),
      `polar`, `ternary`, `geo`, `3d`, `none`, as a registered extension point. (§4.5)
      **Done when:** a mark reused across two coords renders in each; a new coord can
      register dimensions, channel/geom compatibility, batched project/invert,
      clipping, and axis/grid generation without core edits; missing inversion
      disables only the interactions that require it.

- [ ] **2.5 Scales & guides.** Named scale nodes (all types), `domain`
      explicit/`auto`, `range`, and `guide` config; shared vs. independent. (§4.6)
      **Done when:** scale unit tests map domain→range for every type incl.
      degenerate domains; a shared scale drives two marks with one domain.

- [ ] **2.6 Stats.** Engine-abstracted transforms (`bin`, `bin2d`, `count`,
      aggregation, `density`/`kde`, `quantile`, `smooth`/`regression`, `ecdf`,
      `hexbin`, `contour`) requested by channel or mark. (§4.7)
      **Done when:** a histogram is expressed as `geom: bar` + `stat: bin` and its
      output matches a reference computation.

- [ ] **2.7 Classes, inheritance & merge algebra.** Flat class registry, linear
      `extends`, C3 linearization, and the full merge algebra + patch operators. (§4.8)
      **Done when:** tests cover linear/diamond/ambiguous/cyclic graphs (the last two
      as errors), plus one case per merge rule and operator; results are deterministic.

- [ ] **2.8 Constraint layout solver.** Frames/boxes, units (`fr`/`px`/`%`/`auto`,
      `min`/`max`/`aspect`), edge attachment + fractional grid; single-pass solve;
      resolved boxes written to the compiled doc. (§4.9)
      **Done when:** layout fixtures produce expected boxes; `auto` axis sizing reacts
      to tick labels; a resize reflows deterministically.

- [ ] **2.9 Multi-axis, subcharts, faceting.** Multiple axes per edge; arbitrarily
      nested subcharts; `facet_wrap`/`facet_grid` via the grid with shared/independent
      scales. (§4.9)
      **Done when:** a dual-y fixture, a 3-level nested subchart, and a facet grid each
      lay out correctly; shared facet scales share a domain, independent ones don't.

- [ ] **2.10 In-IR interactions & selections.** `interactions` on marks/frames naming
      selections/callbacks; `selections` nodes holding predicate/key set. (§4.10)
      **Done when:** a document declaring `on_select`/`on_click` validates and the
      referenced selection/callback resolves.

- [ ] **2.11 Business-time scale contract.** Named/versioned calendars drive time
      mapping, inversion, ticks, pan/zoom, interval selection, and export. (§4.6)
      **Done when:** a known market calendar omits closed periods consistently in
      rendering, inverse mapping, brushing, and export; a missing calendar or
      ambiguous timezone fails validation; DST overlap/gap policies and resolved
      calendar identity appear in provenance.

---

## Phase 3 — Compiler

- [ ] **3.1 Semantic validation pass.** Schema + cross-node rules (references resolve,
      IDs unique, scales/coords exist, channels valid for geom); node-addressed
      diagnostics; runs without a renderer. (§5.1)
      **Done when:** invalid documents fail with precise messages; a valid corpus
      passes; validation is renderer-independent.

- [ ] **3.2 Resolver + compiled document.** Flatten inheritance, resolve encodings to
      explicit geoms, materialize scale domains, compute layout boxes. (§5.2)
      **Done when:** the §17 example compiles to a golden snapshot; the resolver is a
      pure function of `(document, registry)`.

- [ ] **3.3 Provenance annotation.** Per-leaf origin (class/theme/override/default/
      planner) in the compiled document. (§5.2)
      **Done when:** every leaf of the compiled §17 example reports a correct origin.

- [ ] **3.4 Optimization-pass framework + named passes.** Ordered, individually
      testable `VIR → VIR` passes: LOD, transform fusion, constant folding, viewport
      pruning, batching, CSE/caching. (§5.3)
      **Done when:** each pass has a before/after golden; passes compose
      deterministically; disabling a pass changes only its expected output.

- [ ] **3.5 Authoring diagnostics & timings.** Attribute resolve/data-fetch time per
      node; warn when composition is slow without blocking. (§16)
      **Done when:** an expensive fixture emits a per-node warning; a cheap fixture
      emits none.

---

## Phase 4 — Data Model & Planner

- [ ] **4.1 Data sources & column references.** Named sources (engine ref, inline,
      derived); channel `field` is a column reference, never an array. (§6)
      **Done when:** a document referencing a 100M-row source serializes under a fixed
      small byte size with no inline data.

- [ ] **4.2 Engine adapter interface + capability flags.** Adapter interface (sources,
      ops, capability flags, cost hints). (§8)
      **Done when:** a mock adapter implements the interface and a contract test
      validates required methods and flag semantics.

- [ ] **4.3 Deephaven adapter — static tables.** Source resolution + columnar fetch for
      non-ticking tables. **Depends on:** 0.2, 4.2. (§6)
      **Done when:** an integration test fetches a known table into internal buffers
      with expected values/dtypes.

- [ ] **4.4 Deephaven adapter — ticking + delta producer.** Subscribe to ticking tables
      and emit data patches (`added`/`modified`/`removed`/`shifts`). (§9)
      **Done when:** an integration test drives a ticking table and asserts the data
      patches exactly reflect the updates.

- [ ] **4.5 Query planner.** Cost- and capability-based server/client op placement with
      configurable budgets; fail loud on missing capability. (§8)
      **Done when:** table-driven tests place `(op, size, capabilities)` inputs on the
      expected side using common-unit setup/input/group/output/update/cache cost hints
      and confidence bounds; unknown hard-budget costs and unsupported ops fail with
      precise diagnostics.

- [ ] **4.6 Columnar data-plane conformance.** Define chunk schema, ownership,
      generation/lease lifecycle, Arrow metadata, dictionary identity, and dtype
      lowerings. (§6.1)
      **Done when:** multi-chunk numeric, temporal, categorical, string, decimal,
      boolean, and null fixtures cross adapter/worker boundaries without precision
      loss; compatible buffers are zero-copy, measured fallbacks are explicit, and
      v1 numeric/temporal/decimal/boolean/categorical/UTF-8 lowerings follow §6.1;
      unsupported nested/object dtypes fail before execution.

---

## Phase 5 — Delta Protocol & Transport

- [ ] **5.1 Model patch channel (RFC 6902).** Apply/produce model patches by JSON
      Pointer. (§9)
      **Done when:** fuzz tests confirm `apply(base, diff(base, next)) == next` for
      representative mutations.

- [ ] **5.2 Data patch channel.** Columnar row-range patches driving GPU buffer
      sub-range updates. **Depends on:** 4.4. (§9)
      **Done when:** a ticking fixture updates via sub-range uploads only (no full
      re-upload), verified by instrumentation.

- [ ] **5.3 Coalescing & atomic application.** Coalesce model + data patches per frame;
      apply atomically. (§9)
      **Done when:** a burst of patches in one frame yields a single coalesced,
      all-or-nothing application with no intermediate observable state.

- [ ] **5.4 Transfer cache.** Content-addressed, immutable, generation+filter-keyed
      entries; LRU byte-budget eviction. (§9.1)
      **Done when:** a changed tile gets a new ID (never an overwrite) and re-requesting
      a held ID transfers 0 bytes.

- [ ] **5.5 Manifest handshake.** Producer sends needed ID list; client requests only
      what it lacks. **Depends on:** 5.4. (§9.1)
      **Done when:** pan within cached tiles transfers 0 bytes, and a fresh (empty-cache)
      client recovers via the same path with no special case; fragmented/checksummed
      manifests obey in-flight byte limits, retry idempotently, and resync after
      repeated timeout or corruption.

- [ ] **5.6 Zone maps.** Per-chunk `min/max/count/null_count/sum/sum_sq` (+ dictionary
      cardinality) at ingest. (§9.2)
      **Done when:** autorange is O(chunks) (instrumented) and viewport pruning skips
      non-intersecting chunks.

- [ ] **5.7 Sequencing, reconnect & resync.** Carry source/document generation,
      sequence, and base generation on patch transactions; detect duplicates, gaps,
      and stale bases with a bounded reorder window. (§9)
      **Done when:** reordered/duplicated/gapped patch tests either converge exactly
      or request snapshot + manifest resync, and reconnect uses that same path.

- [ ] **5.8 Dependency invalidation & schema changes.** Atomically invalidate
      dependent stats, domains, LOD tiles, selections, geometry neighbors, and cache
      entries; version source schema changes. (§6.1, §9)
      **Done when:** cell/shift/schema-change fixtures equal a from-scratch render;
      compatible additions replan and incompatible changes fail revalidation before
      new-schema data is applied.

---

## Phase 6 — Rendering

- [ ] **6.1 Backend interface & scene graph.** Backend-agnostic scene graph (draw
      batches, buffers, uniforms, hit-test metadata) and the backend interface. (§7)
      **Done when:** a headless mock backend consumes a scene graph and records the
      expected draw batches for a fixture.

- [ ] **6.2 WebGPU device & columnar buffers.** GPU-resident columnar buffers with
      sub-range updates. (§7)
      **Done when:** a sub-range update uploads only the changed range (instrumented)
      and renders the correct result.

- [ ] **6.3 Point & line geoms + hit-testers.** WebGPU pipelines + CPU hit-testers.
      **Depends on:** 6.1, 6.2, 2.5. (§7, §4.4)
      **Done when:** rendered fixtures match pixel-goldens within tolerance and
      hit-testing returns the correct nearest primitive.

- [ ] **6.4 Axes & gridlines.** Render axes/ticks/gridlines bound to scales and attached
      to frame edges, incl. dual-y. **Depends on:** 2.8. (§4.9)
      **Done when:** a dual-y fixture renders both axes correctly against a pixel-golden.

- [ ] **6.5 Offset-encoded f32 precision.** f64/i64 canonical on CPU; per-trace/axis
      `offset+scale` relative f32 folded into the view transform; deep-zoom
      re-centering from zone maps; log/symlog pin offset 0; ticks/hover in f64/i64.
      **Depends on:** 5.6. (§7)
      **Done when:** a 1-second span inside a 10-year ms-timestamp series renders without
      quantization; deep zoom stays sub-pixel and zoom-out recovers the point spread;
      tick/hover values are exact where geometry is f32-quantized.

- [ ] **6.6 GPU picking.** Integer-ID offscreen target + async readback; exact row at
      direct/decimated, bin summary + drill-to-top-k at aggregated tiers. (§7)
      **Done when:** hover resolves the correct target within ≤2 frames regardless of
      point count, and aggregated-tier hover reports a bin summary + top-k drill.

- [ ] **6.7 GPU ceilings.** Tier selection accounts for fill-rate
      (`count × mark_pixel_area × overdraw`); chunk large buffers into multi-buffer
      draws. (§7)
      **Done when:** a dense large-marker scatter trips aggregation below the
      vertex-count ceiling, and a >1 GB dataset allocates without a single oversized
      buffer.

- [ ] **6.8 WebGL2 fallback (points + lines).** Same scene graph on WebGL2. (§7)
      **Done when:** WebGL2 passes the same point/line pixel-goldens (within tolerance)
      as WebGPU on shared fixtures.

- [ ] **6.9 Recoverability.** Rebuild all GPU state from scene graph + canonical store on
      device/context loss. (§7)
      **Done when:** a simulated context loss recovers via reupload with no data loss.

- [ ] **6.10 End-to-end delta render.** Wire ticking data patches through to GPU
      sub-range updates and redraw. **Depends on:** 5.2, 5.3, 6.2. (§9)
      **Done when:** a ticking fixture updates on screen via sub-range uploads only,
      verified against a golden sequence.

---

## Phase 7 — LOD & Latency

- [ ] **7.1 Series downsampling (LTTB + M4/min-max).** Client-side reduction with
      `downsample: { mode, target, strategy }`. (§11.1)
      **Done when:** output point count matches target and the min/max envelope is
      preserved for known inputs.

- [ ] **7.2 Server-side downsampling via engine op.** Expose downsampling as an engine op
      the planner can push to Deephaven. **Depends on:** 4.5, 7.1. (§11.1)
      **Done when:** a large-series fixture routes to server-side reduction and only the
      reduced data crosses the adapter boundary.

- [ ] **7.3 Density reduction (bin2d/hexbin).** Degrade point-heavy marks to density,
      sharing the `stat: bin2d` path. (§11.1)
      **Done when:** a scatter above budget renders as a density heatmap and bin counts
      match a reference.

- [ ] **7.4 Aggregated tile pyramid.** Data-space power-of-two density tiles; pan = tile
      reuse; zoom = adjacent level; re-bin only below the finest level and only the
      visible window. **Depends on:** 7.3. (§11.1)
      **Done when:** pan re-bins nothing and per-frame cost is O(visible tiles),
      instrumented; bin counts match a reference.

- [ ] **7.5 Semantic-zoom tiers.** Tiers (points → hexbin → heatmap) as marks whose
      visibility binds to a zoom signal. (§11.1)
      **Done when:** zooming switches tiers at configured thresholds with no re-fetch when
      data is already resident.

- [ ] **7.6 Out-of-core tiling.** Chunked columns paged by viewport with overview tiles;
      screen-bounded resident memory. **Depends on:** 7.4. (§11.1)
      **Done when:** a larger-than-RAM fixture renders interactively under a memory
      ceiling asserted in the benchmark.

- [ ] **7.7 Latency budgets.** Uniform-only pan/zoom; stale-while-revalidate tier swaps;
      progressive refinement; async hover. (§11.2)
      **Done when:** pan/zoom never blocks on recompute (same-frame uniform update) and a
      coarse density appears within one frame of a large re-bin.

---

## Phase 8 — Interactions, Events, Filtering & Linked Views

- [ ] **8.1 Filter Tier A — indexed range predicates.** Zone-map tile pruning + boundary
      re-bin. **Depends on:** 5.6. (§10)
      **Done when:** a range filter recomputes only boundary tiles (verified) and matches
      a full-recompute reference.

- [ ] **8.2 Filter Tier B — visible-window re-bin.** Arbitrary predicates re-binned over
      the visible window server-side under SWR. **Depends on:** 7.7. (§10)
      **Done when:** an arbitrary-predicate filter updates the visible view correctly with
      no full-dataset scan.

- [ ] **8.3 Selection bitmask.** Per-row 1-bit selection driving styled
      selected/unselected rendering at every tier; aggregated tiers carry a
      selected-count channel. (§10)
      **Done when:** a selection dims unselected direct marks and lights up selected
      density on an aggregated tier.

- [ ] **8.4 Filter Tier C — summed-area index (linked brushing).** Cumulative-sum index on
      the active dimension; brushes as cumsum differences across passive views; engine
      pushdown. **Depends on:** 8.3. (§10)
      **Done when:** a brush updates ≥5 linked views at interactive rates on a large
      fixture, index size scales with bins not rows, and pushdown filters the engine.

- [ ] **8.5 Signals.** Worker-evaluated reactive values (zoom, selection, hovered key)
      bound by marks and LOD tiers. (§10)
      **Done when:** changing a signal updates dependent marks/tiers without a full
      re-resolve.

- [ ] **8.6 Filter-tier logging.** Record which tier served each filter application. (§10)
      **Done when:** structured provenance/telemetry records tier, plan generation,
      approximation/error, rows/bytes scanned, and latency; no silent full rescans
      occur in tests.

- [ ] **8.7 Event envelope & input dispatch.** Marshal pointer, wheel/pinch,
      keyboard, drag, and legend input into the backend-independent event envelope
      and dispatch through picking/hit-testing. (§4.10)
      **Done when:** synthetic events resolve the correct node and carry exact
      generation, coordinate, modifier, and source/key or aggregate-bin identity
      without copying arbitrary row payloads.

- [ ] **8.8 Hover, tooltip & crosshair.** Resolve hover through GPU picking and
      render templated tooltip/crosshair chrome. **Depends on:** 6.6, 8.7. (§4.10)
      **Done when:** hovering direct, decimated, and aggregated fixtures produces the
      documented exact-row/bin payload and correct tooltip/crosshair position.

- [ ] **8.9 Pan/zoom with history.** Emit scale-domain model patches for pan,
      wheel/pinch zoom, and reset, with undo/redo history. (§4.10, §11.2)
      **Done when:** a gesture sequence remains uniform-only on the immediate frame
      and history restores each prior domain exactly while stale rebuilds are dropped.

- [ ] **8.10 Box/lasso selection & legend filtering.** Update named selections from
      gestures and legend activation using stable keys/predicates. (§4.10, §10)
      **Done when:** selection survives add/remove/shift patches, updates every linked
      dependent, and uses chunked resident bitmasks plus engine-owned global state.

- [ ] **8.11 Server callbacks & preventable defaults.** Invoke allowlisted callback
      registry entries with the event envelope; accept returned model patches and
      bounded prevent-default decisions. (§4.10)
      **Done when:** click/select/legend callbacks receive the correct payload;
      returning `False` suppresses the documented default, while timeout/error applies
      it and emits a diagnostic.

- [ ] **8.12 Streaming enter/update/exit.** Animate only keyed changed primitives
      across data patches using `key`/`order`. (§4.10, §9)
      **Done when:** a ticking fixture preserves old-to-new identity through shifts
      and animates only entered, modified, or removed primitives.

- [ ] **8.13 Filter expression IR.** Implement the typed, canonically serialized AST
      for literals/parameters, fields, boolean/comparison/null/set/range/string ops,
      and registered pure functions with engine-independent type/null semantics. (§10)
      **Done when:** adapters compile supported ASTs equivalently; unsupported nodes
      fail before execution; depth/node/literal/work limits reject adversarial inputs;
      equivalent canonical predicates produce the same `filter_hash`.

---

## Phase 9 — Robustness

- [ ] **9.1 Validity bitmaps & null-as-gap.** Arrow validity bitmaps end to end; NaN out of
      vertex buffers; lines segment at nulls; aggregations expose `count_valid` vs
      `count`. (§12)
      **Done when:** a series with interior nulls renders gaps (no invented segments), no
      NaN reaches a buffer, and null-handling matches a reference.

- [ ] **9.2 CPU reference rasterizer + perceptual-diff CI.** Deterministic software
      rasterizer as oracle; perceptual-diff every backend; assert reduced buffers
      bit-identical across backends. **Depends on:** 6.8. (§12)
      **Done when:** WebGPU and WebGL2 pass perceptual diff vs the CPU reference and
      aggregate/decimated buffers are asserted bit-identical.

- [ ] **9.3 LOD-decision contract tests.** Given `(data, viewport)`, assert the chosen tier
      and reduced output are deterministic. (§12)
      **Done when:** tier/output are asserted for a fixture set and a change bisects to
      layout, LOD, or raster.

- [ ] **9.4 Context governor.** Keep a page under a live-context budget with LRU eviction
      of off-screen charts and rebuild-on-scroll. (§12)
      **Done when:** a large dashboard keeps visible charts live and no chart permanently
      blanks.

- [ ] **9.5 Performance benchmark gates.** Record reference hardware/browser,
      viewport, datasets, budgets, and statistically robust baselines for frame time,
      update latency, resident/in-flight bytes, and transfer volume. (§2.1, §12)
      **Done when:** 100M-row static and ticking referenced sources keep steady-state
      pan/zoom below 16.7 ms per frame at 60 Hz with pixel-bounded materialization,
      and configured regressions fail CI.

- [ ] **9.6 Protocol fuzz & ticking soak.** Fuzz VIR/migrations, model/data patches,
      Arrow metadata, planner placement, reconnect/resync, and device recovery; soak
      long ticking/filter sessions. (§12)
      **Done when:** runs remain within memory/queue budgets, report no unhandled
      errors, and their final render/state equals a from-scratch rebuild.

- [ ] **9.7 Resource and trust-boundary enforcement.** Apply depth/count/byte/work
      limits, bounded filter expressions, callback allowlists, plugin host policy, and
      tenant-aware cache identities. (§12)
      **Done when:** adversarial fixtures fail with bounded, node-addressed diagnostics,
      do not allocate/execute past limits, and cannot reuse another source/tenant's
      cache entries.

- [ ] **9.8 Runtime observability.** Emit queue/stale-work, transfer/resident bytes,
      cache/tile hit rate, stage/update/frame latency, selected LOD/filter tier, and
      recovery metrics. (§12)
      **Done when:** a scripted interaction/ticking scenario accounts for all work and
      bytes and attributes every budget violation to a stage and document node.

---

## Phase 10 — Styling & Theming

- [ ] **10.1 CSS-native chrome.** Axes/labels/legend/tooltips/container as DOM/SVG
      styleable by plain CSS/Tailwind with full cascade. (§13a)
      **Done when:** a utility-class stylesheet restyles all chrome and a
      `@media (prefers-color-scheme)` rule flips chrome theming.

- [ ] **10.2 `--chart-*` token bridge.** Read documented tokens at mount, map to GPU
      uniforms/LUTs; normalize export-unsafe colors at the boundary. (§13b)
      **Done when:** setting `--chart-*` (incl. via a cascading parent) restyles marks,
      and `oklch()`/`color-mix()` resolve to fixed channels.

- [ ] **10.3 Live re-resolution (0-byte theme change).** Watch `matchMedia` +
      `MutationObserver`; apply theme changes as uniform/LUT updates, not data
      re-uploads. **Depends on:** 10.2, 5.4. (§13b)
      **Done when:** a dark-mode toggle repaints via uniforms only (0 wire bytes) at frame
      rate on a large chart.

- [ ] **10.4 Programmatic parity + export snapshot.** Frontend API sets everything tokens
      set; client snapshots resolved tokens to the server for export parity; bind to
      web-client-ui theme vars. (§13)
      **Done when:** a chart inherits the app theme with no per-chart config and a
      kernel-side export matches the on-screen CSS theme.

---

## Phase 11 — Extensibility & Frontends

- [ ] **11.1 Extension-point registries.** Stable interfaces for geoms, stats, scales,
      coords, engine adapters, renderer backends. (§14)
      **Done when:** one namespaced/versioned extension of each kind declares schema,
      capabilities, typed batch interface, delta/determinism/resource semantics, and
      lowering support; host policy controls code/callback/resource permissions;
      registration needs no core edits and fails early on collisions or unsupported
      lowering.

- [ ] **11.2 Low-level Python frontend.** Typed builders map one-to-one to every VIR
      node, binding, interaction, and immutable transform. (§14)
      **Done when:** Python-built documents normalize byte-for-byte to hand-authored
      fixtures and expose no behavior absent from the VIR.

- [ ] **11.3 High-level Python frontend.** Arity-sugar and pinned-geom constructors,
      `plot_by`, themes-as-classes, validation, serialization, and immutable editing.
      **Depends on:** 11.2. (§14)
      **Done when:** ergonomic calls lower to the same normalized VIR as equivalent
      low-level builders; `plot_by`, business-calendar binding, and theme changes
      match expected marks/provenance.

- [ ] **11.4 Second frontend conformance.** A second frontend (e.g. TypeScript) emits
      VIR for a shared visualization corpus. **Depends on:** 11.2. (§14)
      **Done when:** both frontends produce structurally identical documents after
      canonical normalization.

- [ ] **11.5 Library-as-backend adapter.** Lower the scene graph onto an existing library
      (Plotly/Vega/ECharts/deck.gl). **Depends on:** 6.1. (§7, §14)
      **Done when:** a representative document renders through the adapter and preserves
      semantics (a scatter stays a scatter in the target's model).

- [ ] **11.6 Second engine adapter.** Implement a non-Deephaven columnar adapter to
      prove source/op/planner portability. (§6, §8, §14)
      **Done when:** an existing static and ticking corpus renders unchanged, capability
      gaps fail early, and supported ops are placed according to the same cost model.

- [ ] **11.7 Plugin domain bundle (proof).** One domain plugin (e.g. financial or
      graph/network) registering geoms/coords/marks with no core changes. (§14)
      **Done when:** the plugin adds a working visualization type via public extension
      points only and passes serialization, delta, determinism, resource, and supported-
      backend conformance.

---

## Phase 12 — Reduction / Export Kernel

- [ ] **12.1 Reduction kernel (native).** Shared primitives: M4/min-max decimation, bin2d,
      tile-pyramid build, summed-area index, behind the engine-adapter interface. (§15)
      **Done when:** kernel outputs are bit-identical to the Deephaven-side reductions for
      a fixture set.

- [ ] **12.2 WASM build.** Same kernel compiled to WASM for the browser fallback tier and
      static HTML export. **Depends on:** 12.1. (§15)
      **Done when:** a chart reduces client-side (no engine round-trip) and matches the
      native kernel output.

- [ ] **12.3 Deterministic headless export.** Kernel drives the CPU reference rasterizer
      for SVG/PNG/PDF and self-contained static HTML export. **Depends on:** 12.1,
      9.2. (§15, §16)
      **Done when:** exporting a fixture twice yields byte-identical output and the emitted
      VIR re-renders to the same chart; HTML bundles only the declared bounded data
      snapshot/cache manifest needed offline and performs no hidden source fetch.

---

## Phase 13 — Tooling

- [ ] **13.1 Schema-driven editor.** Generate the editor UI from the JSON Schema; edits
      emit model patches. (§16)
      **Done when:** editing a value produces the correct model patch and the UI covers all
      schema-declared fields for a sample document.

- [ ] **13.2 Provenance / "explain" view.** Surface the compiled, annotated document as a
      human/agent-readable explanation. **Depends on:** 3.3. (§16)
      **Done when:** the view for the §17 example lists each resolved trait with its origin,
      matching the compiled snapshot.

- [ ] **13.3 Accessibility layer.** Keyboard nav, ARIA roles, and generated text
      descriptions derived from the document and zone-map summaries. (§16)
      **Done when:** an automated a11y audit passes for a sample chart and keyboard
      navigation reaches every interactive element.

- [ ] **13.4 Visual-regression harness.** Wire deterministic export into CI as a gate.
      **Depends on:** 12.3, 0.4. (§16)
      **Done when:** an intentional visual change fails the gate and an approved baseline
      update clears it.

---

## Phase 14 — Conformance

- [ ] **14.1 Reference example round-trip.** The §17 example document validates, resolves,
      renders, and its compiled provenance matches the documented excerpt. (§17)
      **Done when:** the example passes validate → resolve → render and its compiled
      snapshot (incl. provenance) matches.

- [ ] **14.2 Supported-plots matrix.** Implement every row of the matrix as
      geom(s) + coord + scales + stat + downsample with no special-case path. (§18)
      **Done when:** each matrix row has a fixture that renders correctly through the
      generic pipeline (no per-plot code branch).

- [ ] **14.3 Complete built-in conformance corpus.** Cover every declared geom, stat,
      scale, coord, interaction, dtype, and extension interface beyond the minimum
      plot matrix. (§18)
      **Done when:** each has positive, invalid-input, delta, serialization,
      determinism, and supported-backend fixtures; every named built-in is either
      implemented and passing or explicitly removed from the design before v1.

---

## Cross-cutting definition of done

Every task above must also satisfy:

- [ ] Automated tests (unit/integration/golden/benchmark as appropriate) run in CI.
- [ ] Public surfaces are typed and documented; JSON Schema updated if the IR changed.
- [ ] No main-thread per-row work introduced (resolve/plan/diff stay in the worker).
- [ ] Delta path preserved: the feature updates incrementally, never via full re-render.
- [ ] The change satisfies the performant / composable / extensible gate (design §2).
- [ ] Specifications stay immutable: any transformation is a pure `VIR → VIR` function.
