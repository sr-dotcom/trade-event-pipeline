// Source of docs/images/architecture.png.
// Build: typst compile docs/images/architecture.typ docs/images/architecture.png --ppi 160
#import "@preview/fletcher:0.5.8" as fletcher: diagram, node, edge
#let ink = rgb("#1f2328")
#let muted = rgb("#57606a")
#let built = rgb("#2f6fdb")
#let unverified = rgb("#d97706")
#set page(width: 1000pt, height: auto, margin: 14pt, fill: white)
#set text(fill: ink, size: 12pt)
#let ok(p, t) = node(p, fill: built.transparentize(82%), stroke: built, t)
#let todo(p, t) = node(p, fill: unverified.transparentize(82%), stroke: (paint: unverified, dash: "dashed"), t)
#let note(t) = text(size: 10pt, fill: muted, t)

#diagram(
  node-shape: rect, edge-stroke: ink, spacing: (30pt, 40pt), crossing-fill: white,

  ok((0, 1), [*Generator*\ fake events\ seed and profile]),
  ok((0, 3), [*Known answer*\ files written by\ the generator]),
  ok((1, 0), [*events.ndjson*\ raw event file]),
  todo((1, 1), [*Event Hubs*\ Kafka endpoint]),
  todo((1, 2), [*Capture archive*\ Avro files\ in ADLS]),
  ok((2, 1), [*Bronze*\ raw lines\ plus `ingest_time`]),
  ok((3, 0), [*Quarantine*\ bad records\ with a reason]),
  ok((3, 1), [*Silver*\ valid events\ `is_late` flag]),
  ok((3, 2), [*Duplicates*\ repeated\ deliveries]),
  ok((4, 1), [*Current state*\ one row per trade\ highest version wins]),
  ok((5, 1), [*Gold report*\ per book and currency\ numbered versions]),
  ok((4, 3), [*Compare script*\ known answer\ against tables]),

  edge((0, 1), (1, 0), "->", label: note[writes], label-side: left),
  edge((1, 0), (2, 1), "->", label: note[local run\ and rebuild], label-side: right),
  edge((0, 1), (1, 1), "-->", label: note[sender], label-side: right),
  edge((1, 1), (2, 1), "-->"),
  edge((1, 1), (1, 2), "-->", label: note[Capture], label-side: left),
  edge((1, 2), (2, 1), "-->", label: note[rebuild], label-side: right),
  edge((2, 1), (3, 1), "->"),
  edge((2, 1), (3, 0), "->", bend: -15deg),
  edge((2, 1), (3, 2), "->", bend: 15deg),
  edge((3, 1), (4, 1), "->"),
  edge((4, 1), (5, 1), "->"),
  edge((0, 1), (0, 3), "->", label: note[writes], label-side: left),
  edge((0, 3), (4, 3), "-->", label: note[reads the files, not the pipeline's own counts], label-side: left),
  edge((4, 1), (4, 3), "->", label: note[reads five tables,\ not Gold], label-side: left),
)

#v(6pt)
#grid(columns: 2, gutter: 18pt,
  box(fill: built.transparentize(82%), stroke: built, inset: 4pt)[#note[Built and tested on a laptop, with fake data]],
  box(fill: unverified.transparentize(82%), stroke: (paint: unverified, dash: "dashed"), inset: 4pt)[#note[Built, tested only with fake or local stand-ins, never run on Azure]],
)
