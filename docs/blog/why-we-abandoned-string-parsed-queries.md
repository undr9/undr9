# Why We Abandoned String-Parsed Queries (SQL/Cypher) for a Typed JSON Variant Graph Engine in Rust

UNDR9 is a graph-native memory database for AI agents. That phrasing matters, because agent workloads are not the same as human-operated BI dashboards or ad hoc analyst consoles.

In a traditional database stack, the cost of parsing a string query is often acceptable because a human typed the query once, the query runs for milliseconds or seconds, and the parser is nowhere near the dominant cost. In an agent loop, the shape is different:

- the caller may issue many small retrievals instead of a few large ones
- requests are often generated programmatically instead of authored by humans
- latency variance matters because the loop is interactive
- the same query template may be issued thousands of times with only the parameters changing

That pushed us away from a SQL- or Cypher-like string frontend and toward a typed JSON variant engine built around Rust enums, serde, and fixed execution plans.

## The Short Version

We removed an entire class of frontend work from the hot path:

- no SQL lexer
- no Cypher lexer
- no grammar parser
- no AST normalization layer
- no operator-tree optimizer
- no string interpolation bugs in clients

Instead, the server accepts a typed `QueryRequest` enum that deserializes directly from JSON and then enters a small, explicit planner implemented as a `match` over known variants in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L19-L130) and [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L504-L759).

That design was not chosen because JSON is fashionable. It was chosen because AI agents do not need a human query language. They need a transport-safe, type-safe, machine-generated request format with predictable execution behavior.

## The Exact CPU Cycle Cost We Eliminated

Here is the most important engineering statement in this post:

> Inside UNDR9, the exact CPU cycle cost of lexing, parsing, and planning SQL/Cypher strings is **zero**.

That cost is exactly zero because UNDR9 does not ship a SQL parser, a Cypher parser, a grammar-driven planner, or a textual query optimizer in the request path. The relevant server entry point accepts `Json<QueryRequest>` directly in the API layer, not `String`, in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/api/src/lib.rs#L1715-L1778).

The planner then performs a direct enum dispatch in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L504-L759):

- `GetNodeById` maps to `PlanKind::ExactLookup`
- `FilterNodes` maps to `PlanKind::FilterScan`
- `Traverse` maps to `PlanKind::Traversal`
- `VectorSearch` maps to `PlanKind::VectorSimilarity`
- `RankedRetrieval` maps to `PlanKind::RankedHybrid`

There is no intermediate text AST. There is no token stream to walk. There is no second-stage optimizer trying to infer what the caller meant.

That does **not** mean requests are free. If you call the HTTP API, the server still pays for JSON tokenization and deserialization. But the cost profile is meaningfully different:

- one transport decode step into a strongly typed enum
- one fixed validation pass over known fields
- one direct plan classification step

Instead of:

- parse transport bytes
- lex SQL/Cypher text
- parse grammar into an AST
- normalize aliases, names, and expressions
- bind the AST to the graph schema
- lower the AST into an execution plan
- validate semantic correctness after parse time

If someone asks for the exact lexing or planning cycle count of SQL/Cypher inside UNDR9, the honest answer is simple: there is no such stage to measure.

## Why We Refuse To Invent Fake Parser Numbers

It would be easy to write a dramatic sentence like "string parsing costs 20,000 cycles per query" and leave it at that. We are not doing that here, because it would be sloppy engineering.

The cycle cost of string parsing depends on:

- the specific SQL or Cypher grammar
- the query length
- the parser implementation
- how much semantic binding is done after parse time
- allocator behavior
- branch predictor state
- the exact CPU model and clock behavior

Those numbers are real only when they are tied to a specific benchmark harness. This post is intentionally narrower and more defensible:

- the removed SQL/Cypher pipeline inside UNDR9 costs exactly `0` cycles because it does not exist
- the replacement path is visible in code and bounded by enum deserialization plus fixed validation

That is the engineering claim we can stand behind.

## The Request Model We Use Instead

The core request type is `QueryRequest` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L19-L91):

```rust
pub enum QueryRequest {
    GetNodeById { node_id: NodeId },
    GetNodeByUniqueKey { unique_key: String },
    FilterNodes {
        #[serde(default)]
        label: Option<String>,
        #[serde(rename = "where")]
        filter: FilterExpression,
        limit: Option<usize>,
    },
    ListNeighbors { ... },
    Traverse { ... },
    ShortestPath { ... },
    SearchByLabel { ... },
    TimeRange { ... },
    VectorSearch { ... },
    RankedRetrieval { ... },
}
```

This buys us several things immediately:

- the request shape is explicit
- the set of supported operations is finite and inspectable
- each variant has a known execution path
- serde controls the wire format without requiring a string DSL

For example:

- `FilterExpression` uses an internally tagged representation with `#[serde(tag = "op", rename_all = "snake_case")]` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L101-L111)
- `PropertyValue` uses `kind` plus `value` tags in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/core/src/lib.rs#L6-L15)
- optional fields like `vector_name` and `label` rely on `#[serde(default)]` instead of parser-side omission rules

The result is a wire protocol that is both JSON-friendly and strongly modeled.

## Why This Matters In Agent Loops

Agent loops do not usually "author queries." They synthesize requests.

A string language pushes the caller toward:

- assembling templates
- escaping user values
- learning a textual DSL
- validating requests after a parse step
- handling syntax errors that are orthogonal to the actual retrieval intent

A typed variant model pushes the caller toward:

- constructing a request object
- serializing it
- sending it

That is exactly what a machine client should do.

For example, a Rust client can construct a ranked retrieval request as a value, not as a string:

```rust
use undr9_query::QueryRequest;

let request = QueryRequest::RankedRetrieval {
    query_vector: Some(vec![0.11, 0.42, 0.77]),
    reference_node_id: None,
    edge_type: None,
    from_epoch_ms: None,
    to_epoch_ms: None,
    vector_name: Some("default".to_owned()),
    limit: 10,
    top_k: Some(100),
    now_epoch_ms: 1_782_300_000_000,
    retrieval_profile: Some("v1-default".to_owned()),
};
```

That object can be serialized directly. No query builder needs to render text. No client needs to escape quotes or commas. No server parser needs to recover structure from a human language.

## How Serde-Friendly Enums Enable Compile-Time Validation

This point needs precision.

The enum design does **not** provide compile-time validation for every possible error. It does not statically prove that:

- `limit <= 1000`
- `timeout_ms <= 30000`
- `from_epoch_ms <= to_epoch_ms`
- a vector has the desired dimensionality

Those checks still happen at runtime in the planner, in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L504-L759).

What the enum design **does** provide is compile-time validation of the query **shape** in statically typed clients:

- the variant name must exist
- the field names must exist
- the field types must match
- exhaustive matches remain possible on the client side

That is a huge improvement over string parsing.

With string SQL or Cypher, all of these are deferred:

- a typo in an operation name
- a missing field
- a wrong field type encoded as text
- a syntactically malformed predicate

With typed variants, many of those errors become ordinary compiler errors in Rust clients and schema-level errors in generated clients for other languages.

The serde annotations also keep the wire format stable without leaking Rust-specific syntax:

- `filter` becomes `where` on the wire
- `FilterExpression` carries its operator in a clear `op` tag
- `PropertyValue` retains type information across the network

In other words, the server stays machine-friendly without forcing clients to parse or emit a custom grammar.

## Planning Is A Direct Classification Step, Not A Text Compiler

The planner in UNDR9 is intentionally small.

`Planner::plan` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L504-L759) does two jobs:

- validates request invariants
- assigns a fixed `PlanKind`

That is a deliberate design choice.

We did **not** want:

- AST rewriting passes
- join-order search
- alias binding
- expression canonicalization
- cost-based optimizer instability in a memory retrieval engine

Instead, each request variant already implies its access path:

- `GetNodeById` means direct id lookup
- `SearchByLabel` means label index scan
- `TimeRange` means timestamp-bucket query or explicit scan fallback
- `VectorSearch` means semantic candidate generation plus similarity scoring
- `RankedRetrieval` means hybrid reranking over structural, semantic, temporal, importance, and confidence signals

That directness is visible in `Executor::execute_iter` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L762-L998).

This is closer to calling a typed API than compiling a mini language.

## The Ranked Retrieval Formula Is Explicit

`RankedRetrieval` is not just a vector search. It combines multiple retrieval signals in a way that is intentionally closer to memory recall than to a flat nearest-neighbor query.

The scoring weights live in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/memory/src/lib.rs#L16-L29) and [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/memory/src/lib.rs#L52-L69):

- structural: `0.30`
- semantic: `0.30`
- temporal: `0.15`
- importance: `0.15`
- confidence: `0.10`

The score assembly happens in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/query/src/lib.rs#L1574-L1624):

```text
final_score =
    0.30 * structural
  + 0.30 * semantic
  + 0.15 * temporal
  + 0.15 * importance
  + 0.10 * confidence
```

Because the query surface is typed, the engine does not need to infer whether the caller wants graph traversal, pure vector search, or a hybrid rerank. The request variant already encodes the intent.

That matters for predictability. In agent systems, deterministic query semantics are usually more valuable than clever parser magic.

## The Rust Memory Layout Decisions That Matter

The phrase "memory packing" needs the same honesty as the parser section.

UNDR9 is not a hand-written arena of bit-packed structs from top to bottom. The engine uses straightforward Rust collections in memory and separates concerns carefully:

- typed graph records in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/core/src/lib.rs#L17-L33)
- explicit indexes in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/index/src/lib.rs#L44-L58)
- archived snapshots in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/storage/src/lib.rs#L67-L149)

The main memory-sensitive choices were these.

### 1. We Kept The In-Memory Indexes Compact And Purpose-Built

`GraphIndex` stores:

- `adjacency_index: BTreeMap<NodeId, Vec<EdgeId>>`
- `reverse_adjacency_index: BTreeMap<NodeId, Vec<EdgeId>>`
- `label_type_index: BTreeMap<String, Vec<NodeId>>`
- `temporal_index: BTreeMap<i64, Vec<NodeId>>`
- `vector_index` state

See [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/index/src/lib.rs#L44-L58).

That layout matters because it avoids materializing heavyweight interpreted plan state. Once a request is classified, execution runs against pre-built index buckets instead of a generic AST execution engine.

### 2. We Split Vectors Out Of The Main Node Snapshot

`StoredNodeRecord` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/storage/src/lib.rs#L89-L93) contains:

- `id`
- `node_type`
- `properties`

Vectors are stored separately in `NodeVectorRecord` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/storage/src/lib.rs#L134-L137):

- `node_id`
- `vectors: BTreeMap<String, Vec<f32>>`

This was intentional. Embeddings are materially larger than ordinary scalar properties. Separating them keeps the snapshot model cleaner and lets us reason about vector persistence independently of scalar graph state.

### 3. We Used Archived Snapshots Instead Of Re-Interpreting Rich Text Formats

Node, edge, and vector snapshots derive `RkyvArchive`, `RkyvSerialize`, and `RkyvDeserialize` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/storage/src/lib.rs#L67-L149), and snapshot bytes are emitted through `rkyv::to_bytes::<_, 256>` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/storage/src/lib.rs#L2476-L2489).

That gives us a compact binary snapshot path rather than repeatedly rehydrating the entire store from verbose text formats.

### 4. We Preserved Typed Values Instead Of Storing Everything As Strings

`PropertyValue` in [lib.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/core/src/lib.rs#L6-L15) preserves:

- `String`
- `Integer`
- `Float`
- `Boolean`
- `StringList`
- `FloatList`

That means the executor can evaluate filter operators and ranking metadata without reparsing property text back into numbers.

### 5. We Measure Peak RSS Explicitly

The benchmark tool records `peak_rss_bytes` as part of each scale report in [undr9-bench.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/cli/src/bin/undr9-bench.rs#L62-L71), populated by `getrusage()` in [undr9-bench.rs](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/crates/cli/src/bin/undr9-bench.rs#L1130-L1146).

On the published Apple Silicon benchmark artifacts, that yielded:

| Scale | Nodes | Edges | `peak_rss_bytes` |
| :--- | :--- | :--- | :--- |
| `100k` | `100,000` | `99,999` | `1,108,377,600` |
| `1M` | `1,000,000` | `999,999` | `2,699,001,856` |
| `10M` | `10,000,000` | `9,999,999` | `7,545,044,992` |

Source artifacts:

- [single-node-benchmark-100k.json](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/docs/operations/single-node-benchmark-100k.json#L17-L20)
- [single-node-benchmark-1m-storage.json](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/docs/operations/single-node-benchmark-1m-storage.json#L11-L14)
- [single-node-benchmark-10m-storage.json](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9/docs/operations/single-node-benchmark-10m-storage.json#L11-L14)

Those numbers are not a generic "Rust memory usage" claim. They are the measured footprint of this engine under this benchmark shape.

## Why This Layout Helps Peak RSS

The main win is not one magical packed struct. It is the combination of several boring but important design decisions:

- no text-query compiler state in the request path
- no parser AST allocations for every query
- direct execution against stable index buckets
- typed scalar values without reparsing
- vectors separated from core scalar snapshots
- binary archived snapshots for durable state

This is exactly the kind of systems work that reduces memory pressure over time: remove intermediate representations, keep indexes purpose-built, and avoid work that the machine client never needed in the first place.

## What We Gave Up

Abandoning string-parsed queries was not free.

We gave up some things that SQL and Cypher are genuinely good at:

- ad hoc human exploration
- copy-paste query ergonomics
- a familiar textual surface for developers coming from relational or graph BI tools
- generic parser ecosystem reuse

We made that trade on purpose.

UNDR9 is not trying to be a general-purpose analyst console first. It is trying to be a predictable memory engine for agent systems.

That changes what "good query ergonomics" means.

For us, good ergonomics are:

- a small set of explicit operations
- machine-friendly serialization
- schema-shaped requests
- predictable plan mapping
- easy client generation

## The Engineering Lesson

If your dominant caller is a person, a string query language may be the right abstraction.

If your dominant caller is an AI agent or a typed service, a string query language can become an unnecessary tax:

- extra CPU work
- extra allocations
- extra failure modes
- extra surface area to secure
- extra ambiguity when the caller already knows the intended operation

UNDR9 removed that tax.

We did not replace SQL or Cypher with "JSON because JSON." We replaced a human query compiler with a typed request model that better matches how agents actually work.

The key outcomes are simple:

- the exact SQL/Cypher lexing and planning cost inside UNDR9 is `0`
- the request surface is statically representable in Rust and code-generatable elsewhere
- the planner is a direct classifier, not a language compiler
- the storage and index layout keep `peak_rss_bytes` grounded in explicit, measurable structures

That is the entire philosophy in one sentence:

> When the caller is a machine, we would rather accept a typed request than pretend we still need a human query language.
