# Spark Interview Study Plan

**Goal:** Deep understanding of Spark internals for senior/lead data engineer technical interviews.
**Environment:** Local PySpark → GCP Dataproc (Phase 2).
**Approach:** Every notebook builds a concept, maps it to the Spark UI, and closes with the interview questions that concept unlocks.

---

## How to use this plan

1. Work through notebooks in order — each tier assumes the previous one.
2. After every code cell that runs an action, open the Spark UI and trace the execution using the checklist at the bottom of each section.
3. Before moving to the next notebook, answer the interview questions from memory. If you can't, re-read the relevant cells.
4. Use `explain(mode='formatted')` as your primary debugging tool throughout.

---

## Roadmap overview

| # | Notebook | Tier | Status | Core interview question |
|---|---|---|---|---|
| 01 | Lazy evaluation & the DAG | 1 — Execution mechanics | ✅ done | "What is the difference between a transformation and an action?" |
| 02 | Jobs → Stages → Tasks → Physical plan | 1 — Execution mechanics | ✅ done | "Walk me through how a `groupBy` executes end-to-end." |
| 03 | Partitioning & skew | 1 — Execution mechanics | ✅ done | "A stage has one task running 10× longer. What do you do?" |
| 04 | Join strategies | 2 — Optimization mechanics | ✅ done | "A join that used to take 3 min now takes 20 min. Where do you start?" |
| 05 | Catalyst optimizer | 2 — Optimization mechanics | ✅ done | "What is predicate pushdown and why does it matter?" |
| 06 | AQE deep dive | 2 — Optimization mechanics | 🔄 in progress | "What are AQE's three features and when does each fire?" |
| 07 | Memory model & spill | 3 — Resource management | 📋 planned | "A task shows high GC time and disk spill. What is the root cause?" |
| 08 | Caching & persistence | 3 — Resource management | 📋 planned | "When should you cache? What are the trade-offs between storage levels?" |
| 09 | Data formats & storage-layer I/O | 4 — Storage & I/O | 📋 planned | "Why does reading a filtered Parquet file outperform filtered CSV by 10×?" |
| 10 | Pipeline design patterns | 5 — Production engineering | 📋 planned | "How do you make a Spark job idempotent?" |
| 11 | GCP / Dataproc | 5 — Production engineering | 📋 planned | "How does the BigQuery Spark connector read data?" |
| 12 | Structured Streaming | 6 — Advanced / optional | 📋 planned | "How does Spark handle late-arriving data in a streaming job?" |

---

## Sequencing rationale

**Joins (04) before Catalyst (05):** Joins are the most concrete interview-visible operation. Once you have seen `SortMergeJoin` and `BroadcastHashJoin` in `explain()` output, Catalyst's join selection rules become tangible rather than abstract. Learn the operations first, then learn the optimizer that chooses between them.

**Catalyst (05) before AQE (06):** AQE is Catalyst running at runtime, after each shuffle stage, with access to actual data statistics. You need to understand static Catalyst (rule-based plan rewriting, cost-based optimization) before the adaptive layer makes sense.

**Memory model (07) before caching (08):** Caching draws from the storage memory pool inside executor JVM memory. Without knowing how the execution/storage split works, questions like "why did my cache get evicted mid-job?" and "which storage level should I use?" have no principled answer.

**Data formats (09) after memory:** Predicate pushdown inside Parquet, Delta Lake, or the BigQuery connector happens at the file-reader level — before Spark even constructs a partition. This is a different layer than Catalyst pushdown and builds on the understanding of I/O vs compute costs established in the resource management tier.

---

## Tier 1 — Execution mechanics
> The question being answered: "Can you explain what Spark is doing internally when it runs a query?"

---

### Notebook 01 — Lazy evaluation & the Spark DAG
**File:** `notebooks/01_lazy_evaluation.ipynb`
**Status:** ✅ done

**Concepts covered:**
- Transformations build a logical plan (DAG); actions trigger execution
- Narrow vs wide transformations
- `Exchange` nodes as stage boundaries
- `explain()` modes: simple, extended, formatted, codegen
- Caching: `.cache()` is lazy, first action materializes, subsequent actions reuse
- Schema inference on `read.json/csv` fires a hidden sampling job
- AQE basics: skipped stages, coalesced shuffle partitions

**Spark UI checklist for this notebook:**
- Jobs tab: confirm `show()` and `count()` each produce a separate job
- SQL tab: count `Exchange` nodes in the formatted plan — each one is a shuffle
- SQL tab: compare `isFinalPlan=false` (pre-execution) to `isFinalPlan=true` (post-execution) for AQE changes
- Stages tab: verify narrow-only pipeline produces 1 stage; wide pipeline produces 2+

**Interview questions this notebook unlocks:**
1. What is lazy evaluation in Spark? Why is it designed that way?
2. What is the difference between a transformation and an action? Give three examples of each.
3. Why does `groupBy` create a stage boundary but `filter` does not?
4. What does `explain(mode='formatted')` show? How do you read it?
5. If you call `count()` twice on the same DataFrame without caching, what happens? How many jobs run?
6. You are reading a JSON file without an explicit schema. What hidden cost occurs and why?
7. What is AQE? What problem does it solve that the static Catalyst optimizer cannot?

---

### Notebook 02 — Jobs, Stages, Tasks & Physical Plan Mapping
**File:** `notebooks/02_jobs_stages_tasks_analysis_detailed.ipynb`
**Status:** ✅ done

**Concepts covered:**
- Logical plan → analyzed logical plan → optimized logical plan → physical plan
- One action can produce multiple Spark jobs
- Stages are bounded by shuffle writes; tasks are the parallel units within a stage
- `WholeStageCodegen`: Tungsten fuses multiple operators into one JVM bytecode loop
- `mapPartitionsInternal`: the partition-level execution wrapper for driver collection
- RDD lineage across jobs: `ParallelCollectionRDD`, `MapPartitionsRDD`, `ShuffledRowRDD`, `InMemoryTableScan`
- `AQEShuffleRead`: how AQE coalesces shuffle partitions post-execution

**Spark UI checklist for this notebook:**
- Jobs tab: confirm that `show()` on a result with `orderBy` produces 3+ jobs (hash shuffle + range shuffle + collect)
- Stages tab: match each stage to an `Exchange` node in the physical plan
- SQL tab: find `WholeStageCodegen` boundaries — operators inside a box are fused; crossing a box boundary is a virtual function call
- Task metrics: with small data, task durations are sub-millisecond; note what each column represents for when you have production-scale data

**Interview questions this notebook unlocks:**
1. Walk me through how `df.filter(...).groupBy(...).orderBy(...).show()` executes — how many jobs, stages, and shuffles?
2. What is `WholeStageCodegen`? What does Tungsten gain by fusing operators?
3. What does `AdaptiveSparkPlan isFinalPlan=false` mean in the physical plan?
4. Why does a query sometimes produce more jobs than there are actions in the code?
5. What is the difference between `ShuffledRowRDD` and `MapPartitionsRDD` in the Spark UI DAG?
6. What is a task? How many tasks run in a given stage, and what determines that number?

---

### Notebook 03 — Partitioning & Skew
**File:** `notebooks/03_partitioning_and_skew.ipynb`
**Status:** 🔄 in progress

**Concepts covered:**
- `repartition()` vs `coalesce()`: full shuffle vs narrow merge
- How `repartition(8)` distributes rows round-robin (balanced source) while `groupBy` creates key-aligned shuffle partitions (skewed)
- Identifying skew: one task processes 80% of data while others idle
- Salting: splitting hot keys across sub-partitions, two-pass aggregation, cost of the extra shuffle
- Broadcast vs Sort-Merge join: `BroadcastHashJoin` (zero shuffle on small side) vs `SortMergeJoin` (two shuffles)
- Filter position relative to `Exchange`: early filter (before shuffle) vs late filter (after shuffle)
- AQE coalescing: pre/post execution plan showing `Exchange hashpartitioning(key, 200)` → `AQEShuffleRead`

**Spark UI checklist for this notebook:**
- Task Metrics (Stage N+1 after a skewed groupBy): one task's Input Records column = 800, others = 40–100 → hot key confirmed
- Duration histogram: one bar far to the right = skew signal
- SQL tab: count `Exchange` nodes before and after salting — salting adds one Exchange (extra shuffle)
- SQL tab: compare SMJ plan (Exchange on both branches, Sort on both sides) vs BHJ plan (BroadcastExchange on small side only)
- SQL tab: verify filter position — below Exchange (pushed down) vs above Exchange (late filter)

**Interview questions this notebook unlocks:**
1. What is partition skew? How do you detect it from the Spark UI?
2. What is the difference between `repartition(n)` and `coalesce(n)`? When do you choose each?
3. Explain salting. What problem does it solve and what does it cost?
4. A `groupBy` on a column where one key represents 80% of the data — what happens at execution? What are your options?
5. What is the difference between `BroadcastHashJoin` and `SortMergeJoin`? How does Spark decide which to use?
6. Where does a filter appear in the physical plan relative to an `Exchange`, and what does that position imply about performance?

---

## Tier 2 — Optimization mechanics
> The question being answered: "Why does Spark choose this plan, and how can you change it?"

---

### Notebook 04 — Join Strategies
**File:** `notebooks/04_join_strategies.ipynb`
**Status:** ✅ done

**Concepts to cover:**
- The four join strategies: `BroadcastHashJoin`, `SortMergeJoin`, `ShuffleHashJoin`, `BroadcastNestedLoopJoin`
- How Spark chooses: `autoBroadcastJoinThreshold`, table statistics, join type
- Explicit hints: `broadcast()`, `merge()`, `shuffle_hash()`, `shuffle_replicate_nl()`
- The BHJ → SMJ regression pattern: lookup table grows past threshold silently
- Join order matters: filter-before-join vs filter-after-join
- Skewed joins: AQE skew join handling, salting for joins vs aggregations
- Cross joins and cartesian products: when they appear and the blast radius
- Bucketing: pre-sorted, pre-partitioned tables that avoid shuffle at join time

**Spark UI checklist:**
- SQL tab: find the join node (`BroadcastHashJoin` / `SortMergeJoin`), count Exchange nodes on each branch
- SQL tab: verify which side of a BHJ carries the `BroadcastExchange` node (always the smaller side)
- Stages tab: compare stage counts between SMJ (3+ stages) and BHJ (1–2 stages)
- Task Metrics: with a skewed join key, one task holds disproportionate Input Records — leads to AQE skew join rewrite if enabled

**Interview questions to answer:**
1. What are the four join strategies in Spark? When does each apply?
2. How does Spark decide whether to use a broadcast join? What are the risks of broadcasting a large table?
3. You added a new dataset to a pipeline and the runtime tripled. How do you investigate whether a join strategy changed?
4. What is bucketing in Spark? What join scenario does it optimize?
5. A join between a 10B-row fact table and a 1M-row dimension table is slow. What strategies would you consider?
6. How does AQE handle skewed join keys differently than static Catalyst?

---

### Notebook 05 — Catalyst Optimizer
**File:** `notebooks/05_catalyst_optimizer.ipynb`
**Status:** 🔄 in progress

**Concepts to cover:**
- The four plan stages: parsed → analyzed → optimized logical → physical
- Rule-based optimization (RBO): predicate pushdown, column pruning, constant folding, boolean simplification
- Cost-based optimization (CBO): table statistics, column statistics, how CBO chooses join order
- `ANALYZE TABLE` and how Spark collects statistics
- Plan stages in `explain(mode='extended')`: what changes between analyzed and optimized
- Catalyst extensibility: custom rules and strategies (conceptual)
- Tungsten: off-heap memory management, binary format, cache-aware algorithms
- `WholeStageCodegen` revisited: which operators can be fused, which cannot (hash joins break fusion)

**Spark UI checklist:**
- SQL tab: verify filter appears below aggregate in the optimized plan (predicate pushdown fired)
- SQL tab: verify only referenced columns appear in `LocalTableScan` node (column pruning fired)
- Use `explain(mode='extended')` and compare Analyzed vs Optimized logical plan to see exactly which rules transformed the plan

**Interview questions to answer:**
1. Explain the Catalyst optimizer. What are its four plan stages?
2. What is predicate pushdown? Give a concrete example of where it does and does not fire.
3. What is column pruning? Why does it matter for columnar file formats like Parquet?
4. What is the difference between rule-based and cost-based optimization in Catalyst?
5. What statistics does Spark use for CBO? How are they collected?
6. What is Tungsten? How does it differ from standard JVM memory management?
7. Which operators break `WholeStageCodegen` fusion, and why?

---

### Notebook 06 — AQE Deep Dive
**File:** `notebooks/06_aqe_deep_dive.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- AQE's three core features:
  1. **Coalescing shuffle partitions**: planned N → actual M partitions based on shuffle file sizes
  2. **Converting SMJ to BHJ at runtime**: after Stage 0 writes shuffle files, AQE can see the actual build-side size and switch from SortMergeJoin to BroadcastHashJoin
  3. **Skew join optimization**: splitting oversized shuffle partitions into sub-partitions and processing them in parallel
- AQE configuration: `spark.sql.adaptive.enabled`, `spark.sql.adaptive.coalescePartitions.*`, `spark.sql.adaptive.skewJoin.*`
- Reading AQE decisions in the post-execution plan: `AQEShuffleRead`, `isFinalPlan=true`
- When to disable AQE: deterministic plans for debugging, pipelines where plan stability matters
- Dynamic Partition Pruning (DPP): filtering fact-table partitions at runtime based on the dimension-table result

**Spark UI checklist:**
- SQL tab: `AdaptiveSparkPlan isFinalPlan=true` — find `AQEShuffleRead` and note the actual vs planned partition count
- SQL tab: AQE SMJ→BHJ conversion appears as a `BroadcastHashJoin` in the post-execution plan where the pre-execution plan showed `SortMergeJoin`
- Stages tab: skipped stages after an AQE rewrite — fewer stages than originally planned

**Interview questions to answer:**
1. What are AQE's three main features? Give one example of each in a concrete query.
2. How does AQE coalesce shuffle partitions? What triggers it and what is the result?
3. How does AQE convert a SortMergeJoin to a BroadcastHashJoin at runtime? Why can it do this when static Catalyst could not?
4. What is AQE skew join optimization? How is it different from manual salting?
5. What is Dynamic Partition Pruning? In what query pattern does it fire?
6. When would you disable AQE? What are the trade-offs?

---

## Tier 3 — Resource management
> The question being answered: "Why is this job running out of memory or spilling to disk, and what do you tune?"

---

### Notebook 07 — Memory Model & Spill
**File:** `notebooks/07_memory_model_and_spill.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Executor JVM memory layout:
  - `spark.executor.memory`: JVM heap (execution + storage pools)
  - `spark.executor.memoryOverhead`: off-heap (Python workers, netty buffers)
  - `spark.memory.fraction` (default 0.6): share of heap for Spark vs user code
  - `spark.memory.storageFraction` (default 0.5): split between execution and storage within Spark's share
- Execution memory: used by shuffles, sorts, joins, aggregations
- Storage memory: used by cached RDDs and broadcast variables
- Unified memory model (Spark 1.6+): execution can borrow from storage and evict cached data
- Spill to disk: when execution memory is exhausted mid-operation, Spark writes intermediate results to local disk → dramatically slower
- GC pressure: large objects on heap → frequent GC → task pauses → slow task metrics
- OOM patterns: executor OOM vs driver OOM, heap vs overhead
- Diagnosis: Shuffle Spill (Disk) column in Task Metrics, GC Time column, executor logs

**Spark UI checklist:**
- Task Metrics: `Shuffle Spill (Disk)` column > 0 → a task exhausted execution memory mid-operation
- Task Metrics: `GC Time` > 10% of task duration → memory pressure, objects living too long on heap
- Executor tab: check executor memory usage over time; evicted storage blocks = cache eviction under pressure

**Interview questions to answer:**
1. How is executor JVM memory divided in Spark? Describe each segment.
2. What is the difference between `spark.executor.memory` and `spark.executor.memoryOverhead`? When does OOM come from overhead rather than heap?
3. What causes disk spill in Spark? How do you detect it, and how do you fix it?
4. A task shows GC Time at 40% of its total duration. What is the likely cause, and what are your tuning options?
5. What is the unified memory model? How does execution memory borrow from storage memory?
6. You have a job that OOMs on executors intermittently. Walk me through how you diagnose it.

---

### Notebook 08 — Caching & Persistence
**File:** `notebooks/08_caching_and_persistence.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Storage levels: `MEMORY_ONLY`, `MEMORY_AND_DISK`, `MEMORY_ONLY_SER`, `DISK_ONLY`, `_2` (replicated) variants
- `cache()` = `persist(MEMORY_AND_DISK)` in PySpark (not `MEMORY_ONLY` as in Scala)
- When caching helps: a DataFrame used more than once downstream in the same application
- When caching hurts: caching a large DataFrame used only once (wastes memory, evicts other data)
- Cache invalidation: transformations on a cached DataFrame produce a new uncached DataFrame
- Unpersist: always call `.unpersist()` when done — Spark does not eagerly evict
- Broadcast variables vs cached DataFrames: broadcast is for small lookup tables used inside UDFs or joins
- Checkpoint: breaking lineage to avoid recomputing long chains (different from cache)
- Long pipeline pattern: cache at the "diamond" — where one DataFrame feeds two or more downstream paths

**Spark UI checklist:**
- SQL tab: `InMemoryTableScan` node = reading from cache (vs `LocalTableScan` = reading from source)
- Stages tab: a job that reads from cache has Stage N skipped (AQE sees cached result is available)
- Storage tab: shows cached RDDs, their storage level, fraction in memory vs on disk, size

**Interview questions to answer:**
1. What is the difference between `MEMORY_ONLY` and `MEMORY_AND_DISK`? When would you choose each?
2. You cache a DataFrame and then call `.filter()` on it. Is the filtered result also cached?
3. A long pipeline fans out from one DataFrame into three separate aggregations. Where do you cache and why?
4. What is the difference between `.cache()` and `.checkpoint()`? When is checkpointing preferable?
5. You cached a 50 GB DataFrame on a cluster with 40 GB executor memory total. What happens?
6. How does a broadcast variable differ from a cached DataFrame? When do you use each?

---

## Tier 4 — Storage & I/O
> The question being answered: "How does the storage layer interact with Spark's optimization, and what is different about real file-based I/O?"

---

### Notebook 09 — Data Formats & Storage-Layer I/O
**File:** `notebooks/09_data_formats_and_io.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Columnar vs row formats: why Parquet/ORC dramatically outperform CSV/JSON for analytical queries
- Parquet internals: row groups, column chunks, page-level statistics (min/max, null count)
- Predicate pushdown at the file reader: `PushedFilters` in `FileScan` plan node — Spark passes filters to the Parquet reader, which skips row groups entirely
- Partition pruning at the directory level: reading `dt=2024-01-01/` instead of scanning the full table
- Column statistics and CBO: `ANALYZE TABLE ... COMPUTE STATISTICS FOR COLUMNS` makes the optimizer smarter
- Delta Lake: ACID transactions, time travel, `OPTIMIZE` (file compaction), Z-ordering (multi-column data skipping)
- BigQuery Spark connector: Storage Read API, how it parallelizes reads, whether it pushes predicates, column projection
- File sizing: the small file problem, how compaction helps, the right target file size (128 MB–1 GB)

**Spark UI checklist:**
- SQL tab: `FileScan parquet` node shows `PushedFilters` list — confirm filter was pushed to the reader
- SQL tab: absence of a `Filter` node above `FileScan` = predicate was pushed all the way down (ideal)
- Jobs tab: reading a partition-pruned path produces a job that reads far fewer bytes than the full table

**Interview questions to answer:**
1. Why is Parquet better than CSV for Spark analytical workloads? Give at least three reasons.
2. What is predicate pushdown at the Parquet level? How is it different from Catalyst's in-memory predicate pushdown?
3. What is partition pruning at the directory level? What determines whether it fires?
4. What is Z-ordering in Delta Lake? What query pattern does it optimize?
5. How does the BigQuery Spark connector read data? Does it push predicates to BigQuery?
6. What is the small file problem in Spark? How does it affect read performance, and how do you fix it?

---

## Tier 5 — Production engineering
> The question being answered: "How do you design, deploy, and operate a Spark pipeline at scale?"

---

### Notebook 10 — Pipeline Design Patterns
**File:** `notebooks/10_pipeline_design_patterns.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Extract → Transform → Load pattern: separation of concerns, testability
- Idempotency: overwrite vs append semantics, partition-level overwrite for incremental loads
- Schema evolution: `mergeSchema`, backward/forward compatibility, handling new columns
- Data quality validation: row counts, null checks, value range checks — where in the pipeline to place them
- Incremental processing: watermark-based filtering, change data capture patterns
- Partitioned writes: choosing partition columns (low-cardinality, frequently filtered)
- Job parameterization and configuration management
- Unit testing Spark jobs: using small DataFrames, schema assertions, output validation

**Interview questions to answer:**
1. How do you make a Spark write operation idempotent? What does that mean for partitioned tables?
2. What is partition overwrite mode in Spark? Why is it safer than full table overwrite for incremental loads?
3. How do you handle schema evolution in a production Spark pipeline reading Parquet files?
4. Where in an ETL pipeline do you place data quality checks, and why?
5. How would you design a pipeline that processes only new records since the last run?
6. How do you test a Spark transformation function in isolation?

---

### Notebook 11 — GCP / Dataproc
**File:** `notebooks/11_gcp_dataproc.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Dataproc vs self-managed Spark: managed lifecycle, versioning, autoscaling, preemptible nodes
- GCS vs HDFS: object store semantics (no atomic rename, eventual consistency on list), implications for `_SUCCESS` files and speculative execution
- BigQuery Spark connector deep dive: Storage Read API, columnar reads, predicate pushdown to BigQuery, write semantics (overwrite vs append)
- Cluster sizing: driver memory, executor cores and memory, number of executors, autoscaling
- Cost optimization: preemptible/spot VMs, ephemeral clusters per job vs persistent clusters, autoscaling policies
- Monitoring: Cloud Logging, Spark History Server on GCS, Cloud Monitoring metrics
- Airflow/Cloud Composer orchestration: `DataprocSubmitJobOperator`, dependency management, retry strategies

**Interview questions to answer:**
1. What are the implications of using GCS instead of HDFS for a Spark pipeline?
2. How does the BigQuery Spark connector read data? What is the Storage Read API?
3. How do you size a Dataproc cluster for a given workload? What are the key knobs?
4. What are preemptible VMs on Dataproc? What are the risks of using them?
5. How does autoscaling work on Dataproc, and what queries benefit from it?
6. How do you monitor a Spark job running on Dataproc? What metrics do you watch?

---

## Tier 6 — Advanced / optional
> For roles that explicitly require streaming knowledge.

---

### Notebook 12 — Structured Streaming
**File:** `notebooks/12_structured_streaming.ipynb`
**Status:** 📋 planned

**Concepts to cover:**
- Micro-batch vs continuous processing: latency vs throughput trade-offs
- Sources and sinks: Kafka, GCS/S3 file source, BigQuery sink
- Watermarks: defining the event-time boundary for late data, how they advance
- Stateful operations: `groupBy` on event time, `flatMapGroupsWithState`, state store
- Triggers: `processingTime`, `once`, `availableNow`, `continuous`
- Checkpointing: required for fault tolerance in stateful jobs, where to store
- Output modes: `append`, `complete`, `update`

**Interview questions to answer:**
1. What is the difference between micro-batch and continuous processing in Structured Streaming?
2. What is a watermark? How does Spark use it to handle late data?
3. What are stateful operations in streaming? What causes them to have memory/performance problems?
4. What is the purpose of a checkpoint in Structured Streaming? What happens if it is missing?
5. What output mode would you use for a streaming aggregation over a 1-hour window?

---

## Universal Spark UI debugging checklist

Apply this checklist for every notebook, for every query that runs an action:

### Step 1 — Jobs tab
- Which action triggered this job? Read the Description column.
- How many stages does the job have? Did the count match what you expected from the `Exchange` count in `explain()`?
- Is there a job that is significantly longer than the others? That is your target.

### Step 2 — Stages tab
- Sort by Duration. The slowest stage is the bottleneck.
- Does the slowest stage have an Exchange node? If yes, it is a shuffle stage — the most common bottleneck.
- Check Input Records for the slowest stage. Is it processing significantly more data than other stages?

### Step 3 — Task Metrics (click into the bottleneck stage)
- Duration histogram: one bar far to the right = hot key skew. One task is making the entire stage wait.
- Shuffle Spill (Disk) column: any value > 0 means a task exhausted execution memory and wrote to local disk.
- GC Time column: > 10% of task duration = memory pressure. Tune executor memory or reduce partition size.
- Input Records per task: one task with 10× the records confirms skew.

### Step 4 — SQL tab
- Find the query for the slow job. Read the plan bottom to top.
- Count Exchange nodes: each one is a shuffle = stage boundary = network round-trip.
- Identify the join strategy: `BroadcastHashJoin` (BroadcastExchange on small side only) vs `SortMergeJoin` (Exchange + Sort on both sides).
- Check Filter position: below Exchange (pushed down, good) vs above Exchange (late filter, avoidable cost).
- Look for `AQEShuffleRead` in the post-execution plan — shows actual vs planned partition count after AQE coalescing.
- `AdaptiveSparkPlan isFinalPlan=true` = you are reading the final executed plan with AQE decisions visible.

### Production debugging decision tree
```
Job is slow or failing →

  Jobs tab: Did stage count increase vs. baseline?
    YES → SQL tab: Join strategy changed? (BHJ → SMJ)
          YES → Lookup table grew past autoBroadcastJoinThreshold
                Fix: F.broadcast() hint or raise threshold
          NO  → Extra Exchange added? Late filter introduced?
                Fix: move filter before join or groupBy

    NO  → Stages tab: Which stage is the bottleneck?
           Shuffle stage (has Exchange)?
             Task Metrics — Duration histogram skewed?
               ONE OUTLIER → Hot key skew
                             Fix: salting (groupBy) or AQE skew join (join)
               ALL SLOW    → Data volume grew
                             Fix: check Input Records, tune parallelism
             Shuffle Spill (Disk) > 0?
               YES → Execution memory exhausted
                     Fix: fewer rows per partition (increase spark.sql.shuffle.partitions)
                          or increase spark.executor.memory
             GC Time > 10%?
               YES → Object pressure on heap
                     Fix: increase executor memory, use off-heap, reduce object creation

           Non-shuffle stage?
             SQL tab → Filter above Exchange? → Late filter → push upstream
             SQL tab → Missing PushedFilters on FileScan? → File format doesn't support it
                        or filter column is not a partition column
```

---

## `explain()` node quick reference

| Node in `explain()` | What it means | What to check |
|---|---|---|
| `Exchange hashpartitioning(col, N)` | Shuffle by hash of `col` into N buckets | Stage boundary; count these for total shuffle cost |
| `Exchange rangepartitioning(col, N)` | Range shuffle for global sort | Triggered by `orderBy`; expensive on large data |
| `AQEShuffleRead` | AQE coalesced shuffle partitions post-execution | Actual partition count vs planned N |
| `BroadcastHashJoin` | Broadcast join (small side in memory on all executors) | BroadcastExchange on the smaller branch |
| `BroadcastExchange` | Small table sent to all executors | Only on one branch; if on both, something is wrong |
| `SortMergeJoin` | Both sides shuffled, sorted, then merged | Exchange + Sort on both branches |
| `HashAggregate(partial)` | Per-partition partial aggregation before shuffle | Reduces shuffle data volume |
| `HashAggregate(final)` | Post-shuffle merge of partial aggregates | Runs after Exchange |
| `Filter (col > X)` | Predicate evaluation | Check position: below Exchange = pushed down (good) |
| `LocalTableScan` | Reading from in-memory DataFrame | No I/O; created via `createDataFrame` |
| `FileScan parquet` | Reading from Parquet files | Check `PushedFilters` and `PartitionFilters` fields |
| `InMemoryTableScan` | Reading from a cached DataFrame | Appears after `.cache()` is materialized |
| `WholeStageCodegen (N)` | Tungsten fuses N operators into one JVM bytecode loop | Operators inside are compiled; crossing boundary has overhead |
| `AdaptiveSparkPlan isFinalPlan=false` | Pre-execution; AQE may still rewrite this | Normal before any action runs |
| `AdaptiveSparkPlan isFinalPlan=true` | Post-execution; reflects actual AQE decisions | Read this for the real plan |

---

## Progress tracker

| Notebook | Date started | Date completed | Interview questions answered? |
|---|---|---|---|
| 01 — Lazy evaluation | — | ✅ | — |
| 02 — Jobs, stages, tasks | — | ✅ | — |
| 03 — Partitioning & skew | — | ✅ | — |
| 04 — Join strategies | — | ✅ | — |
| 05 — Catalyst optimizer | — | ✅ | — |
| 06 — AQE deep dive | — | ✅ | — |
| 07 — Memory model & spill | — | 🔄 | — |
| 08 — Caching & persistence | — | — | — |
| 09 — Data formats & I/O | — | — | — |
| 10 — Pipeline design | — | — | — |
| 11 — GCP / Dataproc | — | — | — |
| 12 — Structured Streaming | — | — | — |
