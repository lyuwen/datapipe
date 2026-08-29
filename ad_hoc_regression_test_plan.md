# Ad-hoc Regression Test Consolidation Plan

## Purpose

Convert the review-time shell reproductions into deterministic pytest coverage.
The tests should preserve the behavioral invariant behind each reproduction,
not its incidental timing or implementation details.

Recommended destinations:

- executor and lifecycle cases: `tests/test_executors.py` or a new
  `tests/test_thread_lifecycle.py`;
- source-failure cases: `tests/test_executors.py`;
- sharding cases: `tests/test_sharding.py`;
- Parquet cases: `tests/test_io_parquet.py`;
- cross-cutting historical regressions may remain in
  `tests/test_review_findings.py`, but new focused files will be easier to
  maintain.

## Test design rules

1. Prefer `threading.Event`, `threading.Barrier`, and locked probe objects over
   `time.sleep()` for ordering. A short timeout may guard against deadlocks,
   but should not be the mechanism that creates the race.
2. Record initialized worker IDs explicitly. Assert teardown once for every
   initialized worker, rather than assuming all configured threads started.
3. Keep shared observation state separate from per-worker stage state. If
   worker cloning intentionally deep-copies stages, use a small probe object
   with an explicit `__deepcopy__` policy or module-level synchronized
   collector.
4. Put process-executor helpers at module scope so they remain pickleable under
   the `spawn` multiprocessing context.
5. Use `pytest.importorskip("pyarrow")` for Parquet coverage.
6. All pipeline runs should set `progress=False`.
7. Avoid asserting private implementation structure unless the behavior cannot
   otherwise be observed. Prefer output, exception, lifecycle, and stats
   assertions.
8. Give concurrency waits bounded timeouts (for example, 5 seconds) so a
   regression fails instead of hanging CI.

## Coverage matrix

| ID | Proposed pytest name | Current status | Required action |
|---|---|---|---|
| A1 | `test_fatal_source_error_propagates_for_all_executors` | Missing | Add |
| A2 | `test_source_keyboard_interrupt_is_not_skipped` | Missing | Add |
| A3 | `test_range_sharding_resolves_total_per_run` | Missing | Add |
| A4 | `test_parquet_filter_can_reference_unprojected_column` | Missing | Add |
| A5 | `test_thread_stage_attribute_state_is_worker_local` | Missing | Add |
| A6 | `test_thread_nested_mutable_state_is_worker_local` | Missing | Add |
| A7 | `test_thread_setup_failure_is_fatal_for_every_error_policy` | Partial | Strengthen |
| A8 | `test_thread_teardown_once_on_owning_thread` | Missing | Add |
| A9 | `test_thread_abort_waits_before_teardown` | Missing | Add |
| A10 | `test_thread_teardown_on_abort_for_every_initialized_worker` | Partial | Strengthen |
| A11 | `test_stage_deepcopy_supports_nested_locks` | Present | Retain |
| A12 | `test_thread_executor_supports_stage_with_nested_lock` | Present | Retain |
| A13 | `test_parquet_hive_partition_filter` | Present | Retain |
| A14 | `test_parquet_hive_partition_filter_sharded` | Present | Strengthen |

## Detailed plans

### A1. Fatal source errors propagate consistently

Define a `BrokenSource` that yields one value and then raises
`OSError("read failed")`. Parameterize over `SequentialExecutor`,
`ThreadExecutor`, and `ProcessExecutor`, and separately over `errors="skip"`
and `errors="return"`.

Assertions:

- `Pipeline.run()` raises the original `OSError` for every executor and error
  policy;
- the first successfully read record may be present in the sink;
- the run must never return successful stats for the truncated source.

This protects the distinction between resumable `SourceRecordError` markers
and fatal iterator/I/O failures.

### A2. `KeyboardInterrupt` while pulling the source is never normalized

Define a source whose iterator immediately raises `KeyboardInterrupt`. Run it
with the future-backed executors under `errors="skip"` and `errors="return"`.

Assertions:

- `KeyboardInterrupt` propagates unchanged;
- no error payload is written;
- source and sink cleanup still run.

Keep this separate from signal-delivery tests: directly raising from the
iterator makes the scheduler boundary deterministic.

### A3. `RangeSharding` resolves source totals per run

Reuse one `RangeSharding()` instance for two complete simulated distributed
runs:

1. collect all ranks for `IterableSource(range(4))`;
2. collect all ranks for `IterableSource(range(8))`.

Assertions:

- the first union is exactly `set(range(4))`;
- the second union is exactly `set(range(8))`;
- rank outputs are pairwise disjoint in both runs;
- the caller-owned strategy still has `total is None` after both runs.

This catches stale source-derived totals and mutation of reusable configuration.

### A4. Parquet filtering may reference an unprojected physical column

Write a Parquet file containing `id` and `name`. Read it with
`columns=["name"]` and `filters=ds.field("id") >= 3`.

Assertions:

- the result is exactly the projected names belonging to IDs 3 and above;
- result dictionaries contain only `name`;
- no `ArrowInvalid` is raised.

Add both expression and legacy tuple filter variants if both are public API.

### A5. Direct stage attributes are isolated per thread

Create a stage whose `setup()` assigns `self.owner_thread` and whose
`process()` verifies it equals `threading.get_ident()`. Store mismatches in an
explicit shared collector. Use enough input and a barrier/event in setup to
ensure at least two workers initialize.

Assertions:

- at least two distinct worker threads initialized;
- every record observes its own worker's `owner_thread`;
- all inputs are emitted exactly once.

This covers the common documented pattern `self.model = load_model(...)`.

### A6. Nested mutable stage state is isolated per thread

Use the same structure as A5, but store ownership in a pre-existing nested
container such as `self.state = {"owner_thread": None}` and mutate the dict in
`setup()`.

Assertions are identical to A5. This specifically distinguishes deep worker
isolation from shallow `copy.copy()` behavior.

### A7. Setup failure is fatal under every record-error policy

The existing `test_thread_setup_failure_aborts_with_skip_policy` covers only
`errors="skip"`. Parameterize it over `"raise"`, `"skip"`, and `"return"`,
with and without an `error_sink` where applicable.

Assertions:

- `WorkerSetupError` (or the documented public initialization exception)
  propagates for every policy;
- its `cause` is the original setup exception;
- `process()` is never called;
- neither primary nor error sink receives a record.

### A8. Successful teardown runs exactly once on each owning thread

Record `(ctx.worker_id, threading.get_ident())` during setup and teardown in a
shared synchronized collector. Use a barrier-backed workload to ensure several
threads initialize.

Assertions:

- setup and teardown have the same worker-ID set;
- each initialized worker has exactly one teardown;
- for each worker, setup and teardown thread identifiers match;
- teardown occurs after that worker's final `process()` call.

Do not merely assert a total teardown count: duplicated teardown on one thread
could otherwise hide a missing teardown on another.

### A9. Abort never tears down a worker while it is processing

Coordinate two records with events:

- one worker enters a blocking `process()` and signals `started`;
- another worker waits for `started` and then raises the aborting error;
- the blocked worker is released by a test-controlled event or short bounded
  handoff;
- teardown records whether `process_active` was still true.

Assertions:

- the pipeline raises the expected stage error;
- no teardown observes an active `process()` call;
- no record observes its resource as closed while processing.

This replaces the earlier sleep-based “use after teardown” reproduction.

### A10. Abort tears down every worker that completed setup

Strengthen `test_thread_teardown_on_abort`. Its current assertion only checks
`teardown_count > 0`, which allows three initialized workers to leak as long as
one is cleaned up.

Use shared sets/counters keyed by `ctx.worker_id`.

Assertions:

- at least two workers complete setup before the controlled failure;
- after `Pipeline.run()` raises, the teardown worker-ID set exactly equals the
  successful setup worker-ID set;
- each worker is torn down once;
- teardown occurs on its owning thread, if thread affinity is part of the
  lifecycle contract.

Also cover a directly raised `KeyboardInterrupt`, because cancellation follows
a different exception path from a stage error.

### A11-A12. Nested synchronization objects survive cloning

Retain the existing unit and integration tests, but extend the unit case to
cover locks nested more deeply than one dict level, for example:

```python
self.state = {"resources": [{"lock": threading.Lock()}]}
```

Assertions:

- cloning does not raise;
- the cloned container graph is independent;
- lock objects are independent when worker-local locking is intended;
- the full pipeline executes all records under `ThreadExecutor`.

If shared locks are also a supported use case, add an explicit stage cloning
hook test demonstrating how a stage opts into shared observation state.

### A13-A14. Hive partition filtering

Retain the existing single-rank test. Strengthen the sharded test by recording
results per rank rather than only concatenating them.

Assertions:

- filtering a partition column returns only the selected partition;
- projected reads may omit the partition column while still filtering by it;
- the union across ranks equals the expected rows exactly;
- rank outputs are pairwise disjoint;
- an empty rank returns an empty iterator without error;
- exercise at least one world size larger than the number of selected files.

## Suggested implementation order

1. A1-A4: deterministic source, sharding, and Parquet correctness tests.
2. A5-A8: normal thread-worker isolation and lifecycle.
3. A9-A10: abort and cancellation lifecycle, using events/barriers.
4. A11-A14: cloning edge cases and expanded partitioned-dataset coverage.

## Completion criteria

- Every matrix row is represented by a named pytest test.
- Thread tests pass repeatedly (for example, 50 local repetitions) without
  timing-dependent failures.
- The ordinary suite passes with `pytest -q`.
- Parquet tests pass when the optional dependency is installed and skip
  cleanly otherwise.
- At least one CI job exercises the process executor under the `spawn` context.
