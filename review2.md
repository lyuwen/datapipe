## Findings

1. **Critical — thread/process executors suppress fatal source failures under `errors="skip"` or `"return"`.**
   [`BoundedMapExecutor.fill()`](/home/lfu/git-projects/python-parallel-harness/datapipe/execution/base.py:166) converts every exception from `next(source_iter)`—including I/O errors and `KeyboardInterrupt`—into a record error, then marks the source exhausted. A source that yielded one row and then raised `OSError("read failed")` returned successfully with truncated output under `ThreadExecutor`, while `SequentialExecutor` correctly raised. Only `SourceRecordError` should follow record-error policy; other source failures must abort.

2. **High — `ThreadExecutor` still lacks isolated per-thread worker lifecycle.**
   [`_ThreadLocalWorker`](/home/lfu/git-projects/python-parallel-harness/datapipe/execution/thread.py:26) gives each thread a context but shares the compiled pipeline and stage instances. Per-thread `setup()` therefore mutates the same stage objects, so normal setup patterns such as storing a client/model on `self` race between threads. A focused test observed incorrect thread-owned state on 75 of 100 records. Additionally, [`teardown_all()`](/home/lfu/git-projects/python-parallel-harness/datapipe/execution/thread.py:62) runs on the coordinator thread, whose thread-local context is empty, so teardown was called zero times despite four successful setups.

3. **High — `RangeSharding()` caches a source-derived total and can silently lose records when reused.**
   [`Source.iter_for_runtime()`](/home/lfu/git-projects/python-parallel-harness/datapipe/io/base.py:72) mutates the caller’s strategy. After processing `range(4)`, the strategy retains `total=4`; reusing it with `range(8)` across two ranks emits only `[0, 1, 2, 3]`. The source-derived total needs to be resolved per run without permanently changing the reusable strategy.

4. **Medium — Parquet filters fail when the predicate column is not projected.**
   [`ParquetSource._iter_paths()`](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:152) projects `columns` before applying the filter expression. For a table containing `id` and `name`, `columns=["name"]` with `filters=field("id") >= 3` raises `ArrowInvalid` because `id` is absent from the batch. Filtering should be performed before final projection, ideally through a dataset scanner for predicate pushdown.

The repository’s complete suite passes: **112 passed in 6.05s**. Compilation and diff whitespace checks also passed. No files were changed.
