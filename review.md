## Findings

1. **Critical — sink finalization failures are suppressed, allowing silent data loss.**
   [`Pipeline.run()`](/home/lfu/git-projects/python-parallel-harness/datapipe/pipeline.py:238) logs and discards exceptions from `sink.close()`. Since [`ParquetSink.close()`](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:190) performs the final buffered write, a schema mismatch or disk error can still produce a successful return and stats. I reproduced this with an explicit `int32` schema and inferred `int64` data: the write failed during close, but `run()` returned successfully.

2. **High — ordered error handling can consume memory proportional to the remaining dataset.**
   With `ordered=True` and `errors="skip"`, a failed record returns before advancing `next_to_emit` ([pipeline.py:294](/home/lfu/git-projects/python-parallel-harness/datapipe/pipeline.py:294)). Every later result remains buffered until shutdown. A failure at sequence 0 with 1,000 inputs produced a reorder high-water mark of 999. This violates the bounded-memory invariant in the plan.

3. **High — aborted ordered runs write non-prefix, out-of-order partial output.**
   The `finally` block flushes the reorder buffer after errors or `KeyboardInterrupt` ([pipeline.py:324](/home/lfu/git-projects/python-parallel-harness/datapipe/pipeline.py:324)). If record 0 is still running when a later record fails, buffered records after the gap are written anyway. I reproduced output `[1, 3, 4, 5, 6, 7]` after record 2 failed, despite `ordered=True`. An interrupted ordered sink should contain only the contiguous completed prefix.

4. **High — `ThreadExecutor` does not provide per-thread worker lifecycle or safe context.**
   A single [`_ThreadWorker`](/home/lfu/git-projects/python-parallel-harness/datapipe/execution/thread.py:18), pipeline instance, and mutable `WorkerContext` are shared across every thread. Setup runs once for the entire pool, not once per worker thread, and concurrent writes to `ctx.record_index` race ([thread.py:36](/home/lfu/git-projects/python-parallel-harness/datapipe/execution/thread.py:36)). A focused run returned the wrong record index for 99 of 100 records.

5. **High — file-backed `error_sink` is unusable.**
   Only the primary source and sink are opened ([pipeline.py:267](/home/lfu/git-projects/python-parallel-harness/datapipe/pipeline.py:267)); `error_sink` is written directly without being opened ([pipeline.py:353](/home/lfu/git-projects/python-parallel-harness/datapipe/pipeline.py:353)) and is never closed. `errors="return", error_sink=JsonlSink(...)` raises `SourceError: JsonlSink.write() before open()`.

6. **High — `ParquetSource.filters` does not filter rows.**
   Filters are passed only while discovering dataset files ([parquet.py:70](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:70)); actual reads use `ParquetFile.iter_batches()` without filters ([parquet.py:92](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:92)). A filter of `id >= 8` over IDs 0–9 returned all ten rows.

7. **High — distributed Parquet output can be overwritten across ranks.**
   [`_ranked_dataset_path()`](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:33) rank-qualifies directory paths only. A plain file path is shared unchanged by every rank, contradicting the requirement that ranks never write the same file. In a two-rank simulation, the second run replaced the first rank’s output.

8. **Medium — explicit Parquet schemas are not applied when constructing batches.**
   [`pa.Table.from_pylist()`](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:172) infers a schema, then the writer is created using the explicit schema. Compatible-but-different inferred types, such as Python integer → `int64` with an explicit `int32` schema, fail at `write_table()`. Combined with finding 1, this failure is currently hidden. Also, `ParquetSink.flush()` calls a nonexistent `ParquetWriter.flush()` method ([parquet.py:185](/home/lfu/git-projects/python-parallel-harness/datapipe/io/parquet.py:185)).

9. **Medium — source decoding failures bypass record error policies.**
   JSON parsing in normal `JsonlSource(raw=False)` occurs while pulling from the source ([jsonl.py:81](/home/lfu/git-projects/python-parallel-harness/datapipe/io/jsonl.py:81)), while the scheduler only handles worker failures. Thus `errors="skip"` cannot skip malformed JSON unless callers know to use `raw=True` plus a JSON stage. The existing malformed-input test is misleading: it feeds already-parsed dictionaries into `JsonLoadStage`, so it fails on the first valid row rather than the malformed line ([test_io_jsonl.py:115](/home/lfu/git-projects/python-parallel-harness/tests/test_io_jsonl.py:115)).

10. **Medium — `RangeSharding` does not obtain totals from sources as documented.**
    `RangeSharding(total=None)` says it can use a source-reported total, but no runner code assigns `source.total`; the strategy is passed directly into logical sharding ([base.py:54](/home/lfu/git-projects/python-parallel-harness/datapipe/io/base.py:54)). Consequently, `RangeSharding()` fails even for `range(10)`.

## Plan status

Phases 0–3 are broadly represented, and the core fused-process/bounded-submission design is correct. Phase 4 remains an explicit CLI skeleton, while later production-hardening items are appropriately deferred.

All existing tests pass: **91 passed in 5.78s**. The issues above expose gaps not covered by that suite, especially finalization failures, thread-local lifecycle, Parquet filters, file error sinks, and ordered abort behavior.

No files were changed.
