Reviewed commit `4ab6144` against both architecture plans.

## Findings

1. **High — `file.py:pipeline` cannot run with the default process executor.**

   The file loader assigns functions a synthetic module name such as `_datapipe_loader.pipeline`, but it neither registers an importable module nor makes that synthetic package available to spawned workers ([loaders.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/loaders.py:98)). The CLI defaults to `ProcessExecutor` ([run.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/run.py:76)).

   I reproduced:

   ```text
   error: Can't pickle <function identity ...>:
   import of module '_datapipe_loader.datapipe_review_pipeline' failed
   ```

   This breaks the primary documented command:

   ```bash
   datapipe run ./pipeline.py:pipeline ...
   ```

   The tests only execute file-loaded pipelines sequentially or with threads; there is no process-executor CLI test ([test_cli.py](/home/lfu/git-projects/python-parallel-harness/tests/test_cli.py:146)).

   The loader needs a module identity that standard pickle can resolve in workers, plus a process-executor end-to-end test.

2. **High — the CLI cannot run worker-side JSON load/dump pipelines described by the architecture.**

   `_build_source()` always constructs `JsonlSource(path)` with `raw=False`, while `_build_sink()` always constructs `JsonlSink(path)` with `raw=False` ([run.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/run.py:260)). There is no CLI option for raw input or output.

   Consequently, a pipeline containing:

   ```python
   Pipeline([JsonLoadStage(), ..., JsonDumpStage()])
   ```

   receives an already-parsed dictionary. I reproduced:

   ```text
   stage 'json_load' failed for record 0:
   TypeError: the JSON object must be str, bytes or bytearray, not dict
   ```

   This conflicts with the foundational design’s explicit worker-side JSON mode ([architecture](/home/lfu/git-projects/python-parallel-harness/parallel_record_pipeline_architecture.md:1201)) and prevents the CLI from running an important class of otherwise valid Python pipelines.

   The CLI needs explicit raw/parsed source and sink modes. It should not attempt to infer this from pipeline stages.

3. **Medium — validly parsed but invalid execution settings escape as tracebacks.**

   Executor and runtime construction occur outside the command’s exception boundary ([run.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/run.py:179)). For example:

   ```bash
   datapipe run ... --executor process --workers 0
   datapipe run ... --rank 2
   ```

   raise uncaught `ValueError` tracebacks instead of returning a controlled CLI error. The second example fails when the detected world size is one.

   Validate positive numeric arguments through argparse or include source/sink, executor, and runtime construction in the command-level error boundary.

4. **Medium — explicit runtime overrides discard detected runtime metadata.**

   `_build_runtime()` starts with `RuntimeContext.auto()`, but reconstructs a new context containing only `rank`, `world_size`, and `local_rank` ([run.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/run.py:311)). Supplying any override therefore discards:

   - `node_rank`
   - `job_id`
   - detected `environment`
   - `metadata`

   For example, adding `--local-rank` to a Slurm-detected run changes its environment back to `"local"`. The plan says explicit fields override environment detection, not that unrelated detected fields disappear.

   Use `RuntimeContext.auto(rank=..., world_size=..., local_rank=...)` or `dataclasses.replace()`.

5. **Medium — `datapipe-install` is published before its planned invocation can be parsed.**

   The commit adds the executable entry point ([pyproject.toml](/home/lfu/git-projects/python-parallel-harness/pyproject.toml:22)), but the `tools install` stub defines no `--editable`, `--force`, or path arguments ([main.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/main.py:69)).

   Therefore the planned command:

   ```bash
   datapipe-install --editable xxx.py
   ```

   exits through argparse with “unrecognized arguments” rather than the intentional “not yet implemented” stub response.

   Either defer publishing the entry point or make its stub accept the planned interface and clearly report that installation is unavailable.

6. **Low — CSV is advertised but guaranteed to fail.**

   `--source` and `--sink` help claim support for `csv:` ([run.py](/home/lfu/git-projects/python-parallel-harness/datapipe/cli/run.py:52)), and the prefix parser recognizes it, but neither source nor sink builder implements CSV. Every `csv:` invocation returns “unsupported format.”

   CSV is not part of either current plan. Remove it from the help and prefix list until an adapter exists.

## Plan alignment

The commit otherwise follows several important architectural requirements:

- `run` and `inspect` share the existing Python pipeline model.
- No CLI-specific executor or stage-wise scheduling was introduced.
- Pipeline inspection does not open data.
- Core execution flags largely match the foundational Phase 4 design.
- DSL and tool packages are clearly marked as unimplemented stubs.
- The compact transform syntax and Python pipeline path remain separate authoring routes.

## Verification

- `tests/test_cli.py`: **31 passed**
- Full suite: **171 passed, 1 failed**
- `git diff --check HEAD^ HEAD`: **passed**

The full-suite failure is `test_thread_teardown_once_on_owning_thread`: teardown ran on a different OS thread from setup. That code was not changed by this commit, so I would not attribute it to `4ab6144`, but the current HEAD cannot be considered fully green.

The two highest-severity CLI failures above are not covered by the committed tests.
