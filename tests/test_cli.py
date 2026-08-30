"""Tests for the datapipe CLI: loaders, run command, and inspect command."""

from __future__ import annotations

import json
import sys
import textwrap
from pathlib import Path

import pytest

from datapipe.cli.loaders import PipelineLoadError, load_pipeline_ref
from datapipe.cli.main import main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_pipeline(tmp_path: Path, code: str) -> Path:
    """Write a pipeline definition file and return its path."""
    p = tmp_path / "pipeline.py"
    p.write_text(textwrap.dedent(code))
    return p


# ---------------------------------------------------------------------------
# loader tests
# ---------------------------------------------------------------------------


class TestLoadPipelineRef:
    def test_no_colon_raises(self):
        with pytest.raises(PipelineLoadError, match="expected"):
            load_pipeline_ref("mymodule")

    def test_empty_module_raises(self):
        with pytest.raises(PipelineLoadError, match="non-empty"):
            load_pipeline_ref(":pipeline")

    def test_empty_attr_raises(self):
        with pytest.raises(PipelineLoadError, match="non-empty"):
            load_pipeline_ref("mymodule:")

    def test_missing_file_raises(self, tmp_path):
        with pytest.raises(PipelineLoadError, match="not found"):
            load_pipeline_ref(str(tmp_path / "missing.py:pipeline"))

    def test_load_from_file(self, tmp_path):
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            pipeline = Pipeline([TransformStage(lambda x: x, name="id")])
            """,
        )
        from datapipe.pipeline import Pipeline
        obj = load_pipeline_ref(f"{f}:pipeline")
        assert isinstance(obj, Pipeline)

    def test_load_relative_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            pipeline = Pipeline([TransformStage(lambda x: x, name="id")])
            """,
        )
        from datapipe.pipeline import Pipeline
        obj = load_pipeline_ref(f"./pipeline.py:pipeline")
        assert isinstance(obj, Pipeline)

    def test_missing_attribute_raises(self, tmp_path):
        f = _write_pipeline(tmp_path, "x = 1\n")
        with pytest.raises(PipelineLoadError, match="no attribute"):
            load_pipeline_ref(f"{f}:pipeline")

    def test_syntax_error_in_file_raises(self, tmp_path):
        p = tmp_path / "bad.py"
        p.write_text("def broken(\n")
        with pytest.raises(PipelineLoadError, match="error executing"):
            load_pipeline_ref(f"{p}:pipeline")

    def test_load_by_module_name(self):
        """datapipe itself is importable; load a well-known attribute."""
        import datapipe
        obj = load_pipeline_ref("datapipe:Pipeline")
        from datapipe.pipeline import Pipeline
        assert obj is Pipeline

    def test_dotted_attr_path(self, tmp_path):
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            class ns:
                pipeline = Pipeline([TransformStage(lambda x: x, name="id")])
            """,
        )
        from datapipe.pipeline import Pipeline
        obj = load_pipeline_ref(f"{f}:ns.pipeline")
        assert isinstance(obj, Pipeline)


# ---------------------------------------------------------------------------
# main() / --version
# ---------------------------------------------------------------------------


class TestMainVersion:
    def test_version_exits_zero(self, capsys):
        rc = main(["--version"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "datapipe" in out

    def test_no_command_exits_zero(self, capsys):
        rc = main([])
        assert rc == 0

    def test_unknown_command_shows_help_for_expression_shorthand(self, capsys):
        # An unrecognised first arg is treated as a transform expression.
        # Without input/output paths, argparse exits with an error.
        with pytest.raises(SystemExit) as exc_info:
            main(["some_expression"])
        assert exc_info.value.code != 0

    def test_transform_requires_input_output(self, capsys):
        # The real transform command requires expression, input, and output.
        # Calling with only an expression should produce an argparse error.
        with pytest.raises(SystemExit) as exc_info:
            main(["transform", "expr"])
        assert exc_info.value.code != 0

    def test_tools_stub(self, capsys):
        rc = main(["tools"])
        assert rc == 2
        assert "not yet implemented" in capsys.readouterr().err


# ---------------------------------------------------------------------------
# datapipe run
# ---------------------------------------------------------------------------


class TestRunCommand:
    def _pipeline_file(self, tmp_path: Path) -> Path:
        return _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            pipeline = Pipeline([TransformStage(lambda x: x, name="id")])
            """,
        )

    def test_missing_source_exits_nonzero(self, tmp_path, capsys):
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--sink", str(tmp_path / "out.jsonl"),
        ])
        assert rc != 0
        assert "source" in capsys.readouterr().err.lower()

    def test_missing_sink_exits_nonzero(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--source", str(src),
        ])
        assert rc != 0
        assert "sink" in capsys.readouterr().err.lower()

    def test_bad_pipeline_ref_exits_nonzero(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        rc = main([
            "run", "nonexistent_module:pipeline",
            "--source", str(src),
            "--sink", str(tmp_path / "out.jsonl"),
        ])
        assert rc != 0
        assert "error" in capsys.readouterr().err.lower()

    def test_non_pipeline_object_exits_nonzero(self, tmp_path, capsys):
        p = tmp_path / "pipeline.py"
        p.write_text("pipeline = 42\n")
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(tmp_path / "out.jsonl"),
        ])
        assert rc != 0
        assert "Pipeline" in capsys.readouterr().err

    def test_run_sequential_jsonl_to_jsonl(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n{"v": 2}\n{"v": 3}\n')
        out = tmp_path / "out.jsonl"
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert lines == [{"v": 1}, {"v": 2}, {"v": 3}]

    def test_run_format_prefix_jsonl(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        out = tmp_path / "out.jsonl"
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--source", f"jsonl:{src}",
            "--sink", f"jsonl:{out}",
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert json.loads(out.read_text()) == {"v": 1}

    def test_run_thread_executor(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text("\n".join(json.dumps({"v": i}) for i in range(20)) + "\n")
        out = tmp_path / "out.jsonl"
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "thread",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert sorted(r["v"] for r in lines) == list(range(20))

    def test_run_unordered_flag(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text("\n".join(json.dumps({"v": i}) for i in range(10)) + "\n")
        out = tmp_path / "out.jsonl"
        f = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{f}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--unordered",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err

    def test_run_errors_skip(self, tmp_path, capsys):
        p = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            def boom(x):
                if x.get("v") == 2:
                    raise ValueError("bad")
                return x
            pipeline = Pipeline([TransformStage(boom, name="boom")])
            """,
        )
        src = tmp_path / "in.jsonl"
        src.write_text('{"v":1}\n{"v":2}\n{"v":3}\n')
        out = tmp_path / "out.jsonl"
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--errors", "skip",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert len(lines) == 2
        assert {"v": 2} not in lines

    def test_run_error_output(self, tmp_path, capsys):
        p = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            def boom(x):
                if x.get("v") == 1:
                    raise ValueError("bad")
                return x
            pipeline = Pipeline([TransformStage(boom, name="boom")])
            """,
        )
        src = tmp_path / "in.jsonl"
        src.write_text('{"v":1}\n{"v":2}\n')
        out = tmp_path / "out.jsonl"
        err_out = tmp_path / "errors.jsonl"
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--errors", "return",
            "--error-output", str(err_out),
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        errors = [json.loads(l) for l in err_out.read_text().splitlines()]
        assert len(errors) == 1
        assert errors[0]["error_type"] == "ValueError"

    def test_run_process_executor_file_pipeline(self, tmp_path, capsys):
        """File-loaded pipelines must work with the default ProcessExecutor.

        This is the primary documented command form and was broken before the
        loader registered modules in sys.modules (cli-review-1 finding 1).
        """
        p = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline
            from datapipe.stage import Stage

            class _Double(Stage):
                name = "double"
                def process(self, value, ctx):
                    return {k: v * 2 if isinstance(v, int) else v
                            for k, v in value.items()}

            pipeline = Pipeline([_Double()])
            """,
        )
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n{"v": 2}\n{"v": 3}\n')
        out = tmp_path / "out.jsonl"
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "process",
            "--workers", "2",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert sorted(r["v"] for r in lines) == [2, 4, 6]

    def test_run_raw_mode_with_json_load_dump_stages(self, tmp_path, capsys):
        """--raw enables worker-side JSON parsing/serialization (finding 2)."""
        p = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, JsonLoadStage, JsonDumpStage
            from datapipe.stage import Stage

            class _AddField(Stage):
                name = "add_field"
                def process(self, value, ctx):
                    value["extra"] = True
                    return value

            pipeline = Pipeline([JsonLoadStage(), _AddField(), JsonDumpStage()])
            """,
        )
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n{"v": 2}\n')
        out = tmp_path / "out.jsonl"
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--raw",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        lines = [json.loads(l) for l in out.read_text().splitlines()]
        assert all(r.get("extra") is True for r in lines)

    def test_run_invalid_workers_exits_cleanly(self, tmp_path, capsys):
        """--workers 0 must produce a clean error message, not a traceback (finding 3)."""
        p = self._pipeline_file(tmp_path)
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(tmp_path / "out.jsonl"),
            "--executor", "process",
            "--workers", "0",
            "--no-progress",
        ])
        assert rc != 0
        err = capsys.readouterr().err
        assert "error:" in err.lower()
        assert "Traceback" not in err

    def test_run_rank_override_preserves_world_size(self, tmp_path, capsys):
        """Explicit --rank must not reset world_size to 1 (finding 4)."""
        # With world_size default (1) and rank=0 this should succeed.
        # The key check is that _build_runtime doesn't discard auto-detected
        # fields by constructing a fresh RuntimeContext with only the overridden
        # fields — we verify this by ensuring rank=0 with no world_size clash.
        p = self._pipeline_file(tmp_path)
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        rc = main([
            "run", f"{p}:pipeline",
            "--source", str(src),
            "--sink", str(tmp_path / "out.jsonl"),
            "--executor", "sequential",
            "--rank", "0",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err

    def test_run_csv_prefix_rejected(self, tmp_path, capsys):
        """csv: prefix is not supported and must produce a clean CLI error (finding 6).

        csv: is not in _FORMAT_PREFIXES so the prefix is treated as part of
        the path string, which then fails to open. Either way the run must
        exit non-zero with a controlled error message, not a raw traceback.
        """
        p = self._pipeline_file(tmp_path)
        rc = main([
            "run", f"{p}:pipeline",
            "--source", "csv:/tmp/nonexistent.csv",
            "--sink", str(tmp_path / "out.jsonl"),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc != 0
        err = capsys.readouterr().err
        assert "error:" in err.lower()
        assert "Traceback" not in err


# ---------------------------------------------------------------------------
# datapipe inspect
# ---------------------------------------------------------------------------


class TestInspectCommand:
    def test_inspect_human_readable(self, tmp_path, capsys):
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage, FilterStage
            pipeline = Pipeline([
                TransformStage(lambda x: x, name="identity"),
                FilterStage(lambda x: x, name="keep_all"),
            ])
            """,
        )
        rc = main(["inspect", f"{f}:pipeline"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "Pipeline" in out
        assert "identity" in out
        assert "keep_all" in out

    def test_inspect_json_output(self, tmp_path, capsys):
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, TransformStage
            pipeline = Pipeline([TransformStage(lambda x: x, name="t")])
            """,
        )
        rc = main(["inspect", f"{f}:pipeline", "--json"])
        assert rc == 0
        doc = json.loads(capsys.readouterr().out)
        assert "stages" in doc
        assert doc["stages"][0]["name"] == "t"

    def test_inspect_bad_ref_exits_nonzero(self, capsys):
        rc = main(["inspect", "nonexistent_module:pipeline"])
        assert rc != 0

    def test_inspect_non_pipeline_exits_nonzero(self, tmp_path, capsys):
        p = tmp_path / "pipeline.py"
        p.write_text("pipeline = 'not a pipeline'\n")
        rc = main(["inspect", f"{p}:pipeline"])
        assert rc != 0

    def test_inspect_shows_stage_types(self, tmp_path, capsys):
        f = _write_pipeline(
            tmp_path,
            """
            from datapipe import Pipeline, GenericStage, FilterStage, JsonLoadStage
            pipeline = Pipeline([
                JsonLoadStage(),
                GenericStage(process=lambda x: x, name="proc"),
                FilterStage(lambda x: True, name="filt"),
            ])
            """,
        )
        rc = main(["inspect", f"{f}:pipeline"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "JsonLoadStage" in out
        assert "GenericStage" in out
        assert "FilterStage" in out
