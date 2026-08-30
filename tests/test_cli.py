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
        # An unrecognised first arg is treated as a transform expression stub
        rc = main(["some_expression"])
        assert rc == 2  # transform stub returns 2
        err = capsys.readouterr().err
        assert "not yet implemented" in err

    def test_transform_stub(self, capsys):
        rc = main(["transform", "expr"])
        assert rc == 2
        assert "not yet implemented" in capsys.readouterr().err

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

    def test_run_prints_stats(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"v": 1}\n')
        out = tmp_path / "out.jsonl"
        f = self._pipeline_file(tmp_path)
        main([
            "run", f"{f}:pipeline",
            "--source", str(src),
            "--sink", str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        stdout = capsys.readouterr().out
        assert "completed" in stdout


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
