"""Phase 3 tests: CompiledToolProgramStage and datapipe transform command."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from datapipe.cli.main import main
from datapipe.dsl.compiler import compile_expression
from datapipe.stages.tool_program import CompiledToolProgramStage


# ---------------------------------------------------------------------------
# CompiledToolProgramStage unit tests
# ---------------------------------------------------------------------------


def _make_ctx():
    from datapipe.context import WorkerContext
    return WorkerContext(rank=0, world_size=1, worker_id=0)


class TestCompiledToolProgramStage:
    def test_basic_fromjson(self):
        stage = CompiledToolProgramStage(compile_expression("fromjson(.data)"))
        ctx = _make_ctx()
        result = stage.process({"data": '{"x": 1}'}, ctx)
        assert result == {"data": {"x": 1}}

    def test_basic_tojson(self):
        stage = CompiledToolProgramStage(compile_expression("tojson(.data)"))
        ctx = _make_ctx()
        result = stage.process({"data": {"x": 1}}, ctx)
        assert result == {"data": '{"x":1}'}

    def test_chained_expression(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.tools) | tojson(.tools[].name)")
        )
        ctx = _make_ctx()
        record = {"tools": '[{"name": "foo"}, {"name": "bar"}]'}
        result = stage.process(record, ctx)
        # fromjson(.tools) decodes the string to a list of objects.
        # tojson(.tools[].name) serializes each .name field in-place.
        assert result["tools"] == [{"name": '"foo"'}, {"name": '"bar"'}]

    def test_wildcard_applies_to_all(self):
        stage = CompiledToolProgramStage(compile_expression("tojson(.items[])"))
        ctx = _make_ctx()
        result = stage.process({"items": [1, 2, 3]}, ctx)
        assert result["items"] == ["1", "2", "3"]

    def test_empty_wildcard_is_noop(self):
        stage = CompiledToolProgramStage(compile_expression("tojson(.items[])"))
        ctx = _make_ctx()
        result = stage.process({"items": []}, ctx)
        assert result["items"] == []

    def test_root_selector(self):
        stage = CompiledToolProgramStage(compile_expression("tojson(.)"))
        ctx = _make_ctx()
        result = stage.process({"a": 1}, ctx)
        assert result == '{"a":1}'

    def test_stage_name_defaults_to_expr(self):
        stage = CompiledToolProgramStage(compile_expression("fromjson(.x)"))
        assert "fromjson" in stage.name

    def test_stage_custom_name(self):
        stage = CompiledToolProgramStage(
            compile_expression("fromjson(.x)"), name="my-stage"
        )
        assert stage.name == "my-stage"

    def test_long_expr_truncated_in_name(self):
        expr = "fromjson(.tools) | fromjson(.meta.ann, recursive=true) | tojson(.x)"
        stage = CompiledToolProgramStage(compile_expression(expr))
        assert len(stage.name) <= 41  # 40 chars + possible ellipsis

    def test_is_stage_subclass(self):
        from datapipe.stage import Stage
        stage = CompiledToolProgramStage(compile_expression("tojson(.x)"))
        assert isinstance(stage, Stage)

    def test_pipeline_integration(self):
        """Stage works correctly when run through Pipeline.run()."""
        from datapipe import (
            IterableSource,
            ListSink,
            Pipeline,
            SequentialExecutor,
        )
        stage = CompiledToolProgramStage(compile_expression("fromjson(.v)"))
        sink = ListSink()
        Pipeline([stage]).run(
            source=IterableSource([{"v": '{"a": 1}'}, {"v": '{"b": 2}'}]),
            sink=sink,
            executor=SequentialExecutor(),
            progress=False,
        )
        assert sink.items == [{"v": {"a": 1}}, {"v": {"b": 2}}]

    def test_motivating_expression(self):
        """The CLI plan's example expression works end-to-end through the stage."""
        stage = CompiledToolProgramStage(compile_expression(
            "fromjson(.tools) | "
            "fromjson(.metadata.annotation, recursive=true) | "
            "tojson(.tools[].function.parameters)"
        ))
        ctx = _make_ctx()
        record = {
            "tools": '[{"function": {"parameters": {"type": "object"}}}]',
            "metadata": {"annotation": '{"label": "test"}'},
        }
        result = stage.process(record, ctx)
        # tojson(.tools[].function.parameters) serializes each .parameters field
        # in-place, leaving the surrounding object structure intact.
        assert result["tools"] == [{"function": {"parameters": '{"type":"object"}'}}]
        assert result["metadata"]["annotation"] == {"label": "test"}


# ---------------------------------------------------------------------------
# datapipe transform CLI tests
# ---------------------------------------------------------------------------


def _write_jsonl(path: Path, records: list) -> None:
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")


def _read_jsonl(path: Path) -> list:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


class TestTransformCommand:
    def test_basic_transform_sequential(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"v": '{"a": 1}'}, {"v": '{"b": 2}'}])

        rc = main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert rows == [{"v": {"a": 1}}, {"v": {"b": 2}}]

    def test_shorthand_syntax(self, tmp_path, capsys):
        """datapipe EXPR INPUT OUTPUT without 'transform' keyword."""
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"x": '{"y": 42}'}])

        rc = main([
            "fromjson(.x)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        assert _read_jsonl(out) == [{"x": {"y": 42}}]

    def test_pipe_expression(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"tools": '[{"name": "t1"}]'}])

        rc = main([
            "transform",
            "fromjson(.tools) | tojson(.tools[].name)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        # tojson(.tools[].name) serializes each .name field in-place.
        assert rows[0]["tools"] == [{"name": '"t1"'}]

    def test_invalid_expression_exits_nonzero(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"x": 1}\n')
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform",
            "no_such_tool(.x)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc != 0
        assert "error" in capsys.readouterr().err.lower()

    def test_syntax_error_exits_nonzero(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"x": 1}\n')
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform",
            "fromjson .x",   # missing parens
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc != 0

    def test_dry_run_prints_stages(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        src.write_text('{"x": 1}\n')
        out = tmp_path / "out.jsonl"

        rc = main([
            "transform",
            "fromjson(.tools)",
            str(src), str(out),
            "--dry-run",
            "--executor", "sequential",
        ])
        assert rc == 0
        out_text = capsys.readouterr().out
        assert "fromjson" in out_text

    def test_errors_skip_policy(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        # One record with invalid JSON in .v (should fail), one good one.
        _write_jsonl(src, [{"v": "not json"}, {"v": '{"ok": true}'}])

        rc = main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", "sequential",
            "--errors", "skip",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert len(rows) == 1
        assert rows[0]["v"] == {"ok": True}

    def test_errors_return_with_error_output(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        err_out = tmp_path / "errors.jsonl"
        _write_jsonl(src, [{"v": "bad json {"}, {"v": '{"k": 1}'}])

        rc = main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", "sequential",
            "--errors", "return",
            "--error-output", str(err_out),
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        errors = _read_jsonl(err_out)
        assert len(errors) == 1

    def test_ordered_output_preserved(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"i": i, "v": json.dumps({"n": i})} for i in range(20)])

        rc = main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", "thread",
            "--workers", "4",
            "--ordered",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert [r["i"] for r in rows] == list(range(20))

    def test_prints_stats_on_completion(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"v": '{"a": 1}'}])

        main([
            "transform",
            "fromjson(.v)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert "completed" in capsys.readouterr().out

    def test_transform_tojson(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"data": {"a": 1, "b": 2}}])

        rc = main([
            "transform",
            "tojson(.data)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert isinstance(rows[0]["data"], str)
        assert json.loads(rows[0]["data"]) == {"a": 1, "b": 2}

    def test_argument_passed_through(self, tmp_path, capsys):
        src = tmp_path / "in.jsonl"
        out = tmp_path / "out.jsonl"
        _write_jsonl(src, [{"v": '{"x": "[1,2]"}'}])

        rc = main([
            "transform",
            "fromjson(.v, recursive=true)",
            str(src), str(out),
            "--executor", "sequential",
            "--no-progress",
        ])
        assert rc == 0, capsys.readouterr().err
        rows = _read_jsonl(out)
        assert rows[0]["v"] == {"x": [1, 2]}
