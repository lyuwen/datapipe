"""CLI and IO completeness tests: format flags, expression inspection,
compressed-reader fd ownership, and per-rank progress totals."""

from __future__ import annotations

import json
import os

import pytest

from datapipe.cli.main import main


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _write_jsonl(path, rows) -> str:
    path.write_text("".join(json.dumps(r) + "\n" for r in rows))
    return str(path)


def _fd_count() -> int:
    """Number of open descriptors for this process, or None if unavailable."""
    try:
        return len(os.listdir("/proc/self/fd"))
    except OSError:
        return None


# ---------------------------------------------------------------------------
# Task 1 — transform --input-format / --output-format (plan §3.1)
# ---------------------------------------------------------------------------


class TestTransformFormatFlags:
    def test_explicit_jsonl_formats_accepted(self, tmp_path, capsys):
        src = _write_jsonl(tmp_path / "in.jsonl", [{"a": "[1,2]"}])
        out = str(tmp_path / "out.jsonl")
        rc = main([
            "transform",
            "--input-format", "jsonl",
            "--output-format", "jsonl",
            "--executor", "sequential", "--no-progress",
            "fromjson(.a)", src, out,
        ])
        assert rc == 0
        capsys.readouterr()
        assert json.loads(open(out).read().strip()) == {"a": [1, 2]}

    def test_formats_default_to_jsonl(self, tmp_path):
        from datapipe.cli.main import build_parser

        args = build_parser().parse_args(["transform", "e", "i", "o"])
        assert args.input_format == "jsonl"
        assert args.output_format == "jsonl"

    @pytest.mark.parametrize("flag", ["--input-format", "--output-format"])
    def test_unsupported_format_rejected(self, flag, capsys):
        with pytest.raises(SystemExit) as exc:
            main(["transform", flag, "parquet", "expr", "i", "o"])
        assert exc.value.code == 2
        err = capsys.readouterr().err
        assert "invalid choice" in err and "jsonl" in err


# ---------------------------------------------------------------------------
# Task 2 — inspect-expression, richer --dry-run, JSON booleans
# ---------------------------------------------------------------------------


class TestInspectExpression:
    def test_command_exists_and_reports_resolution(self, capsys):
        rc = main(["inspect-expression", "fromjson(.tools)"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "fromjson(.tools)" in out
        # Resolved provider, contract, and generated stage chain (§3.3).
        assert "provider:" in out
        assert "cardinality: one_to_one" in out
        assert "JsonLoadStage" in out and "JsonDumpStage" in out
        assert "CompiledToolProgramStage" in out

    def test_json_output_is_valid_json_with_real_booleans(self, capsys):
        rc = main(["inspect-expression", "--json", "fromjson(.a)"])
        assert rc == 0
        raw = capsys.readouterr().out
        # Python repr of a bool would break json.loads / appear as True.
        assert "True" not in raw and "False" not in raw
        doc = json.loads(raw)
        inv = doc["invocations"][0]
        assert inv["tool"] == "fromjson"
        assert inv["selector"] == ".a"
        assert inv["contract"]["deterministic"] is True
        assert inv["arguments"]["recursive"] is False
        assert [s["type"] for s in doc["stages"]] == [
            "JsonLoadStage", "CompiledToolProgramStage", "JsonDumpStage",
        ]

    def test_pipe_expression_reports_every_invocation(self, capsys):
        rc = main(["inspect-expression", "--json", "fromjson(.a) | tojson(.a.b)"])
        assert rc == 0
        doc = json.loads(capsys.readouterr().out)
        assert [i["tool"] for i in doc["invocations"]] == ["fromjson", "tojson"]
        assert [i["index"] for i in doc["invocations"]] == [0, 1]

    def test_invalid_expression_exits_nonzero(self, capsys):
        rc = main(["inspect-expression", "fromjson("])
        assert rc == 1
        assert "error" in capsys.readouterr().err.lower()

    def test_unknown_tool_exits_nonzero(self, capsys):
        rc = main(["inspect-expression", "no_such_tool_xyz(.a)"])
        assert rc == 1
        assert "error" in capsys.readouterr().err.lower()

    def test_reads_no_data(self, capsys):
        """Inspection must not require an input file to exist."""
        rc = main(["inspect-expression", "fromjson(.a)"])
        assert rc == 0

    def test_shorthand_does_not_swallow_the_command(self, capsys):
        """'inspect-expression' is a subcommand, not a transform expression."""
        rc = main(["inspect-expression", "fromjson(.a)"])
        assert rc == 0
        assert "Invocations" in capsys.readouterr().out


class TestTransformDryRun:
    def test_dry_run_shows_providers_contracts_and_stages(self, capsys):
        rc = main(["transform", "--dry-run", "fromjson(.a)", "missing.jsonl", "o"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "provider:" in out
        assert "target:" in out
        assert "input:" in out and "output:" in out
        assert "cardinality:" in out
        assert "JsonLoadStage" in out and "JsonDumpStage" in out

    def test_dry_run_arguments_use_dsl_literals(self, capsys):
        """Bound defaults render as DSL literals (true/false), not Python."""
        rc = main(["transform", "--dry-run", "fromjson(.a)", "i", "o"])
        assert rc == 0
        out = capsys.readouterr().out
        assert "recursive=false" in out
        assert "recursive=False" not in out

    def test_dry_run_json_is_machine_readable(self, capsys):
        rc = main([
            "transform", "--dry-run", "--json", "fromjson(.a)", "i", "o",
        ])
        assert rc == 0
        raw = capsys.readouterr().out
        assert "True" not in raw and "False" not in raw
        doc = json.loads(raw)
        assert doc["expression"] == "fromjson(.a)"
        assert doc["invocations"][0]["provider"]["mode"] == "builtin"

    def test_dry_run_matches_inspect_expression(self, capsys):
        main(["inspect-expression", "--json", "fromjson(.a)"])
        a = json.loads(capsys.readouterr().out)
        main(["transform", "--dry-run", "--json", "fromjson(.a)", "i", "o"])
        b = json.loads(capsys.readouterr().out)
        assert a == b

    def test_dry_run_reports_validate_mode(self, capsys):
        main([
            "transform", "--dry-run", "--json", "--validate-tools", "off",
            "fromjson(.a)", "i", "o",
        ])
        assert json.loads(capsys.readouterr().out)["validate"] == "off"


# ---------------------------------------------------------------------------
# Task 3 — compressed reader fd ownership
# ---------------------------------------------------------------------------


@pytest.mark.skipif(
    _fd_count() is None, reason="/proc/self/fd unavailable on this platform"
)
class TestCompressedReaderFdOwnership:
    def test_gzip_reader_close_releases_underlying_fd(self, tmp_path):
        import gzip

        from datapipe.io.utils import open_reader

        path = str(tmp_path / "data.jsonl.gz")
        with gzip.open(path, "wt") as f:
            f.write('{"a": 1}\n')

        before = _fd_count()
        stream = open_reader(path, "gzip")
        assert stream.read() == b'{"a": 1}\n'
        stream.close()
        assert _fd_count() == before

    def test_gzip_reader_context_manager_releases_fd(self, tmp_path):
        import gzip

        from datapipe.io.utils import open_reader

        path = str(tmp_path / "data.jsonl.gz")
        with gzip.open(path, "wt") as f:
            f.write('{"a": 1}\n')

        before = _fd_count()
        with open_reader(path, "gzip") as stream:
            stream.read()
        assert _fd_count() == before

    def test_uncompressed_reader_close_releases_fd(self, tmp_path):
        from datapipe.io.utils import open_reader

        path = _write_jsonl(tmp_path / "data.jsonl", [{"a": 1}])
        before = _fd_count()
        stream = open_reader(path, None)
        stream.read()
        stream.close()
        assert _fd_count() == before

    def test_zstd_reader_close_releases_fd(self, tmp_path):
        zstandard = pytest.importorskip("zstandard")

        from datapipe.io.utils import open_reader, open_writer

        path = str(tmp_path / "data.jsonl.zst")
        writer = open_writer(path, "zstd")
        writer.write(b'{"a": 1}\n')
        writer.close()

        before = _fd_count()
        stream = open_reader(path, "zstd")
        assert stream.read() == b'{"a": 1}\n'
        stream.close()
        assert _fd_count() == before

    def test_repeated_gzip_reads_do_not_accumulate_fds(self, tmp_path):
        """A leak of one fd per open would exhaust descriptors on a big run."""
        import gzip

        from datapipe.io.utils import open_reader

        path = str(tmp_path / "data.jsonl.gz")
        with gzip.open(path, "wt") as f:
            f.write('{"a": 1}\n')

        open_reader(path, "gzip").close()  # warm any lazy imports
        before = _fd_count()
        for _ in range(20):
            stream = open_reader(path, "gzip")
            stream.read()
            stream.close()
        assert _fd_count() == before

    def test_jsonl_source_over_gzip_does_not_leak(self, tmp_path):
        import gzip

        from datapipe.io.jsonl import JsonlSource

        path = str(tmp_path / "data.jsonl.gz")
        with gzip.open(path, "wt") as f:
            for i in range(5):
                f.write(json.dumps({"i": i}) + "\n")

        list(JsonlSource(path))  # warm imports
        before = _fd_count()
        for _ in range(10):
            assert len(list(JsonlSource(path))) == 5
        assert _fd_count() == before


# ---------------------------------------------------------------------------
# Task 4 — per-rank progress total under physical sharding
# ---------------------------------------------------------------------------


class TestParquetShardTotal:
    @staticmethod
    def _dataset(tmp_path, files: int, rows: int) -> str:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        d = tmp_path / "ds"
        d.mkdir()
        table = pa.table({"a": list(range(rows))})
        for i in range(files):
            pq.write_table(table, str(d / f"part-{i}.parquet"))
        return str(d)

    @staticmethod
    def _single_file(tmp_path, groups: int, rows: int) -> str:
        pa = pytest.importorskip("pyarrow")
        pq = pytest.importorskip("pyarrow.parquet")
        table = pa.table({"a": list(range(rows))})
        path = str(tmp_path / "one.parquet")
        writer = pq.ParquetWriter(path, table.schema)
        for _ in range(groups):
            writer.write_table(table)
        writer.close()
        return path

    def test_directory_shard_total_matches_rows_read(self, tmp_path):
        from datapipe.io.parquet import ParquetSource

        path = self._dataset(tmp_path, files=4, rows=25)
        for rank in range(4):
            source = ParquetSource(path)
            rows = len(list(source.iter_shard(rank, 4)))
            assert rows == 25
            assert source.total == rows

    def test_single_file_row_group_shard_total_matches_rows_read(self, tmp_path):
        from datapipe.io.parquet import ParquetSource

        path = self._single_file(tmp_path, groups=5, rows=10)
        seen = 0
        for rank in range(3):
            source = ParquetSource(path)
            rows = len(list(source.iter_shard(rank, 3)))
            assert source.total == rows
            seen += rows
        assert seen == 50

    def test_uneven_shard_totals_sum_to_full_dataset(self, tmp_path):
        from datapipe.io.parquet import ParquetSource

        path = self._dataset(tmp_path, files=5, rows=10)
        totals = []
        for rank in range(3):
            source = ParquetSource(path)
            list(source.iter_shard(rank, 3))
            totals.append(source.total)
        assert sum(totals) == 50
        assert sorted(totals) == [10, 20, 20]

    def test_single_rank_total_is_full_dataset(self, tmp_path):
        from datapipe.io.parquet import ParquetSource

        path = self._dataset(tmp_path, files=4, rows=25)
        source = ParquetSource(path)
        rows = len(list(source.iter_shard(0, 1)))
        assert rows == 100
        assert source.total == 100

    def test_unsharded_total_is_full_dataset(self, tmp_path):
        from datapipe.io.parquet import ParquetSource

        path = self._dataset(tmp_path, files=4, rows=25)
        source = ParquetSource(path)
        assert source.total == 100
        assert len(list(source)) == 100
        assert source.total == 100

    def test_filtered_shard_total_is_none_not_wrong(self, tmp_path):
        """A predicate is applied at scan time, so metadata counts would lie."""
        ds = pytest.importorskip("pyarrow.dataset")

        from datapipe.io.parquet import ParquetSource

        path = self._dataset(tmp_path, files=4, rows=25)
        source = ParquetSource(path, filters=ds.field("a") < 5)
        rows = len(list(source.iter_shard(0, 2)))
        assert rows == 10
        assert source.total is None

    def test_iter_for_runtime_reports_rank_share(self, tmp_path):
        """End-to-end: the progress total the runtime reads is the rank's own."""
        from datapipe.io.parquet import ParquetSource
        from datapipe.runtime.context import RuntimeContext
        from datapipe.sharding.none import NoSharding

        path = self._dataset(tmp_path, files=4, rows=25)
        source = ParquetSource(path)
        runtime = RuntimeContext(rank=1, world_size=4)
        rows = len(list(source.iter_for_runtime(runtime, NoSharding())))
        assert rows == 25
        assert source.total == 25

    def test_range_sharding_still_sees_full_total(self, tmp_path):
        """RangeSharding resolves its total before sharding is applied."""
        from datapipe.io.parquet import ParquetSource
        from datapipe.runtime.context import RuntimeContext
        from datapipe.sharding.range import RangeSharding

        path = self._dataset(tmp_path, files=4, rows=25)
        source = ParquetSource(path)
        runtime = RuntimeContext(rank=0, world_size=4)
        rows = len(list(source.iter_for_runtime(runtime, RangeSharding())))
        # Physical sharding wins, so the rank reads exactly its file share.
        assert rows == 25


# ---------------------------------------------------------------------------
# Task 5 — DATAPIPE_LOG_LEVEL and the start-up summary
# ---------------------------------------------------------------------------


class TestStartupSummary:
    def test_io_summary_names_source_and_sink(self, tmp_path, caplog):
        import logging

        src = _write_jsonl(tmp_path / "in.jsonl", [{"a": "[1]"}])
        out = str(tmp_path / "out.jsonl")
        with caplog.at_level(logging.INFO, logger="datapipe"):
            rc = main([
                "transform", "--executor", "sequential", "--no-progress",
                "fromjson(.a)", src, out,
            ])
        assert rc == 0
        messages = "\n".join(r.getMessage() for r in caplog.records)
        assert "JsonlSource" in messages and src in messages
        assert "JsonlSink" in messages and out in messages
        assert "%r" not in messages and "%s" not in messages

    def test_summary_includes_error_sink_when_configured(self, tmp_path, caplog):
        import logging

        src = _write_jsonl(tmp_path / "in.jsonl", [{"a": "[1]"}])
        errs = str(tmp_path / "errors.jsonl")
        with caplog.at_level(logging.INFO, logger="datapipe"):
            main([
                "transform", "--executor", "sequential", "--no-progress",
                "--errors", "skip", "--error-output", errs,
                "fromjson(.a)", src, str(tmp_path / "out.jsonl"),
            ])
        messages = "\n".join(r.getMessage() for r in caplog.records)
        assert "error_sink" in messages and errs in messages

    def test_log_level_env_var_is_documented(self):
        """DATAPIPE_LOG_LEVEL must be discoverable, not folklore."""
        from pathlib import Path

        root = Path(__file__).resolve().parent.parent
        docs = (root / "docs" / "cli.md").read_text() + (root / "README.md").read_text()
        assert "DATAPIPE_LOG_LEVEL" in docs


# ---------------------------------------------------------------------------
# Task 6 — documentation accuracy
# ---------------------------------------------------------------------------


class TestDocs:
    @staticmethod
    def _read(*parts) -> str:
        from pathlib import Path

        return (Path(__file__).resolve().parent.parent.joinpath(*parts)).read_text()

    def test_readme_layout_lists_every_top_level_package(self):
        layout = self._read("README.md")
        start = layout.index("## Project layout")
        section = layout[start:start + 1500]
        for pkg in ("dsl/", "stages/", "tools/", "io/", "execution/", "cli/"):
            assert pkg in section, f"{pkg} missing from README project layout"

    def test_cli_docs_document_transform_quoting(self):
        doc = self._read("docs", "cli.md")
        assert "bash" in doc
        assert "zsh" in doc
        # Single-quoting guidance is what keeps the shell out of the expression.
        assert "single quote" in doc.lower() or "single-quote" in doc.lower()

    def test_cli_docs_document_new_flags_and_command(self):
        doc = self._read("docs", "cli.md")
        assert "--input-format" in doc
        assert "--output-format" in doc
        assert "inspect-expression" in doc
