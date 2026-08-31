"""Install-time functional smoke tests for a tool provider (plan §8.2/§8.3).

Three checks live here, all of them run by
:func:`~datapipe.tools.installer.install_provider` before anything is written
to the registry:

``run_examples``
    Executes every ``ToolExample`` declared by every ``@tool`` in the provider
    and validates the actual return value against the tool's declared
    ``output_type`` (and the example's declared ``output``).

``spawn_load_smoke_test``
    Loads the provider in a genuinely fresh ``multiprocessing`` *spawn* worker
    through the same :func:`datapipe.tools.loader.load_provider` path a real
    ``ProcessExecutor`` worker uses.  This catches providers that import fine
    in the installer process but cannot be resolved in actual workers —
    unpickleable module-level state, import side effects that only fire in a
    fresh interpreter, missing imports masked by the installer's own modules.

``check_metadata_limits`` / ``compare_static_dynamic``
    Cheap metadata hygiene checks on the collected contracts.

Isolation rules (both subprocess paths)
---------------------------------------
1. Provider source bytes are passed **over stdin** and exactly those bytes are
   executed.  The child never re-reads the file from disk: the coordinator has
   already hashed and statically validated exactly these bytes, and re-reading
   would reopen a TOCTOU window where a concurrent edit means we smoke-test
   different content than we install.
2. Provider stdout is redirected to ``os.devnull`` for the duration of import
   and example execution, so a top-level ``print()`` in the provider cannot
   corrupt the JSON protocol line.
"""

from __future__ import annotations

import ast
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass, field
from pathlib import Path


class ExampleValidationError(Exception):
    """Raised when a declared ``ToolExample`` fails to run or validate."""


class SpawnSmokeTestError(Exception):
    """Raised when the provider cannot be loaded in a fresh spawned worker."""


class MetadataLimitError(Exception):
    """Raised when tool names or descriptions violate size/character rules."""


# ---------------------------------------------------------------------------
# Metadata size and character limits (plan §8.2: "names and descriptions meet
# size and character rules")
# ---------------------------------------------------------------------------
#
# ``ToolContract.__post_init__`` already enforces that a name is a non-empty,
# non-underscore-leading Python identifier.  Two things it does not enforce,
# and that matter because these strings are echoed into CLI output and stored
# verbatim in registry JSON:
#
#   * Length.  A 10 KiB "name" is a valid identifier.  It would wreck the
#     ``datapipe tools list`` table and bloat every registry read.
#   * ASCII.  ``str.isidentifier()`` accepts non-ASCII identifiers, so
#     ``café`` and homoglyph names like ``tоjson`` (Cyrillic 'о') pass.  Those
#     are indistinguishable from their ASCII lookalikes in terminal output,
#     which makes them a plausible confusion vector when a user is deciding
#     whether to trust a provider.
#
# Descriptions additionally must not carry raw control characters: an ANSI
# escape or a lone carriage return in a description lets a provider rewrite
# what ``datapipe tools inspect`` appears to say.  Newline and tab are allowed
# because multi-line descriptions are normal and harmless.

MAX_TOOL_NAME_CHARS = 64
"""Upper bound on a tool name.  Generous for an identifier used in DSL source
while still keeping ``datapipe tools list`` output tabular."""

MAX_DESCRIPTION_CHARS = 2048
"""Upper bound on a tool description.  Long enough for a full paragraph of
help text; short enough that the registry JSON stays human-readable."""

_ASCII_IDENT_START = frozenset(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ_"
)
_ASCII_IDENT_REST = _ASCII_IDENT_START | frozenset("0123456789")

# Control characters that may not appear in a description.  Tab (0x09) and
# newline (0x0A) are deliberately excluded from the ban.
_FORBIDDEN_CONTROL = frozenset(
    [chr(c) for c in range(0x00, 0x20) if c not in (0x09, 0x0A)] + [chr(0x7F)]
)


def check_metadata_limits(tools: "list[dict]") -> None:
    """Validate name/description size and character rules for *tools*.

    *tools* is the serialised contract list produced by
    :func:`datapipe.tools.validation.validate_dynamic`.

    Raises :class:`MetadataLimitError` listing every violation found, so an
    author fixing a provider sees all of them in one pass.
    """
    problems: list[str] = []

    for entry in tools:
        name = entry.get("name")
        if not isinstance(name, str) or not name:
            problems.append(f"tool metadata has a missing or non-string name: {name!r}")
            continue

        if len(name) > MAX_TOOL_NAME_CHARS:
            problems.append(
                f"tool name {name[:32]!r}... is {len(name)} characters; "
                f"the maximum is {MAX_TOOL_NAME_CHARS}"
            )
        bad_chars = sorted(
            {c for c in name[1:] if c not in _ASCII_IDENT_REST}
            | ({name[0]} if name[0] not in _ASCII_IDENT_START else set())
        )
        if bad_chars:
            rendered = ", ".join(repr(c) for c in bad_chars)
            problems.append(
                f"tool name {name!r} contains non-ASCII or disallowed "
                f"characters ({rendered}); tool names must match "
                "[A-Za-z_][A-Za-z0-9_]* using ASCII characters only"
            )

        description = entry.get("description") or ""
        if not isinstance(description, str):
            problems.append(
                f"tool {name!r}: description must be a string, got "
                f"{type(description).__name__}"
            )
            continue
        if len(description) > MAX_DESCRIPTION_CHARS:
            problems.append(
                f"tool {name!r}: description is {len(description)} characters; "
                f"the maximum is {MAX_DESCRIPTION_CHARS}"
            )
        found_control = sorted({c for c in description if c in _FORBIDDEN_CONTROL})
        if found_control:
            rendered = ", ".join(repr(c) for c in found_control)
            problems.append(
                f"tool {name!r}: description contains control characters "
                f"({rendered}); only printable text, tab, and newline are allowed"
            )

    if problems:
        raise MetadataLimitError(
            "provider metadata violates naming rules:\n  - "
            + "\n  - ".join(problems)
        )


# ---------------------------------------------------------------------------
# Static vs dynamic metadata comparison (plan §8.2 final bullet)
# ---------------------------------------------------------------------------

def static_tool_names(source_bytes: bytes, path: Path) -> "list[str]":
    """Return ``@tool(name=...)`` names visible on *module-level* functions.

    Deliberately narrower than the walk in
    :func:`datapipe.tools.validation.validate_static`, which uses
    ``ast.walk`` and therefore also sees ``@tool`` on functions nested inside
    other functions or inside ``if`` blocks.  Those legitimately may not exist
    as module attributes after import, so including them here would produce
    false "declared but never materialised" reports.  Only unconditional
    module-level definitions are compared.
    """
    try:
        tree = ast.parse(source_bytes.decode("utf-8"), filename=str(path))
    except (SyntaxError, UnicodeDecodeError):  # pragma: no cover — caught earlier
        return []

    names: list[str] = []
    for node in tree.body:
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            func = decorator.func
            is_tool = (isinstance(func, ast.Name) and func.id == "tool") or (
                isinstance(func, ast.Attribute) and func.attr == "tool"
            )
            if not is_tool:
                continue
            for kw in decorator.keywords:
                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    if isinstance(kw.value.value, str):
                        names.append(kw.value.value)
    return names


@dataclass
class MetadataComparison:
    """Result of comparing statically visible names to imported contracts."""

    missing_dynamically: list[str] = field(default_factory=list)
    """Names declared at module level in the source but absent after import."""

    only_dynamically: list[str] = field(default_factory=list)
    """Contracts found after import with no statically visible ``name=``."""

    def describe(self) -> str:
        parts = []
        if self.missing_dynamically:
            parts.append(
                "declared with @tool(name=...) at module level but not present "
                "after import: " + ", ".join(sorted(self.missing_dynamically))
            )
        if self.only_dynamically:
            parts.append(
                "present after import but not statically declared: "
                + ", ".join(sorted(self.only_dynamically))
            )
        return "; ".join(parts)


def compare_static_dynamic(
    source_bytes: bytes,
    path: Path,
    dynamic_tools: "list[dict]",
) -> MetadataComparison:
    """Compare statically visible ``@tool`` names against imported contracts.

    A name in ``missing_dynamically`` is an error: the source says a tool
    exists and the import says it does not, so installing would register a
    provider whose advertised surface does not match its real one.  Common
    causes are a later redefinition shadowing the decorated function or a
    module-level ``del``.

    A name in ``only_dynamically`` is *not* an error — a computed name
    (``name=_PREFIX + "x"``) is legitimate and simply invisible to the AST —
    but it is worth surfacing so the user can see the full tool list before
    confirming.
    """
    static = set(static_tool_names(source_bytes, path))
    dynamic = {t["name"] for t in dynamic_tools if isinstance(t.get("name"), str)}
    return MetadataComparison(
        missing_dynamically=sorted(static - dynamic),
        only_dynamically=sorted(dynamic - static),
    )


# ---------------------------------------------------------------------------
# Example execution (plan §8.3)
# ---------------------------------------------------------------------------

# Helper executed in a fresh subprocess.  Contract:
#   argv[1]  = provider path (module __file__, traceback filenames, sys.path)
#   stdin    = the exact provider source bytes to execute
#   stdout   = exactly one JSON line: {"failures": [...], "ran": N, "tools": M}
#   stderr   = free-form provider noise, captured for error messages
_EXAMPLE_HELPER = r'''
import sys, json, pathlib, os, types

provider_path = sys.argv[1]
p = pathlib.Path(provider_path)
sys.path.insert(0, str(p.parent))

# The bytes to execute arrive on stdin rather than being re-read from disk:
# the coordinator hashed and statically validated exactly these bytes, and
# re-reading here would reopen a TOCTOU window.
source_bytes = sys.stdin.buffer.read()


def _fmt(value):
    """Render a value for a diagnostic without risking a serialisation error."""
    try:
        text = repr(value)
    except Exception:
        text = "<unrepresentable>"
    return text if len(text) <= 300 else text[:297] + "..."


def _strict_eq(a, b):
    """Compare with JSON semantics, keeping bool distinct from int.

    ``True == 1`` in Python, but an example declaring output ``1`` is not
    satisfied by a tool returning ``True``.
    """
    if isinstance(a, bool) != isinstance(b, bool):
        return False
    if isinstance(a, dict) and isinstance(b, dict):
        if a.keys() != b.keys():
            return False
        return all(_strict_eq(a[k], b[k]) for k in a)
    if isinstance(a, list) and isinstance(b, list):
        if len(a) != len(b):
            return False
        return all(_strict_eq(x, y) for x, y in zip(a, b))
    if isinstance(a, (dict, list)) or isinstance(b, (dict, list)):
        return False
    try:
        return bool(a == b)
    except Exception:
        return False


# Redirect provider stdout to devnull for import *and* example execution so a
# print() anywhere in provider code cannot corrupt the JSON protocol line.
_real_stdout = sys.stdout
sys.stdout = open(os.devnull, "w")
try:
    mod = types.ModuleType(p.stem)
    mod.__file__ = str(p)
    sys.modules[p.stem] = mod
    exec(compile(source_bytes, str(p), "exec"), mod.__dict__)

    from datapipe.tools.types import describe as _describe
    from datapipe.tools.types import infer_json_type as _infer

    def _actual_type(value):
        jt = _infer(value)
        return jt.value if jt is not None else type(value).__name__

    failures = []
    ran = 0
    tools_with_examples = 0

    seen = set()
    for attr_name in sorted(dir(mod)):
        obj = getattr(mod, attr_name, None)
        contract = getattr(obj, "__tool_contract__", None)
        if contract is None or not callable(obj):
            continue
        if contract.name in seen:
            continue
        seen.add(contract.name)

        examples = tuple(getattr(contract, "examples", ()) or ())
        if examples:
            tools_with_examples += 1

        for index, example in enumerate(examples):
            ran += 1
            label = getattr(example, "description", "") or "example %d" % index
            base = {
                "tool": contract.name,
                "example": label,
                "index": index,
                "input": _fmt(example.input),
                "arguments": _fmt(getattr(example, "arguments", {}) or {}),
            }

            # The example input must itself satisfy the declared input
            # contract; otherwise the example documents a call the runtime
            # would reject before ever reaching the tool.
            if not contract.input_type.matches(example.input):
                failures.append(dict(base,
                    violated="input_type",
                    expected=_describe(contract.input_type),
                    actual=_actual_type(example.input),
                    detail="example input does not satisfy the declared input type",
                ))
                continue

            try:
                result = obj(example.input, **(getattr(example, "arguments", {}) or {}))
            except BaseException as exc:
                failures.append(dict(base,
                    violated="raised",
                    expected=_fmt(example.output),
                    actual="%s: %s" % (type(exc).__name__, exc),
                    detail="example raised an exception instead of returning",
                ))
                continue

            if not contract.output_type.matches(result):
                failures.append(dict(base,
                    violated="output_type",
                    expected=_describe(contract.output_type),
                    actual="%s (%s)" % (_fmt(result), _actual_type(result)),
                    detail="tool output does not satisfy the declared output type",
                ))
                continue

            if not _strict_eq(result, example.output):
                failures.append(dict(base,
                    violated="output_value",
                    expected=_fmt(example.output),
                    actual=_fmt(result),
                    detail="tool output differs from the example's declared output",
                ))

    payload = {"failures": failures, "ran": ran, "tools": tools_with_examples}
finally:
    sys.stdout = _real_stdout

print(json.dumps(payload))
sys.exit(0)
'''


def _child_env() -> "dict[str, str]":
    """Return an environment whose PYTHONPATH mirrors the parent's sys.path.

    A spawned child inherits the environment but not ``sys.path``, so without
    this a provider doing ``from datapipe.tools import tool`` fails with
    ``ModuleNotFoundError`` whenever datapipe is importable via an editable
    install or the working directory rather than plain site-packages.
    """
    env = dict(os.environ)
    paths = [p for p in sys.path if p]
    existing = env.get("PYTHONPATH", "")
    if existing:
        paths.append(existing)
    env["PYTHONPATH"] = os.pathsep.join(paths)
    return env


@dataclass
class ExampleReport:
    """Outcome of running a provider's declared examples."""

    examples_run: int
    tools_with_examples: int


def run_examples(
    path: Path,
    source_bytes: bytes,
    timeout: float = 30.0,
) -> ExampleReport:
    """Run every declared ``ToolExample`` in a subprocess and validate output.

    For each example the following are checked, in order, stopping at the
    first failure for that example:

    1. the example's input satisfies the tool's declared ``input_type``;
    2. calling the tool does not raise;
    3. the return value satisfies the tool's declared ``output_type``;
    4. the return value equals the example's declared ``output``.

    Returns an :class:`ExampleReport` when every example passes.  Raises
    :class:`ExampleValidationError` describing every failure otherwise, or if
    the subprocess crashes, times out, or emits malformed output.
    """
    import json as _json

    tmp_helper: "str | None" = None
    try:
        fd, tmp_helper = tempfile.mkstemp(suffix="_dp_examples.py", prefix="datapipe_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(_EXAMPLE_HELPER)
        except OSError:
            raise

        try:
            result = subprocess.run(
                [sys.executable, tmp_helper, str(path)],
                input=source_bytes,
                capture_output=True,
                timeout=timeout,
                env=_child_env(),
            )
        except subprocess.TimeoutExpired as exc:
            raise ExampleValidationError(
                f"{path}: example execution timed out after {timeout}s; "
                "a declared example appears to hang"
            ) from exc

        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            raise ExampleValidationError(
                f"{path}: example execution failed (exit {result.returncode}):\n{stderr}"
            )

        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        try:
            payload = _json.loads(stdout)
        except ValueError as exc:
            raise ExampleValidationError(
                f"{path}: example helper produced invalid JSON: {exc}"
            ) from exc
        if not isinstance(payload, dict) or "failures" not in payload:
            raise ExampleValidationError(
                f"{path}: example helper produced an unexpected payload: {stdout[:200]!r}"
            )

        failures = payload["failures"]
        if failures:
            raise ExampleValidationError(_format_failures(path, failures))

        return ExampleReport(
            examples_run=int(payload.get("ran", 0)),
            tools_with_examples=int(payload.get("tools", 0)),
        )
    finally:
        if tmp_helper is not None:
            try:
                os.unlink(tmp_helper)
            except OSError:
                pass


def _format_failures(path: Path, failures: "list[dict]") -> str:
    """Render example failures with enough context to act on."""
    lines = [
        f"{path}: {len(failures)} declared example(s) failed validation:",
    ]
    for f in failures:
        lines.append(
            f"  - tool {f.get('tool')!r} example {f.get('index')} "
            f"({f.get('example')}): {f.get('detail')}"
        )
        lines.append(f"      input:     {f.get('input')}")
        arguments = f.get("arguments")
        if arguments and arguments not in ("{}",):
            lines.append(f"      arguments: {arguments}")
        lines.append(f"      expected:  {f.get('expected')}")
        lines.append(f"      actual:    {f.get('actual')}")
        lines.append(f"      violated:  {f.get('violated')}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Spawn load smoke test (plan §8.2/§8.3)
# ---------------------------------------------------------------------------

# Helper executed as a subprocess whose *own* ``__main__`` launches the real
# multiprocessing spawn child.
#
# Why the extra process layer instead of calling ctx.Process() directly from
# the installer: the spawn start method re-imports the parent's ``__main__``
# module in the child.  When install_provider() is called from a script that
# lacks an ``if __name__ == "__main__":`` guard, that re-import re-runs the
# script and multiprocessing aborts the child with a RuntimeError — an install
# failure caused entirely by the *caller's* file layout, not by the provider.
# Running the spawn from this guarded helper makes the smoke test independent
# of how the installer itself was invoked, while the inner child is still a
# genuine multiprocessing spawn worker.
#
#   argv[1] = JSON descriptor fields
#   stdout  = exactly one JSON line: {"ok": bool, "tools": [...], "detail": str}
_SPAWN_HELPER = r'''
import sys, json, os


def _probe(fields, result_queue):
    """Load the provider exactly as a ProcessExecutor worker does."""
    import os as _os
    import sys as _sys
    import tempfile as _tempfile

    # The spawn helper is written to a tempfile, so the system temp root
    # (e.g. /tmp) ends up on sys.path[0].  A provider whose source also lives
    # in /tmp could then resolve sibling imports from there during the smoke
    # test even though those siblings are absent from the installed snapshot.
    # Remove every sys.path entry that resolves to the temp root so the child
    # sees the same clean import environment that real workers see.
    _tmp_root = _os.path.realpath(_tempfile.gettempdir())
    _sys.path = [p for p in _sys.path if _os.path.realpath(p) != _tmp_root]

    # A top-level print() in the provider must not reach our stdout protocol.
    _real_stdout = _sys.stdout
    try:
        _sys.stdout = open(_os.devnull, "w")
    except OSError:
        pass
    try:
        from datapipe.tools.descriptor import ProviderDescriptor
        from datapipe.tools.loader import load_provider

        entry = load_provider(ProviderDescriptor(**fields))
        result_queue.put((True, sorted(entry["tools"])))
    except BaseException as exc:
        # Never let this escape: a provider exception (or its __cause__ chain)
        # may be unpickleable, and losing the real diagnostic to a pickling
        # error in the result channel would make the failure unactionable.
        import traceback as _tb
        try:
            detail = "".join(
                _tb.format_exception(type(exc), exc, exc.__traceback__)
            ).strip()
        except Exception:
            detail = "%s: %s" % (type(exc).__name__, exc)
        try:
            result_queue.put((False, detail))
        except Exception:
            pass
    finally:
        try:
            _sys.stdout.close()
        except Exception:
            pass
        _sys.stdout = _real_stdout


def main():
    import multiprocessing
    import queue as _queue

    fields = json.loads(sys.argv[1])
    ctx = multiprocessing.get_context("spawn")
    result_queue = ctx.Queue()
    proc = ctx.Process(target=_probe, args=(fields, result_queue), daemon=True)
    proc.start()

    payload = None
    try:
        while True:
            try:
                payload = result_queue.get(timeout=0.1)
                break
            except _queue.Empty:
                if not proc.is_alive():
                    # The child is gone.  Give the queue feeder thread one
                    # last chance to flush an already-enqueued result rather
                    # than blocking until the outer timeout.
                    try:
                        payload = result_queue.get(timeout=0.5)
                    except _queue.Empty:
                        payload = None
                    break
    finally:
        exitcode = proc.exitcode
        if proc.is_alive():
            proc.terminate()
            proc.join(timeout=5)
            if proc.is_alive():
                proc.kill()
                proc.join(timeout=5)
        else:
            proc.join(timeout=5)
        try:
            result_queue.close()
            result_queue.join_thread()
        except Exception:
            pass

    if payload is None:
        out = {
            "ok": False,
            "tools": [],
            "detail": (
                "the spawned worker exited with code %r without reporting a "
                "result" % (exitcode,)
            ),
        }
    else:
        ok, detail = payload
        out = {"ok": bool(ok), "tools": detail if ok else [], "detail": "" if ok else detail}

    print(json.dumps(out))
    sys.exit(0)


if __name__ == "__main__":
    main()
'''


def spawn_load_smoke_test(
    *,
    provider_id: str,
    alias: str,
    mode: str,
    source_path: str,
    digest: str,
    expected_tools: "list[str] | None" = None,
    timeout: float = 60.0,
) -> "list[str]":
    """Load the provider in a fresh spawned worker; return the tool names found.

    Uses the ``spawn`` start method explicitly (the ``ProcessExecutor``
    default) rather than the platform default, because the point of the check
    is to catch spawn-only breakage: module-level state that does not survive
    a fresh interpreter, import side effects that only fire when the parent's
    already-imported modules are absent, and providers that resolve in the
    installer but not in a worker.

    Raises :class:`SpawnSmokeTestError` if the worker cannot load the
    provider, hangs, dies, or reports a tool set that disagrees with
    *expected_tools*.
    """
    import json as _json

    fields = {
        "provider_id": provider_id,
        "alias": alias,
        "mode": mode,
        "source_path": source_path,
        "sha256": digest,
        "api_version": 1,
    }

    tmp_helper: "str | None" = None
    try:
        fd, tmp_helper = tempfile.mkstemp(suffix="_dp_spawn.py", prefix="datapipe_")
        with os.fdopen(fd, "w", encoding="utf-8") as fh:
            fh.write(_SPAWN_HELPER)

        try:
            result = subprocess.run(
                [sys.executable, tmp_helper, _json.dumps(fields)],
                capture_output=True,
                timeout=timeout,
                env=_child_env(),
            )
        except subprocess.TimeoutExpired as exc:
            raise SpawnSmokeTestError(
                f"provider {provider_id!r}: spawn load smoke test timed out after "
                f"{timeout}s; the provider appears to hang on import in a fresh "
                "worker interpreter"
            ) from exc

        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            raise SpawnSmokeTestError(
                f"provider {provider_id!r}: spawn load smoke test could not run "
                f"(exit {result.returncode}):\n{stderr}"
            )

        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        try:
            payload = _json.loads(stdout)
        except ValueError as exc:
            raise SpawnSmokeTestError(
                f"provider {provider_id!r}: spawn smoke test produced invalid "
                f"output: {stdout[:200]!r}"
            ) from exc

        if not payload.get("ok"):
            raise SpawnSmokeTestError(
                f"provider {provider_id!r}: cannot be loaded in a fresh spawned "
                f"worker, so it would fail at runtime for every record:\n"
                f"{payload.get('detail', '')}"
            )

        found = list(payload.get("tools", []))
    finally:
        if tmp_helper is not None:
            try:
                os.unlink(tmp_helper)
            except OSError:
                pass

    if expected_tools is not None:
        missing = sorted(set(expected_tools) - set(found))
        extra = sorted(set(found) - set(expected_tools))
        if missing or extra:
            parts = []
            if missing:
                parts.append("missing in worker: " + ", ".join(missing))
            if extra:
                parts.append("unexpected in worker: " + ", ".join(extra))
            raise SpawnSmokeTestError(
                f"provider {provider_id!r}: the tool set resolved in a spawned "
                f"worker disagrees with the tool set collected during validation "
                f"({'; '.join(parts)})"
            )
    return found
