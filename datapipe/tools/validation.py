"""Static and dynamic validation of a provider .py file.

Static validation:
  - File size check (max 512 KiB)
  - UTF-8 decode
  - ``ast.parse`` for syntax
  - Duplicate ``@tool`` name detection (best-effort)

Dynamic validation runs the provider in a fresh subprocess so any import
side-effects are isolated from the coordinator process.  A helper script is
written to a temp file, executed with ``subprocess.run``, and cleaned up
regardless of outcome.
"""

from __future__ import annotations

import ast
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path


MAX_SOURCE_BYTES = 512 * 1024  # 512 KiB


class StaticValidationError(Exception):
    """Raised when a provider file fails static validation."""


class ProviderValidationError(Exception):
    """Raised when dynamic import/introspection of a provider fails."""


@dataclass
class ProviderMetadata:
    """Metadata collected by dynamic validation."""

    tools: list[dict]  # list of serialised tool-contract dicts


# ---------------------------------------------------------------------------
# Static validation
# ---------------------------------------------------------------------------

def validate_static(path: Path) -> bytes:
    """Validate *path* statically and return its raw bytes.

    Steps:
      1. Verify it is a regular file.
      2. Read bytes; reject if larger than MAX_SOURCE_BYTES.
      3. Decode as UTF-8.
      4. Parse with ``ast.parse``; surface any SyntaxError.
      5. Walk the AST to find ``@tool(...)`` candidate names.
      6. Check for duplicate tool names.

    Returns the raw file bytes on success.
    Raises :class:`StaticValidationError` on any failure.
    """
    if not path.is_file():
        raise StaticValidationError(f"{path}: not a regular file")

    try:
        source_bytes = path.read_bytes()
    except OSError as exc:
        raise StaticValidationError(f"{path}: cannot read file: {exc}") from exc

    if len(source_bytes) > MAX_SOURCE_BYTES:
        raise StaticValidationError(
            f"{path}: file too large ({len(source_bytes)} bytes; max {MAX_SOURCE_BYTES})"
        )

    try:
        source_text = source_bytes.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise StaticValidationError(f"{path}: not valid UTF-8: {exc}") from exc

    try:
        tree = ast.parse(source_text, filename=str(path))
    except SyntaxError as exc:
        raise StaticValidationError(
            f"{path}: syntax error: {exc.msg} (line {exc.lineno})"
        ) from exc

    # Best-effort duplicate @tool name detection on static call arguments.
    tool_names: list[str] = []
    for node in ast.walk(tree):
        if not isinstance(node, ast.FunctionDef):
            continue
        for decorator in node.decorator_list:
            if not isinstance(decorator, ast.Call):
                continue
            func = decorator.func
            is_tool = (
                (isinstance(func, ast.Name) and func.id == "tool")
                or (isinstance(func, ast.Attribute) and func.attr == "tool")
            )
            if not is_tool:
                continue
            for kw in decorator.keywords:
                if kw.arg == "name" and isinstance(kw.value, ast.Constant):
                    tool_names.append(str(kw.value.value))

    seen: set[str] = set()
    for name in tool_names:
        if not name.isidentifier():
            raise StaticValidationError(
                f"{path}: @tool name {name!r} is not a valid Python identifier; "
                "tool names must match [a-zA-Z_][a-zA-Z0-9_]*"
            )
        if name.startswith("_"):
            raise StaticValidationError(
                f"{path}: @tool name {name!r} starts with an underscore; "
                "tool names must be publicly accessible identifiers"
            )
        if name in seen:
            raise StaticValidationError(
                f"{path}: duplicate @tool name {name!r} found in the same file"
            )
        seen.add(name)

    return source_bytes


# ---------------------------------------------------------------------------
# Dynamic validation
# ---------------------------------------------------------------------------

# The helper script that runs inside the subprocess.
#
# Provider stdout is redirected to /dev/null during import so that top-level
# print() calls in the provider cannot corrupt the JSON protocol line.  Only
# the final json.dumps() output reaches the coordinator via stdout; all other
# provider output goes to stderr where it is captured and included in any
# error message.
_HELPER_TEMPLATE = """\
import sys, json, pathlib, os, types

# argv[1] names the provider file (used for sys.path seeding, the module
# __file__, and traceback filenames).  The bytes to execute arrive on stdin
# rather than being re-read from disk: the coordinator has already hashed
# and statically validated exactly these bytes, and re-reading the file here
# would open a TOCTOU window where a concurrent edit means we validate
# different content than we hashed.
provider_path = sys.argv[1]
p = pathlib.Path(provider_path)
sys.path.insert(0, str(p.parent))

source_bytes = sys.stdin.buffer.read()

# Redirect provider stdout to /dev/null during import to prevent top-level
# print() calls from corrupting the JSON protocol output.
_real_stdout = sys.stdout
sys.stdout = open(os.devnull, "w")
try:
    mod = types.ModuleType(p.stem)
    mod.__file__ = str(p)
    sys.modules[p.stem] = mod
    code = compile(source_bytes, str(p), "exec")
    exec(code, mod.__dict__)
finally:
    sys.stdout = _real_stdout

try:
    from datapipe.tools.types import describe as _describe
except Exception:
    def _describe(t):
        return repr(t)

tools = []
for attr_name in dir(mod):
    obj = getattr(mod, attr_name)
    contract = getattr(obj, "__tool_contract__", None)
    if contract is None:
        continue
    params = []
    for ps in contract.parameters:
        params.append({
            "name": ps.name,
            "default": ps.default,
            "required": ps.required,
        })
    try:
        input_type = _describe(contract.input_type)
        output_type = _describe(contract.output_type)
    except Exception:
        input_type = None
        output_type = None
    tools.append({
        "name": contract.name,
        "target": contract.target,
        "cardinality": contract.cardinality.value,
        "deterministic": contract.deterministic,
        "description": contract.description,
        "input": input_type,
        "output": output_type,
        "parameters": params,
    })

print(json.dumps(tools))
sys.exit(0)
"""


def validate_dynamic(
    path: Path,
    source_bytes: bytes,
    timeout: float = 30.0,
) -> ProviderMetadata:
    """Import the provider in a fresh subprocess and collect tool metadata.

    Raises :class:`ProviderValidationError` if the subprocess fails, times
    out, or returns malformed output.
    """
    tmp_helper = None
    try:
        fd, tmp_helper = tempfile.mkstemp(suffix="_dp_helper.py", prefix="datapipe_")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as fh:
                fh.write(_HELPER_TEMPLATE)
        except OSError:
            fd = -1
            raise

        # The child does not inherit sys.path, only the environment.  Seed
        # PYTHONPATH from the parent's sys.path so the provider can import
        # datapipe (and anything else the parent can) exactly as we can --
        # otherwise a provider doing `from datapipe.tools import tool` fails
        # with ModuleNotFoundError whenever datapipe is importable via an
        # editable install or the current working directory rather than
        # plain site-packages.
        child_env = dict(os.environ)
        parent_paths = [p for p in sys.path if p]
        existing = child_env.get("PYTHONPATH", "")
        if existing:
            parent_paths.append(existing)
        child_env["PYTHONPATH"] = os.pathsep.join(parent_paths)

        try:
            result = subprocess.run(
                [sys.executable, tmp_helper, str(path)],
                input=source_bytes,
                capture_output=True,
                timeout=timeout,
                env=child_env,
            )
        except subprocess.TimeoutExpired as exc:
            raise ProviderValidationError(
                f"{path}: dynamic validation timed out after {timeout}s"
            ) from exc

        if result.returncode != 0:
            stderr = result.stderr.decode("utf-8", errors="replace").strip()
            raise ProviderValidationError(
                f"{path}: provider import failed (exit {result.returncode}):\n{stderr}"
            )

        stdout = result.stdout.decode("utf-8", errors="replace").strip()
        try:
            raw = json.loads(stdout)
        except json.JSONDecodeError as exc:
            raise ProviderValidationError(
                f"{path}: helper produced invalid JSON: {exc}"
            ) from exc

        if not isinstance(raw, list):
            raise ProviderValidationError(
                f"{path}: helper output is not a JSON array"
            )

        return ProviderMetadata(tools=raw)

    finally:
        if tmp_helper is not None:
            try:
                os.unlink(tmp_helper)
            except OSError:
                pass


# ---------------------------------------------------------------------------
# Digest
# ---------------------------------------------------------------------------

def compute_digest(source_bytes: bytes) -> str:
    """Return ``"sha256:<hexdigest>"`` of *source_bytes*."""
    return "sha256:" + hashlib.sha256(source_bytes).hexdigest()
