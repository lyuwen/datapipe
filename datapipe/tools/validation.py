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
import sys, json, pathlib, os, types, enum, typing

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

# ---------------------------------------------------------------------------
# Structured, reversible encoding of TypeSpec and parameter annotations.
#
# ``describe()`` is lossy: OneOf(STRING, ARRAY) renders as "string | array",
# which the coordinator cannot reliably parse back, so every composite
# contract used to degrade to JsonType.ANY and accept anything.  These
# encoders emit a tagged JSON structure that datapipe.dsl.compiler decodes
# exactly.  Both are defensive: anything they cannot represent becomes an
# {"kind": "unsupported", "reason": ...} node so a single odd annotation
# degrades one parameter instead of failing the whole validation run.
# ---------------------------------------------------------------------------

try:
    from datapipe.tools.types import JsonType as _JsonType, OneOf as _OneOf, TypeSpec as _TypeSpec
except Exception:
    _JsonType = _OneOf = _TypeSpec = None


def _encode_type_spec(spec, depth=0):
    if depth > 8:
        return {"kind": "unsupported", "reason": "type nesting too deep"}
    if _JsonType is None:
        return {"kind": "unsupported", "reason": "datapipe.tools.types unavailable"}
    if isinstance(spec, _JsonType):
        return {"kind": "json_type", "name": spec.name}
    if _OneOf is not None and isinstance(spec, _OneOf):
        return {
            "kind": "one_of",
            "members": [_encode_type_spec(m, depth + 1) for m in spec.members],
        }
    jt = getattr(spec, "json_type", None)
    if isinstance(jt, _JsonType):
        return {"kind": "json_type", "name": jt.name}
    return {
        "kind": "unsupported",
        "reason": "unrecognised TypeSpec subclass " + type(spec).__name__,
    }


_ANN_BASE_NAMES = {
    str: "str", int: "int", float: "float", bool: "bool",
    list: "list", dict: "dict", type(None): "None",
}


def _encode_annotation(ann, depth=0):
    if depth > 8:
        return {"kind": "unsupported", "reason": "annotation nesting too deep"}
    if ann is None or ann is type(None):
        return {"kind": "base", "name": "None"}
    if ann is typing.Any:
        return {"kind": "any"}
    if isinstance(ann, str):
        # PEP 563 annotation the provider's own get_type_hints() could not
        # resolve; pass the text through so the coordinator can report it.
        return {"kind": "unresolved", "text": ann}
    if isinstance(ann, type) and ann in _ANN_BASE_NAMES:
        return {"kind": "base", "name": _ANN_BASE_NAMES[ann]}
    if isinstance(ann, type) and issubclass(ann, enum.Enum):
        try:
            values = [m.value for m in ann]
            json.dumps(values)
        except Exception:
            return {
                "kind": "unsupported",
                "reason": "enum " + ann.__name__ + " has non-JSON member values",
            }
        return {"kind": "enum", "name": ann.__name__, "values": values}

    origin = typing.get_origin(ann)
    args = typing.get_args(ann)
    if origin is typing.Union or (
        getattr(types, "UnionType", None) is not None and origin is types.UnionType
    ):
        return {
            "kind": "union",
            "members": [_encode_annotation(a, depth + 1) for a in args],
        }
    if origin is typing.Literal:
        try:
            json.dumps(list(args))
        except Exception:
            return {"kind": "unsupported", "reason": "Literal has non-JSON values"}
        return {"kind": "literal", "values": list(args)}
    if origin in (list, set, frozenset, tuple, dict) and origin in _ANN_BASE_NAMES:
        return {
            "kind": "container",
            "origin": _ANN_BASE_NAMES[origin],
            "args": [_encode_annotation(a, depth + 1) for a in args],
        }

    return {
        "kind": "unsupported",
        "reason": "annotation " + repr(ann) + " has no structured encoding",
    }


tools = []
for attr_name in dir(mod):
    obj = getattr(mod, attr_name)
    contract = getattr(obj, "__tool_contract__", None)
    if contract is None:
        continue
    params = []
    for ps in contract.parameters:
        # Serialize annotation as a type-name string so the coordinator can
        # reconstruct ParameterSpec.annotation from registry JSON.
        ann = ps.annotation
        ann_name = None
        if ann is not None:
            ann_name = getattr(ann, "__name__", None) if ann is not type(None) else "None"
        try:
            ann_spec = _encode_annotation(ann) if ann is not None else None
        except Exception as exc:
            ann_spec = {"kind": "unsupported", "reason": "encoder failed: " + str(exc)}
        params.append({
            "name": ps.name,
            "default": ps.default,
            "required": ps.required,
            "annotation": ann_name,
            "annotation_spec": ann_spec,
        })
    try:
        input_type = _describe(contract.input_type)
        output_type = _describe(contract.output_type)
    except Exception:
        input_type = None
        output_type = None
    try:
        input_spec = _encode_type_spec(contract.input_type)
        output_spec = _encode_type_spec(contract.output_type)
    except Exception as exc:
        reason = {"kind": "unsupported", "reason": "encoder failed: " + str(exc)}
        input_spec = output_spec = reason
    tools.append({
        "name": contract.name,
        "target": contract.target,
        "cardinality": contract.cardinality.value,
        "deterministic": contract.deterministic,
        "description": contract.description,
        "input": input_type,
        "output": output_type,
        "input_spec": input_spec,
        "output_spec": output_spec,
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
