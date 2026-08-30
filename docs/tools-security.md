# Security and Trust

## Provider code executes with your permissions

When you install a provider and run `datapipe transform`, the tool functions
in that provider execute inside datapipe worker processes with the same
operating-system permissions as your user account. This means an installed
provider can read files, make network requests, spawn subprocesses, write to
disk, or take any other action your account is allowed to take.

datapipe validation (static AST analysis, isolated import, signature checks,
and example smoke tests) can catch mistakes and some forms of malformed code.
It cannot make hostile Python safe, detect obfuscated actions, or sandbox
execution. Treat an installed provider the same way you would treat adding a
new Python dependency to your project.

## Trust model

Install providers only from sources you control or trust:

- your own files;
- files from colleagues whose code you can review;
- open-source providers whose source you have read.

Do not install providers from untrusted URLs, paste bins, or third parties
without reviewing the source code first.

## Installation confirmation prompt

The installer always prints a confirmation prompt showing the provider ID,
source path, mode, and tool names before writing anything to the registry:

```
Provider: local:my-tools
Source:   /absolute/path/my_tools.py
Mode:     copied
Tools:    normalize_text, redact

This provider contains executable Python and will run inside datapipe workers
with your user permissions. Install? [y/N]
```

For CI/CD pipelines where you have already reviewed the code, pass `--yes` to
skip the prompt:

```bash
datapipe tools install --yes ./my_tools.py
```

## Registry location

The registry lives at:

```
~/.local/share/datapipe/registry.json
```

Override with the `DATAPIPE_USER_DATA` environment variable:

```bash
DATAPIPE_USER_DATA=/path/to/project-local/datapipe datapipe transform ...
```

This is useful for project-specific installs that should not pollute the
user-global registry, and for CI environments where you want a clean isolated
state.

## Copied vs. editable mode

**Copied mode** (default) takes a snapshot of the source bytes at install time
and stores them in the registry directory. Later edits to the original file do
not affect the installed snapshot. Workers verify the snapshot's SHA-256
digest before loading it; a corrupted or modified snapshot is rejected.

**Editable mode** (`--editable`) stores a pointer to the original file without
copying it. Workers load whatever bytes are currently on disk. This mode is
intended for development workflows where you edit the provider file and want
the next run to pick up your changes immediately. It should not be used in
production deployments where reproducibility matters, because the tool behavior
can change between runs without any registry action.

## Digest verification

For copied providers, every worker verifies the SHA-256 digest of the snapshot
before importing it. If the snapshot has been modified after installation —
whether by accident, by a concurrent process, or deliberately — the worker
rejects it and logs a warning. The run continues with other providers intact;
only the broken provider's tools become unavailable.

For editable providers, digest enforcement is intentionally skipped because
live editing is the entire purpose of the mode. Workers always load the current
bytes on disk.

## Subprocess isolation during validation

Dynamic validation imports the provider in a fresh subprocess with a configurable
timeout (default 30 seconds). The subprocess has a separate Python interpreter
and cannot interfere with the installer process. Its stdout is strictly
captured; any accidental prints are separated from the protocol output.

If the provider hangs during import, the subprocess is killed after the timeout
and installation fails cleanly.

## What validation does and does not guarantee

Validation checks:

- file size, UTF-8 encoding, and syntax (static);
- that the module can be imported without hanging (dynamic);
- that all `@tool`-decorated functions have valid contracts;
- that declared examples produce the expected output.

Validation does not check for:

- runtime behavior on arbitrary inputs beyond declared examples;
- network calls, file I/O, or other side effects during import or execution;
- code that is syntactically valid but semantically harmful;
- obfuscated or generated code.

The security boundary is social, not technical: review the source before
installing it.
