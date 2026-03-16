# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Ray is a unified distributed computing framework for scaling AI and Python applications. It consists of:
- **Ray Core**: Distributed runtime with Tasks (stateless functions), Actors (stateful workers), and Objects (immutable values)
- **Ray Data**: Scalable datasets for ML
- **Ray Train**: Distributed training
- **Ray Tune**: Hyperparameter tuning
- **Ray Serve**: Scalable model serving
- **Ray RLlib**: Reinforcement learning
- **Ray AIR**: Unified ML platform abstraction

## Technology Stack

- **Languages**: Python >=3.9, C++ (C++17)
- **Build**: Bazel **6.5.0** (exact version required — see `.bazelversion`)
- **RPC**: gRPC with Protocol Buffers
- **State store**: Redis (embedded in release builds)
- **Python-C++ binding**: Cython (`python/ray/_raylet.pyx`)

## Build Commands

```bash
# Install Ray in development mode
pip install -e python/

# Build the full Ray Python package
bazel build //:ray_pkg

# Build and install Python protos
bazel build //:install_py_proto

# Build raylet (C++ node manager)
bazel build //src/ray/raylet

# Build GCS server
bazel build //src/ray/gcs:gcs_server

# Generate clangd compile_commands.json
bazel run //:refresh_compile_commands
```

## Testing

```bash
# Run a Python test file
pytest python/ray/tests/test_basic.py -v

# Run a single test
pytest python/ray/tests/test_basic.py::test_function -v

# Run C++ tests via Bazel
bazel test //src/ray/common:test_util --test_output=errors

# Run all C++ tests in a module
bazel test //src/ray/core_worker:all

# Increase timeout (default is 180s from pytest.ini)
pytest python/ray/tests/test_basic.py --timeout=300
```

Test locations:
- Python unit tests: `python/ray/<module>/tests/`
- C++ tests: `src/ray/<module>/test/` or `*_test.cc`
- Release/benchmark tests: `release/`

## Code Style & Linting

```bash
# Format Python
black python/ray/

# Lint Python
ruff check python/ray/

# Run all pre-commit checks before committing
pre-commit run --all-files
```

- **Python line length**: 88 chars (Black default)
- **Python formatter**: Black 22.10.0
- **Python linter**: Ruff (see `pyproject.toml` for rules)
- **Docstring style**: Google style
- **C++ formatter**: clang-format 12.0.1
- **C++ linter**: cpplint
- **Bazel formatter**: Buildifier

## Architecture

### Process Architecture

A Ray cluster runs these key processes:
- **GCS Server** (head node): Global Control Service — manages cluster metadata (nodes, actors, jobs, resources) via Redis
- **Raylet** (every node): Node manager — handles local scheduling, worker pool, and resource management
- **Worker processes**: Execute tasks and actor methods; each wraps a C++ `CoreWorker`
- **Dashboard**: Web UI (React frontend in `python/ray/dashboard/client/`)

### Code Layers

```
AI Libraries (Data / Train / Tune / Serve / RLlib)
        ↓
Ray Core Python API  (python/ray/__init__.py, actor.py, remote_function.py)
        ↓
_private Python layer  (python/ray/_private/worker.py — the main Worker class)
        ↓
Cython binding  (python/ray/_raylet.pyx → _raylet.so)
        ↓
C++ CoreWorker  (src/ray/core_worker/)
        ↓
Raylet / GCS  (src/ray/raylet/, src/ray/gcs/)
```

### Key Python Files

| File | Role |
|------|------|
| `python/ray/__init__.py` | Public API entry point |
| `python/ray/_private/worker.py` | Core `Worker` class; task submission, object store interaction |
| `python/ray/remote_function.py` | `@ray.remote` decorator for functions |
| `python/ray/actor.py` | `@ray.remote` decorator for classes (actors) |
| `python/ray/exceptions.py` | Exception hierarchy |
| `python/ray/_private/ray_constants.py` | Constants and environment variable definitions |

### Key C++ Components

| Path | Role |
|------|------|
| `src/ray/core_worker/` | Per-worker task execution, object references, task manager |
| `src/ray/raylet/` | Node manager, local/cluster task scheduler, worker pool |
| `src/ray/gcs/` | Global Control Service — actor, node, job, resource managers |
| `src/ray/object_manager/` | Plasma shared-memory object store |
| `src/ray/protobuf/` | All `.proto` definitions for gRPC |

### Object Store

Ray uses Plasma (shared memory) for zero-copy object sharing within a node. Objects spill to local disk or remote storage (S3/GCS/HDFS) when memory is exhausted.

### Module Dependencies

```
ray.train  →  ray.data, ray.tune
ray.serve  →  ray._private.worker
ray.data   →  ray._private.worker
ray.tune   →  ray._private.worker
ray.{all}  →  ray._raylet (C++ binding)
```

## Proto Changes

After editing `.proto` files in `src/ray/protobuf/`, regenerate Python bindings:
```bash
bazel build //:install_py_proto
```
Generated files live in `python/ray/core/generated/` and `python/ray/serve/generated/` — do not edit them manually.

## Key Configuration Files

| File | Purpose |
|------|---------|
| `.bazelversion` | Pins Bazel to 6.5.0 |
| `.bazelrc` | Build flags (opt/dbg, sanitizers, CI settings) |
| `BUILD.bazel` | Root Bazel build file |
| `WORKSPACE` | Bazel workspace and external deps |
| `pyproject.toml` | Ruff configuration |
| `pytest.ini` | Default 180s test timeout |
| `.pre-commit-config.yaml` | All linting/formatting hooks |

## Important Notes

- **Bazel version**: Must be exactly 6.5.0 (use `bazelisk` to manage this automatically)
- **Generated files**: Do not edit `python/ray/core/generated/`, `python/ray/cloudpickle/`, or `python/ray/thirdparty_files/`
- **Debug logging**: Set `RAY_BACKEND_LOG_LEVEL=debug` to enable verbose C++ logs
- **Cleanup after tests**: Use `ray stop --force` or kill `raylet`/`gcs_server` processes if ports conflict
