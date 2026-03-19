# Ray Project Guide for AI Coding Agents

Essential reference for AI agents working on the Ray distributed computing framework.

## Extended Documentation

- **[Architecture Analysis](ARCHITECTURE_ANALYSIS.md)** — deep-dive into layers, data flow, component internals
- **[Newbie Contributing Guide](CONTRIBUTING_GUIDE_FOR_NEWBIES.md)** — setup, finding issues, PR workflow
- **[Improvement Opportunities](IMPROVEMENT_OPPORTUNITIES.md)** — concrete tasks: docs, error messages, tests, refactors
- **[Performance Analysis](PERFORMANCE_ANALYSIS.md)** — performance bottlenecks and optimization opportunities: scheduler, serialization, object store, Ray Data, gRPC

---

## Project Overview

Ray is a unified framework for scaling AI and Python applications.

| Module | Role |
|--------|------|
| **Ray Core** | Distributed runtime: Tasks (stateless), Actors (stateful), Objects (immutable) |
| **Ray Data** | Scalable ML datasets |
| **Ray Train** | Distributed training |
| **Ray Tune** | Hyperparameter tuning |
| **Ray Serve** | Model serving |
| **Ray RLlib** | Reinforcement learning |
| **Ray AIR** | Unified ML platform abstraction |

Website: https://docs.ray.io/

---

## Technology Stack

| Component | Technology |
|-----------|------------|
| Languages | Python >=3.9, C++ (C++17) |
| Build | Bazel **6.5.0** (exact — see `.bazelversion`) |
| RPC | gRPC + Protocol Buffers |
| State store | Redis (embedded in release builds) |
| Python-C++ binding | Cython (`python/ray/_raylet.pyx`) |
| Python formatter | Black 22.10.0 (88-char lines) |
| Python linter | Ruff |
| C++ formatter | clang-format 12.0.1 |
| C++ linter | cpplint |
| Test runner | pytest (180s default timeout) |
| CI/CD | Buildkite |

---

## Project Structure

```
ray/
├── python/ray/           # Main Python codebase
│   ├── _private/         # Private implementation
│   ├── actor.py          # Actor API
│   ├── remote_function.py
│   ├── data/             # Ray Data
│   ├── train/            # Ray Train
│   ├── tune/             # Ray Tune
│   ├── serve/            # Ray Serve
│   ├── dashboard/        # Web UI (React in client/)
│   └── tests/            # Core Python tests
├── src/ray/              # C++ core
│   ├── core_worker/
│   ├── gcs/
│   ├── raylet/
│   ├── object_manager/   # Plasma store
│   └── protobuf/         # .proto definitions
├── rllib/
├── doc/
├── ci/
└── release/              # Release/benchmark tests
```

---

## Architecture

### Code Layers

```
AI Libraries (Data / Train / Tune / Serve / RLlib)
        ↓
Ray Core Python API  (python/ray/__init__.py, actor.py, remote_function.py)
        ↓
_private Python layer  (python/ray/_private/worker.py)
        ↓
Cython binding  (python/ray/_raylet.pyx → _raylet.so)
        ↓
C++ CoreWorker  (src/ray/core_worker/)
        ↓
Raylet / GCS  (src/ray/raylet/, src/ray/gcs/)
```

### Process Architecture

| Process | Node | Role |
|---------|------|------|
| GCS Server | head | Global Control Service — cluster metadata via Redis |
| Raylet | every | Node manager — scheduling, worker pool, resources |
| Worker | every | Executes tasks/actors; wraps C++ `CoreWorker` |
| Dashboard | head | Web UI |

### Module Dependencies

```
ray.train  →  ray.data, ray.tune
ray.serve  →  ray._private.worker
ray.data   →  ray._private.worker
ray.tune   →  ray._private.worker
ray.{all}  →  ray._raylet (C++ binding)
```

### Object Store

Plasma (shared memory) for zero-copy object sharing within a node. Spills to local disk or remote storage (S3/GCS/HDFS) when memory is exhausted.

---

## Key Files

### Python

| File | Role |
|------|------|
| `python/ray/__init__.py` | Public API entry point |
| `python/ray/_private/worker.py` | Core `Worker` class |
| `python/ray/remote_function.py` | `@ray.remote` for functions |
| `python/ray/actor.py` | `@ray.remote` for classes |
| `python/ray/exceptions.py` | Exception hierarchy |
| `python/ray/_private/ray_constants.py` | Constants and env var definitions |

### C++

| Path | Role |
|------|------|
| `src/ray/core_worker/` | Task execution, object refs, task manager |
| `src/ray/raylet/` | Node manager, scheduler, worker pool |
| `src/ray/gcs/` | Actor, node, job, resource managers |
| `src/ray/object_manager/` | Plasma shared-memory store |
| `src/ray/protobuf/` | All `.proto` definitions |

---

## Build Commands

```bash
# Dev install
pip install -e python/

# Full Python package
bazel build //:ray_pkg

# Python protos
bazel build //:install_py_proto

# Raylet (C++ node manager)
bazel build //src/ray/raylet

# GCS server
bazel build //src/ray/gcs:gcs_server

# clangd compile_commands.json
bazel run //:refresh_compile_commands
```

### Build Configurations

| Config | Description |
|--------|-------------|
| `-c opt` | Optimized (default) |
| `-c dbg` | Debug |
| `--config=asan` | AddressSanitizer |
| `--config=tsan` | ThreadSanitizer |
| `--config=ci` | CI settings |

---

## Testing

```bash
# Python test file
pytest python/ray/tests/test_basic.py -v

# Single test
pytest python/ray/tests/test_basic.py::test_function -v

# Longer timeout (default 180s)
pytest python/ray/tests/test_basic.py --timeout=300

# C++ tests
bazel test //src/ray/common:test_util --test_output=errors
bazel test //src/ray/core_worker:all
```

Test locations:
- Python: `python/ray/<module>/tests/`
- C++: `src/ray/<module>/test/` or `*_test.cc`
- Release/benchmark: `release/`

---

## Code Style

```bash
black python/ray/          # format
ruff check python/ray/     # lint
pre-commit run --all-files # all checks
```

- Python: Black (88 chars), Ruff, Google-style docstrings
- C++: clang-format 12.0.1, cpplint, C++17
- Bazel: Buildifier

---

## Proto Changes

After editing `.proto` files in `src/ray/protobuf/`:
```bash
bazel build //:install_py_proto
```
Generated files in `python/ray/core/generated/` and `python/ray/serve/generated/` — **do not edit manually**.

---

## Development Workflow

### Setup

```bash
git clone https://github.com/ray-project/ray.git
cd ray
pip install -e python/
pre-commit install
```

### Making Changes

| Change type | Location | Verify |
|-------------|----------|--------|
| Python | `python/ray/` | pytest |
| C++ | `src/ray/` | bazel build + bazel test |
| Proto | `src/ray/protobuf/` | bazel build //:install_py_proto |
| Public API | `python/ray/__init__.py` | pytest |

---

## CI/CD

Buildkite pipelines in `.buildkite/`:
- `pipeline.build.yml` — build
- `pipeline.test.yml` — basic tests
- `pipeline.ml.yml` — ML library tests
- `pipeline.gpu.yml` — GPU tests

---

## Key Configuration Files

| File | Purpose |
|------|---------|
| `.bazelversion` | Pins Bazel to 6.5.0 |
| `.bazelrc` | Build flags |
| `BUILD.bazel` | Root build file |
| `WORKSPACE` | External deps |
| `pyproject.toml` | Ruff config |
| `pytest.ini` | 180s test timeout |
| `.pre-commit-config.yaml` | All lint/format hooks |

---

## Important Notes

- **Bazel version**: Must be exactly 6.5.0 — use `bazelisk` to manage automatically
- **Generated files**: Never edit `python/ray/core/generated/`, `python/ray/cloudpickle/`, `python/ray/thirdparty_files/`
- **Debug logging**: `RAY_BACKEND_LOG_LEVEL=debug` for verbose C++ logs
- **Cleanup**: `ray stop --force` or kill `raylet`/`gcs_server` if ports conflict
- **Cython binding**: `python/ray/_raylet.pyx` — requires rebuild after C++ changes
- **Redis**: Embedded in release builds; used by GCS for cluster state

## Security

- No secrets in code — use environment variables
- Semgrep scanning via pre-commit hooks
- Ray supports TLS/mTLS for cluster communication

## Getting Help

- Docs: https://docs.ray.io/
- Forum: https://discuss.ray.io/
- Issues: https://github.com/ray-project/ray/issues
- Slack: https://www.ray.io/join-slack
- Contributing: see `CONTRIBUTING.rst`
