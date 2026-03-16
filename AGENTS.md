# Ray Project Guide for AI Coding Agents

This document provides essential information for AI coding agents working on the Ray project.

## Project Overview

Ray is a unified framework for scaling AI and Python applications. It consists of:

- **Ray Core**: Distributed runtime with Tasks (stateless functions), Actors (stateful workers), and Objects (immutable values)
- **Ray Data**: Scalable datasets for ML
- **Ray Train**: Distributed training framework
- **Ray Tune**: Scalable hyperparameter tuning
- **Ray RLlib**: Scalable reinforcement learning
- **Ray Serve**: Scalable and programmable model serving
- **Ray AIR**: Unified ML platform abstraction layer

Website: https://docs.ray.io/

## Technology Stack

| Component | Technology |
|-----------|------------|
| Core Languages | Python (>=3.9), C++ (C++17) |
| Build System | Bazel 6.5.0 |
| RPC Framework | gRPC |
| State Store | Redis |
| Python Linter | Ruff |
| Python Formatter | Black |
| C++ Linter | cpplint |
| Test Runner | pytest |
| CI/CD | Buildkite |

## Project Structure

```
ray/
├── python/ray/           # Main Python codebase
│   ├── _private/         # Private implementation details
│   ├── _common/          # Common utilities
│   ├── actor.py          # Actor API
│   ├── remote_function.py # Task/remote function API
│   ├── autoscaler/       # Autoscaling for clusters
│   ├── data/             # Ray Data (scalable datasets)
│   ├── train/            # Ray Train (distributed training)
│   ├── tune/             # Ray Tune (hyperparameter tuning)
│   ├── serve/            # Ray Serve (model serving)
│   ├── dashboard/        # Ray Dashboard
│   ├── dag/              # Ray DAG (computation graphs)
│   ├── air/              # Ray AIR (ML platform)
│   ├── includes/         # Cython headers (.pxd)
│   └── tests/            # Core Python tests
├── src/ray/              # C++ core implementation
│   ├── common/           # Common C++ utilities
│   ├── core_worker/      # Core worker implementation
│   ├── gcs/              # Global Control Service
│   ├── raylet/           # Raylet (node manager)
│   ├── object_manager/   # Object management
│   ├── pubsub/           # Pub/sub messaging
│   ├── protobuf/         # Protocol buffer definitions
│   └── util/             # Utility libraries
├── rllib/                # RLlib (reinforcement learning)
├── java/                 # Java language bindings
├── cpp/                  # C++ API
├── doc/                  # Documentation source
├── ci/                   # CI/CD scripts and configs
├── release/              # Release tests and benchmarks
├── bazel/                # Bazel build configuration
└── docker/               # Docker images
```

## Build System

### Prerequisites

- Python >= 3.9
- Bazel 6.5.0 (exact version required)
- C++ compiler with C++17 support
- uv (for wheel building): https://docs.astral.sh/uv/

### Key Build Commands

```bash
# Build Ray Python package (development)
bazel build //:ray_pkg

# Build and install Python protos
bazel build //:install_py_proto

# Build raylet (C++ node manager)
bazel build //src/ray/raylet

# Build GCS server
bazel build //src/ray/gcs:gcs_server

# Generate compile_commands.json for clangd
bazel run //:refresh_compile_commands

# Build wheels locally
./build-wheel.sh
```

### Build Configurations

| Config | Description |
|--------|-------------|
| `-c opt` | Optimized build (default) |
| `-c dbg` | Debug build |
| `--config=asan` | AddressSanitizer build |
| `--config=tsan` | ThreadSanitizer build |
| `--config=ci` | CI-specific settings |

## Testing

### Running Python Tests

```bash
# Run all tests with pytest (recommended for local development)
pytest python/ray/tests/test_basic.py -v

# Run specific test
pytest python/ray/tests/test_basic.py::test_function -v

# Run with timeout (default is 180s)
pytest python/ray/tests/test_basic.py --timeout=300
```

### Running C++ Tests

```bash
# Run C++ tests via Bazel
bazel test //src/ray/common:test_util

# Run with test output
bazel test //src/ray/common:test_util --test_output=errors

# Run all core worker tests
bazel test //src/ray/core_worker:all
```

### Running RLlib Tests

```bash
# RLlib tests are in rllib/ directory
pytest rllib/tests/test_catalog.py -v
```

### Test Organization

- Unit tests: `python/ray/<module>/tests/`
- Release tests: `release/`
- C++ tests: `src/ray/<module>/test/` or `*_test.cc` files

## Code Style Guidelines

### Python

- **Line length**: 88 characters (Black default)
- **Formatter**: Black (version 22.10.0)
- **Linter**: Ruff
- **Import style**: Google-style with `isort` sections
- **Docstrings**: Google style

Key lint rules in `pyproject.toml`:
```toml
[tool.ruff]
line-length = 88
extend-select = ["I", "B", "Q", "C4", "W"]
```

### C++

- **Standard**: C++17
- **Linter**: cpplint with custom filters
- **Formatter**: clang-format (version 12.0.1)

### Pre-commit Hooks

Run all checks before committing:
```bash
pre-commit run --all-files
```

Pre-commit includes:
- Ruff (Python linting)
- Black (Python formatting)
- cpplint (C++ linting)
- Buildifier (Bazel formatting)
- clang-format (C++ formatting)
- Google Java Format
- Shellcheck
- Semgrep security scanning

## Development Workflow

### Setting up Development Environment

1. Install dependencies:
```bash
pip install -e python/
```

2. Set up pre-commit hooks:
```bash
pre-commit install
```

3. Build Ray from source:
```bash
bazel build //:ray_pkg
```

### Making Changes

1. **Python changes**: Edit files in `python/ray/`, test with pytest
2. **C++ changes**: Edit files in `src/ray/`, rebuild with Bazel, test with bazel test
3. **Proto changes**: Edit `.proto` files in `src/ray/protobuf/`, rebuild protos
4. **API changes**: Update public APIs in `python/ray/__init__.py`

### Common Development Tasks

#### Adding a New Module

1. Create directory under appropriate location (e.g., `python/ray/mymodule/`)
2. Add `__init__.py` with public API exports
3. Add tests in `python/ray/mymodule/tests/`
4. Update BUILD.bazel if needed

#### Modifying Protocol Buffers

1. Edit `.proto` files in `src/ray/protobuf/`
2. Regenerate Python protos:
```bash
bazel build //:install_py_proto
```

#### Adding Tests

1. Add test files following naming convention: `test_*.py`
2. Use pytest fixtures from `python/ray/conftest.py` or local `conftest.py`
3. Set timeout if test may run longer than 180s: `@pytest.mark.timeout(300)`

## CI/CD

### Buildkite Pipelines

Pipelines are defined in `.buildkite/`:
- `pipeline.build.yml` - Build dependencies
- `pipeline.test.yml` - Basic tests
- `pipeline.ml.yml` - ML library tests (TensorFlow, PyTorch)
- `pipeline.gpu.yml` - GPU tests
- `pipeline.gpu.large.yml` - Multi-GPU tests

### Test Conditions

Tests can specify conditions for when they run:
- `NO_WHEELS_REQUIRED` - Can run with latest binaries
- File patterns - Tests run when matching files change

### Docker Images

CI uses Docker images defined in `ci/docker/`:
- `build.Dockerfile` - Build environment
- `test.Dockerfile` - Test environment
- `ml.Dockerfile` - ML library environment
- `gpu.Dockerfile` - GPU environment

## Security Considerations

1. **No secrets in code**: Use environment variables for credentials
2. **Semgrep scanning**: Security issues are caught by pre-commit hooks
3. **Sandboxed execution**: Ray runs user code in isolated workers
4. **Authentication**: Ray supports TLS/mTLS for cluster communication

## Key Configuration Files

| File | Purpose |
|------|---------|
| `pyproject.toml` | Python project config, Ruff settings |
| `pytest.ini` | pytest configuration |
| `.bazelrc` | Bazel build configuration |
| `.bazelversion` | Bazel version (6.5.0) |
| `BUILD.bazel` | Root Bazel build file |
| `WORKSPACE` | Bazel workspace definition |
| `.pre-commit-config.yaml` | Pre-commit hooks config |
| `pylintrc` | Legacy pylint config |

## Important Notes

1. **Bazel version**: Must use exactly Bazel 6.5.0
2. **Cython**: Ray uses Cython for Python-C++ bindings (`_raylet.pyx`)
3. **Redis**: Ray uses Redis for cluster state; embedded in release builds
4. **Cloudpickle**: Modified vendored copy in `python/ray/cloudpickle/`
5. **Dashboard**: React frontend in `python/ray/dashboard/client/`
6. **Generated code**: Protocol buffer generated files are in `python/ray/core/generated/`

## Getting Help

- Documentation: https://docs.ray.io/
- Discourse forum: https://discuss.ray.io/
- GitHub Issues: https://github.com/ray-project/ray/issues
- Slack: https://www.ray.io/join-slack

## Contributing

See `CONTRIBUTING.rst` for contribution guidelines.

Key points:
1. All PRs require review from a committer
2. Pre-commit hooks must pass
3. CI must pass (buildkite)
4. Add tests for new features
5. Update documentation for user-facing changes
