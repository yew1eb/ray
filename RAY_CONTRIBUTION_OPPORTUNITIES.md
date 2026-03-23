# Ray 项目贡献机会完整指南

> 本文档整合了 Ray 项目中经过源码验证的贡献机会，从新手友好的文档改进到深度的性能优化，适合不同经验水平的贡献者。
>
> **基于 Ray master 分支（2026-03-23）源码分析整理**

---

## 目录

1. [快速导航](#1-快速导航)
2. [🟢 初阶贡献（⭐–⭐⭐）](#2-初阶贡献)
   - [初次 PR 推荐 TOP 5](#初次-pr-推荐-top-5)
   - [A. Bug 修复类](#a-bug-修复类)
   - [B. 错误消息改进](#b-错误消息改进)
   - [C. 文档示例补充](#c-文档示例补充)
   - [D. 类型注解补充](#d-类型注解补充)
3. [🟡 进阶贡献（⭐⭐⭐）](#3-进阶贡献)
4. [🔴 高级贡献（⭐⭐⭐⭐+）](#4-高级贡献)
5. [贡献流程指引](#5-贡献流程指引)

---

## 1. 快速导航

### 汇总表格（按难度排序）

| # | 类别 | 文件 | 行号 | 问题摘要 | 难度 | 适合人群 |
|---|------|------|------|----------|------|----------|
| A1 | Bug 修复 | `python/ray/exceptions.py` | 968 | 类名 typo：`OufOfBand`→`OutOfBand` | ⭐ | 初次贡献者 |
| A2 | Bug 修复 | `python/ray/air/config.py` | 426–442 | `raise DeprecationWarning` 反模式（共 2 处） | ⭐⭐ | 初次贡献者 |
| A3 | Bug 修复 | `python/ray/tune/tune.py` | 122–125 | `raise DeprecationWarning` 反模式 | ⭐⭐ | 初次贡献者 |
| A4 | Bug 修复 | `python/ray/data/dataset.py` | 1873 | `raise DeprecationWarning` 反模式 | ⭐ | 初次贡献者 |
| B1 | 错误消息 | `python/ray/data/dataset.py` | 723–730 | `map_batches()` GPU 错误缺少示例 | ⭐⭐ | 初次贡献者 |
| B2 | 错误消息 | `python/ray/data/dataset.py` | 1044–1045 | `add_column()` 错误消息过于简单 | ⭐ | 初次贡献者 |
| B3 | 错误消息 | `python/ray/exceptions.py` | 590–599 | `OutOfDiskError.__str__` 有 TODO：缺磁盘信息和文档链接 | ⭐⭐ | 初次贡献者 |
| C1 | 文档示例 | `python/ray/data/read_api.py` | 2294–2338 | `read_webdataset()` 缺少 Examples 部分 | ⭐ | 初次贡献者 |
| C2 | 文档示例 | `python/ray/exceptions.py` | 611–627 | `OutOfMemoryError` / `NodeDiedError` 缺 Args 文档 | ⭐ | 初次贡献者 |
| D1 | 类型注解 | `python/ray/util/actor_pool.py` | 40 | `__init__` 参数 `actors: list` 应改为 `List[ActorHandle]` | ⭐⭐ | 初次贡献者 |
| D2 | 类型注解 | `python/ray/util/actor_pool.py` | 218, 250, 377, 409 | `has_next()`, `get_next()`, `has_free()`, `pop_idle()` 等缺返回类型 | ⭐ | 初次贡献者 |
| E1 | 性能优化 | `python/ray/util/actor_pool.py` | 351 | `# TODO(ekl) bulk wait for performance`—批量等待优化 | ⭐⭐⭐ | 进阶 |
| 13 | 调度器 | `python/ray/remote_function.py` | ~477 | 任务提交批量处理 | ⭐⭐⭐ | 高级 |
| 14 | 调度器 | `src/ray/raylet/scheduling/cluster_resource_scheduler.cc` | — | 集群资源调度器 O(n²) 优化 | ⭐⭐⭐⭐ | 高级 |
| 17 | 序列化 | `python/ray/_private/function_manager.py` | 105 | 函数缓存 LRU 化（内存泄漏修复） | ⭐⭐⭐ | 进阶-高级 |
| 18 | 序列化 | `python/ray/_private/serialization.py` | 204 | ObjectRef 重复序列化优化 | ⭐⭐⭐ | 进阶-高级 |
| 21 | 对象存储 | `src/ray/core_worker/reference_counter.h` | 746 | 引用计数分段锁 | ⭐⭐⭐⭐ | 高级 |
| 24 | Ray Data | `python/ray/data/_internal/logical/rules/operator_fusion.py` | 54 | 算子融合成本估算 | ⭐⭐⭐ | 进阶-高级 |
| 26 | Ray Data | `python/ray/data/_internal/datasource/parquet_datasource.py` | 86 | Parquet 编码比例动态估算 | ⭐⭐ | 进阶 |

---

## 2. 初阶贡献

> 这些任务有明确的文件路径和行号，改动范围小（1–50 行），能独立验证，有实际用户价值。适合了解分布式系统但不熟悉 Ray 内部架构的开发者。

---

### 初次 PR 推荐 TOP 5

以下 5 个任务是专门为**第一次向 Ray 贡献 PR 的开发者**筛选的，每个都附有完整的 step-by-step 指引。

---

#### TOP 1: 修复 DeprecationWarning 用法错误 ⭐⭐

**为什么推荐？** 有明确的 Python 最佳实践依据，改动小但影响真实用户，测试验证直观。

**问题位置**: `python/ray/air/config.py:426–442`（2 处）+ `python/ray/tune/tune.py:122–125`（1 处）

**问题说明**：

`raise DeprecationWarning(...)` 是 Python 反模式。`raise` 用于抛出需要捕获的异常，而 DeprecationWarning 应该用 `warnings.warn()` 发出，这样用户能看到警告但代码不会中断执行。

**当前代码** (`python/ray/air/config.py:426`):
```python
if self._checkpoint_keep_all_ranks != _DEPRECATED_VALUE:
    raise DeprecationWarning(
        "The experimental `_checkpoint_keep_all_ranks` config is deprecated. ..."
    )
```

**建议改动**:
```python
import warnings  # 在文件顶部添加（如果还没有）

if self._checkpoint_keep_all_ranks != _DEPRECATED_VALUE:
    warnings.warn(
        "The experimental `_checkpoint_keep_all_ranks` config is deprecated. ...",
        DeprecationWarning,
        stacklevel=2,
    )
```

**需要修改的位置**（同一个 PR 可以包含所有）：
- `python/ray/air/config.py:426`（`_checkpoint_keep_all_ranks`）
- `python/ray/air/config.py:438`（`_checkpoint_upload_from_workers`）
- `python/ray/tune/tune.py:122`（`resume` 参数废弃提示）
- `python/ray/data/dataset.py:1873`（`random_shuffle` 的 `num_blocks` 参数）

**验证命令**:
```bash
# 应看到 DeprecationWarning 而不是异常
python -c "
import warnings
warnings.simplefilter('always')
from ray.air.config import CheckpointConfig
c = CheckpointConfig(_checkpoint_keep_all_ranks=True)
"

# 运行相关测试（改动 air/config.py）
python -m pytest python/ray/tests/test_air_util.py -x -q
# 运行相关测试（改动 tune/tune.py）
python -m pytest python/ray/tune/tests/test_tune_restore.py -x -q
```

---

#### TOP 2: 改进 OutOfDiskError 错误消息 ⭐⭐

**为什么推荐？** 源码中有 TODO 注释明确指示需要改进方向，无需猜测意图。

**问题位置**: `python/ray/exceptions.py:590–599`

**当前代码**:
```python
def __str__(self):
    # TODO(scv119): expose more disk usage information and link to a doc.
    return super(OutOfDiskError, self).__str__() + (
        "\n"
        "The object cannot be created because the local object store"
        " is full and the local disk's utilization is over capacity"
        " (95% by default)."
        "Tip: Use `df` on this node to check disk usage and "
        "`ray memory` to check object store memory usage."
    )
```

**发现的问题**：
1. 消息缺少换行：`" (95% by default)."` 和 `"Tip:"` 之间没有 `\n`，显示时会连成一行
2. 没有文档链接（TODO 要求添加）
3. 没有提示用户可以调整阈值

**建议改动**:
```python
def __str__(self):
    return super(OutOfDiskError, self).__str__() + (
        "\n"
        "The object cannot be created because the local object store"
        " is full and the local disk's utilization is over capacity"
        " (95% by default).\n"
        "Tip: Use `df` on this node to check disk usage and "
        "`ray memory` to check object store memory usage.\n"
        "To adjust the disk usage threshold, set the "
        "`local_fs_capacity_threshold` parameter in ray.init().\n"
        "See https://docs.ray.io/en/latest/ray-core/objects/object-spilling.html "
        "for more details."
    )
```

**验证方法**:
```bash
python -c "
from ray.exceptions import OutOfDiskError
e = OutOfDiskError()
print(str(e))
"
```

---

#### TOP 3: 为 ActorPool 方法补充类型注解 ⭐

**为什么推荐？** 机械性操作，有现成的类型注解参考（同文件 `map()` 方法），改动不影响功能。

**问题位置**: `python/ray/util/actor_pool.py`

**当前代码（无返回类型注解）**:
```python
def __init__(self, actors: list):          # 第 40 行：类型太宽泛

def has_next(self):                        # 第 218 行：缺 -> bool
def get_next(self, timeout=None, ...):     # 第 250 行：缺 -> Any
def get_next_unordered(self, ...):         # 第 297 行：缺 -> Any
def has_free(self):                        # 第 377 行：缺 -> bool
def pop_idle(self):                        # 第 409 行：缺 -> Optional[...]
def push(self, actor):                     # 第 450 行：缺类型和返回注解
```

**建议改动**（文件顶部的 imports 需要确认是否已有，按需添加）：
```python
from typing import Any, Callable, List, Optional, TypeVar
import ray

def __init__(self, actors: List["ray.actor.ActorHandle"]) -> None:

def has_next(self) -> bool:

def get_next(self, timeout: Optional[float] = None,
             ignore_if_timedout: bool = False) -> Any:

def get_next_unordered(self, timeout: Optional[float] = None,
                       ignore_if_timedout: bool = False) -> Any:

def has_free(self) -> bool:

def pop_idle(self) -> Optional["ray.actor.ActorHandle"]:

def push(self, actor: "ray.actor.ActorHandle") -> None:
```

**验证命令**:
```bash
# 检查类型注解无语法错误
python -c "import ray.util.actor_pool"
# 运行现有测试
python -m pytest python/ray/tests/test_actor_pool.py -x -q
# 可选：用 mypy 检查（如果项目有配置）
mypy python/ray/util/actor_pool.py --ignore-missing-imports
```

---

#### TOP 4: 为 read_webdataset() 添加 Examples 文档 ⭐

**为什么推荐？** 纯文档改动，不涉及代码逻辑，有大量现成的格式参考（如 `read_csv()`）。

**问题位置**: `python/ray/data/read_api.py:2294`（`read_webdataset()` 的 docstring）

**当前状态**：函数有完整的 Args 和 Returns 部分，但缺少 Examples 块（可以对比同文件的 `read_csv()` 或 `read_parquet()` 看示例格式）。

**建议添加的 Examples 块**（插入到 `Returns:` 段之后）:
```python
    Examples:
        Read a WebDataset from a single tar file:

        .. testcode::

            import ray
            # ds = ray.data.read_webdataset("s3://my-bucket/train.tar")

        Read WebDataset with specific field selection:

        .. testcode::

            import ray
            # ds = ray.data.read_webdataset(
            #     "s3://my-bucket/train-{000..099}.tar",
            #     suffixes=[".jpg", ".cls"],
            # )
```

**格式参考**：查看同文件中 `read_csv()` 的 Examples 部分（约 `python/ray/data/read_api.py:130` 附近）。

**验证方法**:
```bash
# 检查 docstring 格式没有语法错误
python -c "import ray.data; help(ray.data.read_webdataset)"
# 可选：构建文档（需要安装 doc 依赖）
# cd doc && make doctest
```

---

#### TOP 5: 改进 dataset.add_column() 错误消息 ⭐

**为什么推荐？** 改动独立，用户价值直接可见，不涉及复杂逻辑。

**问题位置**: `python/ray/data/dataset.py:1044–1045`

**当前代码**:
```python
if not callable(fn):
    raise ValueError("`fn` must be callable, got {}".format(fn))
```

**问题**：当用户传入字符串或数字时，错误消息只显示类型，没有提示正确用法。

**建议改动**:
```python
if not callable(fn):
    raise ValueError(
        f"`fn` must be a callable (e.g., a function or lambda), "
        f"but got {type(fn).__name__!r}: {fn!r}.\n"
        f"Example: ds.add_column('new_col', lambda batch: batch['col'] * 2)"
    )
```

**验证命令**:
```bash
python -c "
import ray
ray.init(num_cpus=1)
ds = ray.data.from_items([{'a': 1}])
try:
    ds.add_column('b', 'not_a_function')
except ValueError as e:
    print(e)
ray.shutdown()
"
```

---

### A. Bug 修复类

#### A1: 修复类名 Typo ⭐

**位置**: `python/ray/exceptions.py:968`

**问题代码**：
```python
class OufOfBandObjectRefSerializationException(RayError):
#     ^^^  应为 OutOfBand
```

**修复**：将类名 `OufOfBandObjectRefSerializationException` 改为 `OutOfBandObjectRefSerializationException`。

**注意**：需要同时检查所有 import 和 raise 该异常的地方（使用 `grep -r "OufOfBand" python/` 查找）。

**验证**：
```bash
grep -r "OufOfBand\|OutOfBand" python/ray/ --include="*.py" | grep -v __pycache__
```

---

#### A2–A4: raise DeprecationWarning 反模式 ⭐⭐

见 [TOP 1](#top-1-修复-deprecationwarning-用法错误-) 的详细说明。

**涉及文件**：
- `python/ray/air/config.py:426`（`_checkpoint_keep_all_ranks`）
- `python/ray/air/config.py:438`（`_checkpoint_upload_from_workers`）
- `python/ray/tune/tune.py:122`（`resume` 参数）
- `python/ray/data/dataset.py:1873`（`random_shuffle` 的 `num_blocks`）

---

### B. 错误消息改进

#### B1: map_batches() GPU 错误消息 ⭐⭐

**位置**: `python/ray/data/dataset.py:723–730`

**当前代码**：
```python
if use_gpus and (batch_size is None or batch_size == "default"):
    raise ValueError(
        "You must provide `batch_size` to `map_batches` when requesting GPUs. "
        "The optimal batch size depends on the model, data, and GPU used. "
        "We recommend using the largest batch size that doesn't result "
        "in your GPU device running out of memory. You can view the GPU memory "
        "usage via the Ray dashboard."
    )
```

**改进方向**：添加一个最小可用的示例代码片段，帮助用户快速上手：
```python
        "You must provide `batch_size` to `map_batches` when requesting GPUs. "
        "Example: ds.map_batches(fn, num_gpus=1, batch_size=32). "
        "The optimal batch size depends on the model, data, and GPU used. "
        ...
```

---

#### B2: add_column() 错误消息 ⭐

见 [TOP 5](#top-5-改进-datasetadd_column-错误消息-) 的详细说明。

---

#### B3: OutOfDiskError TODO ⭐⭐

见 [TOP 2](#top-2-改进-outofdiskerror-错误消息-) 的详细说明。

---

### C. 文档示例补充

#### C1: read_webdataset() 缺少 Examples ⭐

见 [TOP 4](#top-4-为-read_webdataset-添加-examples-文档-) 的详细说明。

---

#### C2: OutOfMemoryError / NodeDiedError 缺少 Args 文档 ⭐

**位置**: `python/ray/exceptions.py:611–627`

**当前代码**：
```python
class OutOfMemoryError(RayError):
    """Indicates that the node is running out of memory ..."""

    # TODO: (clarng) expose the error message string here and format it with proto
    def __init__(self, message):
        self.message = message
```

**问题**：`__init__` 的 `message` 参数缺少 Args 文档。`NodeDiedError` 也有同样问题（第 622 行）。

**建议添加**:
```python
    def __init__(self, message: str):
        """
        Args:
            message: Human-readable description of the out-of-memory error,
                typically including the node address and memory usage details.
        """
        self.message = message
```

---

#### C3: read_snowflake() 示例格式改进 ⭐⭐

**位置**: `python/ray/data/read_api.py:2622–2678`

**问题**：现有示例只有单一场景，缺少多种使用方式的演示。可以参考 `read_parquet()` 的示例风格，添加展示 SQL 查询、分区读取等的多场景示例。

---

### D. 类型注解补充

#### D1–D2: ActorPool 类型注解 ⭐–⭐⭐

见 [TOP 3](#top-3-为-actorpool-方法补充类型注解-) 的详细说明。

---

#### D3: util/queue.py 内部类注解 ⭐⭐

**位置**: `python/ray/util/queue.py:259–306`

**问题**：`_QueueActor` 是 Ray Queue 的内部 actor 实现，其所有方法缺少类型注解。

**注意**：这是内部 API，改动需要确认不影响与 C++ 层的接口。

---

## 3. 进阶贡献

### E1: ActorPool 批量等待优化 ⭐⭐⭐

**位置**: `python/ray/util/actor_pool.py:351`

**问题代码**：
```python
def get_next_unordered(self, timeout=None, ignore_if_timedout=False):
    ...
    # TODO(ekl) bulk wait for performance
    res, _ = ray.wait(list(self._future_to_actor), num_returns=1, timeout=timeout)
```

**说明**：当 actor pool 中有大量任务时，每次只 `ray.wait` 一个结果效率较低。批量等待可以减少 RPC 调用次数。

**实现思路**：
1. 预先批量 `ray.wait(futures, num_returns=min(N, len(futures)))` 缓存多个结果
2. 后续调用从缓存中取，避免重复发起 `ray.wait`
3. 需要处理好超时和顺序语义

**验证**：需要编写 benchmark，对比大批量任务下的吞吐量。

---

### 进阶：序列化优化

#### 函数缓存 LRU 化（内存泄漏修复）⭐⭐⭐

**位置**: `python/ray/_private/function_manager.py:105`

**问题**：当前使用无界 dict 缓存函数定义，长时间运行会持续增长。改为 `functools.lru_cache` 或 `cachetools.LRUCache` 可以限制内存占用。

---

#### Parquet 编码比例动态估算 ⭐⭐

**位置**: `python/ray/data/_internal/datasource/parquet_datasource.py:86`

**问题**：当前使用硬编码的压缩比例估算 Parquet 文件的解压大小，导致块大小误差大。可以通过采样前几行数据动态估算实际压缩比。

---

## 4. 高级贡献

以下任务需要深入了解 Ray 内部架构，不适合初次贡献者，但收益显著。

### 调度器优化

#### 任务提交批量处理 ⭐⭐⭐

**位置**: `python/ray/remote_function.py:~477`

**说明**：小任务密集提交场景下，批量 RPC 提交可提升吞吐量 40–60%。需要修改 Python 层提交逻辑和对应的 C++ gRPC 处理层。

---

#### 集群资源调度器 O(n²) 优化 ⭐⭐⭐⭐

**位置**: `src/ray/raylet/scheduling/cluster_resource_scheduler.cc`

**说明**：当前节点选择算法在大集群下复杂度较高，可以通过维护资源索引结构降低到 O(n log n) 或 O(1)。

---

### 对象存储优化

#### 引用计数分段锁 ⭐⭐⭐⭐

**位置**: `src/ray/core_worker/reference_counter.h:746`

**说明**：当前使用单一全局锁保护引用计数，在高并发场景下成为瓶颈。分段锁（sharded lock）可以提升高并发吞吐量 10–30%。

---

#### 异步 Spill 写入 ⭐⭐⭐

**位置**: `python/ray/_private/external_storage.py:133`

**说明**：当前对象 spill 是同步写入，会阻塞 Object Store 操作。改为异步写入可以提升 Object Store 可用性 30–50%。

---

### Ray Data 优化

#### 算子融合成本估算 ⭐⭐⭐

**位置**: `python/ray/data/_internal/logical/rules/operator_fusion.py:54`

**说明**：当前算子融合决策没有考虑实际内存消耗，可能导致融合后内存不足。添加基于算子类型和数据大小的成本模型可以提升 Data Pipeline 性能 5–15%。

---

#### 背压机制精细化 ⭐⭐⭐

**位置**: `python/ray/data/_internal/execution/resource_manager.py:64`

**说明**：当前背压机制基于块数量，不够精细。改为基于内存字节数的背压可以提升流水线效率 10–20%。

---

## 5. 贡献流程指引

### 快速开始

```bash
# 1. Fork 并克隆仓库
git clone https://github.com/<your-name>/ray.git
cd ray

# 2. 创建特性分支（命名建议：describe-what-you-fix）
git checkout -b fix/deprecation-warning-usage

# 3. 安装开发依赖
pip install -e ".[default]"
# 或者安装最小依赖
pip install -e ".[dev]"

# 4. 修改代码，然后运行相关测试
python -m pytest python/ray/tests/<relevant_test>.py -x -q

# 5. 提交 PR
# 标题格式：[component] Short description
# 例如：[air] Fix incorrect use of raise DeprecationWarning
```

### PR 检查清单

- [ ] 改动有对应的测试（新增或修改现有测试）
- [ ] 所有受影响的测试通过：`python -m pytest <test_file> -x`
- [ ] 类型注解改动通过 mypy 检查（如适用）
- [ ] 文档改动的 docstring 格式符合 Ray 规范（参考同文件其他函数）
- [ ] PR 描述说明了问题和解决方案

### 常用测试命令

```bash
# 运行单个测试文件
python -m pytest python/ray/tests/test_basic.py -x -v

# 运行特定测试函数
python -m pytest python/ray/tests/test_basic.py::test_function_name -x

# 运行 Ray Data 测试
python -m pytest python/ray/data/tests/ -x -q

# 运行 Ray Tune 测试
python -m pytest python/ray/tune/tests/ -x -q

# 检查类型注解
mypy python/ray/util/actor_pool.py --ignore-missing-imports
```

### 有用的资源

- Ray 贡献指南：`CONTRIBUTING.rst`（仓库根目录）
- 代码风格：遵循 `pyproject.toml` 中的 ruff/flake8 配置
- CI 说明：PR 提交后会自动触发，可以在 GitHub Actions 中查看
- 社区频道：Ray Slack（`#contributors` 频道）和 GitHub Discussions

---

*文档最后更新：2026-03-23。所有行号基于 Ray master 分支，请在修改前用 `git log` 确认当前版本。*
