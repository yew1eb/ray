# Ray 项目新手贡献指南

> 专为第一次参与Ray项目贡献的开发者准备的入门指南。

## 目录

1. [开始之前](#1-开始之前)
2. [开发环境搭建](#2-开发环境搭建)
3. [找到你的第一个Issue](#3-找到你的第一个issue)
4. [推荐的新手任务](#4-推荐的新手任务)
5. [代码贡献流程](#5-代码贡献流程)
6. [常见问题解决](#6-常见问题解决)
7. [进阶学习路径](#7-进阶学习路径)

---

## 1. 开始之前

### 1.1 了解Ray的基本概念

在贡献代码前，你需要理解Ray的核心概念:

- **Tasks**: 无状态的远程函数
- **Actors**: 有状态的分布式类实例
- **Objects**: 分布式共享对象
- **Placement Groups**: 资源放置策略

**推荐学习资源:**
- [Ray官方文档](https://docs.ray.io/)
- [Ray Core Walkthrough](https://docs.ray.io/en/latest/ray-core/walkthrough.html)
- [10分钟入门教程](https://docs.ray.io/en/latest/ray-overview/getting-started.html)

### 1.2 技术储备要求

| 技能 | 级别 | 用途 |
|------|------|------|
| Python 3.9+ | 精通 | 主要开发语言 |
| 分布式系统基础 | 了解 | 理解架构设计 |
| gRPC/Protocol Buffers | 了解 | RPC通信 |
| C++ (可选) | 基础 | 核心组件开发 |
| Bazel构建系统 | 基础 | 项目构建 |

---

## 2. 开发环境搭建

### 2.1 环境要求

```bash
# 必需软件
- Python >= 3.9
- Bazel 6.5.0 (必须精确版本)
- C++编译器 (支持C++17)
- Git
- uv (用于构建wheel)
```

### 2.2 快速搭建步骤

```bash
# 1. 克隆仓库
git clone https://github.com/ray-project/ray.git
cd ray

# 2. 安装Bazel 6.5.0
# macOS:
brew install bazel@6.5.0
# Ubuntu:
sudo apt install bazel-6.5.0

# 3. 创建Python虚拟环境
python -m venv venv
source venv/bin/activate  # Linux/macOS
# 或 venv\Scripts\activate  # Windows

# 4. 安装Ray开发版本
pip install -e python/

# 5. 验证安装
python -c "import ray; ray.init(); print(ray.__version__)"
```

### 2.3 常用开发命令

```bash
# 构建Ray包
bazel build //:ray_pkg

# 运行Python测试
pytest python/ray/tests/test_basic.py -v

# 运行特定测试
pytest python/ray/tests/test_basic.py::test_function -v

# 运行C++测试
bazel test //src/ray/common:test_util

# 代码格式化
black python/ray/
ruff check python/ray/

# 预提交检查
pre-commit run --all-files
```

---

## 3. 找到你的第一个Issue

### 3.1 推荐的Issue标签

在GitHub Issues中搜索以下标签:

| 标签 | 说明 | 适合度 |
|------|------|--------|
| `good first issue` | 新手友好 | ⭐⭐⭐⭐⭐ |
| `help wanted` | 需要帮助 | ⭐⭐⭐⭐ |
| `documentation` | 文档改进 | ⭐⭐⭐⭐⭐ |
| `bug` + `low priority` | 低优先级bug | ⭐⭐⭐⭐ |
| `testing` | 测试改进 | ⭐⭐⭐⭐ |

### 3.2 在代码中寻找改进点

你也可以直接在代码中寻找改进机会:

```bash
# 查找TODO注释
grep -r "TODO" python/ray/ --include="*.py" | head -20

# 查找FIXME注释
grep -r "FIXME" python/ray/ --include="*.py" | head -20

# 查找简单的文档改进机会
grep -r "# TODO.*doc" python/ray/ --include="*.py"
```

---

## 4. 推荐的新手任务

### 4.1 文档改进类 (⭐ 最简单)

**任务示例:**

1. **修复文档拼写错误**
   - 文件: `python/ray/` 下的 `.py` 文件
   - 技能: 细心阅读
   - 难度: ⭐

2. **完善函数文档字符串**
   ```python
   # 改进前
   def my_function(x):
       """Do something."""
       pass
   
   # 改进后
   def my_function(x):
       """Process the input data.
       
       Args:
           x: The input data to process.
           
       Returns:
           The processed result.
           
       Example:
           >>> result = my_function(data)
       """
       pass
   ```

3. **添加类型注解**
   ```python
   # 改进前
   def process_data(data):
       return data * 2
   
   # 改进后
   from typing import Any
   
   def process_data(data: Any) -> Any:
       """Process data by doubling it."""
       return data * 2
   ```

**推荐文件位置:**
- `python/ray/util/` - 工具函数
- `python/ray/_private/` - 内部实现
- `python/ray/data/` - 数据处理

### 4.2 测试改进类 (⭐⭐ 简单)

**任务示例:**

1. **添加单元测试覆盖率**
   ```python
   # python/ray/tests/test_my_feature.py
   import pytest
   import ray
   
   def test_new_feature_basic():
       """Test basic functionality of new feature."""
       ray.init()
       
       @ray.remote
       def simple_task():
           return 42
       
       result = ray.get(simple_task.remote())
       assert result == 42
       
       ray.shutdown()
   ```

2. **修复 flaky 测试**
   - 查找有 `@pytest.mark.flaky` 标记的测试
   - 添加重试机制或修复竞争条件

3. **添加边界条件测试**
   - 空输入测试
   - 大输入测试
   - 异常输入测试

### 4.3 Bug修复类 (⭐⭐⭐ 中等)

**适合新手的Bug类型:**

| Bug类型 | 示例 | 查找方式 |
|---------|------|----------|
| 错误信息不清晰 | 异常提示缺少上下文 | 搜索 `raise ValueError` |
| 边缘情况处理 | 空列表、None值处理 | 查看参数验证代码 |
| 日志记录问题 | 缺少关键日志 | 搜索 `logger.` 使用 |
| 配置验证 | 参数范围检查 | 查看配置类 |

**示例: 改进错误信息**
```python
# 改进前
if resource < 0:
    raise ValueError("Invalid resource")

# 改进后
if resource < 0:
    raise ValueError(
        f"Invalid resource value: {resource}. "
        "Resource must be a non-negative number. "
        "Please check your ray.init() or @ray.remote() configuration."
    )
```

### 4.4 小型功能增强 (⭐⭐⭐⭐ 进阶)

**适合新手的增强:**

1. **添加新的工具函数**
   - 位置: `python/ray/util/`
   - 示例: 添加便捷的数据转换函数

2. **改进CLI输出格式**
   - 位置: `python/ray/scripts/`
   - 示例: 改进 `ray status` 的输出格式

3. **添加配置选项**
   - 位置: `python/ray/_private/ray_constants.py`
   - 示例: 添加新的环境变量支持

---

## 5. 代码贡献流程

### 5.1 标准贡献流程

```
1. Fork仓库 ────────────────────────────────┐
                                            │
2. 创建分支 (git checkout -b fix-xxx)       │
                                            │
3. 编写代码 ────────────────────────────────┤
                                            │
4. 本地测试 (pytest)                        │
                                            │
5. 代码格式化 (black, ruff)                 │
                                            │
6. 预提交检查 (pre-commit)                  │
                                            │
7. 提交PR ──────────────────────────────────┤
                                            │
8. 代码审查 ────────────────────────────────┤
                                            │
9. 合并 ────────────────────────────────────┘
```

### 5.2 提交信息规范

```bash
# 格式
<type>(<scope>): <subject>

<body>

<footer>

# 示例
feat(data): add parquet compression option

Add support for specifying compression codec when writing
parquet files. Supported codecs: 'snappy', 'gzip', 'brotli'.

Closes #12345

# Type说明
feat:     新功能
fix:      Bug修复
docs:     文档更新
style:    代码格式调整
refactor: 重构
test:     测试相关
chore:    构建/工具变更
```

### 5.3 PR描述模板

```markdown
## Summary
简要描述这个PR解决的问题或实现的功能。

## Changes
- 修改1
- 修改2
- 修改3

## Related Issues
Fixes #12345

## Testing
- [ ] 本地测试通过
- [ ] 新增测试用例
- [ ] 文档已更新

## Checklist
- [ ] 代码遵循项目风格
- [ ] 提交信息符合规范
- [ ] 预提交检查通过
```

---

## 6. 常见问题解决

### 6.1 构建问题

**问题1: Bazel版本不匹配**
```bash
# 错误: Bazel版本必须是6.5.0
ERROR: Current Bazel version is 7.0.0

# 解决: 使用bazelisk或手动安装6.5.0
brew install bazelisk
bazelisk build //:ray_pkg
```

**问题2: C++编译失败**
```bash
# 错误: C++17特性不支持
# 解决: 升级编译器
# macOS
xcode-select --install

# Ubuntu
sudo apt update
sudo apt install build-essential
```

**问题3: Python导入错误**
```bash
# 错误: ImportError: No module named 'ray._raylet'
# 解决: 重新构建并安装
bazel build //:ray_pkg
pip install -e python/ --force-reinstall
```

### 6.2 测试问题

**问题1: 测试超时**
```bash
# 增加超时时间
pytest python/ray/tests/test_xxx.py --timeout=300 -v
```

**问题2: 端口冲突**
```bash
# 清理残留进程
ray stop --force
# 或
killall raylet
killall gcs_server
```

**问题3: 资源不足**
```bash
# 运行小规模测试
pytest python/ray/tests/test_basic.py -v -x
```

### 6.3 代码审查反馈处理

常见反馈及应对:

| 反馈 | 应对策略 |
|------|----------|
| "添加更多测试" | 补充边界条件测试、异常测试 |
| "文档需要完善" | 补充docstring、类型注解、使用示例 |
| "代码可以简化" | 重构复杂逻辑，提取函数 |
| "考虑向后兼容" | 添加deprecation警告，保留旧接口 |

---

## 7. 进阶学习路径

### 7.1 按模块深入

```
新手路线:
├── 第1-2周: 熟悉代码风格
│   └── 文档改进、简单bug修复
│
├── 第3-4周: 理解Core模块
│   └── Worker、Task、Actor相关代码
│
├── 第5-8周: 选择专精方向
│   ├── Data模块: 数据处理流水线
│   ├── Serve模块: 模型服务部署
│   ├── Train模块: 分布式训练
│   ├── Tune模块: 超参数调优
│   └── Core模块: 分布式运行时
│
└── 第9周+: 贡献核心功能
    └── 新特性开发、性能优化
```

### 7.2 推荐阅读代码

**入门级 (理解架构):**
- `python/ray/__init__.py` - 公共API
- `python/ray/_private/worker.py` - Worker核心
- `python/ray/remote_function.py` - 远程函数

**进阶级 (理解机制):**
- `python/ray/_private/function_manager.py` - 函数管理
- `python/ray/_private/serialization.py` - 序列化
- `python/ray/runtime_env/` - 运行时环境

**高级 (核心实现):**
- `src/ray/core_worker/` - C++ CoreWorker
- `src/ray/raylet/` - 节点管理器
- `src/ray/gcs/` - 全局控制服务

### 7.3 社区参与

- **Slack**: [Ray Slack](https://www.ray.io/join-slack) - 实时讨论
- **Discourse**: [Ray Forum](https://discuss.ray.io/) - 技术讨论
- **GitHub Issues**: Bug报告和功能请求
- **每周社区会议**: 关注Slack获取会议链接

---

## 附录: 快速参考

### 文件组织结构

```
python/ray/
├── __init__.py              # 公共API入口
├── _private/                # 内部实现
│   ├── worker.py           # Worker核心
│   ├── function_manager.py # 函数管理
│   └── serialization.py    # 序列化
├── actor.py                # Actor API
├── remote_function.py      # 远程函数
├── data/                   # Ray Data
├── train/                  # Ray Train
├── tune/                   # Ray Tune
├── serve/                  # Ray Serve
├── util/                   # 工具函数
└── tests/                  # 核心测试
```

### 常用命令速查

```bash
# 构建
bazel build //:ray_pkg

# 测试
pytest python/ray/tests/test_basic.py -v
bazel test //src/ray/common:test_util

# 格式化
black python/ray/
ruff check python/ray/

# 调试
RAY_BACKEND_LOG_LEVEL=debug python script.py

# 清理
bazel clean
ray stop --force
```

### 重要链接

- [贡献指南](https://docs.ray.io/en/latest/ray-contribute/getting-involved.html)
- [代码风格指南](https://docs.ray.io/en/latest/ray-contribute/code-style.html)
- [架构白皮书](https://docs.google.com/document/d/1tBw9A4j62ruI5omIJbMxly-la5w4q_TjyJgJL_jN2fI/preview)
- [GitHub Issues](https://github.com/ray-project/ray/issues)

---

**祝你在Ray项目的贡献之旅顺利！如有问题，随时在社区寻求帮助。**
