# Ray 项目架构分析

> 本文档为Ray分布式计算框架的深度架构分析，适用于希望理解项目结构、参与代码贡献的开发者。

## 目录

1. [项目概述](#1-项目概述)
2. [整体架构](#2-整体架构)
3. [核心组件详解](#3-核心组件详解)
4. [数据流分析](#4-数据流分析)
5. [模块依赖关系](#5-模块依赖关系)
6. [关键技术选型](#6-关键技术选型)
7. [扩展性设计](#7-扩展性设计)

---

## 1. 项目概述

Ray是一个统一的分布式计算框架，用于扩展AI和Python应用程序。它将单机Python代码无缝扩展到集群环境，无需修改代码逻辑。

### 1.1 核心设计理念

| 概念 | 说明 |
|------|------|
| **Tasks** | 无状态函数，在集群中分布式执行 |
| **Actors** | 有状态的工作进程，维护内部状态 |
| **Objects** | 不可变的分布式共享数据 |
| **Placement Groups** | 资源放置策略，控制任务/actor的调度位置 |

### 1.2 主要功能模块

```
Ray
├── Ray Core          # 核心分布式运行时
├── Ray Data          # 可扩展数据集处理
├── Ray Train         # 分布式训练
├── Ray Tune          # 超参数调优
├── Ray Serve         # 模型服务部署
├── Ray RLlib         # 强化学习
└── Ray AIR           # ML平台统一抽象层
```

---

## 2. 整体架构

### 2.1 架构分层

```
┌─────────────────────────────────────────────────────────────┐
│                      AI Libraries Layer                      │
│  ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌─────────┐ ┌────────┐ │
│  │  Data   │ │  Train  │ │  Tune   │ │  Serve  │ │ RLlib  │ │
│  └─────────┘ └─────────┘ └─────────┘ └─────────┘ └────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Ray Core API Layer                        │
│         Tasks  │  Actors  │  Objects  │  PlacementGroups    │
├─────────────────────────────────────────────────────────────┤
│                   Python Core Layer                          │
│      Worker  │  Scheduler  │  Object Store  │  GCS Client    │
├─────────────────────────────────────────────────────────────┤
│                     C++ Core Layer                           │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │ CoreWorker  │ │   Raylet    │ │    GCS Server           ││
│  │  (Worker)   │ │(Node Mgr)   │ │ (Global Control)        ││
│  └─────────────┘ └─────────────┘ └─────────────────────────┘│
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────────┐│
│  │Object Store │ │  Pub/Sub    │ │   RPC (gRPC)            ││
│  │  (Plasma)   │ │             │ │                         ││
│  └─────────────┘ └─────────────┘ └─────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

### 2.2 关键进程架构

```
┌────────────────────────────────────────────────────────────────┐
│                        Cluster                                  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Head Node                              │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐  │  │
│  │  │  GCS Server  │ │  Raylet      │ │  Dashboard       │  │  │
│  │  │  (Redis)     │ │  (Scheduler) │ │  (Web UI)        │  │  │
│  │  └──────────────┘ └──────────────┘ └──────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Worker Node 1                          │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐  │  │
│  │  │  Raylet      │ │  Worker      │ │  Worker          │  │  │
│  │  │  (Node Mgr)  │ │  (Task/Actor)│ │  (Task/Actor)    │  │  │
│  │  └──────────────┘ └──────────────┘ └──────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
│  ┌──────────────────────────────────────────────────────────┐  │
│  │                    Worker Node 2                          │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────────┐  │  │
│  │  │  Raylet      │ │  Worker      │ │  Worker          │  │  │
│  │  │  (Node Mgr)  │ │  (Task/Actor)│ │  (Task/Actor)    │  │  │
│  │  └──────────────┘ └──────────────┘ └──────────────────┘  │  │
│  └──────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────┘
```

---

## 3. 核心组件详解

### 3.1 Python层核心模块

#### 3.1.1 Worker模块 (`python/ray/_private/worker.py`)

Worker是Ray的执行单元，负责:
- 任务执行上下文管理
- 对象存储交互 (put/get/wait)
- Actor生命周期管理
- 序列化/反序列化

关键类:
```python
class Worker:
    """Ray工作进程的核心实现"""
    def __init__(self):
        self.mode = None  # SCRIPT_MODE | WORKER_MODE | SPILL_WORKER_MODE
        self.core_worker = None  # C++ CoreWorker的Python绑定
```

#### 3.1.2 RemoteFunction模块 (`python/ray/remote_function.py`)

远程函数装饰器的实现:
```python
class RemoteFunction:
    """被@ray.remote装饰的函数"""
    def __init__(self, language, function, function_descriptor, task_options):
        self._function = function
        self._default_options = task_options
        # ...
    
    def remote(self, *args, **kwargs):
        """提交任务到集群"""
        # 序列化函数和参数
        # 通过CoreWorker提交任务
```

#### 3.1.3 Actor模块 (`python/ray/actor.py`)

Actor的实现机制:
```python
class ActorClass:
    """Actor类装饰器"""
    def remote(self, *args, **kwargs):
        """创建Actor实例"""
        # 创建Actor句柄
        # 调度到指定节点
        
class ActorHandle:
    """Actor远程调用句柄"""
    def __getattr__(self, method_name):
        """返回远程方法调用器"""
```

### 3.2 C++核心层

#### 3.2.1 CoreWorker (`src/ray/core_worker/`)

CoreWorker是每个Worker进程的核心C++组件:

| 组件 | 文件 | 职责 |
|------|------|------|
| CoreWorker | `core_worker.h/cc` | 主工作线程，处理任务执行 |
| TaskManager | `task_manager.h` | 任务生命周期管理 |
| ObjectRecovery | `object_recovery_manager.h` | 对象故障恢复 |
| ReferenceCounter | `reference_counter.h` | 对象引用计数 |
| LeasePolicy | `lease_policy.h` | 任务租约策略 |

#### 3.2.2 Raylet (`src/ray/raylet/`)

Raylet是节点管理器:

```
Raylet
├── NodeManager        # 节点资源管理
├── ClusterTaskManager   # 集群任务调度
├── LocalTaskManager     # 本地任务管理
├── WorkerPool         # Worker进程池
└── LocalResourceManager # 本地资源管理
```

#### 3.2.3 GCS Server (`src/ray/gcs/`)

全局控制服务(GCS)组件:

| 管理器 | 职责 |
|--------|------|
| GcsNodeManager | 集群节点管理 |
| GcsActorManager | Actor元数据管理 |
| GcsJobManager | 作业生命周期管理 |
| GcsPlacementGroupManager | 放置组管理 |
| GcsTaskManager | 任务状态管理 |
| GcsResourceManager | 集群资源管理 |

### 3.3 对象存储系统

Ray使用共享内存对象存储(Plasma):

```
┌─────────────────────────────────────────────────────────┐
│                    Object Store                         │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────────────┐│
│  │ In-Memory   │ │ Spilling    │ │ External Storage    ││
│  │ Store       │ │ (磁盘溢出)   │ │ (S3/GCS/HDFS)       ││
│  └─────────────┘ └─────────────┘ └─────────────────────┘│
│                                                         │
│  Object Lifecycle:                                      │
│  Create → Seal → Share → Reference Count → Delete       │
└─────────────────────────────────────────────────────────┘
```

---

## 4. 数据流分析

### 4.1 任务提交与执行流程

```
User Code                    Python Worker                CoreWorker (C++)
    │                              │                              │
    │  @ray.remote                 │                              │
    │  def task(): ...             │                              │
    │──────────────▶               │                              │
    │                              │  RemoteFunction.remote()     │
    │  task.remote(args)           │─────────────────────────────▶│
    │──────────────▶               │                              │
    │                              │                              │  SubmitTask()
    │                              │                              │──────────▶
    │                              │                              │
    │                              │                              │  GCS/Raylet
    │                              │                              │  (调度决策)
    │                              │                              │◀──────────
    │                              │                              │
    │                              │  Return ObjectRef            │
    │◀─────────────────────────────│                              │
    │                              │                              │
    │  ray.get(ref)               │                              │
    │──────────────▶               │                              │
    │                              │                              │  GetObject()
    │                              │                              │──────────▶
    │                              │                              │
    │                              │  Wait for object             │
    │                              │  (可能从其他节点拉取)          │
    │◀─────────────────────────────│◀─────────────────────────────│
```

### 4.2 Actor创建与调用流程

```
User Code                    ActorClass                   GCS ActorManager
    │                              │                              │
    │  class MyActor:              │                              │
    │      ...                     │                              │
    │──────────────▶               │                              │
    │                              │                              │
    │  MyActor.remote()            │                              │
    │──────────────▶               │                              │
    │                              │  Export actor class          │
    │                              │─────────────────────────────▶│
    │                              │                              │
    │                              │                              │  Choose node
    │                              │                              │  Create actor
    │                              │◀─────────────────────────────│
    │                              │                              │
    │  Return ActorHandle          │                              │
    │◀─────────────────────────────│                              │
    │                              │                              │
    │  actor.method.remote()       │                              │
    │──────────────▶               │                              │
    │                              │  Submit task to actor        │
    │                              │  (direct actor call)         │
    │                              │─────────────────────────────▶│
```

### 4.3 对象传输流程

```
Node A (Producer)                           Node B (Consumer)
       │                                           │
       │  ray.put(data)                            │
       │───────────────────────────────────────────│
       │                                           │
       │  Store in local object store              │
       │  └── Serialize with pyarrow/cloudpickle   │
       │                                           │
       │  Return ObjectRef (包含对象ID和位置信息)     │
       │                                           │
       │                               ray.get(ref)
       │◄──────────────────────────────────────────│
       │                                           │
       │  Pull object request                      │
       │──────────────────────────────────────────▶│
       │                                           │
       │  Send object data                         │
       │──────────────────────────────────────────▶│
       │                                           │
       │                                           │  Store in local object store
       │                                           │  Return to user
```

---

## 5. 模块依赖关系

### 5.1 Python模块依赖图

```
ray/__init__.py (公共API入口)
    ├── ray._private.worker (核心Worker实现)
    │       ├── ray._raylet (C++绑定)
    │       ├── ray.actor (Actor实现)
    │       └── ray.remote_function (远程函数)
    │
    ├── ray.actor (Actor API)
    ├── ray.data (数据处理)
    │       └── ray._private.worker
    │
    ├── ray.train (分布式训练)
    │       ├── ray.data
    │       └── ray.tune
    │
    ├── ray.tune (超参数调优)
    │       └── ray.train
    │
    ├── ray.serve (模型服务)
    │       └── ray._private.worker
    │
    └── ray.util (工具函数)
```

### 5.2 C++模块依赖关系

```
gcs_server (全局控制服务)
    ├── gcs_node_manager
    ├── gcs_actor_manager
    ├── gcs_job_manager
    ├── gcs_task_manager
    └── gcs_resource_manager

raylet (节点管理器)
    ├── node_manager
    ├── cluster_task_manager
    ├── local_task_manager
    └── worker_pool

core_worker (工作进程)
    ├── task_manager
    ├── object_recovery_manager
    ├── reference_counter
    └── future_resolver
```

---

## 6. 关键技术选型

### 6.1 通信机制

| 组件 | 协议 | 用途 |
|------|------|------|
| GCS ↔ 所有组件 | gRPC + Redis | 集群元数据管理 |
| Raylet ↔ Worker | gRPC | 任务分发、资源管理 |
| Worker ↔ Worker | gRPC + 共享内存 | 对象传输 |
| Driver ↔ GCS | gRPC | 集群状态查询 |

### 6.2 序列化方案

```
Python Objects
     │
     ├──► 基本类型 (int, float, str, bytes) → pickle
     │
     ├──► NumPy数组 → Arrow Tensor
     │
     ├──► Pandas DataFrame → Arrow Table
     │
     └──► 函数/类 → cloudpickle
```

### 6.3 存储技术栈

| 层级 | 技术 | 说明 |
|------|------|------|
| 内存存储 | Shared Memory (Plasma) | 零拷贝对象共享 |
| 本地磁盘 | Local FS | 对象溢出(spill) |
| 远程存储 | S3/GCS/HDFS | 大对象持久化 |
| 元数据存储 | Redis | GCS状态存储 |

---

## 7. 扩展性设计

### 7.1 水平扩展机制

```
┌─────────────────────────────────────────────────────────────┐
│                     Autoscaler                              │
│                                                             │
│   ┌───────────────┐      ┌───────────────────────────────┐  │
│   │ Resource Demands│     │ Node Providers                │  │
│   │ - Task queues   │────▶│ - AWS                         │  │
│   │ - Actor requests│     │ - GCP                         │  │
│   │ - Placement grps│     │ - Azure                       │  │
│   └───────────────┘      │ - Kubernetes                  │  │
│                            └───────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### 7.2 插件化架构

Ray支持多种扩展点:

1. **Runtime Environments** (`python/ray/runtime_env/`)
   - 自定义依赖包
   - 工作目录
   - 环境变量
   - Conda/Pip配置

2. **Custom Resources**
   - 自定义资源类型
   - 资源调度策略

3. **Metrics & Observability**
   - OpenTelemetry集成
   - 自定义指标
   - 分布式追踪

---

## 附录: 关键文件索引

### Python核心文件

| 文件路径 | 说明 |
|----------|------|
| `python/ray/__init__.py` | 公共API入口 |
| `python/ray/_private/worker.py` | Worker核心实现 |
| `python/ray/actor.py` | Actor API实现 |
| `python/ray/remote_function.py` | 远程函数实现 |
| `python/ray/exceptions.py` | 异常定义 |

### C++核心文件

| 文件路径 | 说明 |
|----------|------|
| `src/ray/core_worker/core_worker.h` | CoreWorker主类 |
| `src/ray/raylet/node_manager.h` | 节点管理器 |
| `src/ray/gcs/gcs_server.h` | GCS服务 |
| `src/ray/common/id.h` | ID类型定义 |
| `src/ray/protobuf/*.proto` | 协议定义 |

### 配置文件

| 文件路径 | 说明 |
|----------|------|
| `pyproject.toml` | Python项目配置 |
| `.bazelrc` | Bazel构建配置 |
| `BUILD.bazel` | 根构建文件 |
