# 11. 批处理
- 批处理的定位：对一批有界输入做离线计算，强调吞吐量、可重跑、可回滚；在线系统则围绕请求-响应，强调低延迟与高可用。
- 批处理的输出通常是**派生数据**（见 [“记录系统与派生数据”](https://ddia.vonng.com/ch1/#sec_introduction_derived)），因此出错后常见做法是删除错误输出并重跑。
- 典型场景包括训练 AI 模型、海量数据转换、超大规模分析、ETL（见 [“数据仓库”](https://ddia.vonng.com/ch1/#sec_introduction_dwh)）。
- 批处理与流处理的分界在于输入是否有界；流处理的衔接见 [第十二章](https://ddia.vonng.com/ch12/#ch_stream)。
## 使用 Unix 工具的批处理
### 简单日志分析
- 场景是分析 nginx access.log，统计访问量最高的页面。
- 原文示例的关键点是：先提取 URL，再排序聚集相同键，再计数，再按次数取 TopN。
- 这类处理体现了批处理最基础的思路：先把“同类数据”聚到一起，再做归约。
```bash
cat /var/log/nginx/access.log | #1
  awk '{print $7}' | #2
  sort             | #3
  uniq -c          | #4
  sort -r -n       | #5
  head -n 5          #6
```
### 命令链与自定义程序
- Unix 命令链示例：`awk` 提取 URL，`sort` 聚集相同键，`uniq -c` 计数，再按计数排序后取前 5。
- 程序化示例可用散列表维护“键 -> 计数”，本质上与命令链达到相同结果。
```python
from collections import defaultdict

counts = defaultdict(int) #1

with open('/var/log/nginx/access.log', 'r') as file:
    for line in file:
        url = line.split()[6] #2
        counts[url] += 1 #3

top5 = sorted(((count, url) for url, count in counts.items()), reverse=True)[:5] #4

for count, url in top5:  #5
    print(f"{count} {url}")
```
- 两种方式的差别在于：前者依赖外部排序，后者依赖内存聚合。
### 排序与内存聚合
- **内存聚合**适合工作集能放入内存的情况，逻辑更直观。
- **外部排序**适合超内存数据，能利用磁盘顺序访问的优势（见 [“日志结构存储”](https://ddia.vonng.com/ch4/#sec_storage_log_structured) 和 [“SSD 上的顺序写与随机写”](https://ddia.vonng.com/ch4/#sidebar_sequential)）。
## 分布式系统中的批处理
- 分布式批处理框架可以看作“分布式操作系统”：它有存储层、编排层和计算层。
### 分布式文件系统
- DFS 的基本机制是把大文件切块，分散到多台数据节点上，再通过网络读取远端块。
- 元数据服务负责维护“块位置与权限”。
- 容错通常依赖多副本或纠删码；副本语义关联 [第六章](https://ddia.vonng.com/ch6/#ch_replication)。
- DFS 属于**无共享架构**（见 [“共享内存、共享磁盘与无共享架构”](https://ddia.vonng.com/ch2/#sec_introduction_shared_nothing)）。
### 对象存储
- 对象存储以 key 读写对象，常见行为是写后不可变，更新通常需要整体重写。
- 它更强调存算解耦与弹性扩展；与 DFS 的主要差异在于是否利用数据本地性调度计算。
- 对象存储与数据库边界正在融合（见 [“由对象存储支撑的数据库”](https://ddia.vonng.com/ch6/#sec_replication_object_storage)）。
### 分布式作业编排
- 作业请求通常包含任务数、CPU/内存/磁盘、镜像或代码位置、输入输出参数、凭据和硬件约束等信息。
- 核心组件包括任务执行器、资源管理器和调度器。
- 调度器的任务是决定任务放置并处理抢占；协调依赖通常借助共识型协调服务（见 [“协调服务”](https://ddia.vonng.com/ch10/#sec_consistency_coordination)）。
#### 资源分配权衡
- 调度本质上是在公平与效率之间做折中。
- 常见问题包括饥饿、抢占导致的重跑开销，以及大规模场景下接近 NP-hard 的调度复杂度。
#### 工作流调度
- 一个作业的输出可能成为多个下游作业的输入，因此批处理常以 DAG 形式组织。
- 两种常见衔接方式是：直接管道/网络传输，或者先落 DFS/对象存储再触发下游。
- 这与 [“持久化执行与工作流”](https://ddia.vonng.com/ch5/#sec_encoding_dataflow_workflows) 属于同类思想，但批处理更强调大数据离线流水线。
#### 故障处理
- 主要问题来自硬件故障、网络不可靠（见 [“硬件与软件故障”](https://ddia.vonng.com/ch2/#sec_introduction_hardware_faults) 和 [“不可靠网络”](https://ddia.vonng.com/ch9/#sec_distributed_networks)）、以及抢占式资源回收。
- 常见方案是任务级重试、血缘重算、检查点或中间结果持久化。
- 风险在于如果失败粒度退化成整作业重跑，成本会急剧上升。
## 批处理模型
### MapReduce
- MapReduce 的基本流程是：切分记录、map 产出键值、按键排序、reduce 聚合同键。
- 它把业务逻辑约束成“可并行 map + 可归约 reduce”。
- 局限在于：复杂连接表达冗长，中间落盘和阶段边界会带来额外开销。
### 数据流引擎（Spark/Flink 等）
- 数据流引擎把整条工作流作为一个作业统一优化。
- 相比 MapReduce，它不必强制每个阶段都排序，支持算子融合和前推执行，也能更充分利用内存与本地盘中的中间状态。
- 因此通常更快，也更容易编程。
### 混洗（Shuffle）
- Shuffle 的作用是把数据按键重分区并排序，使同键记录汇聚到同一个处理节点；它不是随机打乱，而是确定性重排。
- 典型过程是：map 端分桶与局部排序，reduce 端拉取并归并排序，再执行同键聚合。
- 如图11-1所示，多个 mapper/reducer 通过 shuffle 连接形成完整作业拓扑。
- ![](https://ddia.vonng.com/fig/ddia_1101.png)
#### JOIN 与 GROUP BY
- 原文示例是用户活动日志与用户画像按 user_id 连接，属于事实表与维度表的关联（星型分析模型见 [“星型与雪花型：分析模式”](https://ddia.vonng.com/ch3/#sec_datamodels_analytics)）。
- 两侧都以 user_id 为键进入 shuffle，同键在 reducer 汇合后做排序合并连接（sort-merge join），再可继续按 URL 做 group by 聚合。
- 如图11-2、图11-3所示，先连接再聚合是典型批分析路径。
- ![](https://ddia.vonng.com/fig/ddia_1102.png)
- ![](https://ddia.vonng.com/fig/ddia_1103.png)
### 查询语言与 DataFrame
- SQL 成为批处理、数据流引擎和云数据仓库的通用接口，优点是表达简洁、可交互、容易被优化器重写执行计划。
- DataFrame 是数据科学工作流的重要接口，背景见 [“DataFrame、矩阵与数组”](https://ddia.vonng.com/ch3/#sec_datamodels_dataframes)；分布式 DataFrame 通常不保留本地索引和顺序语义。
- 批处理系统与云数据仓库正在相互靠拢，交汇点见 [“云数据仓库”](https://ddia.vonng.com/ch4/#sec_cloud_data_warehouses)。
- 但复杂图迭代、部分 ML/多模态任务并不一定适合纯 SQL 表达。
## 批处理用例
### ETL/ELT
- 批处理适合并行转换、可重试和可追踪的数据集成工作流。
- 组织层面正在从中心化数据工程团队走向 data mesh / data contract 的协作方式。
### 分析（OLAP）
- 分析型负载通常包含预聚合报表和临时探索查询。
- 分析型系统范式见 [“操作型系统与分析型系统”](https://ddia.vonng.com/ch1/#sec_introduction_analytics)，预聚合视图见 [“物化视图与数据立方”](https://ddia.vonng.com/ch4/#sec_storage_materialized_views)。
### 机器学习
- 批处理在 ML 中主要用于特征工程、训练和批量推理。
- 图计算可用 BSP/Pregel 思想，图模型背景见 [“图状数据模型”](https://ddia.vonng.com/ch3/#sec_datamodels_graph)。
### 对外提供派生数据
- 批任务直接逐条写生产库的问题是：网络开销大、会冲击在线库、会破坏作业的原子可见性。
- 更好的方案是先写入流系统（如 Kafka）再由下游消费，或者采用“批构建 + 批导入 + 原子切换”。
- 若缺少“完成信号/提交语义”，下游可能读到部分结果；这种可见性问题可类比 [“读已提交”](https://ddia.vonng.com/ch8/#sec_transactions_read_committed)。
## 本章小结
- 批处理的核心是“有界输入 -> 派生输出”，因此适合重算、调试和容错。
- 分布式批处理可以抽象成三层：编排层、存储层、计算层。
- **Shuffle** 是分布式 join/group by 的基础能力，**SQL/DataFrame** 是主要接口，**DAG 编排** 是工程落地基础。
- 批处理、云数据仓库、流系统和机器学习平台在实践中正不断融合。
- 下一步自然衔接到流处理：输入从“有界”变为“无界”，系统设计会明显改变（见 [第十二章](https://ddia.vonng.com/ch12/#ch_stream)）。