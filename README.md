# PWMAT 介电函数并行计算

面向 MCloud/Slurm 的 PWMAT 介电函数自动计算脚本。

脚本将每个时刻的 step 1–4 保持严格串行，同时允许多个时刻并行推进。每次 Slurm 提交只等待当前作业自己的 JobID，不会误等当前用户的其他作业。

## 文件

```text
pwmat_dielectric_parallel.py   主脚本
README.md                      使用说明
```

旧的 `main.py`、`main_divide.py` 已删除。

## 环境要求

- MCloud 已加载好的 PWMAT 环境；
- Python 3.8.3；
- Slurm 命令：`sbatch`、`squeue`、`sacct`、`scancel`。

每次进入新的 Shell 后，必须先加载 Python 模块：

```bash
module load python/3.8.3
python --version
```

加载后，`python` 会指向 Python 3.8.3。本文所有命令统一使用 `python`。如果跳过 `module load`，系统默认的 `python` 可能仍指向 Python 2。

脚本不需要额外安装 Python 第三方包。

## 主要配置

直接修改脚本顶部的配置区：

```python
DEFAULT_BASE_DIR = "/data/home/wangl/2.41ev,515nm/515nm_0.024_200fs"
DEFAULT_SOURCE_DIR = "/data/home/wangl/pwmat_auto_ops/sources"
DEFAULT_GAP_NAME = "gap"
DEFAULT_DIELECTRIC_NAME = "dielectric"

STEP_START = 56
STEP_END = 200
STEP_LEN = 2
CALC_TIMES = list(range(STEP_START, STEP_END + 1, STEP_LEN))

MAX_PARALLEL = 4
POLL_SECONDS = 20.0
JOB_HEARTBEAT_SECONDS = 60.0
SBATCH_MAX_ATTEMPTS = 5
SBATCH_RETRY_SECONDS = 60.0
FINAL_DIELECTRIC_FILE = "dielectric_imag.dat"
```

默认目录结构：

```text
<BASE_DIR>/
├── TDDOS/
├── gap/
│   ├── 56/
│   └── 58/
├── dielectric/
│   ├── 56/
│   └── 58/
└── logs/
    ├── pwmat_parallel.log
    ├── pwmat_parallel.pid
    └── pwmat_parallel_status.json
```

- `<BASE_DIR>/gap` 和 `<BASE_DIR>/dielectric` 是两个计算根目录；
- `gap/<t>`、`dielectric/<t>` 是具体时刻目录；
- `dielectric/<t>/dielectric_imag.dat` 是时刻最终完成标记。

## 智能运行

默认运行就是智能模式，不需要区分“新算”和“续算”：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py \
  --daemon \
  --max-parallel 4
```

程序会自动判断：

1. `gap` 和 `dielectric` 都不存在时，创建根目录、全部时刻目录并从头计算；
2. 根目录存在但时刻目录缺失时，只补充缺失的时刻目录；
3. `dielectric/<t>/dielectric_imag.dat` 不存在时：
   - 取消该时刻目录下遗留的 Slurm 作业；
   - 删除 `gap/<t>` 和 `dielectric/<t>`；
   - 重新复制输入；
   - 从 step 1 完整重算该时刻；
4. `dielectric_imag.dat` 已存在时，直接跳过该时刻。

普通智能运行不会重建或备份已有的 `gap`、`dielectric` 根目录。

## 指定时刻

只处理一个时刻：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py --daemon --time 58
```

处理多个时刻：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py \
  --daemon \
  --times 58,60,62,64 \
  --max-parallel 2
```

使用代码顶部配置的 `CALC_TIMES`：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py --daemon --all
```

`--time 58` 和 `--times 58,60` 同样会先执行完整的智能判断，不会绕过最终文件检查。

## 后台运行

`--daemon` 会让 Python 调度程序脱离终端运行：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py \
  --daemon \
  --max-parallel 4
```

程序使用双 `fork`、`setsid()` 和 PID 文件锁，防止重复启动两个调度程序。

查看 PID：

```bash
cat <BASE_DIR>/logs/pwmat_parallel.pid
```

查看进程：

```bash
ps -fp $(cat <BASE_DIR>/logs/pwmat_parallel.pid)
```

## 查看进度

推荐使用 `watch`：

```bash
module load python/3.8.3
watch -n 5 -d \
  'python pwmat_dielectric_parallel.py --show-status'
```

状态视图会优先显示：

1. 失败时刻；
2. 运行中或部分完成的时刻；
3. 下一批尚未开始的时刻；
4. 本次运行已完成、失败、运行中、待处理数量统计。

状态文件中的 `run_times` 保存本次启动选择的时刻。`Overall`、`Total` 和 `Counts`
只根据本次运行的时刻计算，不会把历史运行中已完成的时刻混入本次统计。

查看日志：

```bash
tail -f <BASE_DIR>/logs/pwmat_parallel.log
```

查看完整原始 JSON：

```bash
cat <BASE_DIR>/logs/pwmat_parallel_status.json
```

## 修改默认目录

可以通过命令行覆盖目录，避免修改源码：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py \
  --daemon \
  --base-dir /data/home/wangl/能量A \
  --source-dir /data/home/wangl/pwmat_auto_ops/sources \
  --tddos-dir /data/home/wangl/能量A/TDDOS \
  --max-parallel 2
```

使用自定义目录查询状态时，必须传入同样的路径：

```bash
module load python/3.8.3

python pwmat_dielectric_parallel.py \
  --show-status \
  --base-dir /data/home/wangl/能量A
```

## 同时计算多个能量

不同能量必须使用不同的 `BASE_DIR`，否则 PID 文件锁和 `IN.OCC` 会冲突。

```bash
module load python/3.8.3

COMMON_SOURCE=/data/home/wangl/pwmat_auto_ops/sources

python pwmat_dielectric_parallel.py \
  --daemon \
  --base-dir /data/home/wangl/energy_A \
  --source-dir "$COMMON_SOURCE" \
  --max-parallel 2

python pwmat_dielectric_parallel.py \
  --daemon \
  --base-dir /data/home/wangl/energy_B \
  --source-dir "$COMMON_SOURCE" \
  --max-parallel 2
```

两个实例会共同使用同一个 Slurm 账户和 GPU 队列，因此应适当降低各自的 `--max-parallel`。

## 停止调度程序

```bash
kill $(cat <BASE_DIR>/logs/pwmat_parallel.pid)
```

停止 Python 调度程序不会自动取消已经提交的 Slurm 作业。重新启动智能模式后，程序会针对缺少 `dielectric_imag.dat` 的时刻取消遗留作业并重算。

不要使用 `scancel -u $USER` 取消账户下所有作业，除非确定没有其他计算需要保留。

## 维护参数

这些参数用于调试或维护，正常计算无需使用：

```bash
--step 0
```

强制重建 `gap` 和 `dielectric`，已有根目录会改名为带时间戳的备份。

```bash
--step 1
--step 2
--step 3
--step 4
```

手工执行单个步骤，会绕过智能续算判断。

```bash
--clear-out-wg
```

清理所选时刻的 `OUT.WG` 文件。

## Slurm 瞬时错误重试

提交作业时遇到以下瞬时错误会自动重试：

```text
Failed to query the balance
查询余额失败
Socket timed out
Resource temporarily unavailable
```

默认最多尝试 5 次，每次间隔 60 秒：

```python
SBATCH_MAX_ATTEMPTS = 5
SBATCH_RETRY_SECONDS = 60.0
```

如果持续出现余额查询失败，请联系 MCloud 管理员。

## 常见问题

### 为什么必须先 module load python/3.8.3？

脚本使用 Python 3 的语法和类型注解。必须先执行：

```bash
module load python/3.8.3
```

加载后 `python` 指向 Python 3.8.3。如果没有加载模块，默认的 `python` 可能指向
Python 2，并出现：

```text
SyntaxError: invalid syntax
```

查看状态：

```bash
module load python/3.8.3
python pwmat_dielectric_parallel.py --show-status
```

### 状态文件不存在

先检查日志和 PID：

```bash
ls -la <BASE_DIR>/logs

tail -n 100 <BASE_DIR>/logs/pwmat_parallel.log

cat <BASE_DIR>/logs/pwmat_parallel.pid
```

`--show-status` 找不到状态文件时，也会自动显示最近的日志和 PID 诊断信息。

### 后台程序登出后消失

正常 `--daemon` 会产生无控制终端的后台进程。如果 MCloud 在登出时清理整个用户会话/cgroup，则需要由 Slurm 作业托管调度程序。
