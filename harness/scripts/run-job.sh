#!/bin/bash
# run-job.sh — 包装运行 InStock 作业脚本
# PONYTAIL：最小、自包含、可执行。密钥用环境变量传递。
set -euo pipefail

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# 设置 PYTHONPATH 为项目根（本地运行）
export PYTHONPATH="${PROJECT_ROOT}"

# 设置数据库环境变量（若未设置则使用默认值）
export db_host="${db_host:-localhost}"
export db_port="${db_port:-3306}"
export db_user="${db_user:-root}"
export db_database="${db_database:-instockdb}"

if [ -z "${db_password:-}" ]; then
  echo "[run-job] 警告：db_password 未设置，使用占位符 \${db_password}。请在调用前 export db_password=<真实密码>。"
  export db_password="${db_password}"
else
  export db_password="${db_password}"
fi

# 东方财富 Cookie（可选）
if [ -n "${EAST_MONEY_COOKIE:-}" ]; then
  export EAST_MONEY_COOKIE
fi

echo "[run-job] PYTHONPATH=${PYTHONPATH}"
echo "[run-job] 执行作业：python instock/job/execute_daily_job.py $*"
echo "[run-job] 用法："
echo "    无参数       — 整体作业（当天）"
echo "    2023-03-01   — 单日作业"
echo "    2023-03-01,2023-03-02 — 批量作业"
echo "    2023-03-01 2023-03-21 — 区间作业"

# 执行作业脚本，透传所有参数
cd "${PROJECT_ROOT}"
python instock/job/execute_daily_job.py "$@"
