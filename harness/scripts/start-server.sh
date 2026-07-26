#!/bin/bash
# start-server.sh — 启动 InStock Tornado Web 服务（端口 9988）
# PONYTAIL：最小、自包含、可执行。密钥用环境变量传递，绝不硬编码。
set -euo pipefail

# 项目根目录（脚本位于 harness/scripts/，向上两级）
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"

# 设置 PYTHONPATH 为项目根（本地运行）
export PYTHONPATH="${PROJECT_ROOT}"
# 若已有 PYTHONPATH 环境变量则优先使用（Docker 内为 /data/InStock）
if [ -n "${PYTHONPATH_OVERRIDE:-}" ]; then
  export PYTHONPATH="${PYTHONPATH_OVERRIDE}"
fi

# 设置数据库环境变量（若未设置则使用默认值）
export db_host="${db_host:-localhost}"
export db_port="${db_port:-3306}"
export db_user="${db_user:-root}"
export db_database="${db_database:-instockdb}"

if [ -z "${db_password:-}" ]; then
  echo "[start-server] 警告：db_password 未设置，使用占位符 \${db_password}。请在调用前 export db_password=<真实密码>。"
  export db_password="${db_password}"
else
  export db_password="${db_password}"
fi

# 东方财富 Cookie（可选，未设置则回退到 config/eastmoney_cookie.txt 或内置默认）
if [ -n "${EAST_MONEY_COOKIE:-}" ]; then
  export EAST_MONEY_COOKIE
else
  echo "[start-server] 提示：EAST_MONEY_COOKIE 未设置，将回退到 instock/config/eastmoney_cookie.txt 或内置默认 Cookie。"
fi

echo "[start-server] PYTHONPATH=${PYTHONPATH}"
echo "[start-server] db_host=${db_host} db_port=${db_port} db_user=${db_user} db_database=${db_database}"
echo "[start-server] 启动 Tornado Web 服务，访问地址：http://localhost:9988/"

# 启动 Web 服务
cd "${PROJECT_ROOT}"
python instock/web/web_service.py
