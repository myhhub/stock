#!/bin/bash
# setup-env.sh — 启动 InStock 运行时依赖服务（MariaDB）
# PONYTAIL：最小、自包含、可执行。密钥用 ${db_password} 占位，绝不硬编码真实密码。
set -euo pipefail

# 项目根目录（脚本位于 harness/scripts/，向上两级）
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

# 从 environment.json 读取默认值（若 jq 可用），否则使用硬编码默认
DB_HOST="${db_host:-localhost}"
DB_PORT="${db_port:-3306}"
DB_USER="${db_user:-root}"
DB_DATABASE="${db_database:-instockdb}"

# 数据库密码：优先继承已导出的环境变量；否则提示用户用 ${db_password} 占位
if [ -n "${db_password:-}" ]; then
  DB_PASSWORD="${db_password}"
else
  DB_PASSWORD="${db_password}"
  echo "[setup-env] 提示：db_password 未设置，使用占位符 \${db_password}。请在调用前 export db_password=<真实密码>。"
fi

echo "[setup-env] 配置：host=${DB_HOST} port=${DB_PORT} user=${DB_USER} db=${DB_DATABASE}"

# 优先用 docker-compose 启动 MariaDB 依赖服务
if command -v docker >/dev/null 2>&1; then
  if docker compose version >/dev/null 2>&1; then
    echo "[setup-env] 检测到 docker，启动 MariaDB 服务（instockdbservice）..."
    docker compose -f "${COMPOSE_FILE}" up -d instockdbservice
    echo "[setup-env] MariaDB 已通过 docker compose 启动。"
  else
    echo "[setup-env] docker 已安装但 docker compose 不可用，尝试 docker-compose v1..."
    if command -v docker-compose >/dev/null 2>&1; then
      docker-compose -f "${COMPOSE_FILE}" up -d instockdbservice
      echo "[setup-env] MariaDB 已通过 docker-compose 启动。"
    else
      echo "[setup-env] 未找到 docker compose 插件，请手动启动 MariaDB（localhost:3306）。"
    fi
  fi
else
  echo "[setup-env] 未检测到 docker。请手动启动 MariaDB："
  echo "    brew services install mariadb  (macOS)"
  echo "    sudo systemctl start mariadb   (Linux)"
  echo "  并确保监听 ${DB_HOST}:${DB_PORT}、用户 ${DB_USER}、数据库 ${DB_DATABASE} 已创建。"
fi

# 导出 db_* 环境变量供后续脚本（start-server.sh / run-job.sh）继承
export db_host="${DB_HOST}"
export db_port="${DB_PORT}"
export db_user="${DB_USER}"
export db_password="${DB_PASSWORD}"
export db_database="${DB_DATABASE}"

echo "[setup-env] 已导出 db_host / db_port / db_user / db_password / db_database 环境变量。"
echo "[setup-env] 完成。"
