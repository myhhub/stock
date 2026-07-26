#!/bin/bash
# teardown-env.sh — 清理 InStock 运行时依赖服务
# PONYTAIL：最小、自包含、可执行。
set -euo pipefail

# 项目根目录
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
COMPOSE_FILE="${PROJECT_ROOT}/docker/docker-compose.yml"

echo "[teardown-env] 开始清理..."

# 若依赖由 docker compose 启动，则关闭
if command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1; then
  echo "[teardown-env] 检测到 docker compose，关闭容器..."
  docker compose -f "${COMPOSE_FILE}" down
  echo "[teardown-env] docker compose 容器已关闭。"
elif command -v docker >/dev/null 2>&1 && command -v docker-compose >/dev/null 2>&1; then
  echo "[teardown-env] 检测到 docker-compose v1，关闭容器..."
  docker-compose -f "${COMPOSE_FILE}" down
  echo "[teardown-env] docker-compose 容器已关闭。"
else
  echo "[teardown-env] 未检测到 docker，若 MariaDB 为本地服务请手动停止："
  echo "    brew services stop mariadb  (macOS)"
  echo "    sudo systemctl stop mariadb   (Linux)"
fi

echo "[teardown-env] 清理完成。"
