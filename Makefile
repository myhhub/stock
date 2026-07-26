# InStock harness Makefile
# ponytail: 最小可用目标集，不堆砌

PY ?= python3
LINT_DEPS := scripts/lint-deps.py
LINT_QUALITY := scripts/lint-quality.py

.PHONY: help install-deps lint lint-arch lint-quality build test run run-job setup-env teardown-env

help:  ## 列出可用目标
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

install-deps:  ## 安装 Python 依赖（系统级 TA-Lib native 库需另行安装，见 docs/DEVELOPMENT.md）
	$(PY) -m pip install -r requirements.txt

lint: lint-arch lint-quality  ## 跑全部 linter

lint-arch:  ## 检查包层级依赖（lib 不得 import core 等）
	$(PY) $(LINT_DEPS) instock

lint-quality:  ## 检查基础代码质量
	$(PY) $(LINT_QUALITY) instock

build:  ## 编译检查（无未提交的语法错误）
	$(PY) -m compileall -q instock

test:  ## 运行测试（项目暂无测试套件，占位——加测试后在此接入）
	@echo "尚无测试套件；新增 tests/ 后在此目标接入 pytest"

run:  ## 启动 Web 服务（端口 9988）
	bash harness/scripts/start-server.sh

run-job:  ## 跑全量 job，可传日期参数：make run-job ARGS="2023-03-01"
	bash harness/scripts/run-job.sh $(ARGS)

setup-env:  ## 启动依赖服务（MariaDB）
	bash harness/scripts/setup-env.sh

teardown-env:  ## 关闭依赖服务
	bash harness/scripts/teardown-env.sh
