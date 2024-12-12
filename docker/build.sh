#!/bin/sh

set -ex

rm -rf stock
rsync -av --progress ../../stock . --exclude .git --exclude .idea --exclude *.md --exclude *.bat --exclude __pycache__ --exclude .gitignore --exclude stock/cron --exclude stock/img --exclude stock/docker --exclude instock/cache --exclude instock/log --exclude instock/test
rm -rf cron
cp -r ../../stock/cron .

DOCKER_NAME=mayanghua/instock
TAG1=$(date "+%Y%m")
TAG2=latest

echo " docker build -f Dockerfile -t ${DOCKER_NAME} ."
docker buildx build --platform linux/amd64,linux/arm64 -f Dockerfile -t ${DOCKER_NAME}:${TAG1} -t ${DOCKER_NAME}:${TAG2} .

rm -rf stock
rm -rf cron