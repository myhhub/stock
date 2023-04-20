#!/bin/sh

ps -ef | grep python3 | grep '/data/InStock/instock/web/web_service.py' | awk '{print$2}' | xargs kill -9
