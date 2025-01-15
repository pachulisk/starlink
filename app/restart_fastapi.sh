#!/bin/bash
# 列出所有fastapi进程
pids=$(ps -aux | grep "fastapi" | grep -v grep | awk '{print $2}')

# 杀掉所有fastapi进程
for pid in $pids
do
  # 将指定pid号的进程杀掉，执行结果放到on_kill里面
  on_kill=`kill -9 $pid`
  # 如果on_kill不等于137，则表示成功
  if [ "$on_kill" -ne 137];
  then
    echo "kill $pid failed"
    exit 1
  fi
done

# 重新启动fastapi进程
nohup fasatapi run main.py &