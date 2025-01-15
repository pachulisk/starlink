#!/bin/bash

# server address is 107.172.190.217, user is root, password is 8pdNtZdKk73x1U62CO
set -euo pipefail

# 检查 sshpass 是否已安装
if ! command -v sshpass &> /dev/null
then
    echo "sshpass 未安装，正在安装..."
    brew install hudochenkov/sshpass/sshpass
fi

# 检查并创建 /home/ubuntu/app 目录
sshpass -p '8pdNtZdKk73x1U62CO' ssh root@107.172.190.217 'mkdir -p /home/ubuntu/app'

# 1. 复制 requirements.txt 到服务器
sshpass -p '8pdNtZdKk73x1U62CO' scp requirements.txt root@107.172.190.217:/home/ubuntu/app

# 2. 复制 app 目录下的所有文件到服务器
sshpass -p '8pdNtZdKk73x1U62CO' scp -r ./app root@107.172.190.217:/home/ubuntu/app

# 3. SSH 到服务器并安装依赖
sshpass -p '8pdNtZdKk73x1U62CO' ssh root@107.172.190.217 << 'EOF'
    cd /home/ubuntu/app
    pip3 install -r requirements.txt
EOF
