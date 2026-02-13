#!/bin/bash
# install.sh - Cron Manager 快速安装脚本

set -e

echo "====================================="
echo "  Cron Manager 安装脚本"
echo "====================================="
echo ""

# 检查Python
if ! command -v python3 &> /dev/null; then
    echo "错误: 未找到 Python 3"
    exit 1
fi

echo "✓ Python 版本: $(python3 --version)"

# 安装依赖
echo ""
echo "安装依赖..."
pip3 install pyyaml paramiko colorama --break-system-packages || \
pip3 install pyyaml paramiko colorama --user

echo "✓ 依赖安装完成"

# 创建目录结构
echo ""
echo "创建目录结构..."
mkdir -p config
mkdir -p logs

# 复制配置示例
if [ ! -f "config/dev.yaml" ]; then
    cat > config/dev.yaml << 'EOF'
# 开发环境配置示例
environment:
  PATH: /usr/local/bin:/usr/bin:/bin
  SHELL: /bin/bash

servers:
  - group: dev
    hosts:
      - localhost

jobs:
  - name: test-job
    description: 测试任务
    schedule: "*/5 * * * *"
    command: "echo 'Hello from cron-manager'"
    user: root
    enabled: true
EOF
    echo "✓ 创建示例配置: config/dev.yaml"
fi

# 添加执行权限
chmod +x cron_manager.py

# 创建软链接（可选）
if [ -w /usr/local/bin ]; then
    ln -sf "$(pwd)/cron_manager.py" /usr/local/bin/cron-manager
    echo "✓ 创建软链接: /usr/local/bin/cron-manager"
fi

echo ""
echo "====================================="
echo "  安装完成！"
echo "====================================="
echo ""
echo "快速开始:"
echo "  1. 编辑配置: vim config/dev.yaml"
echo "  2. 验证配置: python3 cron_manager.py validate dev"
echo "  3. 查看任务: python3 cron_manager.py list dev"
echo "  4. 部署测试: python3 cron_manager.py deploy dev --dry-run"
echo ""
echo "更多帮助: python3 cron_manager.py --help"
echo ""
