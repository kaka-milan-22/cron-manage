#!/bin/bash
# cron-wrapper.sh - Cron任务执行包装器
# 
# 功能:
# - 记录执行时间和退出码
# - 失败时发送告警
# - 超时控制
#
# 用法: cron-wrapper.sh <job_name> <timeout_seconds> <command>
# 示例: 0 2 * * * /opt/cron-wrapper.sh backup-db 3600 /opt/scripts/backup.sh

JOB_NAME="${1:-unknown}"
TIMEOUT="${2:-3600}"
shift 2
COMMAND="$@"

LOG_DIR="/var/log/cron-jobs"
LOG_FILE="${LOG_DIR}/execution.log"
ERROR_LOG="${LOG_DIR}/${JOB_NAME}.error.log"

# 创建日志目录
mkdir -p "$LOG_DIR"

# 记录开始
START_TIME=$(date +%s)
START_DATETIME=$(date +'%Y-%m-%d %H:%M:%S')

echo "[$START_DATETIME] Starting job: $JOB_NAME" >> "$LOG_FILE"

# 执行命令（带超时）
timeout "$TIMEOUT" bash -c "$COMMAND" 2>&1 | tee -a "${LOG_DIR}/${JOB_NAME}.output.log"
EXIT_CODE=${PIPESTATUS[0]}

# 记录结束
END_TIME=$(date +%s)
END_DATETIME=$(date +'%Y-%m-%d %H:%M:%S')
DURATION=$((END_TIME - START_TIME))

# 判断状态
if [ $EXIT_CODE -eq 124 ]; then
    STATUS="TIMEOUT"
elif [ $EXIT_CODE -eq 0 ]; then
    STATUS="SUCCESS"
else
    STATUS="FAILED"
fi

# 写入执行日志
echo "$END_DATETIME|$JOB_NAME|$STATUS|$EXIT_CODE|${DURATION}s" >> "$LOG_FILE"

# 失败处理
if [ "$STATUS" != "SUCCESS" ]; then
    ERROR_MSG="Cron job $STATUS: $JOB_NAME (exit code: $EXIT_CODE, duration: ${DURATION}s)"
    echo "[$END_DATETIME] $ERROR_MSG" >> "$ERROR_LOG"
    
    # 发送告警（可选）
    if command -v mail &> /dev/null; then
        echo "$ERROR_MSG" | mail -s "Cron Alert: $JOB_NAME" ops@company.com
    fi
    
    # 或使用企业微信/钉钉
    # curl -X POST https://qyapi.weixin.qq.com/... -d "{\"text\":\"$ERROR_MSG\"}"
fi

exit $EXIT_CODE
