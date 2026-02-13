#!/usr/bin/env python3
"""
cron-monitor.py - Cron任务执行监控脚本

功能:
- 解析执行日志
- 统计成功率
- 检测异常
- 生成报告

用法:
  python3 cron-monitor.py --log /var/log/cron-jobs/execution.log
  python3 cron-monitor.py --check-last 24  # 检查最近24小时
  python3 cron-monitor.py --report          # 生成报告
"""

import argparse
import sys
from datetime import datetime, timedelta
from collections import defaultdict
from typing import Dict, List
import re

class CronMonitor:
    def __init__(self, log_file: str):
        self.log_file = log_file
        self.executions = []
    
    def parse_log(self, hours: int = 24):
        """解析日志文件"""
        cutoff_time = datetime.now() - timedelta(hours=hours)
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    # 格式: 2024-02-12 10:30:00|backup-db|SUCCESS|0|120s
                    if '|' not in line:
                        continue
                    
                    parts = line.strip().split('|')
                    if len(parts) != 5:
                        continue
                    
                    timestamp_str, job_name, status, exit_code, duration = parts
                    
                    try:
                        timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
                        if timestamp < cutoff_time:
                            continue
                        
                        duration_sec = int(duration.rstrip('s'))
                        
                        self.executions.append({
                            'timestamp': timestamp,
                            'job_name': job_name,
                            'status': status,
                            'exit_code': int(exit_code),
                            'duration': duration_sec
                        })
                    except (ValueError, IndexError):
                        continue
        
        except FileNotFoundError:
            print(f"错误: 日志文件不存在: {self.log_file}")
            sys.exit(1)
    
    def get_stats(self) -> Dict:
        """获取统计信息"""
        if not self.executions:
            return {}
        
        stats = {
            'total': len(self.executions),
            'success': 0,
            'failed': 0,
            'timeout': 0,
            'by_job': defaultdict(lambda: {'total': 0, 'success': 0, 'failed': 0, 'timeout': 0}),
            'slowest': [],
            'recent_failures': []
        }
        
        for exec in self.executions:
            job = exec['job_name']
            status = exec['status']
            
            stats['by_job'][job]['total'] += 1
            
            if status == 'SUCCESS':
                stats['success'] += 1
                stats['by_job'][job]['success'] += 1
            elif status == 'TIMEOUT':
                stats['timeout'] += 1
                stats['by_job'][job]['timeout'] += 1
            else:
                stats['failed'] += 1
                stats['by_job'][job]['failed'] += 1
                stats['recent_failures'].append(exec)
        
        # 最慢的任务
        stats['slowest'] = sorted(self.executions, key=lambda x: x['duration'], reverse=True)[:10]
        
        # 最近的失败
        stats['recent_failures'] = sorted(
            [e for e in self.executions if e['status'] != 'SUCCESS'],
            key=lambda x: x['timestamp'],
            reverse=True
        )[:10]
        
        return stats
    
    def print_report(self, hours: int = 24):
        """打印监控报告"""
        self.parse_log(hours)
        stats = self.get_stats()
        
        if not stats:
            print(f"最近 {hours} 小时内没有执行记录")
            return
        
        print("\n" + "="*80)
        print(f"Cron 任务执行报告（最近 {hours} 小时）")
        print("="*80 + "\n")
        
        # 总体统计
        total = stats['total']
        success = stats['success']
        failed = stats['failed']
        timeout = stats['timeout']
        success_rate = (success / total * 100) if total > 0 else 0
        
        print("总体统计:")
        print(f"  总执行次数: {total}")
        print(f"  成功: {success} ({success_rate:.1f}%)")
        print(f"  失败: {failed}")
        print(f"  超时: {timeout}")
        print()
        
        # 各任务统计
        print("任务统计:")
        print(f"{'任务名称':<30} {'总数':<8} {'成功':<8} {'失败':<8} {'成功率':<10}")
        print("-" * 80)
        
        for job_name, job_stats in sorted(stats['by_job'].items()):
            total = job_stats['total']
            success = job_stats['success']
            failed = job_stats['failed']
            success_rate = (success / total * 100) if total > 0 else 0
            
            print(f"{job_name:<30} {total:<8} {success:<8} {failed:<8} {success_rate:<9.1f}%")
        
        print()
        
        # 最慢的任务
        if stats['slowest']:
            print("最慢的任务（Top 10）:")
            print(f"{'任务名称':<30} {'执行时间':<15} {'耗时':<10}")
            print("-" * 80)
            
            for exec in stats['slowest'][:10]:
                timestamp = exec['timestamp'].strftime('%m-%d %H:%M')
                duration = f"{exec['duration']}s"
                print(f"{exec['job_name']:<30} {timestamp:<15} {duration:<10}")
            
            print()
        
        # 最近的失败
        if stats['recent_failures']:
            print("最近的失败:")
            print(f"{'时间':<20} {'任务名称':<30} {'状态':<10} {'退出码':<8}")
            print("-" * 80)
            
            for exec in stats['recent_failures'][:10]:
                timestamp = exec['timestamp'].strftime('%Y-%m-%d %H:%M:%S')
                print(f"{timestamp:<20} {exec['job_name']:<30} {exec['status']:<10} {exec['exit_code']:<8}")
            
            print()
        
        # 告警
        if failed > 0 or timeout > 0:
            print("⚠️  警告:")
            if failed > 0:
                print(f"   - {failed} 个任务执行失败")
            if timeout > 0:
                print(f"   - {timeout} 个任务执行超时")
            print()
        else:
            print("✓ 所有任务执行正常\n")
    
    def check_health(self) -> bool:
        """健康检查"""
        self.parse_log(1)  # 检查最近1小时
        
        if not self.executions:
            print("✓ 最近1小时内没有执行记录")
            return True
        
        stats = self.get_stats()
        failed = stats['failed']
        timeout = stats['timeout']
        
        if failed == 0 and timeout == 0:
            print(f"✓ 健康检查通过 (共 {stats['total']} 次执行)")
            return True
        else:
            print(f"✗ 健康检查失败:")
            print(f"  失败: {failed}")
            print(f"  超时: {timeout}")
            return False


def main():
    parser = argparse.ArgumentParser(description='Cron任务执行监控')
    
    parser.add_argument('--log', default='/var/log/cron-jobs/execution.log',
                       help='日志文件路径')
    parser.add_argument('--check-last', type=int, default=24,
                       help='检查最近N小时的记录')
    parser.add_argument('--report', action='store_true',
                       help='生成详细报告')
    parser.add_argument('--health', action='store_true',
                       help='健康检查（最近1小时）')
    
    args = parser.parse_args()
    
    monitor = CronMonitor(args.log)
    
    if args.health:
        success = monitor.check_health()
        sys.exit(0 if success else 1)
    elif args.report:
        monitor.print_report(args.check_last)
    else:
        monitor.print_report(args.check_last)


if __name__ == '__main__':
    main()
