#!/usr/bin/env python3
"""
Cron Manager - Crontab集中管理工具

功能:
- 配置版本控制（Git）
- YAML配置 → Crontab转换
- 批量SSH部署
- 多环境支持
- 配置对比
- 回滚能力
- 语法验证

作者: DevOps Team
版本: 1.0.0
"""

import argparse
import sys
import os
import yaml
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Dict, List, Tuple, Optional
from datetime import datetime
from colorama import init, Fore, Style
import paramiko
from concurrent.futures import ThreadPoolExecutor, as_completed
import re

init(autoreset=True)


class CronValidator:
    """Cron表达式验证器"""
    
    @staticmethod
    def validate_schedule(schedule: str) -> Tuple[bool, str]:
        """验证cron调度表达式"""
        # 标准cron: 分 时 日 月 周
        parts = schedule.split()
        
        if len(parts) != 5:
            return False, "Cron表达式必须有5个字段: 分 时 日 月 周"
        
        # 验证每个字段
        ranges = [
            (0, 59, "分钟"),    # 分
            (0, 23, "小时"),    # 时
            (1, 31, "日期"),    # 日
            (1, 12, "月份"),    # 月
            (0, 7, "星期"),     # 周 (0和7都表示周日)
        ]
        
        for i, (part, (min_val, max_val, name)) in enumerate(zip(parts, ranges)):
            if part == "*":
                continue
            
            # 处理步长 */5
            if part.startswith("*/"):
                try:
                    step = int(part[2:])
                    if step <= 0:
                        return False, f"{name}的步长必须大于0"
                except ValueError:
                    return False, f"{name}的步长格式错误"
                continue
            
            # 处理范围 1-5
            if "-" in part:
                try:
                    start, end = map(int, part.split("-"))
                    if not (min_val <= start <= max_val and min_val <= end <= max_val):
                        return False, f"{name}范围 {start}-{end} 超出有效范围 {min_val}-{max_val}"
                    if start > end:
                        return False, f"{name}范围起始值不能大于结束值"
                except ValueError:
                    return False, f"{name}范围格式错误"
                continue
            
            # 处理列表 1,3,5
            if "," in part:
                try:
                    values = [int(v) for v in part.split(",")]
                    for v in values:
                        if not (min_val <= v <= max_val):
                            return False, f"{name}值 {v} 超出有效范围 {min_val}-{max_val}"
                except ValueError:
                    return False, f"{name}列表格式错误"
                continue
            
            # 单个数字
            try:
                value = int(part)
                if not (min_val <= value <= max_val):
                    return False, f"{name}值 {value} 超出有效范围 {min_val}-{max_val}"
            except ValueError:
                return False, f"{name}值格式错误: {part}"
        
        return True, "验证通过"
    
    @staticmethod
    def validate_command(command: str) -> Tuple[bool, str]:
        """验证命令"""
        if not command or not command.strip():
            return False, "命令不能为空"
        
        # 警告：检查危险命令
        dangerous_patterns = [
            r'rm\s+-rf\s+/',
            r'dd\s+if=.*of=/dev/',
            r'mkfs\.',
            r'format\s+',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, command, re.IGNORECASE):
                return False, f"检测到危险命令，请仔细检查: {command}"
        
        return True, "命令验证通过"


class CronConfig:
    """Cron配置管理"""
    
    def __init__(self, config_file: str):
        self.config_file = config_file
        self.config = self._load_config()
    
    def _load_config(self) -> dict:
        """加载配置文件"""
        try:
            with open(self.config_file, 'r', encoding='utf-8') as f:
                config = yaml.safe_load(f)
                if not config:
                    raise ValueError("配置文件为空")
                return config
        except FileNotFoundError:
            print(f"{Fore.RED}错误: 配置文件不存在: {self.config_file}")
            sys.exit(1)
        except yaml.YAMLError as e:
            print(f"{Fore.RED}错误: YAML格式错误: {e}")
            sys.exit(1)
    
    def validate(self) -> Tuple[bool, List[str]]:
        """验证配置"""
        errors = []
        
        # 检查必需字段
        if 'servers' not in self.config:
            errors.append("缺少 'servers' 配置")
        
        if 'jobs' not in self.config:
            errors.append("缺少 'jobs' 配置")
        
        if errors:
            return False, errors
        
        # 验证服务器配置
        for i, server_group in enumerate(self.config.get('servers', [])):
            if 'group' not in server_group:
                errors.append(f"服务器组 {i} 缺少 'group' 字段")
            
            if 'hosts' not in server_group:
                errors.append(f"服务器组 '{server_group.get('group', i)}' 缺少 'hosts' 字段")
            elif not server_group['hosts']:
                errors.append(f"服务器组 '{server_group.get('group', i)}' 的 hosts 列表为空")
        
        # 验证任务配置
        job_names = set()
        for i, job in enumerate(self.config.get('jobs', [])):
            job_id = f"任务 {i}"
            
            if 'name' not in job:
                errors.append(f"{job_id} 缺少 'name' 字段")
            else:
                name = job['name']
                if name in job_names:
                    errors.append(f"任务名称重复: {name}")
                job_names.add(name)
                job_id = f"任务 '{name}'"
            
            if 'schedule' not in job:
                errors.append(f"{job_id} 缺少 'schedule' 字段")
            else:
                valid, msg = CronValidator.validate_schedule(job['schedule'])
                if not valid:
                    errors.append(f"{job_id} 调度表达式错误: {msg}")
            
            if 'command' not in job:
                errors.append(f"{job_id} 缺少 'command' 字段")
            else:
                valid, msg = CronValidator.validate_command(job['command'])
                if not valid:
                    errors.append(f"{job_id} 命令验证失败: {msg}")
        
        return len(errors) == 0, errors
    
    def get_hosts(self, group: Optional[str] = None) -> List[str]:
        """获取主机列表"""
        hosts = []
        for server_group in self.config.get('servers', []):
            if group is None or server_group.get('group') == group:
                hosts.extend(server_group.get('hosts', []))
        return hosts
    
    def get_jobs(self, enabled_only: bool = True) -> List[dict]:
        """获取任务列表"""
        jobs = self.config.get('jobs', [])
        if enabled_only:
            jobs = [j for j in jobs if j.get('enabled', True)]
        return jobs
    
    def generate_crontab(self, user: Optional[str] = None) -> str:
        """生成crontab内容"""
        lines = []
        lines.append("# Generated by Cron Manager")
        lines.append(f"# Generated at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        lines.append(f"# Config file: {self.config_file}")
        lines.append("")
        
        # 环境变量
        if 'environment' in self.config:
            for key, value in self.config['environment'].items():
                lines.append(f"{key}={value}")
            lines.append("")
        
        # 任务
        for job in self.get_jobs():
            # 如果指定了用户过滤
            if user and job.get('user') != user:
                continue
            
            lines.append(f"# {job['name']}")
            if 'description' in job:
                lines.append(f"# {job['description']}")
            
            schedule = job['schedule']
            command = job['command']
            
            # 添加日志重定向（如果配置了）
            if job.get('log_stdout'):
                command += f" >> {job['log_stdout']}"
            if job.get('log_stderr'):
                command += f" 2>> {job['log_stderr']}"
            elif job.get('log_stdout'):
                command += " 2>&1"
            
            lines.append(f"{schedule} {command}")
            lines.append("")
        
        return "\n".join(lines)


class CronDeployer:
    """Cron部署器"""
    
    def __init__(self, ssh_user: str = 'root', ssh_key: Optional[str] = None,
                 ssh_password: Optional[str] = None, ssh_port: int = 22):
        self.ssh_user = ssh_user
        self.ssh_key = ssh_key
        self.ssh_password = ssh_password
        self.ssh_port = ssh_port
    
    def _get_ssh_client(self, host: str) -> paramiko.SSHClient:
        """创建SSH连接"""
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        
        try:
            if self.ssh_key:
                client.connect(
                    host,
                    port=self.ssh_port,
                    username=self.ssh_user,
                    key_filename=self.ssh_key,
                    timeout=10
                )
            else:
                client.connect(
                    host,
                    port=self.ssh_port,
                    username=self.ssh_user,
                    password=self.ssh_password,
                    timeout=10
                )
            return client
        except Exception as e:
            raise Exception(f"SSH连接失败: {str(e)}")
    
    def backup_crontab(self, host: str, user: str = 'root') -> Tuple[bool, str]:
        """备份现有crontab"""
        try:
            client = self._get_ssh_client(host)
            
            # 创建备份目录
            backup_dir = f"/var/backups/crontab"
            backup_file = f"{backup_dir}/crontab.{user}.{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            
            commands = [
                f"mkdir -p {backup_dir}",
                f"crontab -u {user} -l > {backup_file} 2>/dev/null || echo 'No crontab'",
            ]
            
            for cmd in commands:
                stdin, stdout, stderr = client.exec_command(cmd)
                stdout.channel.recv_exit_status()
            
            client.close()
            return True, backup_file
        
        except Exception as e:
            return False, str(e)
    
    def deploy_crontab(self, host: str, crontab_content: str,
                      user: str = 'root', backup: bool = True) -> Tuple[bool, str]:
        """部署crontab到目标主机"""
        try:
            client = self._get_ssh_client(host)
            
            # 备份
            if backup:
                success, msg = self.backup_crontab(host, user)
                if not success:
                    print(f"{Fore.YELLOW}  警告: 备份失败 - {msg}")
            
            # 上传新crontab
            with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
                f.write(crontab_content)
                temp_file = f.name
            
            try:
                sftp = client.open_sftp()
                remote_temp = f"/tmp/crontab.{user}.{os.getpid()}"
                sftp.put(temp_file, remote_temp)
                sftp.close()
                
                # 安装crontab
                cmd = f"crontab -u {user} {remote_temp} && rm -f {remote_temp}"
                stdin, stdout, stderr = client.exec_command(cmd)
                exit_code = stdout.channel.recv_exit_status()
                
                if exit_code != 0:
                    error = stderr.read().decode()
                    raise Exception(f"安装crontab失败: {error}")
                
                # 验证
                stdin, stdout, stderr = client.exec_command(f"crontab -u {user} -l | wc -l")
                line_count = stdout.read().decode().strip()
                
                client.close()
                return True, f"部署成功，共 {line_count} 行"
            
            finally:
                os.unlink(temp_file)
        
        except Exception as e:
            return False, str(e)
    
    def get_current_crontab(self, host: str, user: str = 'root') -> Tuple[bool, str]:
        """获取当前crontab"""
        try:
            client = self._get_ssh_client(host)
            
            stdin, stdout, stderr = client.exec_command(f"crontab -u {user} -l")
            exit_code = stdout.channel.recv_exit_status()
            
            if exit_code == 0:
                content = stdout.read().decode()
                client.close()
                return True, content
            else:
                client.close()
                return True, "# No crontab"
        
        except Exception as e:
            return False, str(e)


class CronManager:
    """Cron管理器主类"""
    
    def __init__(self, config_dir: str = "config"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(exist_ok=True)
    
    def list_configs(self) -> List[str]:
        """列出所有配置环境"""
        configs = []
        for f in self.config_dir.glob("*.yaml"):
            configs.append(f.stem)
        return sorted(configs)
    
    def load_config(self, env: str) -> CronConfig:
        """加载指定环境的配置"""
        config_file = self.config_dir / f"{env}.yaml"
        if not config_file.exists():
            print(f"{Fore.RED}错误: 配置文件不存在: {config_file}")
            sys.exit(1)
        return CronConfig(str(config_file))
    
    def list_jobs(self, env: str):
        """列出任务"""
        config = self.load_config(env)
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"环境: {Fore.GREEN}{env}{Fore.CYAN}")
        print(f"配置文件: {self.config_dir / f'{env}.yaml'}")
        print(f"{'='*80}{Style.RESET_ALL}\n")
        
        jobs = config.get_jobs(enabled_only=False)
        
        if not jobs:
            print(f"{Fore.YELLOW}没有配置任务")
            return
        
        # 表格显示
        print(f"{Fore.CYAN}{'名称':<20} {'调度':<15} {'用户':<10} {'状态':<8} {'命令':<40}")
        print(f"{'-'*20} {'-'*15} {'-'*10} {'-'*8} {'-'*40}{Style.RESET_ALL}")
        
        for job in jobs:
            name = job['name'][:19]
            schedule = job['schedule']
            user = job.get('user', 'root')[:9]
            enabled = job.get('enabled', True)
            status = f"{Fore.GREEN}启用" if enabled else f"{Fore.RED}禁用"
            command = job['command'][:39]
            
            print(f"{name:<20} {schedule:<15} {user:<10} {status:<15} {command:<40}{Style.RESET_ALL}")
        
        print(f"\n{Fore.CYAN}总计: {len(jobs)} 个任务{Style.RESET_ALL}")
    
    def validate_config(self, env: str):
        """验证配置"""
        print(f"{Fore.CYAN}验证配置: {env}{Style.RESET_ALL}\n")
        
        config = self.load_config(env)
        valid, errors = config.validate()
        
        if valid:
            print(f"{Fore.GREEN}✓ 配置验证通过{Style.RESET_ALL}")
            
            # 显示统计
            jobs = config.get_jobs(enabled_only=False)
            enabled_jobs = [j for j in jobs if j.get('enabled', True)]
            hosts = config.get_hosts()
            
            print(f"\n统计信息:")
            print(f"  任务总数: {len(jobs)}")
            print(f"  启用任务: {len(enabled_jobs)}")
            print(f"  禁用任务: {len(jobs) - len(enabled_jobs)}")
            print(f"  目标主机: {len(hosts)}")
            
            return True
        else:
            print(f"{Fore.RED}✗ 配置验证失败:{Style.RESET_ALL}\n")
            for error in errors:
                print(f"  {Fore.RED}• {error}{Style.RESET_ALL}")
            return False
    
    def diff_configs(self, env1: str, env2: str):
        """对比两个环境的配置"""
        config1 = self.load_config(env1)
        config2 = self.load_config(env2)
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"对比配置: {Fore.GREEN}{env1}{Fore.CYAN} vs {Fore.GREEN}{env2}{Fore.CYAN}")
        print(f"{'='*80}{Style.RESET_ALL}\n")
        
        # 生成crontab内容
        content1 = config1.generate_crontab()
        content2 = config2.generate_crontab()
        
        # 使用diff命令
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.cron') as f1:
            f1.write(content1)
            file1 = f1.name
        
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.cron') as f2:
            f2.write(content2)
            file2 = f2.name
        
        try:
            # 调用git diff获得彩色输出
            result = subprocess.run(
                ['git', 'diff', '--no-index', '--color=always', file1, file2],
                capture_output=True,
                text=True
            )
            
            if result.returncode == 0:
                print(f"{Fore.GREEN}✓ 配置完全相同{Style.RESET_ALL}")
            else:
                print(result.stdout)
        
        finally:
            os.unlink(file1)
            os.unlink(file2)
    
    def deploy(self, env: str, hosts: Optional[List[str]] = None,
               ssh_user: str = 'root', ssh_key: Optional[str] = None,
               ssh_password: Optional[str] = None, dry_run: bool = False,
               max_workers: int = 10):
        """部署配置"""
        config = self.load_config(env)
        
        # 验证配置
        valid, errors = config.validate()
        if not valid:
            print(f"{Fore.RED}配置验证失败，停止部署:{Style.RESET_ALL}")
            for error in errors:
                print(f"  {Fore.RED}• {error}{Style.RESET_ALL}")
            return False
        
        # 获取目标主机
        if hosts is None:
            hosts = config.get_hosts()
        
        if not hosts:
            print(f"{Fore.RED}错误: 没有目标主机{Style.RESET_ALL}")
            return False
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"部署配置: {Fore.GREEN}{env}{Fore.CYAN}")
        print(f"目标主机: {len(hosts)} 台")
        print(f"并发数: {max_workers}")
        print(f"模式: {Fore.YELLOW}演习{Fore.CYAN if dry_run else Fore.GREEN}执行{Fore.CYAN}")
        print(f"{'='*80}{Style.RESET_ALL}\n")
        
        # 生成crontab内容
        crontab_content = config.generate_crontab()
        
        if dry_run:
            print(f"{Fore.YELLOW}演习模式 - 将要部署的内容:{Style.RESET_ALL}\n")
            print(crontab_content)
            print(f"\n{Fore.YELLOW}演习模式 - 将部署到以下主机:{Style.RESET_ALL}")
            for host in hosts:
                print(f"  • {host}")
            return True
        
        # 实际部署
        deployer = CronDeployer(ssh_user, ssh_key, ssh_password)
        
        success_count = 0
        failed_count = 0
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            
            for host in hosts:
                future = executor.submit(
                    deployer.deploy_crontab,
                    host,
                    crontab_content,
                    ssh_user,
                    backup=True
                )
                futures[future] = host
            
            for future in as_completed(futures):
                host = futures[future]
                try:
                    success, message = future.result()
                    if success:
                        print(f"{Fore.GREEN}✓ {host}: {message}{Style.RESET_ALL}")
                        success_count += 1
                    else:
                        print(f"{Fore.RED}✗ {host}: {message}{Style.RESET_ALL}")
                        failed_count += 1
                except Exception as e:
                    print(f"{Fore.RED}✗ {host}: 异常 - {str(e)}{Style.RESET_ALL}")
                    failed_count += 1
        
        # 汇总
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"部署完成")
        print(f"{'='*80}{Style.RESET_ALL}")
        print(f"{Fore.GREEN}成功: {success_count}")
        print(f"{Fore.RED}失败: {failed_count}")
        print(f"总计: {success_count + failed_count}{Style.RESET_ALL}")
        
        return failed_count == 0
    
    def show_crontab(self, env: str):
        """显示将要生成的crontab内容"""
        config = self.load_config(env)
        
        print(f"\n{Fore.CYAN}{'='*80}")
        print(f"Crontab 内容预览: {Fore.GREEN}{env}{Fore.CYAN}")
        print(f"{'='*80}{Style.RESET_ALL}\n")
        
        content = config.generate_crontab()
        print(content)


def main():
    parser = argparse.ArgumentParser(
        description='Crontab集中管理工具',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:

  # 列出所有任务
  %(prog)s list prod

  # 验证配置
  %(prog)s validate prod

  # 对比两个环境
  %(prog)s diff prod test

  # 预览crontab内容
  %(prog)s show prod

  # 演习部署
  %(prog)s deploy prod --dry-run

  # 实际部署
  %(prog)s deploy prod --ssh-key ~/.ssh/id_rsa

  # 部署到指定主机
  %(prog)s deploy prod --hosts web-01,web-02
        """
    )
    
    parser.add_argument('command', choices=['list', 'validate', 'diff', 'deploy', 'show'],
                       help='命令')
    parser.add_argument('env', help='环境名称（如: prod, test, dev）')
    parser.add_argument('env2', nargs='?', help='第二个环境（用于diff命令）')
    
    parser.add_argument('--config-dir', default='config',
                       help='配置目录（默认: config）')
    parser.add_argument('--hosts', help='目标主机列表，逗号分隔')
    parser.add_argument('--ssh-user', default='root', help='SSH用户')
    parser.add_argument('--ssh-key', help='SSH私钥路径')
    parser.add_argument('--ssh-password', help='SSH密码')
    parser.add_argument('--dry-run', action='store_true', help='演习模式')
    parser.add_argument('--workers', type=int, default=10, help='并发数')
    
    args = parser.parse_args()
    
    manager = CronManager(args.config_dir)
    
    try:
        if args.command == 'list':
            manager.list_jobs(args.env)
        
        elif args.command == 'validate':
            success = manager.validate_config(args.env)
            sys.exit(0 if success else 1)
        
        elif args.command == 'diff':
            if not args.env2:
                print(f"{Fore.RED}错误: diff命令需要两个环境参数")
                sys.exit(1)
            manager.diff_configs(args.env, args.env2)
        
        elif args.command == 'show':
            manager.show_crontab(args.env)
        
        elif args.command == 'deploy':
            hosts = None
            if args.hosts:
                hosts = [h.strip() for h in args.hosts.split(',')]
            
            # 获取SSH密码（如果需要）
            ssh_password = args.ssh_password
            if not args.ssh_key and not ssh_password:
                import getpass
                ssh_password = getpass.getpass('SSH密码: ')
            
            success = manager.deploy(
                args.env,
                hosts=hosts,
                ssh_user=args.ssh_user,
                ssh_key=args.ssh_key,
                ssh_password=ssh_password,
                dry_run=args.dry_run,
                max_workers=args.workers
            )
            sys.exit(0 if success else 1)
    
    except KeyboardInterrupt:
        print(f"\n{Fore.YELLOW}操作已取消{Style.RESET_ALL}")
        sys.exit(1)
    except Exception as e:
        print(f"\n{Fore.RED}错误: {str(e)}{Style.RESET_ALL}")
        sys.exit(1)


if __name__ == '__main__':
    main()
