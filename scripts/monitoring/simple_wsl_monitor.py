#!/usr/bin/env python3
"""
Simple WSL System Monitor without psutil dependency
Sends hourly system status to Slack using basic system commands
"""

import subprocess
import json
import requests
import time
import socket
from datetime import datetime
import os

SLACK_WEBHOOK = "https://hooks.slack.com/services/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"

def run_command(cmd):
    """Run shell command and return output"""
    try:
        result = subprocess.run(cmd, shell=True, capture_output=True, text=True, timeout=10)
        return result.stdout.strip() if result.returncode == 0 else None
    except:
        return None

def get_system_metrics():
    """Get basic system metrics using system commands"""
    metrics = {
        'hostname': socket.gethostname(),
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'uptime': None,
        'memory_info': None,
        'disk_info': None,
        'cpu_info': None,
        'docker_status': None,
        'postgres_status': None,
        'load_avg': None
    }
    
    # Get uptime
    uptime_output = run_command("uptime")
    if uptime_output:
        metrics['uptime'] = uptime_output.split('up ')[1].split(',')[0] if 'up ' in uptime_output else uptime_output
        # Extract load average from uptime
        if 'load average:' in uptime_output:
            load_part = uptime_output.split('load average:')[1].strip()
            metrics['load_avg'] = load_part
    
    # Get memory info
    mem_output = run_command("free -h")
    if mem_output:
        lines = mem_output.split('\n')
        if len(lines) > 1:
            mem_line = lines[1].split()
            if len(mem_line) >= 3:
                metrics['memory_info'] = f"Total: {mem_line[1]}, Used: {mem_line[2]}, Available: {mem_line[6] if len(mem_line) > 6 else 'N/A'}"
    
    # Get disk info
    disk_output = run_command("df -h /")
    if disk_output:
        lines = disk_output.split('\n')
        if len(lines) > 1:
            disk_line = lines[1].split()
            if len(disk_line) >= 5:
                metrics['disk_info'] = f"Size: {disk_line[1]}, Used: {disk_line[2]} ({disk_line[4]}), Available: {disk_line[3]}"
    
    # Get CPU info
    cpu_output = run_command("lscpu | grep 'Model name'")
    if cpu_output:
        metrics['cpu_info'] = cpu_output.split(':')[1].strip() if ':' in cpu_output else cpu_output
    
    # Check Docker status
    docker_output = run_command("docker ps --format 'table {{.Names}}\t{{.Status}}' 2>/dev/null")
    if docker_output:
        lines = docker_output.split('\n')[1:]  # Skip header
        running_containers = [line for line in lines if line and 'Up' in line]
        metrics['docker_status'] = f"{len(running_containers)} containers running"
        if running_containers:
            metrics['docker_containers'] = '\n'.join(running_containers[:5])  # Show first 5
    else:
        metrics['docker_status'] = "Docker not accessible"
    
    # Check PostgreSQL
    pg_check = run_command("python3 /home/jianjun/ats-genai-model/scripts/run_dev.py query --query 'SELECT version()' 2>/dev/null")
    if pg_check and 'PostgreSQL' in pg_check:
        metrics['postgres_status'] = "✅ Connected"
    else:
        metrics['postgres_status'] = "❌ Not accessible"
    
    # Check ATS processes
    ats_processes = run_command("pgrep -f 'polygon.*backfill' | wc -l")
    if ats_processes and ats_processes.isdigit():
        metrics['ats_backfill'] = f"{'✅ Active' if int(ats_processes) > 0 else '❌ Inactive'} ({ats_processes} processes)"
    
    # Check ATS data size
    ats_data_size = run_command("du -sh /mnt/d/ats-data 2>/dev/null | cut -f1")
    if ats_data_size:
        metrics['ats_data_size'] = ats_data_size
    
    return metrics

def send_slack_message(message, title="WSL System Status"):
    """Send message to Slack"""
    payload = {
        "username": "ATS System Monitor", 
        "icon_emoji": ":computer:",
        "attachments": [{
            "color": "#36a64f",
            "title": title,
            "text": message,
            "footer": "WSL System Monitor",
            "ts": int(datetime.now().timestamp())
        }]
    }
    
    try:
        response = requests.post(SLACK_WEBHOOK, json=payload, timeout=10)
        return response.status_code == 200
    except Exception as e:
        print(f"Failed to send Slack message: {e}")
        return False

def format_status_message(metrics):
    """Format system metrics into Slack message"""
    message = f"""**System Health Report**

🖥️ **Host:** {metrics['hostname']}
⏱️ **Time:** {metrics['timestamp']}
📊 **Uptime:** {metrics['uptime'] or 'Unknown'}
📈 **Load Average:** {metrics['load_avg'] or 'Unknown'}

💾 **Memory:** {metrics['memory_info'] or 'Unknown'}
💿 **Disk:** {metrics['disk_info'] or 'Unknown'}
⚙️ **CPU:** {metrics['cpu_info'] or 'Unknown'}

🐳 **Docker:** {metrics['docker_status'] or 'Unknown'}
🗄️ **PostgreSQL:** {metrics['postgres_status'] or 'Unknown'}
📊 **ATS Backfill:** {metrics.get('ats_backfill', 'Unknown')}
📁 **ATS Data Size:** {metrics.get('ats_data_size', 'Unknown')}
"""

    if metrics.get('docker_containers'):
        message += f"\n**Running Containers:**\n```\n{metrics['docker_containers']}\n```"
    
    return message

def send_test_alert():
    """Send a test alert"""
    print("Sending test alert...")
    metrics = get_system_metrics()
    message = format_status_message(metrics)
    success = send_slack_message(message, "🧪 WSL System Monitor Test")
    
    if success:
        print("✅ Test alert sent successfully!")
        return True
    else:
        print("❌ Failed to send test alert")
        return False

def run_monitoring_loop(interval_minutes=60):
    """Run continuous monitoring with hourly updates"""
    print(f"Starting WSL system monitoring (interval: {interval_minutes} minutes)")
    print(f"Slack webhook configured: {SLACK_WEBHOOK[:50]}...")
    
    # Send initial status
    print("Sending initial system status...")
    metrics = get_system_metrics()
    message = format_status_message(metrics)
    send_slack_message(message, "🚀 WSL System Monitor Started")
    
    while True:
        try:
            print(f"Waiting {interval_minutes} minutes until next update...")
            time.sleep(interval_minutes * 60)
            
            print(f"Collecting system metrics at {datetime.now().strftime('%H:%M:%S')}...")
            metrics = get_system_metrics()
            message = format_status_message(metrics)
            
            success = send_slack_message(message, f"📊 Hourly System Status - {datetime.now().strftime('%H:%M')}")
            
            if success:
                print(f"✅ Hourly status sent at {datetime.now().strftime('%H:%M:%S')}")
            else:
                print(f"❌ Failed to send hourly status at {datetime.now().strftime('%H:%M:%S')}")
                
        except KeyboardInterrupt:
            print("\nMonitoring stopped by user")
            send_slack_message("🛑 WSL System Monitor stopped by user", "Monitor Shutdown")
            break
        except Exception as e:
            print(f"Error in monitoring loop: {e}")
            time.sleep(300)  # Wait 5 minutes before retrying

def main():
    """Main entry point"""
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--test':
        send_test_alert()
    elif len(sys.argv) > 1 and sys.argv[1] == '--status':
        metrics = get_system_metrics()
        message = format_status_message(metrics)
        print(message)
    elif len(sys.argv) > 1 and sys.argv[1] == '--hourly':
        # Run hourly monitoring
        run_monitoring_loop(60)
    elif len(sys.argv) > 1 and sys.argv[1] == '--frequent':
        # Run frequent monitoring (every 5 minutes for testing)
        run_monitoring_loop(5)
    else:
        print("Usage:")
        print("  python3 simple_wsl_monitor.py --test      # Send test alert")
        print("  python3 simple_wsl_monitor.py --status    # Show current status")
        print("  python3 simple_wsl_monitor.py --hourly    # Run hourly monitoring")
        print("  python3 simple_wsl_monitor.py --frequent  # Run every 5 minutes (testing)")

if __name__ == "__main__":
    main()