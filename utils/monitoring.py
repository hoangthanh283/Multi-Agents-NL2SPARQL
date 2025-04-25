import json
import logging
import os
import threading
import time
from typing import Any, Dict

import psutil
from prometheus_client import Counter, Gauge

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# System metrics
CPU_USAGE = Gauge('system_cpu_usage', 'System CPU usage percentage')
MEMORY_USAGE = Gauge('system_memory_usage_bytes', 'System memory usage in bytes')
DISK_USAGE = Gauge('system_disk_usage_bytes', 'System disk usage in bytes')
NETWORK_IO = Counter('system_network_io_bytes', 'System network IO in bytes', ['direction'])

class SystemMonitor:
    def __init__(self, interval: int = 60):
        self.interval = interval
        self.running = False
        self.monitor_thread = None

    def start(self):
        """Start the system monitoring thread"""
        if not self.running:
            self.running = True
            self.monitor_thread = threading.Thread(target=self._monitor_loop)
            self.monitor_thread.daemon = True
            self.monitor_thread.start()
            logger.info("System monitoring started")

    def stop(self):
        """Stop the system monitoring thread"""
        self.running = False
        if self.monitor_thread:
            self.monitor_thread.join()
            logger.info("System monitoring stopped")

    def _monitor_loop(self):
        """Main monitoring loop"""
        while self.running:
            try:
                self._collect_metrics()
                time.sleep(self.interval)
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")

    def _collect_metrics(self):
        """Collect and update system metrics"""
        # CPU usage
        cpu_percent = psutil.cpu_percent(interval=1)
        CPU_USAGE.set(cpu_percent)

        # Memory usage
        memory = psutil.virtual_memory()
        MEMORY_USAGE.set(memory.used)

        # Disk usage
        disk = psutil.disk_usage('/')
        DISK_USAGE.set(disk.used)

        # Network IO
        network = psutil.net_io_counters()
        NETWORK_IO.labels(direction='sent').inc(network.bytes_sent)
        NETWORK_IO.labels(direction='received').inc(network.bytes_recv)

class MetricsLogger:
    def __init__(self, log_dir: str = "logs"):
        self.log_dir = log_dir
        os.makedirs(log_dir, exist_ok=True)

    def log_metrics(self, metrics: Dict[str, Any]):
        """Log metrics to a JSON file"""
        timestamp = time.strftime("%Y%m%d_%H%M%S")
        filename = os.path.join(self.log_dir, f"metrics_{timestamp}.json")
        
        try:
            with open(filename, 'w') as f:
                json.dump({
                    "timestamp": timestamp,
                    "metrics": metrics
                }, f, indent=2)
        except Exception as e:
            logger.error(f"Error logging metrics: {e}")

class HealthCheck:
    def __init__(self):
        self.services = {}

    def register_service(self, name: str, check_func):
        """Register a service health check"""
        self.services[name] = check_func

    async def check_health(self) -> Dict[str, str]:
        """Run health checks for all registered services"""
        results = {}
        for name, check_func in self.services.items():
            try:
                await check_func()
                results[name] = "healthy"
            except Exception as e:
                results[name] = f"unhealthy: {str(e)}"
        return results

# Initialize system monitor
system_monitor = SystemMonitor()
metrics_logger = MetricsLogger()
health_check = HealthCheck()

def start_monitoring():
    """Start system monitoring"""
    system_monitor.start()

def stop_monitoring():
    """Stop system monitoring"""
    system_monitor.stop()