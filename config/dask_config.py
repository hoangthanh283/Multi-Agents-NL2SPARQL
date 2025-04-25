import os

from dask.distributed import Client, LocalCluster


def get_dask_client():
    """Get or create Dask client for distributed processing"""
    scheduler_host = os.getenv("DASK_SCHEDULER", "localhost")
    scheduler_port = int(os.getenv("DASK_SCHEDULER_PORT", "8786"))
    
    try:
        # Try to connect to existing scheduler
        client = Client(f"{scheduler_host}:{scheduler_port}")
    except Exception:
        # Create local cluster if no scheduler found
        cluster = LocalCluster(
            n_workers=4,
            threads_per_worker=2,
            memory_limit='2GB'
        )
        client = Client(cluster)
    
    return client

def configure_dask_dashboard():
    """Configure Dask dashboard settings"""
    return {
        'dashboard.link': '/dashboard',
        'distributed.dashboard.link': '/status',
        'distributed.scheduler.allowed-failures': 3,
        'distributed.scheduler.work-stealing': True,
        'distributed.scheduler.bandwidth': 100000000,  # 100 MB/s
        'distributed.worker.memory.target': 0.6,  # 60% memory threshold
        'distributed.worker.memory.spill': 0.7,  # 70% memory spill to disk
        'distributed.worker.memory.pause': 0.8,  # 80% memory pause worker
        'distributed.worker.memory.terminate': 0.95,  # 95% memory terminate worker
    }