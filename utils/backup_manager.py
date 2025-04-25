import asyncio
import logging
import os
import shutil
import subprocess
import tarfile
from datetime import datetime
from typing import Dict, List, Optional

import aiofiles
from prometheus_client import Counter, Gauge

from config.logging_config import get_logger

logger = get_logger(__name__, 'backup')

# Metrics
BACKUP_OPERATIONS = Counter('backup_operations_total', 'Total number of backup operations', ['operation', 'status'])
BACKUP_SIZE = Gauge('backup_size_bytes', 'Size of backups in bytes', ['service'])

class BackupManager:
    def __init__(self, backup_dir: str = "backups"):
        self.backup_dir = backup_dir
        self.backup_retention_days = int(os.getenv("BACKUP_RETENTION_DAYS", "7"))
        os.makedirs(backup_dir, exist_ok=True)

    async def create_backup(self, service: str) -> bool:
        """Create a backup for a specific service"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = os.path.join(self.backup_dir, f"{service}_{timestamp}")
        
        try:
            if service == "graphdb":
                success = await self._backup_graphdb(backup_path)
            elif service == "redis":
                success = await self._backup_redis(backup_path)
            elif service == "kafka":
                success = await self._backup_kafka(backup_path)
            else:
                success = await self._backup_service_data(service, backup_path)

            if success:
                # Update metrics
                size = os.path.getsize(f"{backup_path}.tar.gz")
                BACKUP_SIZE.labels(service=service).set(size)
                BACKUP_OPERATIONS.labels(operation='create', status='success').inc()
                logger.info(f"Successfully created backup for {service}")
            else:
                BACKUP_OPERATIONS.labels(operation='create', status='failure').inc()
                logger.error(f"Failed to create backup for {service}")

            return success

        except Exception as e:
            logger.error(f"Error creating backup for {service}: {e}")
            BACKUP_OPERATIONS.labels(operation='create', status='failure').inc()
            return False

    async def restore_backup(self, service: str, backup_path: str) -> bool:
        """Restore a backup for a specific service"""
        try:
            if service == "graphdb":
                success = await self._restore_graphdb(backup_path)
            elif service == "redis":
                success = await self._restore_redis(backup_path)
            elif service == "kafka":
                success = await self._restore_kafka(backup_path)
            else:
                success = await self._restore_service_data(service, backup_path)

            status = 'success' if success else 'failure'
            BACKUP_OPERATIONS.labels(operation='restore', status=status).inc()

            return success

        except Exception as e:
            logger.error(f"Error restoring backup for {service}: {e}")
            BACKUP_OPERATIONS.labels(operation='restore', status='failure').inc()
            return False

    async def _backup_graphdb(self, backup_path: str) -> bool:
        """Backup GraphDB repository"""
        try:
            # Use GraphDB backup API
            cmd = [
                "curl", "-X", "POST",
                "http://localhost:7200/rest/repositories/CHeVIE/backup",
                "-H", "Content-Type: application/json",
                "-d", f'{{"location": "{backup_path}"}}'
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            success = process.returncode == 0

            if success:
                # Compress the backup
                await self._compress_backup(backup_path)
            
            return success

        except Exception as e:
            logger.error(f"Error backing up GraphDB: {e}")
            return False

    async def _backup_redis(self, backup_path: str) -> bool:
        """Backup Redis data"""
        try:
            # Trigger Redis SAVE command
            process = await asyncio.create_subprocess_exec(
                'redis-cli', 'SAVE',
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            await process.communicate()
            
            # Copy dump.rdb to backup location
            shutil.copy('/var/lib/redis/dump.rdb', backup_path)
            
            # Compress the backup
            await self._compress_backup(backup_path)
            
            return True

        except Exception as e:
            logger.error(f"Error backing up Redis: {e}")
            return False

    async def _backup_kafka(self, backup_path: str) -> bool:
        """Backup Kafka topics and configurations"""
        try:
            # Create topics list
            process = await asyncio.create_subprocess_exec(
                'kafka-topics.sh',
                '--bootstrap-server', 'localhost:9092',
                '--list',
                stdout=asyncio.subprocess.PIPE
            )
            
            stdout, _ = await process.communicate()
            topics = stdout.decode().strip().split('\n')
            
            os.makedirs(backup_path, exist_ok=True)
            
            # Backup each topic
            for topic in topics:
                topic_backup_path = os.path.join(backup_path, topic)
                process = await asyncio.create_subprocess_exec(
                    'kafka-dump-log.sh',
                    '--files', f'/var/lib/kafka/data/{topic}-*',
                    '--print-data-log',
                    stdout=asyncio.subprocess.PIPE
                )
                
                stdout, _ = await process.communicate()
                async with aiofiles.open(f"{topic_backup_path}.log", 'wb') as f:
                    await f.write(stdout)
            
            # Compress the backup
            await self._compress_backup(backup_path)
            
            return True

        except Exception as e:
            logger.error(f"Error backing up Kafka: {e}")
            return False

    async def _backup_service_data(self, service: str, backup_path: str) -> bool:
        """Backup service-specific data"""
        try:
            service_data_path = f"/var/lib/{service}"
            if os.path.exists(service_data_path):
                shutil.copytree(service_data_path, backup_path)
                await self._compress_backup(backup_path)
                return True
            return False

        except Exception as e:
            logger.error(f"Error backing up {service} data: {e}")
            return False

    async def _restore_graphdb(self, backup_path: str) -> bool:
        """Restore GraphDB from backup"""
        try:
            # Extract backup if compressed
            if backup_path.endswith('.tar.gz'):
                await self._extract_backup(backup_path)
                backup_path = backup_path[:-7]  # Remove .tar.gz

            # Use GraphDB restore API
            cmd = [
                "curl", "-X", "POST",
                "http://localhost:7200/rest/repositories/CHeVIE/restore",
                "-H", "Content-Type: application/json",
                "-d", f'{{"location": "{backup_path}"}}'
            ]
            
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            
            stdout, stderr = await process.communicate()
            return process.returncode == 0

        except Exception as e:
            logger.error(f"Error restoring GraphDB: {e}")
            return False

    async def _restore_redis(self, backup_path: str) -> bool:
        """Restore Redis from backup"""
        try:
            if backup_path.endswith('.tar.gz'):
                await self._extract_backup(backup_path)
                backup_path = backup_path[:-7]

            # Stop Redis server
            await asyncio.create_subprocess_exec('redis-cli', 'SHUTDOWN')
            
            # Replace dump.rdb
            shutil.copy(backup_path, '/var/lib/redis/dump.rdb')
            
            # Start Redis server
            process = await asyncio.create_subprocess_exec('redis-server')
            
            return True

        except Exception as e:
            logger.error(f"Error restoring Redis: {e}")
            return False

    async def _restore_kafka(self, backup_path: str) -> bool:
        """Restore Kafka from backup"""
        try:
            if backup_path.endswith('.tar.gz'):
                await self._extract_backup(backup_path)
                backup_path = backup_path[:-7]

            # Get list of topics from backup
            topics = [f for f in os.listdir(backup_path) if f.endswith('.log')]
            
            for topic_file in topics:
                topic = topic_file[:-4]  # Remove .log extension
                
                # Create topic if it doesn't exist
                process = await asyncio.create_subprocess_exec(
                    'kafka-topics.sh',
                    '--bootstrap-server', 'localhost:9092',
                    '--create',
                    '--if-not-exists',
                    '--topic', topic
                )
                
                await process.communicate()
                
                # Restore topic data
                process = await asyncio.create_subprocess_exec(
                    'kafka-restore-log.sh',
                    '--input-files', os.path.join(backup_path, topic_file),
                    '--topic', topic
                )
                
                await process.communicate()
            
            return True

        except Exception as e:
            logger.error(f"Error restoring Kafka: {e}")
            return False

    async def _restore_service_data(self, service: str, backup_path: str) -> bool:
        """Restore service-specific data"""
        try:
            if backup_path.endswith('.tar.gz'):
                await self._extract_backup(backup_path)
                backup_path = backup_path[:-7]

            service_data_path = f"/var/lib/{service}"
            if os.path.exists(backup_path):
                shutil.rmtree(service_data_path, ignore_errors=True)
                shutil.copytree(backup_path, service_data_path)
                return True
            return False

        except Exception as e:
            logger.error(f"Error restoring {service} data: {e}")
            return False

    async def _compress_backup(self, backup_path: str):
        """Compress a backup directory"""
        try:
            with tarfile.open(f"{backup_path}.tar.gz", "w:gz") as tar:
                tar.add(backup_path, arcname=os.path.basename(backup_path))
            shutil.rmtree(backup_path)
        except Exception as e:
            logger.error(f"Error compressing backup: {e}")
            raise

    async def _extract_backup(self, backup_path: str):
        """Extract a compressed backup"""
        try:
            with tarfile.open(backup_path, "r:gz") as tar:
                tar.extractall(path=os.path.dirname(backup_path))
        except Exception as e:
            logger.error(f"Error extracting backup: {e}")
            raise

    async def cleanup_old_backups(self):
        """Clean up backups older than retention period"""
        try:
            current_time = datetime.now()
            for backup_file in os.listdir(self.backup_dir):
                backup_path = os.path.join(self.backup_dir, backup_file)
                file_time = datetime.fromtimestamp(os.path.getctime(backup_path))
                
                if (current_time - file_time).days > self.backup_retention_days:
                    os.remove(backup_path)
                    logger.info(f"Removed old backup: {backup_file}")

        except Exception as e:
            logger.error(f"Error cleaning up old backups: {e}")

# Initialize backup manager
backup_manager = BackupManager()