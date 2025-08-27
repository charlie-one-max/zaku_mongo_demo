#!/usr/bin/env python3
"""
Enhanced TaskQ - Integrated Redis + MongoDB Hybrid Storage
=============================================================

Automatic memory pressure detection, intelligent data migration,
and seamless CRUD operations.
"""

import time
import logging
import os
from typing import Any, Dict, List, Optional
from uuid import uuid4
from dotenv import load_dotenv

from hybrid_storage import HybridStorage, StorageConfig

# Load environment variables
load_dotenv()

logger = logging.getLogger(__name__)


class HybridTaskQ:
    """Enhanced Task Queue supporting Redis + MongoDB hybrid storage."""

    def __init__(
        self,
        name: str,
        uri: str = os.getenv("ZAKU_SERVER_URI", "http://localhost:9000"),
        redis_uri: str = os.getenv("REDIS_URI", "redis://localhost:6379"),
        mongodb_uri: str = os.getenv("MONGODB_URI", "mongodb://localhost:27017"),
        database: str = os.getenv("MONGODB_DATABASE", "zaku_hybrid"),
        collection: str = None,
        memory_threshold: float = float(os.getenv("STORAGE_MEMORY_THRESHOLD", "0.8")),
        memory_check_interval: int = int(os.getenv("STORAGE_MEMORY_CHECK_INTERVAL", "5")),
        batch_size: int = int(os.getenv("STORAGE_BATCH_SIZE", "100")),
    ):
        """
        Initialize the hybrid storage TaskQ.

        Args:
            name: Queue name
            uri: Zaku server URI
            redis_uri: Redis connection URI
            mongodb_uri: MongoDB connection URI
            database: MongoDB database name
            collection: MongoDB collection name (defaults to queue name)
            memory_threshold: Redis memory usage threshold
            memory_check_interval: Memory check interval in seconds
            batch_size: Batch operation size
        """
        self.name = name
        self.uri = uri
        self.collection = collection or f"queue_{name}"

        # Hybrid storage configuration
        config = StorageConfig(
            redis_uri=redis_uri,
            mongodb_uri=mongodb_uri,
            database=database,
            collection=self.collection,
            memory_threshold=memory_threshold,
            memory_check_interval=memory_check_interval,
            batch_size=batch_size,
        )

        # Initialize hybrid storage
        self.storage = HybridStorage(config)

        # Queue-related keys
        self.prefix = f"zaku:queue:{name}:"
        self.task_prefix = f"{self.prefix}task:"
        self.meta_prefix = f"{self.prefix}meta:"

        logger.info(f"Hybrid TaskQ initialized successfully: {name}")

    def _generate_key(self, task_id: str = None) -> str:
        """Generate storage key."""
        if task_id:
            return f"{self.task_prefix}{task_id}"
        return f"{self.meta_prefix}counter"

    def _generate_task_id(self) -> str:
        """Generate a unique task ID."""
        return f"{int(time.time() * 1000)}_{id(self)}_{str(uuid4())}"

    def add(self, data: Any, priority: int = 0) -> str:
        """
        Add a task to the queue.

        Args:
            data: Task data
            priority: Task priority (lower number = higher priority)

        Returns:
            Task ID
        """
        task_id = self._generate_task_id()
        task_data = {
            "id": task_id,
            "data": data,
            "priority": priority,
            "created_at": time.time(),
            "status": "pending",
        }

        key = self._generate_key(task_id)
        if self.storage.add(key, task_data):
            logger.info(f"Task added successfully: {task_id}")
            return task_id
        else:
            raise Exception(f"Failed to add task: {task_id}")

    def take(self, timeout: float = None) -> Optional[Any]:
        """
        Take a task from the queue.

        Args:
            timeout: Timeout in seconds

        Returns:
            Task data, or None if no task available
        """
        start_time = time.time()

        while True:
            task_keys = self._get_all_task_keys()

            if not task_keys:
                if timeout and (time.time() - start_time) > timeout:
                    logger.debug("Task waiting timeout reached")
                    return None
                time.sleep(0.1)
                continue

            # Sort tasks by priority
            sorted_tasks = self._sort_tasks_by_priority(task_keys)

            for task_key in sorted_tasks:
                task_data = self.storage.get(task_key)
                if task_data and task_data.get("status") == "pending":
                    task_data["status"] = "processing"
                    task_data["taken_at"] = time.time()
                    self.storage.add(task_key, task_data)

                    logger.info(f"Task taken successfully: {task_data['id']}")
                    return task_data["data"]

            if timeout and (time.time() - start_time) > timeout:
                logger.debug("Task waiting timeout reached")
                return None

            time.sleep(0.1)

    def _get_all_task_keys(self) -> List[str]:
        """Get all task keys."""
        try:
            return self.storage.scan_keys(f"{self.task_prefix}*")
        except Exception as e:
            logger.error(f"Failed to fetch task keys: {e}")
            return []

    def _sort_tasks_by_priority(self, task_keys: List[str]) -> List[str]:
        """Sort tasks by priority (lower number = higher priority)."""
        tasks_with_priority = []

        for key in task_keys:
            task_data = self.storage.get(key)
            if task_data:
                tasks_with_priority.append((key, task_data.get("priority", 0)))

        sorted_tasks = sorted(tasks_with_priority, key=lambda x: x[1])
        return [task[0] for task in sorted_tasks]

    def complete(self, task_id: str) -> bool:
        """
        Mark a task as completed.

        Args:
            task_id: Task ID

        Returns:
            Success status
        """
        key = self._generate_key(task_id)
        task_data = self.storage.get(key)

        if task_data:
            task_data["status"] = "completed"
            task_data["completed_at"] = time.time()

            if self.storage.add(key, task_data):
                logger.info(f"Task marked as completed: {task_id}")
                return True

        logger.warning(f"Failed to mark task as completed: {task_id}")
        return False

    def fail(self, task_id: str, error: str = None) -> bool:
        """
        Mark a task as failed.

        Args:
            task_id: Task ID
            error: Error message

        Returns:
            Success status
        """
        key = self._generate_key(task_id)
        task_data = self.storage.get(key)

        if task_data:
            task_data["status"] = "failed"
            task_data["failed_at"] = time.time()
            task_data["error"] = error

            if self.storage.add(key, task_data):
                logger.info(f"Task marked as failed: {task_id}")
                return True

        logger.warning(f"Failed to mark task as failed: {task_id}")
        return False

    def delete(self, task_id: str) -> bool:
        """
        Delete a task.

        Args:
            task_id: Task ID

        Returns:
            Success status
        """
        key = self._generate_key(task_id)
        if self.storage.delete(key):
            logger.info(f"Task deleted successfully: {task_id}")
            return True

        logger.warning(f"Failed to delete task: {task_id}")
        return False

    def get_task_info(self, task_id: str) -> Optional[Dict]:
        """
        Get task details.

        Args:
            task_id: Task ID

        Returns:
            Task information dictionary
        """
        key = self._generate_key(task_id)
        return self.storage.get(key)

    def get_queue_stats(self) -> Dict[str, Any]:
        """
        Get queue statistics.

        Returns:
            Queue statistics dictionary
        """
        stats = self.storage.get_storage_stats()
        stats.update({"queue_name": self.name, "queue_uri": self.uri})
        return stats

    def get_task_count_by_status(self) -> Dict[str, int]:
        """
        Get task counts by status.

        Returns:
            Dictionary of counts by status
        """
        status_counts = {"pending": 0, "processing": 0, "completed": 0, "failed": 0}

        try:
            task_keys = self._get_all_task_keys()
            for key in task_keys:
                task_data = self.storage.get(key)
                if task_data:
                    status = task_data.get("status", "unknown")
                    status_counts[status] = status_counts.get(status, 0) + 1
        except Exception as e:
            logger.error(f"Failed to get task status counts: {e}")

        return status_counts

    def reset(self) -> bool:
        """
        Reset the queue (clear all tasks).

        Returns:
            Success status
        """
        if self.storage.clear():
            logger.info(f"Queue reset successfully: {self.name}")
            return True

        logger.warning(f"Failed to reset queue: {self.name}")
        return False

    def cleanup_completed_tasks(self, max_age_hours: int = 24) -> int:
        """
        Clean up completed/failed tasks older than a given age.

        Args:
            max_age_hours: Maximum age to keep in hours

        Returns:
            Number of tasks cleaned up
        """
        cleaned_count = 0
        cutoff_time = time.time() - (max_age_hours * 3600)

        try:
            task_keys = self._get_all_task_keys()
            for key in task_keys:
                task_data = self.storage.get(key)
                if task_data:
                    status = task_data.get("status")
                    completed_at = task_data.get("completed_at", 0)

                    if status in ["completed", "failed"] and completed_at < cutoff_time:
                        if self.storage.delete(key):
                            cleaned_count += 1

            if cleaned_count > 0:
                logger.info(f"Cleaned up {cleaned_count} completed tasks")

        except Exception as e:
            logger.error(f"Failed to clean up completed tasks: {e}")

        return cleaned_count

    def migrate_tasks(self, target_storage: str = "auto") -> Dict[str, int]:
        """
        Migrate tasks to the specified storage.

        Args:
            target_storage: Target storage ("redis", "mongodb", "auto")

        Returns:
            Migration statistics
        """
        migration_stats = {"to_redis": 0, "to_mongodb": 0}

        try:
            if target_storage == "auto":
                if (
                    self.storage.redis_storage
                    and self.storage.redis_storage.is_memory_pressure_high()
                ):
                    task_keys = self._get_all_task_keys()
                    migration_stats["to_mongodb"] = self.storage.migrate_to_mongodb(
                        task_keys
                    )
                else:
                    task_keys = self._get_all_task_keys()
                    migration_stats["to_redis"] = self.storage.migrate_to_redis(
                        task_keys
                    )

            elif target_storage == "redis":
                task_keys = self._get_all_task_keys()
                migration_stats["to_redis"] = self.storage.migrate_to_redis(task_keys)

            elif target_storage == "mongodb":
                task_keys = self._get_all_task_keys()
                migration_stats["to_mongodb"] = self.storage.migrate_to_mongodb(
                    task_keys
                )

            logger.info(f"Task migration finished: {migration_stats}")

        except Exception as e:
            logger.error(f"Task migration failed: {e}")

        return migration_stats

    def publish(self, message: Any, topic: str) -> int:
        """
        Publish a message to a topic (API-compatible placeholder).

        Args:
            message: Message content
            topic: Topic name

        Returns:
            Number of subscribers
        """
        logger.info(f"Message published to topic: {topic}")
        return 1

    def subscribe_one(self, topic: str, timeout: float = None) -> Optional[Any]:
        """
        Subscribe to a single message from a topic (API-compatible placeholder).

        Args:
            topic: Topic name
            timeout: Timeout in seconds

        Returns:
            Message content or None
        """
        logger.info(f"Subscribed to topic: {topic}")
        return None

    def subscribe_stream(self, topic: str, timeout: float = None):
        """
        Subscribe to a stream of messages from a topic (API-compatible placeholder).

        Args:
            topic: Topic name
            timeout: Timeout in seconds

        Yields:
            Message content
        """
        logger.info(f"Subscribed to topic stream: {topic}")
        return []


class HybridTaskQFactory:
    """Factory class for creating HybridTaskQ instances."""

    @staticmethod
    def create(
        name: str,
        redis_uri: str = "redis://localhost:6379",
        mongodb_uri: str = "mongodb://localhost:27017",
        **kwargs,
    ) -> HybridTaskQ:
        """
        Create a HybridTaskQ instance.

        Args:
            name: Queue name
            redis_uri: Redis connection URI
            mongodb_uri: MongoDB connection URI
            **kwargs: Other configuration parameters

        Returns:
            HybridTaskQ instance
        """
        return HybridTaskQ(
            name=name,
            redis_uri=redis_uri,
            mongodb_uri=mongodb_uri,
            **kwargs,
        )


# Alias for backward compatibility
TaskQ = HybridTaskQ