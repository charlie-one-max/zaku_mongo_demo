#!/usr/bin/env python3
"""
Redis + MongoDB Hybrid Storage System
========================================

Automatic memory pressure detection, intelligent data migration,
seamless CRUD operations.
"""

import json
import time
import logging
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional
from dataclasses import dataclass
from enum import Enum

# Logging configuration
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class StorageType(Enum):
    """Storage type enumeration"""
    REDIS = "redis"
    MONGODB = "mongodb"


@dataclass
class StorageConfig:
    """Storage configuration"""
    redis_uri: str = "redis://localhost:6379"
    mongodb_uri: str = "mongodb://localhost:27017"
    database: str = "zaku_hybrid"
    collection: str = "task_queue"
    memory_threshold: float = 0.8  # Redis memory usage threshold (80%)
    memory_check_interval: int = 5  # Interval for memory check (seconds)
    batch_size: int = 100  # Batch operation size


class MemoryMonitor:
    """Redis memory monitor"""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.last_check = 0
        self.cached_memory_usage = 0.0

    def get_memory_usage(self) -> float:
        """Get Redis memory usage ratio"""
        current_time = time.time()

        # Cache memory usage to avoid frequent queries
        if current_time - self.last_check > 5:
            try:
                info = self.redis_client.info('memory')
                used_memory = info.get('used_memory', 0)
                max_memory = info.get('maxmemory', 0)

                if max_memory > 0:
                    self.cached_memory_usage = used_memory / max_memory
                else:
                    # If maxmemory is not set, assume 50%
                    self.cached_memory_usage = 0.5

                self.last_check = current_time
                logger.debug(
                    f"Redis memory usage: {self.cached_memory_usage:.2%}"
                )

            except Exception as e:
                logger.warning(f"Failed to get Redis memory info: {e}")
                self.cached_memory_usage = 0.5  # Default value

        return self.cached_memory_usage

    def is_memory_pressure_high(self) -> bool:
        """Check if memory pressure is high"""
        return self.get_memory_usage() > 0.8


class BaseStorage(ABC):
    """Abstract base class for storage"""

    @abstractmethod
    def add(self, key: str, data: Any) -> bool:
        """Add data"""
        pass

    @abstractmethod
    def get(self, key: str) -> Optional[Any]:
        """Get data"""
        pass

    @abstractmethod
    def delete(self, key: str) -> bool:
        """Delete data"""
        pass

    @abstractmethod
    def exists(self, key: str) -> bool:
        """Check if data exists"""
        pass

    @abstractmethod
    def clear(self) -> bool:
        """Clear all data"""
        pass

    @abstractmethod
    def get_size(self) -> int:
        """Get number of records"""
        pass

    @abstractmethod
    def scan_keys(self, pattern: str = "*") -> List[str]:
        """Scan keys"""
        pass


class RedisStorage(BaseStorage):
    """Redis storage implementation"""

    def __init__(self, redis_client):
        self.redis_client = redis_client
        self.memory_monitor = MemoryMonitor(redis_client)

    def add(self, key: str, data: Any) -> bool:
        """Add data to Redis"""
        try:
            if isinstance(data, (dict, list)):
                serialized_data = json.dumps(data, ensure_ascii=False)
            else:
                serialized_data = str(data)

            self.redis_client.set(key, serialized_data)
            return True
        except Exception as e:
            logger.error(f"Failed to add data to Redis: {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """Get data from Redis"""
        try:
            data = self.redis_client.get(key)
            if data:
                try:
                    return json.loads(data)
                except json.JSONDecodeError:
                    return data.decode('utf-8')
            return None
        except Exception as e:
            logger.error(f"Failed to get data from Redis: {e}")
            return None

    def delete(self, key: str) -> bool:
        """Delete data from Redis"""
        try:
            return bool(self.redis_client.delete(key))
        except Exception as e:
            logger.error(f"Failed to delete data from Redis: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if data exists in Redis"""
        try:
            return bool(self.redis_client.exists(key))
        except Exception as e:
            logger.error(f"Failed to check existence in Redis: {e}")
            return False

    def clear(self) -> bool:
        """Clear all data in Redis"""
        try:
            self.redis_client.flushdb()
            return True
        except Exception as e:
            logger.error(f"Failed to clear Redis data: {e}")
            return False

    def get_size(self) -> int:
        """Get number of records in Redis"""
        try:
            return self.redis_client.dbsize()
        except Exception as e:
            logger.error(f"Failed to get Redis size: {e}")
            return 0

    def scan_keys(self, pattern: str = "*") -> List[str]:
        """Scan Redis keys"""
        try:
            keys = []
            cursor = 0

            while True:
                cursor, batch_keys = self.redis_client.scan(
                    cursor=cursor,
                    match=pattern,
                    count=100
                )
                keys.extend(batch_keys)

                if cursor == 0:
                    break

            return [
                key.decode('utf-8') if isinstance(key, bytes) else key
                for key in keys
            ]
        except Exception as e:
            logger.error(f"Failed to scan Redis keys: {e}")
            return []

    def is_memory_pressure_high(self) -> bool:
        """Check memory pressure in Redis"""
        return self.memory_monitor.is_memory_pressure_high()


class MongoDBStorage(BaseStorage):
    """MongoDB storage implementation"""

    def __init__(self, mongodb_client, database: str, collection: str):
        self.db = mongodb_client[database]
        self.collection = self.db[collection]
        self._ensure_indexes()

    def _ensure_indexes(self):
        """Ensure required indexes exist"""
        try:
            self.collection.create_index("key", unique=True)
            self.collection.create_index("created_at")
            logger.info("MongoDB indexes created successfully")
        except Exception as e:
            logger.warning(f"Failed to create MongoDB indexes: {e}")

    def add(self, key: str, data: Any) -> bool:
        """Add data to MongoDB"""
        try:
            document = {
                "key": key,
                "data": data,
                "created_at": time.time(),
                "updated_at": time.time()
            }
            self.collection.replace_one(
                {"key": key},
                document,
                upsert=True
            )
            return True
        except Exception as e:
            logger.error(f"Failed to add data to MongoDB: {e}")
            return False

    def get(self, key: str) -> Optional[Any]:
        """Get data from MongoDB"""
        try:
            document = self.collection.find_one({"key": key})
            if document:
                return document.get("data")
            return None
        except Exception as e:
            logger.error(f"Failed to get data from MongoDB: {e}")
            return None

    def delete(self, key: str) -> bool:
        """Delete data from MongoDB"""
        try:
            result = self.collection.delete_one({"key": key})
            return result.deleted_count > 0
        except Exception as e:
            logger.error(f"Failed to delete data from MongoDB: {e}")
            return False

    def exists(self, key: str) -> bool:
        """Check if data exists in MongoDB"""
        try:
            return bool(self.collection.count_documents({"key": key}))
        except Exception as e:
            logger.error(f"Failed to check existence in MongoDB: {e}")
            return False

    def clear(self) -> bool:
        """Clear all data in MongoDB"""
        try:
            self.collection.delete_many({})
            return True
        except Exception as e:
            logger.error(f"Failed to clear MongoDB data: {e}")
            return False

    def get_size(self) -> int:
        """Get number of records in MongoDB"""
        try:
            return self.collection.count_documents({})
        except Exception as e:
            logger.error(f"Failed to get MongoDB size: {e}")
            return 0

    def scan_keys(self, pattern: str = "*") -> List[str]:
        """Scan MongoDB keys"""
        try:
            cursor = self.collection.find({}, {"key": 1})
            return [doc["key"] for doc in cursor]
        except Exception as e:
            logger.error(f"Failed to scan MongoDB keys: {e}")
            return []

    def batch_get(self, keys: List[str]) -> Dict[str, Any]:
        """Batch get data from MongoDB"""
        try:
            documents = self.collection.find({"key": {"$in": keys}})
            return {doc["key"]: doc["data"] for doc in documents}
        except Exception as e:
            logger.error(f"Failed to batch get data from MongoDB: {e}")
            return {}

    def batch_delete(self, keys: List[str]) -> int:
        """Batch delete data from MongoDB"""
        try:
            result = self.collection.delete_many({"key": {"$in": keys}})
            return result.deleted_count
        except Exception as e:
            logger.error(f"Failed to batch delete from MongoDB: {e}")
            return 0


class HybridStorage:
    """Hybrid storage system using Redis + MongoDB"""

    def __init__(self, config: StorageConfig):
        self.config = config
        self.redis_storage = None
        self.mongodb_storage = None
        self._init_storages()

    def _init_storages(self):
        """Initialize storage systems"""
        try:
            import redis
            redis_client = redis.from_url(self.config.redis_uri)
            self.redis_storage = RedisStorage(redis_client)
            logger.info("Redis storage initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Redis storage: {e}")
            self.redis_storage = None

        try:
            from pymongo import MongoClient
            mongodb_client = MongoClient(self.config.mongodb_uri)
            self.mongodb_storage = MongoDBStorage(
                mongodb_client,
                self.config.database,
                self.config.collection
            )
            logger.info("MongoDB storage initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize MongoDB storage: {e}")
            self.mongodb_storage = None

    def add(self, key: str, data: Any) -> bool:
        """Smart add data"""
        if self.redis_storage and not self.redis_storage.is_memory_pressure_high():
            if self.redis_storage.add(key, data):
                logger.debug(f"Data added to Redis: {key}")
                return True

        if self.mongodb_storage:
            if self.mongodb_storage.add(key, data):
                logger.info(f"Data added to MongoDB: {key}")
                return True

        logger.error(f"Failed to add data: {key}")
        return False

    def get(self, key: str) -> Optional[Any]:
        """Smart get data"""
        if self.redis_storage:
            data = self.redis_storage.get(key)
            if data is not None:
                logger.debug(f"Data retrieved from Redis: {key}")
                return data

        if self.mongodb_storage:
            data = self.mongodb_storage.get(key)
            if data is not None:
                logger.info(f"Data retrieved from MongoDB: {key}")
                if self.redis_storage and not self.redis_storage.is_memory_pressure_high():
                    self.redis_storage.add(key, data)
                return data

        logger.debug(f"Data not found: {key}")
        return None

    def delete(self, key: str) -> bool:
        """Smart delete data"""
        success = False

        if self.redis_storage and self.redis_storage.delete(key):
            success = True

        if self.mongodb_storage and self.mongodb_storage.delete(key):
            success = True

        if success:
            logger.debug(f"Data deleted: {key}")
        else:
            logger.warning(f"Failed to delete data: {key}")

        return success

    def exists(self, key: str) -> bool:
        """Check if data exists"""
        if self.redis_storage and self.redis_storage.exists(key):
            return True
        if self.mongodb_storage and self.mongodb_storage.exists(key):
            return True
        return False

    def clear(self) -> bool:
        """Clear all data"""
        success = True

        if self.redis_storage:
            success &= self.redis_storage.clear()

        if self.mongodb_storage:
            success &= self.mongodb_storage.clear()

        if success:
            logger.info("All data cleared successfully")
        else:
            logger.warning("Some data failed to clear")

        return success

    def get_size(self) -> int:
        """Get total number of records"""
        total_size = 0
        if self.redis_storage:
            total_size += self.redis_storage.get_size()
        if self.mongodb_storage:
            total_size += self.mongodb_storage.get_size()
        return total_size

    def scan_keys(self, pattern: str = "*") -> List[str]:
        """Scan all keys"""
        all_keys = []
        if self.redis_storage:
            all_keys.extend(self.redis_storage.scan_keys(pattern))
        if self.mongodb_storage:
            all_keys.extend(self.mongodb_storage.scan_keys(pattern))
        return list(set(all_keys))

    def get_storage_stats(self) -> Dict[str, Any]:
        """Get storage statistics"""
        stats = {
            "redis_available": self.redis_storage is not None,
            "mongodb_available": self.mongodb_storage is not None,
            "total_size": self.get_size()
        }

        if self.redis_storage:
            stats.update({
                "redis_size": self.redis_storage.get_size(),
                "redis_memory_pressure": self.redis_storage.is_memory_pressure_high()
            })

        if self.mongodb_storage:
            stats.update({
                "mongodb_size": self.mongodb_storage.get_size()
            })

        return stats

    def migrate_to_redis(self, keys: List[str]) -> int:
        """Migrate data from MongoDB to Redis"""
        if not self.redis_storage or not self.mongodb_storage:
            return 0

        if self.redis_storage.is_memory_pressure_high():
            logger.warning("High Redis memory pressure, migration aborted")
            return 0

        migrated_count = 0
        batch_keys = keys[:self.config.batch_size]

        for key in batch_keys:
            data = self.mongodb_storage.get(key)
            if data and self.redis_storage.add(key, data):
                migrated_count += 1

        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} records to Redis")

        return migrated_count

    def migrate_to_mongodb(self, keys: List[str]) -> int:
        """Migrate data from Redis to MongoDB"""
        if not self.redis_storage or not self.mongodb_storage:
            return 0

        migrated_count = 0
        batch_keys = keys[:self.config.batch_size]

        for key in batch_keys:
            data = self.redis_storage.get(key)
            if data and self.mongodb_storage.add(key, data):
                migrated_count += 1

        if migrated_count > 0:
            logger.info(f"Migrated {migrated_count} records to MongoDB")

        return migrated_count
