#!/usr/bin/env python3
"""
Redis + MongoDB Hybrid Storage System Demo
==============================================

Demonstrates automatic memory pressure detection,
intelligent data migration, and seamless CRUD operations.
"""

import time
import threading
import random
from hybrid_taskq import HybridTaskQ, HybridTaskQFactory


def demo_basic_operations():
    """Demonstrate basic operations"""
    print("🔧 Basic Operations Demo")
    print("=" * 40)

    # Create hybrid task queue
    queue = HybridTaskQ(
        name="demo-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017"
    )

    try:
        # Add tasks
        print("📝 Adding tasks...")
        task_ids = []
        for i in range(5):
            task_id = queue.add({
                "task_number": i + 1,
                "description": f"Demo task {i + 1}",
                "priority": random.randint(1, 5)
            }, priority=random.randint(1, 5))
            task_ids.append(task_id)
            print(f"  ✅ Task {i + 1} added successfully: {task_id}")

        # Get queue stats
        print("\n📊 Queue Stats:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Get task status stats
        print("\n📈 Task Status Stats:")
        status_stats = queue.get_task_count_by_status()
        for status, count in status_stats.items():
            print(f"  {status}: {count}")

        # Take tasks
        print("\n📤 Taking tasks...")
        for i in range(3):
            task_data = queue.take(timeout=5.0)
            if task_data:
                print(f"  ✅ Took task: {task_data}")
                # Mark task as completed
                queue.complete(task_ids[i])
            else:
                print(f"  ❌ Failed to take task")

        # Get updated status stats
        print("\n📈 Updated Task Status Stats:")
        status_stats = queue.get_task_count_by_status()
        for status, count in status_stats.items():
            print(f"  {status}: {count}")

        # Clean completed tasks
        print("\n🧹 Cleaning completed tasks...")
        cleaned_count = queue.cleanup_completed_tasks(
            max_age_hours=0)  # Immediate cleanup
        print(f"  ✅ Cleaned {cleaned_count} tasks")

        # Reset queue
        print("\n🔄 Resetting queue...")
        if queue.reset():
            print("  ✅ Queue reset successfully")
        else:
            print("  ❌ Queue reset failed")

    except Exception as e:
        print(f"❌ Basic operations demo failed: {e}")


def demo_memory_pressure_simulation():
    """Demonstrate memory pressure simulation"""
    print("\n💾 Memory Pressure Simulation Demo")
    print("=" * 40)

    queue = HybridTaskQ(
        name="pressure-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017",
        memory_threshold=0.6  # Lower threshold for demo
    )

    try:
        # Add large number of tasks to simulate memory pressure
        print("📝 Adding large number of tasks to simulate pressure...")
        task_ids = []

        for i in range(20):
            large_data = {
                "task_id": f"large_task_{i}",
                "data": "x" * 1000,  # 1KB data
                "timestamp": time.time(),
                "metadata": {
                    "size": 1000,
                    "type": "large_task",
                    "priority": random.randint(1, 10)
                }
            }

            task_id = queue.add(large_data, priority=random.randint(1, 10))
            task_ids.append(task_id)

            if (i + 1) % 5 == 0:
                print(f"  ✅ Added {i + 1} tasks")

        # Get storage stats
        print("\n📊 Storage Stats:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        # Simulate memory pressure detection
        print("\n🔍 Memory Pressure Detection:")
        if queue.storage.redis_storage:
            memory_usage = queue.storage.redis_storage.memory_monitor.get_memory_usage()
            is_high = queue.storage.redis_storage.is_memory_pressure_high()
            print(f"  Redis memory usage: {memory_usage:.2%}")
            print(f"  Pressure status: {'High' if is_high else 'Normal'}")

        # Manual migration
        print("\n🚚 Manual Task Migration...")
        migration_stats = queue.migrate_tasks(target_storage="auto")
        print(f"  Migration stats: {migration_stats}")

        # Get stats after migration
        print("\n📊 Storage Stats After Migration:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        queue.reset()

    except Exception as e:
        print(f"❌ Memory pressure simulation failed: {e}")


def demo_concurrent_operations():
    """Demonstrate concurrent operations"""
    print("\n⚡ Concurrent Operations Demo")
    print("=" * 40)

    queue = HybridTaskQ(
        name="concurrent-queue",
        redis_uri="redis://localhost:6379",
        mongodb_uri="mongodb://localhost:27017"
    )

    def producer(producer_id: int, task_count: int):
        """Producer: add tasks"""
        for i in range(task_count):
            task_data = {
                "producer_id": producer_id,
                "task_number": i + 1,
                "timestamp": time.time()
            }

            try:
                task_id = queue.add(task_data, priority=random.randint(1, 5))
                print(f"  🏭 Producer {producer_id} added task: {task_id}")
                time.sleep(random.uniform(0.1, 0.3))
            except Exception as e:
                print(f"  ❌ Producer {producer_id} failed to add task: {e}")

    def consumer(consumer_id: int, task_count: int):
        """Consumer: process tasks"""
        processed_count = 0

        while processed_count < task_count:
            try:
                task_data = queue.take(timeout=2.0)
                if task_data:
                    print(
                        f"  🏪 Consumer {consumer_id} processed task: {task_data}")
                    processed_count += 1
                    time.sleep(random.uniform(0.1, 0.2))
                else:
                    time.sleep(0.1)
            except Exception as e:
                print(
                    f"  ❌ Consumer {consumer_id} failed to process task: {e}")
                time.sleep(0.1)

        print(
            f"  ✅ Consumer {consumer_id} finished, processed {processed_count} tasks")

    try:
        print("🚀 Starting concurrent operations...")

        producer_threads = []
        consumer_threads = []

        # Start 3 producers, each producing 5 tasks
        for i in range(3):
            thread = threading.Thread(target=producer, args=(i + 1, 5))
            producer_threads.append(thread)
            thread.start()

        # Start 2 consumers, each consuming 8 tasks
        for i in range(2):
            thread = threading.Thread(target=consumer, args=(i + 1, 8))
            consumer_threads.append(thread)
            thread.start()

        for thread in producer_threads:
            thread.join()

        for thread in consumer_threads:
            thread.join()

        print("✅ All concurrent operations completed")

        print("\n📊 Final Queue Stats:")
        stats = queue.get_queue_stats()
        for key, value in stats.items():
            print(f"  {key}: {value}")

        queue.reset()

    except Exception as e:
        print(f"❌ Concurrent operations demo failed: {e}")


def demo_factory_pattern():
    """Demonstrate factory pattern"""
    print("\n🏭 Factory Pattern Demo")
    print("=" * 40)

    try:
        queue1 = HybridTaskQFactory.create(
            name="factory-queue-1",
            redis_uri="redis://localhost:6379",
            mongodb_uri="mongodb://localhost:27017"
        )

        queue2 = HybridTaskQFactory.create(
            name="factory-queue-2",
            redis_uri="redis://localhost:6379",
            mongodb_uri="mongodb://localhost:27017"
        )

        print("✅ Queues created using factory pattern")
        print(f"  Queue1: {queue1.name}")
        print(f"  Queue2: {queue2.name}")

        task_id = queue1.add({"test": "factory pattern"})
        print(f"  ✅ Queue1 added task successfully: {task_id}")

        task_data = queue1.take(timeout=5.0)
        if task_data:
            print(f"  ✅ Queue1 took task successfully: {task_data}")

        queue1.reset()
        queue2.reset()

    except Exception as e:
        print(f"❌ Factory pattern demo failed: {e}")


def main():
    """Main function"""
    print("🚀 Redis + MongoDB Hybrid Storage System Demo")
    print("=" * 60)
    print()

    try:
        demo_basic_operations()
        demo_memory_pressure_simulation()
        demo_concurrent_operations()
        demo_factory_pattern()

        print("\n🎉 All demos completed!")
        print("\n📚 System Features Summary:")
        print("• ✅ Automatic memory pressure detection")
        print("• ✅ Intelligent data migration")
        print("• ✅ Seamless CRUD operations")
        print("• ✅ High concurrency support")
        print("• ✅ Task priority management")
        print("• ✅ Automatic cleanup mechanism")
        print("• ✅ Factory pattern support")

    except Exception as e:
        print(f"\n❌ Demo execution failed: {e}")
        print("\n🔧 Please ensure:")
        print("1. Redis service is running")
        print("2. MongoDB service is running")
        print("3. Required dependencies are installed")


if __name__ == "__main__":
    main()
