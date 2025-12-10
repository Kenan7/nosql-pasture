"""
Redis schema for real-time metrics, alerts, and caching.
Uses hashes for field metrics, streams for alerts, sorted sets for scheduling.
"""

import redis
import json
from datetime import datetime, timedelta


class PastureRedisManager:
    """Manage Redis operations for pasture management system."""
    
    def __init__(self, host='localhost', port=6379, db=0):
        self.client = redis.Redis(host=host, port=port, db=db, decode_responses=True)
    
    # ===== Field Metrics (Hashes) =====
    
    def update_field_metrics(self, field_id: str, metrics: dict):
        """
        Update latest metrics for a field using Redis hash.
        Key pattern: field:{field_id}
        """
        key = f"field:{field_id}"
        # Add timestamp
        metrics['updated_at'] = datetime.utcnow().isoformat()
        
        # Store each metric as a hash field
        self.client.hset(key, mapping=metrics)
        
        # Set expiration to 7 days
        self.client.expire(key, 7 * 24 * 3600)
        
        return key
    
    def get_field_metrics(self, field_id: str):
        """Get all metrics for a field."""
        key = f"field:{field_id}"
        return self.client.hgetall(key)
    
    def get_field_metric(self, field_id: str, metric_name: str):
        """Get a specific metric for a field."""
        key = f"field:{field_id}"
        return self.client.hget(key, metric_name)
    
    # ===== Alerts (Streams) =====
    
    def publish_alert(self, field_id: str, alert_type: str, value: float, 
                     threshold: float, severity: str = 'warning'):
        """
        Publish an alert to Redis stream.
        Stream name: alerts
        """
        alert_data = {
            'field_id': field_id,
            'type': alert_type,
            'value': str(value),
            'threshold': str(threshold),
            'severity': severity,
            'timestamp': datetime.utcnow().isoformat()
        }
        
        message_id = self.client.xadd('alerts', alert_data)
        
        # Trim stream to last 10000 entries
        self.client.xtrim('alerts', maxlen=10000, approximate=True)
        
        return message_id
    
    def read_alerts(self, count=10, block_ms=None):
        """Read recent alerts from stream."""
        streams = {'alerts': '0'} if block_ms is None else {'alerts': '$'}
        
        if block_ms:
            results = self.client.xread(streams, count=count, block=block_ms)
        else:
            results = self.client.xrevrange('alerts', '+', '-', count=count)
            return results
        
        return results
    
    def get_alerts_for_field(self, field_id: str, count=10):
        """Get recent alerts for a specific field."""
        all_alerts = self.client.xrevrange('alerts', '+', '-', count=100)
        field_alerts = [
            alert for alert in all_alerts 
            if alert[1].get('field_id') == field_id
        ]
        return field_alerts[:count]
    
    # ===== Maintenance Schedule (Sorted Sets) =====
    
    def schedule_maintenance(self, task_id: str, task_data: dict, scheduled_time: datetime):
        """
        Schedule a maintenance task using sorted set.
        Score = timestamp for ordering
        """
        score = scheduled_time.timestamp()
        value = json.dumps(task_data)
        
        self.client.zadd('maintenance_schedule', {f"{task_id}:{value}": score})
        return task_id
    
    def get_upcoming_maintenance(self, days=7):
        """Get maintenance tasks scheduled in next N days."""
        now = datetime.utcnow().timestamp()
        future = (datetime.utcnow() + timedelta(days=days)).timestamp()
        
        tasks = self.client.zrangebyscore('maintenance_schedule', now, future, withscores=True)
        
        result = []
        for task_data, score in tasks:
            task_id, data = task_data.split(':', 1)
            result.append({
                'task_id': task_id,
                'data': json.loads(data),
                'scheduled_time': datetime.fromtimestamp(score).isoformat()
            })
        
        return result
    
    def complete_maintenance(self, task_id: str):
        """Remove completed maintenance task."""
        # Find and remove tasks matching task_id
        tasks = self.client.zrange('maintenance_schedule', 0, -1)
        for task in tasks:
            if task.startswith(f"{task_id}:"):
                self.client.zrem('maintenance_schedule', task)
                return True
        return False
    
    # ===== Risk Assessment Cache =====
    
    def cache_risk_assessment(self, field_id: str, risk_score: float, 
                             factors: dict, ttl_seconds=300):
        """Cache risk assessment results."""
        key = f"risk:{field_id}"
        data = {
            'risk_score': risk_score,
            'factors': json.dumps(factors),
            'computed_at': datetime.utcnow().isoformat()
        }
        
        self.client.hset(key, mapping=data)
        self.client.expire(key, ttl_seconds)
        
        return key
    
    def get_risk_assessment(self, field_id: str):
        """Get cached risk assessment."""
        key = f"risk:{field_id}"
        data = self.client.hgetall(key)
        
        if data and 'factors' in data:
            data['factors'] = json.loads(data['factors'])
        
        return data if data else None
    
    # ===== Analytics Cache =====
    
    def cache_query_result(self, query_key: str, result: dict, ttl_seconds=600):
        """Cache expensive query results."""
        self.client.setex(
            f"cache:query:{query_key}",
            ttl_seconds,
            json.dumps(result)
        )
    
    def get_cached_query(self, query_key: str):
        """Get cached query result."""
        result = self.client.get(f"cache:query:{query_key}")
        return json.loads(result) if result else None


# Example usage patterns
EXAMPLE_OPERATIONS = """
# Initialize
redis_mgr = PastureRedisManager()

# Update field metrics
redis_mgr.update_field_metrics('field_001', {
    'latest_ndvi': '0.72',
    'latest_soil_moisture': '18.5',
    'latest_temp': '22.3',
    'grass_height_cm': '12.5'
})

# Get field metrics
metrics = redis_mgr.get_field_metrics('field_001')

# Publish alert
redis_mgr.publish_alert(
    field_id='field_001',
    alert_type='low_soil_moisture',
    value=9.2,
    threshold=12.0,
    severity='warning'
)

# Read recent alerts
alerts = redis_mgr.read_alerts(count=10)

# Schedule maintenance
from datetime import datetime, timedelta
redis_mgr.schedule_maintenance(
    task_id='maint_001',
    task_data={
        'field_id': 'field_001',
        'task_type': 'fertilizer_application',
        'details': 'Apply NPK 50-25-25'
    },
    scheduled_time=datetime.utcnow() + timedelta(days=3)
)

# Get upcoming tasks
tasks = redis_mgr.get_upcoming_maintenance(days=7)
"""


if __name__ == "__main__":
    # Example usage
    redis_mgr = PastureRedisManager()
    
    # Test operations
    print("Testing Redis operations...")
    
    # Update metrics
    redis_mgr.update_field_metrics('field_test', {
        'ndvi': '0.65',
        'soil_moisture': '15.2'
    })
    
    # Get metrics
    metrics = redis_mgr.get_field_metrics('field_test')
    print(f"Field metrics: {metrics}")
    
    # Publish alert
    msg_id = redis_mgr.publish_alert(
        'field_test', 'low_ndvi', 0.45, 0.5, 'warning'
    )
    print(f"Alert published: {msg_id}")
    
    # Schedule maintenance
    task_id = redis_mgr.schedule_maintenance(
        'test_task',
        {'type': 'irrigation', 'amount_mm': 25},
        datetime.utcnow() + timedelta(hours=2)
    )
    print(f"Maintenance scheduled: {task_id}")
    
    print("Redis operations test complete")
