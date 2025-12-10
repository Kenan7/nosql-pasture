"""
Example queries demonstrating analytics across all 4 NoSQL databases.
Answers key agronomic questions using multi-database integration.
"""

from pymongo import MongoClient
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
import sys
sys.path.append('..')

from models.redis_schema import PastureRedisManager
from models.neo4j_schema import PastureNeo4jManager


class PastureAnalytics:
    """Multi-database analytics for pasture management."""
    
    def __init__(self):
        # Initialize connections
        self.mongo_client = MongoClient('mongodb://admin:password@localhost:27017/')
        self.mongodb = self.mongo_client['pasture_management']
        
        cluster = Cluster(['localhost'], port=9042)
        self.cassandra = cluster.connect('pasture_sensors')
        
        self.redis_mgr = PastureRedisManager()
        self.neo4j_mgr = PastureNeo4jManager()
    
    def find_at_risk_fields(self):
        """
        Query 1: Which fields are currently at risk of low forage quality?
        Combines Redis (latest metrics), MongoDB (field metadata), and Neo4j (treatment history).
        """
        print("\n=== Query 1: At-Risk Fields Analysis ===\n")
        
        at_risk_fields = []
        
        # Get all fields from MongoDB
        fields = list(self.mongodb.fields.find({}, {'field_id': 1, 'name': 1, 'soil_type': 1}))
        
        for field in fields:
            field_id = field['field_id']
            
            # Get latest metrics from Redis
            metrics = self.redis_mgr.get_field_metrics(field_id)
            
            if not metrics:
                continue
            
            # Extract key metrics
            ndvi = float(metrics.get('ndvi', 1.0))
            soil_moisture = float(metrics.get('soil_moisture', 100.0))
            temperature = float(metrics.get('temperature', 0.0))
            grass_height = float(metrics.get('grass_height', 100.0))
            
            # Risk criteria
            is_at_risk = False
            risk_factors = []
            risk_score = 0
            
            if ndvi < 0.5:
                is_at_risk = True
                risk_factors.append(f"Low NDVI ({ndvi})")
                risk_score += 3
            
            if soil_moisture < 15.0:
                is_at_risk = True
                risk_factors.append(f"Low soil moisture ({soil_moisture}%)")
                risk_score += 2
            
            if temperature > 30.0:
                is_at_risk = True
                risk_factors.append(f"High temperature ({temperature}°C)")
                risk_score += 1
            
            if grass_height < 6.0:
                is_at_risk = True
                risk_factors.append(f"Low grass height ({grass_height}cm)")
                risk_score += 2
            
            if is_at_risk:
                # Get treatment history from Neo4j
                treatments = self.neo4j_mgr.find_high_risk_fields_with_history(risk_threshold=0)
                
                at_risk_fields.append({
                    'field_id': field_id,
                    'name': field['name'],
                    'soil_type': field['soil_type'],
                    'risk_score': risk_score,
                    'risk_factors': risk_factors,
                    'metrics': {
                        'ndvi': ndvi,
                        'soil_moisture': soil_moisture,
                        'temperature': temperature,
                        'grass_height': grass_height
                    }
                })
        
        # Sort by risk score
        at_risk_fields.sort(key=lambda x: x['risk_score'], reverse=True)
        
        print(f"Found {len(at_risk_fields)} at-risk fields:\n")
        for i, field in enumerate(at_risk_fields[:5], 1):  # Show top 5
            print(f"{i}. {field['name']} ({field['field_id']})")
            print(f"   Risk Score: {field['risk_score']}")
            print(f"   Factors: {', '.join(field['risk_factors'])}")
            print(f"   Metrics: NDVI={field['metrics']['ndvi']:.2f}, "
                  f"Moisture={field['metrics']['soil_moisture']:.1f}%, "
                  f"Temp={field['metrics']['temperature']:.1f}°C")
            print()
        
        return at_risk_fields
    
    def analyze_time_series(self, field_id, days=30):
        """
        Query 2: Time-series analysis in Cassandra.
        Calculate average grass height per field over last N days.
        """
        print(f"\n=== Query 2: Time-Series Analysis for {field_id} ===\n")
        
        # Query Cassandra for grass height over time
        query = """
            SELECT sensor_ts, metric_value
            FROM sensor_data_by_field
            WHERE field_id = %s
              AND metric_type = 'grass_height'
              AND sensor_ts >= %s
            ALLOW FILTERING
        """
        
        start_date = datetime.utcnow() - timedelta(days=days)
        rows = self.cassandra.execute(query, (field_id, start_date))
        
        # Aggregate by day
        daily_data = {}
        for row in rows:
            day = row.sensor_ts.date()
            if day not in daily_data:
                daily_data[day] = []
            daily_data[day].append(row.metric_value)
        
        # Calculate daily averages
        daily_averages = {
            day: sum(values) / len(values)
            for day, values in daily_data.items()
        }
        
        if daily_averages:
            overall_avg = sum(daily_averages.values()) / len(daily_averages)
            values_list = list(daily_averages.values())
            trend = values_list[-1] - values_list[0] if len(values_list) > 1 else 0
            
            print(f"Grass Height Analysis (last {days} days):")
            print(f"  Average height: {overall_avg:.1f} cm")
            print(f"  Number of readings: {sum(len(v) for v in daily_data.values())}")
            print(f"  Trend: {'Increasing' if trend > 0 else 'Decreasing'} ({trend:.1f} cm)\n")
            
            # Show weekly summary
            print("Weekly Summary:")
            sorted_days = sorted(daily_averages.keys())
            for i, day in enumerate(sorted_days[::7]):  # Every 7th day
                avg = daily_averages[day]
                print(f"  Week {i+1} ({day}): {avg:.1f} cm")
        else:
            print(f"No grass height data found for {field_id}")
        
        return daily_averages
    
    def geospatial_query(self, center_lon, center_lat, radius_km=5):
        """
        Query 3: Geospatial query in MongoDB.
        Find all fields within N km of a point (e.g., weather station).
        """
        print(f"\n=== Query 3: Geospatial Search (within {radius_km}km) ===\n")
        
        # Convert km to meters
        radius_meters = radius_km * 1000
        
        # MongoDB geospatial query
        query = {
            'boundary': {
                '$near': {
                    '$geometry': {
                        'type': 'Point',
                        'coordinates': [center_lon, center_lat]
                    },
                    '$maxDistance': radius_meters
                }
            }
        }
        
        # Note: This requires a 2dsphere index on boundary field
        # For this example, we'll use a simpler approach
        
        all_fields = list(self.mongodb.fields.find({}))
        
        # Calculate approximate distance
        from math import radians, cos, sin, asin, sqrt
        
        def haversine(lon1, lat1, lon2, lat2):
            """Calculate distance between two points on Earth."""
            lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
            dlon = lon2 - lon1
            dlat = lat2 - lat1
            a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
            c = 2 * asin(sqrt(a))
            km = 6371 * c
            return km
        
        nearby_fields = []
        for field in all_fields:
            # Get center of polygon
            coords = field['boundary']['coordinates'][0]
            field_lon = sum(c[0] for c in coords) / len(coords)
            field_lat = sum(c[1] for c in coords) / len(coords)
            
            distance = haversine(center_lon, center_lat, field_lon, field_lat)
            
            if distance <= radius_km:
                nearby_fields.append({
                    'field_id': field['field_id'],
                    'name': field['name'],
                    'distance_km': round(distance, 2),
                    'slope': field['terrain']['slope_degrees']
                })
        
        nearby_fields.sort(key=lambda x: x['distance_km'])
        
        print(f"Found {len(nearby_fields)} fields within {radius_km}km:\n")
        for field in nearby_fields[:10]:
            print(f"  {field['name']} ({field['field_id']}): {field['distance_km']}km, Slope: {field['slope']}°")
        
        return nearby_fields
    
    def graph_query_same_treatment(self, farmer_id='farmer_001'):
        """
        Query 4: Neo4j graph query.
        Find fields that share the same farmer and received same treatment in last year.
        """
        print(f"\n=== Query 4: Fields with Same Treatment (Farmer: {farmer_id}) ===\n")
        
        # Find all fields for this farmer
        fields = self.neo4j_mgr.find_fields_by_farmer(farmer_id)
        
        print(f"Farmer {farmer_id} owns {len(fields)} fields")
        
        # Find fields with fertilizer treatment
        fertilizer_fields = self.neo4j_mgr.find_fields_with_same_treatment(
            farmer_id, 'fertilizer', days=365
        )
        
        print(f"\nFields that received fertilizer treatment:")
        for field in fertilizer_fields:
            print(f"  - {field['name']} ({field['field_id']}) on {field.get('treatment_date', 'N/A')}")
        
        # Find fields with irrigation treatment
        irrigation_fields = self.neo4j_mgr.find_fields_with_same_treatment(
            farmer_id, 'irrigation', days=365
        )
        
        print(f"\nFields that received irrigation:")
        for field in irrigation_fields:
            print(f"  - {field['name']} ({field['field_id']}) on {field.get('treatment_date', 'N/A')}")
        
        return {
            'all_fields': fields,
            'fertilizer_fields': fertilizer_fields,
            'irrigation_fields': irrigation_fields
        }
    
    def redis_alert_example(self, field_id):
        """
        Query 5: Redis real-time operations.
        Demonstrate alert publishing and retrieval.
        """
        print(f"\n=== Query 5: Redis Real-Time Alerts for {field_id} ===\n")
        
        # Get current metrics
        metrics = self.redis_mgr.get_field_metrics(field_id)
        
        print(f"Current metrics for {field_id}:")
        for key, value in metrics.items():
            if key != 'updated_at':
                print(f"  {key}: {value}")
        
        # Get recent alerts for this field
        alerts = self.redis_mgr.get_alerts_for_field(field_id, count=5)
        
        print(f"\nRecent alerts ({len(alerts)}):")
        for alert_id, alert_data in alerts:
            print(f"  [{alert_data.get('severity', 'info').upper()}] "
                  f"{alert_data.get('type')}: "
                  f"value={alert_data.get('value')}, "
                  f"threshold={alert_data.get('threshold')}")
        
        # Check if new alert should be triggered
        if 'soil_moisture' in metrics:
            moisture = float(metrics['soil_moisture'])
            if moisture < 12.0:
                print(f"\n⚠️  NEW ALERT: Soil moisture ({moisture}%) below critical threshold (12%)")
                self.redis_mgr.publish_alert(
                    field_id, 'critical_soil_moisture', moisture, 12.0, 'critical'
                )
        
        return metrics, alerts
    
    def get_recommendations(self, field_id):
        """
        Query 6: Get advisory recommendations from Neo4j.
        """
        print(f"\n=== Query 6: Advisory Recommendations for {field_id} ===\n")
        
        rules = self.neo4j_mgr.get_applicable_rules_for_field(field_id)
        
        if rules:
            print(f"Found {len(rules)} applicable recommendations:\n")
            for i, rule in enumerate(rules, 1):
                print(f"{i}. {rule['description']} (Priority: {rule['priority']})")
                print(f"   Conditions: {rule['conditions']}")
                print(f"   Action: {rule['action']}\n")
        else:
            print("No specific recommendations found for this field.")
        
        return rules
    
    def run_all_queries(self):
        """Run all example queries."""
        print("\n" + "="*70)
        print("PASTURE MANAGEMENT ANALYTICS - MULTI-DATABASE QUERIES")
        print("="*70)
        
        # Query 1: At-risk fields
        at_risk = self.find_at_risk_fields()
        
        # Query 2: Time-series analysis (first field)
        if at_risk:
            self.analyze_time_series(at_risk[0]['field_id'], days=30)
        
        # Query 3: Geospatial (San Francisco area)
        self.geospatial_query(-122.4194, 37.7749, radius_km=10)
        
        # Query 4: Graph query for farmer
        self.graph_query_same_treatment('farmer_001')
        
        # Query 5: Redis alerts
        if at_risk:
            self.redis_alert_example(at_risk[0]['field_id'])
        
        # Query 6: Recommendations
        if at_risk:
            self.get_recommendations(at_risk[0]['field_id'])
        
        print("\n" + "="*70)
        print("ANALYTICS QUERIES COMPLETE")
        print("="*70 + "\n")
    
    def close(self):
        """Close all connections."""
        self.mongo_client.close()
        self.neo4j_mgr.close()


if __name__ == "__main__":
    analytics = PastureAnalytics()
    
    try:
        analytics.run_all_queries()
    finally:
        analytics.close()
