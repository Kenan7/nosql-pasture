"""
Main ingestion pipeline for pasture management system.
Loads generated data into MongoDB, Cassandra, Redis, and Neo4j.
"""

import sys
import time
from datetime import datetime, timedelta
sys.path.append('..')

from pymongo import MongoClient
from cassandra.cluster import Cluster
import redis
from neo4j import GraphDatabase

from data_generator import PastureDataGenerator
from models.mongodb_schema import init_mongodb_schema
from models.cassandra_schema import init_cassandra_schema
from models.redis_schema import PastureRedisManager
from models.neo4j_schema import PastureNeo4jManager


class PastureIngestionPipeline:
    """Orchestrates data ingestion across all NoSQL databases."""
    
    def __init__(self):
        """Initialize connections to all databases."""
        print("Initializing database connections...")
        
        # MongoDB
        self.mongo_client = MongoClient('mongodb://admin:password@localhost:27017/')
        self.mongodb = self.mongo_client['pasture_management']
        
        # Cassandra
        cluster = Cluster(['localhost'], port=9042)
        self.cassandra_session = cluster.connect()
        
        # Redis
        self.redis_mgr = PastureRedisManager()
        
        # Neo4j
        self.neo4j_mgr = PastureNeo4jManager()
        
        print("Database connections established")
    
    def initialize_schemas(self):
        """Initialize all database schemas."""
        print("\n=== Initializing Database Schemas ===")
        
        # Clear existing data for clean runs
        print("Cleaning existing data...")
        try:
            # Clear Cassandra
            self.cassandra_session.execute("TRUNCATE pasture_sensors.sensor_data_by_field")
            self.cassandra_session.execute("TRUNCATE pasture_sensors.aggregated_metrics_by_field")
            print("  Cassandra data cleared")
        except:
            pass  # Tables might not exist yet
        
        try:
            # Clear Redis
            self.redis_mgr.client.flushdb()
            print("  Redis data cleared")
        except:
            pass
        
        try:
            # Clear Neo4j
            with self.neo4j_mgr.driver.session() as session:
                session.run("MATCH (n) DETACH DELETE n")
            print("  Neo4j data cleared")
        except:
            pass
        
        # MongoDB
        print("MongoDB: Creating indexes...")
        init_mongodb_schema(self.mongo_client)
        
        # Cassandra
        print("Cassandra: Creating keyspace and tables...")
        init_cassandra_schema(self.cassandra_session)
        
        # Neo4j
        print("Neo4j: Creating constraints...")
        self.neo4j_mgr.init_schema()
        
        print("All schemas initialized successfully\n")
    
    def ingest_metadata(self, generator: PastureDataGenerator):
        """Ingest farm, field, and farmer metadata into MongoDB and Neo4j."""
        print("=== Ingesting Metadata ===")
        
        # Get data from generator
        farms = generator.get_all_farms()
        fields = generator.get_all_fields()
        farmers = generator.get_all_farmers()
        
        # Clear existing data (for idempotent runs)
        print("MongoDB: Clearing existing data...")
        self.mongodb.farms.delete_many({})
        self.mongodb.fields.delete_many({})
        self.mongodb.farmer_profiles.delete_many({})
        self.mongodb.treatment_events.delete_many({})
        
        # Insert into MongoDB
        print(f"MongoDB: Inserting {len(farms)} farms...")
        self.mongodb.farms.insert_many(farms)
        
        print(f"MongoDB: Inserting {len(fields)} fields...")
        self.mongodb.fields.insert_many(fields)
        
        print(f"MongoDB: Inserting {len(farmers)} farmer profiles...")
        farmer_profiles = [{
            'farmer_id': f['farmer_id'],
            'name': f['name'],
            'email': f['email'],
            'phone': f['phone'],
            'farms': [f['farm_id']],
            'preferences': {
                'alert_methods': ['email'],
                'report_frequency': 'weekly'
            }
        } for f in farmers]
        self.mongodb.farmer_profiles.insert_many(farmer_profiles)
        
        # Insert into Neo4j
        print(f"Neo4j: Creating {len(farmers)} farmer nodes...")
        for farmer in farmers:
            self.neo4j_mgr.create_farmer(
                farmer['farmer_id'],
                farmer['name'],
                farmer['email']
            )
        
        print(f"Neo4j: Creating {len(farms)} farm nodes...")
        for farm in farms:
            self.neo4j_mgr.create_farm(
                farm['farm_id'],
                farm['name'],
                farm['location']['coordinates'][0],  # Just store as string
                farm['total_area_hectares']
            )
        
        print(f"Neo4j: Creating {len(fields)} field nodes...")
        for field in fields:
            self.neo4j_mgr.create_field(
                field['field_id'],
                field['name'],
                field['area_hectares'],
                field['soil_type'],
                field['terrain']['slope_degrees']
            )
        
        # Create relationships in Neo4j
        print("Neo4j: Creating relationships...")
        for farmer in farmers:
            self.neo4j_mgr.link_farmer_owns_farm(farmer['farmer_id'], farmer['farm_id'])
        
        for field in fields:
            self.neo4j_mgr.link_farm_contains_field(field['farm_id'], field['field_id'])
            
            # Add species nodes and relationships
            for species in field['current_species']:
                try:
                    self.neo4j_mgr.create_crop_species(species, "[15-25]", "medium")
                except:
                    pass  # Species might already exist
                
                self.neo4j_mgr.link_field_has_species(field['field_id'], species)
        
        print("Metadata ingestion complete\n")
    
    def ingest_sensor_data(self, generator: PastureDataGenerator, num_days=30):
        """Ingest time-series sensor data into Cassandra."""
        print(f"=== Ingesting Sensor Data ({num_days} days) ===")
        
        start_date = datetime.utcnow() - timedelta(days=num_days)
        
        for i, field in enumerate(generator.fields):
            print(f"Processing field {i+1}/{len(generator.fields)}: {field.field_id}")
            
            # Generate sensor data (reduce readings_per_day for faster ingestion)
            sensor_data = generator.generate_sensor_data(
                field, start_date, num_days=num_days, readings_per_day=6
            )
            
            # Batch insert into Cassandra
            insert_query = """
                INSERT INTO pasture_sensors.sensor_data_by_field 
                (field_id, sensor_ts, sensor_id, metric_type, metric_value, quality_flag)
                VALUES (?, ?, ?, ?, ?, ?)
            """
            prepared = self.cassandra_session.prepare(insert_query)
            
            batch_size = 100
            for i in range(0, len(sensor_data), batch_size):
                batch = sensor_data[i:i+batch_size]
                for reading in batch:
                    self.cassandra_session.execute(prepared, (
                        reading['field_id'],
                        reading['timestamp'],
                        reading['sensor_id'],
                        reading['metric_type'],
                        reading['metric_value'],
                        reading['quality_flag']
                    ))
            
            print(f"  Inserted {len(sensor_data)} readings")
        
        print("Sensor data ingestion complete\n")
    
    def ingest_treatment_events(self, generator: PastureDataGenerator, num_days=30):
        """Ingest treatment events into MongoDB and Neo4j."""
        print("=== Ingesting Treatment Events ===")
        
        start_date = datetime.utcnow() - timedelta(days=num_days)
        events = generator.generate_treatment_events(generator.fields, start_date, num_days)
        
        print(f"MongoDB: Inserting {len(events)} treatment events...")
        if events:
            self.mongodb.treatment_events.insert_many(events)
        
        print(f"Neo4j: Creating treatment nodes and relationships...")
        for event in events:
            self.neo4j_mgr.link_field_received_treatment(
                event['field_id'],
                event['event_type'],
                event['event_date'],
                str(event.get('details', {}))
            )
        
        print("Treatment events ingestion complete\n")
    
    def compute_and_cache_metrics(self, generator: PastureDataGenerator):
        """Compute aggregated metrics and cache in Redis."""
        print("=== Computing and Caching Metrics in Redis ===")
        
        for field in generator.fields:
            # Query latest metrics from Cassandra
            query = """
                SELECT metric_type, metric_value
                FROM pasture_sensors.sensor_data_by_field
                WHERE field_id = %s
                LIMIT 50
            """
            
            rows = self.cassandra_session.execute(query, (field.field_id,))
            
            # Aggregate by metric type (take latest value for each)
            metrics = {}
            for row in rows:
                if row.metric_type not in metrics:
                    metrics[row.metric_type] = str(row.metric_value)
            
            if metrics:
                # Store in Redis
                self.redis_mgr.update_field_metrics(field.field_id, metrics)
                print(f"Cached metrics for {field.field_id}: {len(metrics)} metrics")
                
                # Check for alerts
                ndvi = float(metrics.get('ndvi', 1.0))
                soil_moisture = float(metrics.get('soil_moisture', 100.0))
                
                if ndvi < 0.5:
                    self.redis_mgr.publish_alert(
                        field.field_id, 'low_ndvi', ndvi, 0.5, 'warning'
                    )
                
                if soil_moisture < 15.0:
                    self.redis_mgr.publish_alert(
                        field.field_id, 'low_soil_moisture', soil_moisture, 15.0, 'critical'
                    )
        
        print("Metrics cached and alerts published\n")
    
    def create_advisory_rules(self):
        """Create advisory rules in Neo4j."""
        print("=== Creating Advisory Rules ===")
        
        rules = [
            {
                'rule_id': 'adaptive_grazing',
                'description': 'Reduce stocking rate when NDVI declines',
                'priority': 9,
                'conditions': 'NDVI_trend_14d < -0.15 AND grass_height < 6cm',
                'action': 'Reduce stocking rate by 20% for 21 days'
            },
            {
                'rule_id': 'irrigation_needed',
                'description': 'Apply irrigation when soil moisture low',
                'priority': 8,
                'conditions': 'soil_moisture < 12% AND no_rain_forecast_3d',
                'action': 'Apply 25-30mm irrigation'
            },
            {
                'rule_id': 'lime_application',
                'description': 'Apply lime for acidic soils',
                'priority': 6,
                'conditions': 'soil_ph < 5.8',
                'action': 'Apply lime in fall, test rate based on pH'
            },
            {
                'rule_id': 'reseeding_steep_slopes',
                'description': 'Reseed slopes with drought-tolerant species',
                'priority': 7,
                'conditions': 'slope > 10 AND NDVI_loss_pattern',
                'action': 'Overseed with drought-tolerant cultivars'
            },
            {
                'rule_id': 'nitrogen_application',
                'description': 'Apply nitrogen based on soil tests',
                'priority': 7,
                'conditions': 'soil_nitrogen < 30ppm',
                'action': 'Split application: 40kg/ha now, 30kg/ha in 4 weeks'
            }
        ]
        
        for rule in rules:
            self.neo4j_mgr.create_advisory_rule(**rule)
        
        # Link rules to species
        species_rules = {
            'perennial_ryegrass': ['adaptive_grazing', 'nitrogen_application'],
            'white_clover': ['lime_application'],
            'tall_fescue': ['adaptive_grazing', 'irrigation_needed']
        }
        
        for species, rule_ids in species_rules.items():
            for rule_id in rule_ids:
                try:
                    self.neo4j_mgr.link_rule_applies_to_species(rule_id, species)
                except Exception as e:
                    print(f"  Note: {e}")
        
        print(f"Created {len(rules)} advisory rules\n")
    
    def run_full_pipeline(self, num_farms=5, num_days=30):
        """Run the complete ingestion pipeline."""
        print("\n" + "="*60)
        print("PASTURE MANAGEMENT DATA INGESTION PIPELINE")
        print("="*60 + "\n")
        
        start_time = time.time()
        
        # Step 1: Generate data
        print("Step 1: Generating synthetic data...")
        generator = PastureDataGenerator(num_farms=num_farms)
        print(f"  Generated {len(generator.farms)} farms, {len(generator.fields)} fields\n")
        
        # Step 2: Initialize schemas
        self.initialize_schemas()
        
        # Step 3: Ingest metadata
        self.ingest_metadata(generator)
        
        # Step 4: Ingest sensor data
        self.ingest_sensor_data(generator, num_days=num_days)
        
        # Step 5: Ingest treatment events
        self.ingest_treatment_events(generator, num_days=num_days)
        
        # Step 6: Compute and cache metrics
        self.compute_and_cache_metrics(generator)
        
        # Step 7: Create advisory rules
        self.create_advisory_rules()
        
        elapsed_time = time.time() - start_time
        
        print("="*60)
        print(f"PIPELINE COMPLETE in {elapsed_time:.2f} seconds")
        print("="*60)
        print("\nData Summary:")
        print(f"  - Farms: {self.mongodb.farms.count_documents({})}")
        print(f"  - Fields: {self.mongodb.fields.count_documents({})}")
        print(f"  - Treatment Events: {self.mongodb.treatment_events.count_documents({})}")
        print(f"  - Sensor Readings: ~{len(generator.fields) * num_days * 6 * 11} (estimated)")
        print(f"  - Redis Keys: field metrics and alerts cached")
        print(f"  - Neo4j Nodes: farmers, farms, fields, species, rules, treatments")
        print("\n")
    
    def close(self):
        """Close all database connections."""
        self.mongo_client.close()
        self.neo4j_mgr.close()
        print("Database connections closed")


if __name__ == "__main__":
    pipeline = PastureIngestionPipeline()
    
    try:
        # Run with 5 farms and 30 days of data
        pipeline.run_full_pipeline(num_farms=5, num_days=30)
    finally:
        pipeline.close()
