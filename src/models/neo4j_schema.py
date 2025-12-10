"""
Neo4j graph schema for modeling relationships between entities.
Nodes: Farm, Field, Sensor, Farmer, Treatment, CropSpecies, AdvisoryRule
"""

from neo4j import GraphDatabase


class PastureNeo4jManager:
    """Manage Neo4j graph operations."""
    
    def __init__(self, uri="bolt://localhost:7687", user="neo4j", password="password"):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()
    
    def init_schema(self):
        """Initialize constraints and indexes."""
        with self.driver.session() as session:
            # Create constraints
            constraints = [
                "CREATE CONSTRAINT farm_id IF NOT EXISTS FOR (f:Farm) REQUIRE f.farm_id IS UNIQUE",
                "CREATE CONSTRAINT field_id IF NOT EXISTS FOR (f:Field) REQUIRE f.field_id IS UNIQUE",
                "CREATE CONSTRAINT farmer_id IF NOT EXISTS FOR (f:Farmer) REQUIRE f.farmer_id IS UNIQUE",
                "CREATE CONSTRAINT sensor_id IF NOT EXISTS FOR (s:Sensor) REQUIRE s.sensor_id IS UNIQUE",
                "CREATE CONSTRAINT species_name IF NOT EXISTS FOR (s:CropSpecies) REQUIRE s.name IS UNIQUE",
                "CREATE CONSTRAINT rule_id IF NOT EXISTS FOR (r:AdvisoryRule) REQUIRE r.rule_id IS UNIQUE"
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    print(f"Constraint already exists or error: {e}")
            
            print("Neo4j schema initialized")
    
    # ===== Node Creation =====
    
    def create_farm(self, farm_id, name, location, area_hectares):
        """Create a Farm node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (f:Farm {farm_id: $farm_id})
                SET f.name = $name,
                    f.location = $location,
                    f.area_hectares = $area_hectares
                RETURN f
            """, farm_id=farm_id, name=name, location=location, area_hectares=area_hectares)
            return result.single()[0]
    
    def create_field(self, field_id, name, area_hectares, soil_type, slope):
        """Create a Field node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (f:Field {field_id: $field_id})
                SET f.name = $name,
                    f.area_hectares = $area_hectares,
                    f.soil_type = $soil_type,
                    f.slope_degrees = $slope
                RETURN f
            """, field_id=field_id, name=name, area_hectares=area_hectares, 
                 soil_type=soil_type, slope=slope)
            return result.single()[0]
    
    def create_farmer(self, farmer_id, name, email):
        """Create a Farmer node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (f:Farmer {farmer_id: $farmer_id})
                SET f.name = $name,
                    f.email = $email
                RETURN f
            """, farmer_id=farmer_id, name=name, email=email)
            return result.single()[0]
    
    def create_sensor(self, sensor_id, sensor_type, install_date):
        """Create a Sensor node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (s:Sensor {sensor_id: $sensor_id})
                SET s.type = $sensor_type,
                    s.install_date = $install_date
                RETURN s
            """, sensor_id=sensor_id, sensor_type=sensor_type, install_date=install_date)
            return result.single()[0]
    
    def create_crop_species(self, name, optimal_temp_range, drought_tolerance):
        """Create a CropSpecies node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (s:CropSpecies {name: $name})
                SET s.optimal_temp_range = $optimal_temp_range,
                    s.drought_tolerance = $drought_tolerance
                RETURN s
            """, name=name, optimal_temp_range=optimal_temp_range, 
                 drought_tolerance=drought_tolerance)
            return result.single()[0]
    
    def create_advisory_rule(self, rule_id, description, priority, conditions, action):
        """Create an AdvisoryRule node."""
        with self.driver.session() as session:
            result = session.run("""
                MERGE (r:AdvisoryRule {rule_id: $rule_id})
                SET r.description = $description,
                    r.priority = $priority,
                    r.conditions = $conditions,
                    r.action = $action
                RETURN r
            """, rule_id=rule_id, description=description, priority=priority,
                 conditions=conditions, action=action)
            return result.single()[0]
    
    # ===== Relationship Creation =====
    
    def link_farmer_owns_farm(self, farmer_id, farm_id):
        """Create OWNS relationship between Farmer and Farm."""
        with self.driver.session() as session:
            session.run("""
                MATCH (farmer:Farmer {farmer_id: $farmer_id})
                MATCH (farm:Farm {farm_id: $farm_id})
                MERGE (farmer)-[:OWNS]->(farm)
            """, farmer_id=farmer_id, farm_id=farm_id)
    
    def link_farm_contains_field(self, farm_id, field_id):
        """Create CONTAINS relationship between Farm and Field."""
        with self.driver.session() as session:
            session.run("""
                MATCH (farm:Farm {farm_id: $farm_id})
                MATCH (field:Field {field_id: $field_id})
                MERGE (farm)-[:CONTAINS]->(field)
            """, farm_id=farm_id, field_id=field_id)
    
    def link_field_has_sensor(self, field_id, sensor_id):
        """Create HAS_SENSOR relationship."""
        with self.driver.session() as session:
            session.run("""
                MATCH (field:Field {field_id: $field_id})
                MATCH (sensor:Sensor {sensor_id: $sensor_id})
                MERGE (field)-[:HAS_SENSOR]->(sensor)
            """, field_id=field_id, sensor_id=sensor_id)
    
    def link_field_has_species(self, field_id, species_name):
        """Create HAS_SPECIES relationship."""
        with self.driver.session() as session:
            session.run("""
                MATCH (field:Field {field_id: $field_id})
                MATCH (species:CropSpecies {name: $species_name})
                MERGE (field)-[:HAS_SPECIES]->(species)
            """, field_id=field_id, species_name=species_name)
    
    def link_field_received_treatment(self, field_id, treatment_type, date, details=None):
        """Create RECEIVED_TREATMENT relationship with properties."""
        with self.driver.session() as session:
            session.run("""
                MATCH (field:Field {field_id: $field_id})
                MERGE (t:Treatment {type: $treatment_type, date: $date, field_id: $field_id})
                SET t.details = $details
                MERGE (field)-[:RECEIVED_TREATMENT]->(t)
            """, field_id=field_id, treatment_type=treatment_type, date=date, details=details)
    
    def link_rule_applies_to_field(self, rule_id, field_id):
        """Create APPLIES_TO relationship between AdvisoryRule and Field."""
        with self.driver.session() as session:
            session.run("""
                MATCH (rule:AdvisoryRule {rule_id: $rule_id})
                MATCH (field:Field {field_id: $field_id})
                MERGE (rule)-[:APPLIES_TO]->(field)
            """, rule_id=rule_id, field_id=field_id)
    
    def link_rule_applies_to_species(self, rule_id, species_name):
        """Create APPLIES_TO relationship between AdvisoryRule and CropSpecies."""
        with self.driver.session() as session:
            session.run("""
                MATCH (rule:AdvisoryRule {rule_id: $rule_id})
                MATCH (species:CropSpecies {name: $species_name})
                MERGE (rule)-[:APPLIES_TO]->(species)
            """, rule_id=rule_id, species_name=species_name)
    
    # ===== Query Operations =====
    
    def find_fields_by_farmer(self, farmer_id):
        """Find all fields owned by a farmer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (farmer:Farmer {farmer_id: $farmer_id})-[:OWNS]->(farm:Farm)-[:CONTAINS]->(field:Field)
                RETURN field.field_id as field_id, field.name as name
            """, farmer_id=farmer_id)
            return [dict(record) for record in result]
    
    def find_fields_with_same_treatment(self, farmer_id, treatment_type, days=365):
        """Find fields that received same treatment and belong to same farmer."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (farmer:Farmer {farmer_id: $farmer_id})-[:OWNS]->(farm:Farm)-[:CONTAINS]->(field:Field)
                MATCH (field)-[:RECEIVED_TREATMENT]->(t:Treatment {type: $treatment_type})
                WHERE datetime(t.date) > datetime() - duration({days: $days})
                RETURN field.field_id as field_id, field.name as name, t.date as treatment_date
                ORDER BY t.date DESC
            """, farmer_id=farmer_id, treatment_type=treatment_type, days=days)
            return [dict(record) for record in result]
    
    def get_applicable_rules_for_field(self, field_id):
        """Get all advisory rules applicable to a field."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (field:Field {field_id: $field_id})
                OPTIONAL MATCH (rule:AdvisoryRule)-[:APPLIES_TO]->(field)
                OPTIONAL MATCH (field)-[:HAS_SPECIES]->(species:CropSpecies)
                OPTIONAL MATCH (rule2:AdvisoryRule)-[:APPLIES_TO]->(species)
                WITH field, collect(DISTINCT rule) + collect(DISTINCT rule2) as rules
                UNWIND rules as r
                WITH r WHERE r IS NOT NULL
                RETURN DISTINCT r.rule_id as rule_id, r.description as description, 
                       r.priority as priority, r.conditions as conditions, r.action as action
                ORDER BY r.priority DESC
            """, field_id=field_id)
            return [dict(record) for record in result]
    
    def find_high_risk_fields_with_history(self, risk_threshold=0.7):
        """Find fields with sensors showing issues and their treatment history."""
        with self.driver.session() as session:
            result = session.run("""
                MATCH (field:Field)-[:HAS_SENSOR]->(sensor:Sensor)
                WHERE field.slope_degrees > $risk_threshold
                OPTIONAL MATCH (field)-[:RECEIVED_TREATMENT]->(t:Treatment)
                RETURN field.field_id as field_id, field.name as name, 
                       field.slope_degrees as slope,
                       collect({type: t.type, date: t.date}) as treatments
                ORDER BY field.slope_degrees DESC
            """, risk_threshold=risk_threshold)
            return [dict(record) for record in result]


# Example Cypher queries as strings
EXAMPLE_QUERIES = {
    "find_farmer_fields": """
        MATCH (farmer:Farmer {farmer_id: 'farmer_001'})-[:OWNS]->(farm:Farm)-[:CONTAINS]->(field:Field)
        RETURN farmer.name, farm.name, collect(field.name) as fields
    """,
    
    "find_fields_same_treatment": """
        MATCH (f:Field)-[:RECEIVED_TREATMENT]->(t:Treatment {type: 'fertilizer'})
        WHERE datetime(t.date) > datetime() - duration({months: 12})
        WITH f.field_id as field_id, count(t) as treatment_count
        WHERE treatment_count > 0
        RETURN field_id, treatment_count
        ORDER BY treatment_count DESC
    """,
    
    "recommend_for_species": """
        MATCH (field:Field {field_id: 'field_001'})-[:HAS_SPECIES]->(species:CropSpecies)
        MATCH (rule:AdvisoryRule)-[:APPLIES_TO]->(species)
        RETURN rule.description, rule.priority, rule.action
        ORDER BY rule.priority DESC
    """,
    
    "fields_needing_attention": """
        MATCH (field:Field)
        WHERE field.slope_degrees > 10
        OPTIONAL MATCH (field)-[:HAS_SPECIES]->(species:CropSpecies)
        OPTIONAL MATCH (rule:AdvisoryRule)-[:APPLIES_TO]->(species)
        WHERE rule.rule_id = 'reseeding_steep_slopes'
        RETURN field.field_id, field.name, field.slope_degrees, 
               collect(rule.description) as recommendations
    """
}


if __name__ == "__main__":
    # Example usage
    neo4j_mgr = PastureNeo4jManager()
    neo4j_mgr.init_schema()
    
    # Create example nodes
    neo4j_mgr.create_farmer("farmer_001", "John Doe", "john@example.com")
    neo4j_mgr.create_farm("farm_001", "Green Valley", "California", 150.0)
    neo4j_mgr.create_field("field_001", "North Pasture", 25.0, "loam", 5.0)
    
    # Create relationships
    neo4j_mgr.link_farmer_owns_farm("farmer_001", "farm_001")
    neo4j_mgr.link_farm_contains_field("farm_001", "field_001")
    
    print("Example Neo4j data created")
    
    # Query example
    fields = neo4j_mgr.find_fields_by_farmer("farmer_001")
    print(f"Fields owned by farmer_001: {fields}")
    
    neo4j_mgr.close()
