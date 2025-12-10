"""
MongoDB schema initialization and example documents.
Collections: farms, fields, treatment_events, farmer_profiles
"""

from pymongo import MongoClient
from datetime import datetime


def init_mongodb_schema(client: MongoClient):
    """Initialize MongoDB collections and indexes."""
    db = client['pasture_management']
    
    # Create collections with indexes
    collections = {
        'farms': [
            {'field': 'farm_id', 'unique': True},
            {'field': [('location', '2dsphere')], 'geo': True}
        ],
        'fields': [
            {'field': 'field_id', 'unique': True},
            {'field': 'farm_id'},
            {'field': [('boundary', '2dsphere')], 'geo': True}
        ],
        'treatment_events': [
            {'field': 'field_id'},
            {'field': 'event_date'},
            {'field': 'event_type'}
        ],
        'farmer_profiles': [
            {'field': 'farmer_id', 'unique': True},
            {'field': 'email', 'unique': True}
        ]
    }
    
    for coll_name, indexes in collections.items():
        collection = db[coll_name]
        for idx_config in indexes:
            field = idx_config['field']
            is_geo = idx_config.get('geo', False)
            is_unique = idx_config.get('unique', False)
            
            if is_geo:
                # Geospatial index (field is already a list of tuples)
                collection.create_index(field)
            elif is_unique:
                # Unique index
                collection.create_index(field, unique=True)
            else:
                # Regular index
                collection.create_index(field)
    
    print("MongoDB schema initialized successfully")
    return db


# Example documents
EXAMPLE_FARM = {
    "farm_id": "farm_001",
    "name": "Green Valley Farm",
    "owner": "farmer_001",
    "location": {
        "type": "Point",
        "coordinates": [-122.4194, 37.7749]  # [longitude, latitude]
    },
    "total_area_hectares": 150.5,
    "established_date": "2010-03-15",
    "contact": {
        "phone": "+1-555-0123",
        "email": "contact@greenvalley.farm"
    }
}

EXAMPLE_FIELD = {
    "field_id": "field_001",
    "farm_id": "farm_001",
    "name": "North Pasture",
    "boundary": {
        "type": "Polygon",
        "coordinates": [[
            [-122.4204, 37.7759],
            [-122.4184, 37.7759],
            [-122.4184, 37.7739],
            [-122.4204, 37.7739],
            [-122.4204, 37.7759]
        ]]
    },
    "area_hectares": 25.3,
    "soil_type": "loam",
    "soil_ph": 6.2,
    "organic_matter_percent": 4.5,
    "terrain": {
        "elevation_m": 120,
        "slope_degrees": 5,
        "aspect": "south"
    },
    "establishment_date": "2010-04-20",
    "current_species": ["perennial_ryegrass", "white_clover"],
    "notes": ["Well-draining", "Good for dairy cattle"],
    "latest_metrics": {
        "ndvi": 0.72,
        "soil_moisture": 18.5,
        "grass_height_cm": 12.3,
        "updated_at": datetime.utcnow()
    }
}

EXAMPLE_TREATMENT_EVENT = {
    "event_id": "evt_001",
    "field_id": "field_001",
    "event_type": "fertilizer",
    "event_date": "2024-11-15T08:30:00Z",
    "details": {
        "fertilizer_type": "NPK",
        "n_kg_per_ha": 50,
        "p_kg_per_ha": 25,
        "k_kg_per_ha": 25,
        "application_method": "broadcast"
    },
    "cost_usd": 450.00,
    "operator": "John Smith"
}

EXAMPLE_FARMER_PROFILE = {
    "farmer_id": "farmer_001",
    "name": "Robert Johnson",
    "email": "robert@greenvalley.farm",
    "phone": "+1-555-0123",
    "farms": ["farm_001"],
    "preferences": {
        "alert_methods": ["email", "sms"],
        "report_frequency": "weekly",
        "management_style": "rotational_grazing"
    },
    "livestock": {
        "type": "dairy_cattle",
        "average_count": 85
    }
}


if __name__ == "__main__":
    # Example usage
    client = MongoClient('mongodb://admin:password@localhost:27017/')
    db = init_mongodb_schema(client)
    
    # Insert example documents
    db.farms.insert_one(EXAMPLE_FARM)
    db.fields.insert_one(EXAMPLE_FIELD)
    db.treatment_events.insert_one(EXAMPLE_TREATMENT_EVENT)
    db.farmer_profiles.insert_one(EXAMPLE_FARMER_PROFILE)
    
    print("Example documents inserted")
    client.close()
