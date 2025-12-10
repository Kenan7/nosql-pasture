"""
Generate realistic synthetic data for pasture management system.
Includes farms, fields, sensors, and time-series telemetry with realistic patterns.
"""

import numpy as np
import random
from datetime import datetime, timedelta
from dataclasses import dataclass
from typing import List, Dict, Tuple


@dataclass
class FarmConfig:
    """Configuration for a farm."""
    farm_id: str
    name: str
    location: Tuple[float, float]  # (longitude, latitude)
    num_fields: int


@dataclass
class FieldConfig:
    """Configuration for a field."""
    field_id: str
    farm_id: str
    name: str
    center_location: Tuple[float, float]
    area_hectares: float
    soil_type: str
    soil_ph: float
    slope_degrees: float
    aspect: str
    species: List[str]


class PastureDataGenerator:
    """Generate synthetic pasture data with realistic patterns."""
    
    # Base locations (California coordinates)
    BASE_LOCATIONS = [
        (-122.4194, 37.7749),  # San Francisco area
        (-121.8907, 37.3382),  # San Jose area
        (-122.0308, 37.3541),  # Sunnyvale area
        (-122.2711, 37.8044),  # Berkeley area
        (-121.7680, 37.6819),  # Livermore area
    ]
    
    SOIL_TYPES = ['loam', 'clay_loam', 'sandy_loam', 'silt_loam', 'clay']
    ASPECTS = ['north', 'south', 'east', 'west', 'northeast', 'northwest', 'southeast', 'southwest']
    SPECIES = [
        ['perennial_ryegrass', 'white_clover'],
        ['tall_fescue', 'red_clover'],
        ['orchardgrass', 'alfalfa'],
        ['kentucky_bluegrass', 'white_clover'],
        ['timothy', 'meadow_fescue']
    ]
    
    FARMER_NAMES = ['Robert Johnson', 'Mary Williams', 'James Brown', 'Patricia Davis', 'Michael Miller']
    FARM_NAMES = ['Green Valley Farm', 'Sunset Meadows', 'Rolling Hills Ranch', 'Oak Ridge Farm', 'Pleasant View Pastures']
    
    def __init__(self, num_farms=5, seed=42):
        """Initialize generator with configuration."""
        random.seed(seed)
        np.random.seed(seed)
        self.num_farms = num_farms
        self.farms: List[FarmConfig] = []
        self.fields: List[FieldConfig] = []
        self.farmers: List[Dict] = []
        
        self._generate_farm_structure()
    
    def _generate_farm_structure(self):
        """Generate farms, fields, and farmers."""
        for i in range(self.num_farms):
            # Create farmer
            farmer_id = f"farmer_{i+1:03d}"
            farm_id = f"farm_{i+1:03d}"
            
            self.farmers.append({
                'farmer_id': farmer_id,
                'name': self.FARMER_NAMES[i % len(self.FARMER_NAMES)],
                'email': f"{farmer_id}@email.com",
                'phone': f"+1-555-{i+1:04d}",
                'farm_id': farm_id
            })
            
            # Create farm
            num_fields = random.randint(3, 5)
            location = self.BASE_LOCATIONS[i % len(self.BASE_LOCATIONS)]
            
            farm = FarmConfig(
                farm_id=farm_id,
                name=self.FARM_NAMES[i % len(self.FARM_NAMES)],
                location=location,
                num_fields=num_fields
            )
            self.farms.append(farm)
            
            # Create fields for this farm
            for j in range(num_fields):
                field_id = f"field_{i+1:03d}_{j+1:02d}"
                
                # Offset location slightly for each field
                offset_lon = random.uniform(-0.02, 0.02)
                offset_lat = random.uniform(-0.01, 0.01)
                field_location = (
                    location[0] + offset_lon,
                    location[1] + offset_lat
                )
                
                field = FieldConfig(
                    field_id=field_id,
                    farm_id=farm_id,
                    name=f"Pasture {chr(65+j)}",  # A, B, C, etc.
                    center_location=field_location,
                    area_hectares=round(random.uniform(15.0, 40.0), 1),
                    soil_type=random.choice(self.SOIL_TYPES),
                    soil_ph=round(random.uniform(5.5, 7.5), 1),
                    slope_degrees=round(random.uniform(0, 15), 1),
                    aspect=random.choice(self.ASPECTS),
                    species=random.choice(self.SPECIES)
                )
                self.fields.append(field)
    
    def generate_polygon(self, center: Tuple[float, float], area_hectares: float) -> List[List[float]]:
        """Generate a roughly square polygon for a field boundary."""
        # Approximate side length in degrees (very rough)
        side_deg = np.sqrt(area_hectares / 10000) * 0.01
        
        lon, lat = center
        polygon = [
            [lon - side_deg/2, lat + side_deg/2],
            [lon + side_deg/2, lat + side_deg/2],
            [lon + side_deg/2, lat - side_deg/2],
            [lon - side_deg/2, lat - side_deg/2],
            [lon - side_deg/2, lat + side_deg/2]  # Close polygon
        ]
        return polygon
    
    def generate_sensor_data(self, field: FieldConfig, start_date: datetime, 
                           num_days=30, readings_per_day=24) -> List[Dict]:
        """
        Generate realistic time-series sensor data for a field.
        Includes seasonal patterns, weather variations, and correlations.
        """
        sensor_data = []
        
        # Field-specific characteristics that influence sensors
        slope_factor = field.slope_degrees / 15.0  # 0-1 scale
        ph_factor = (field.soil_ph - 5.5) / 2.0  # 0-1 scale
        
        # Determine if this field has issues (20% chance)
        has_drought_stress = random.random() < 0.2
        has_nutrient_deficiency = random.random() < 0.15
        
        for day in range(num_days):
            current_date = start_date + timedelta(days=day)
            
            # Daily weather pattern (temperature cycle)
            for hour in range(readings_per_day):
                timestamp = current_date + timedelta(hours=hour)
                
                # Temperature (Celsius) - daily cycle
                base_temp = 15 + 10 * np.sin((day / 30) * np.pi)  # Seasonal
                hourly_temp = base_temp + 8 * np.sin((hour / 24) * 2 * np.pi - np.pi/2)  # Daily
                temperature = round(hourly_temp + np.random.normal(0, 1.5), 1)
                
                # Humidity (%) - inversely related to temperature
                base_humidity = 70 - (temperature - 15) * 1.5
                humidity = round(np.clip(base_humidity + np.random.normal(0, 5), 30, 95), 1)
                
                # Soil moisture (%) - affected by precipitation and slope
                base_moisture = 25 - slope_factor * 10  # Slopes drain faster
                if has_drought_stress:
                    base_moisture -= 8
                
                # Decrease over time, occasional rain events
                moisture_trend = base_moisture - (day / num_days) * 10
                if random.random() < 0.1:  # 10% chance of rain
                    moisture_trend += random.uniform(5, 15)
                
                soil_moisture = round(np.clip(moisture_trend + np.random.normal(0, 2), 5, 45), 1)
                
                # NDVI (0-1) - related to moisture and temperature
                base_ndvi = 0.75 - slope_factor * 0.15
                if has_drought_stress:
                    base_ndvi -= 0.2
                if soil_moisture < 15:
                    base_ndvi -= 0.1
                
                ndvi = round(np.clip(base_ndvi + np.random.normal(0, 0.05), 0.2, 0.9), 2)
                
                # Grass height (cm) - grows over time, affected by conditions
                base_height = 8 + (day / num_days) * 8
                if has_drought_stress or soil_moisture < 15:
                    base_height *= 0.7
                
                grass_height = round(np.clip(base_height + np.random.normal(0, 1), 4, 25), 1)
                
                # Soil pH - relatively stable
                soil_ph_reading = round(field.soil_ph + np.random.normal(0, 0.1), 1)
                
                # Nutrients (ppm)
                n_base = 45 if not has_nutrient_deficiency else 25
                soil_nitrogen = round(n_base + np.random.normal(0, 5), 1)
                soil_phosphorus = round(25 + np.random.normal(0, 3), 1)
                soil_potassium = round(150 + np.random.normal(0, 10), 1)
                
                # Solar radiation (W/m²) - daytime only
                if 6 <= hour <= 18:
                    solar_radiation = round(800 * np.sin((hour - 6) / 12 * np.pi) + np.random.normal(0, 50), 1)
                else:
                    solar_radiation = 0.0
                
                # Wind speed (m/s)
                wind_speed = round(np.clip(3 + np.random.exponential(2), 0, 15), 1)
                
                # Generate readings for multiple sensors
                metrics = {
                    'temperature': temperature,
                    'humidity': humidity,
                    'soil_moisture': soil_moisture,
                    'ndvi': ndvi,
                    'grass_height': grass_height,
                    'soil_ph': soil_ph_reading,
                    'soil_nitrogen': soil_nitrogen,
                    'soil_phosphorus': soil_phosphorus,
                    'soil_potassium': soil_potassium,
                    'solar_radiation': solar_radiation,
                    'wind_speed': wind_speed
                }
                
                for metric_type, value in metrics.items():
                    sensor_data.append({
                        'field_id': field.field_id,
                        'timestamp': timestamp,
                        'sensor_id': f"sensor_{metric_type}_{field.field_id}",
                        'metric_type': metric_type,
                        'metric_value': value,
                        'quality_flag': 1 if random.random() > 0.05 else 0  # 5% bad readings
                    })
        
        return sensor_data
    
    def generate_treatment_events(self, fields: List[FieldConfig], 
                                 start_date: datetime, num_days=30) -> List[Dict]:
        """Generate realistic treatment events (fertilizer, irrigation, etc.)."""
        events = []
        
        for field in fields:
            # Fertilizer application (1-2 times per field)
            num_fertilizer = random.randint(0, 2)
            for _ in range(num_fertilizer):
                event_date = start_date + timedelta(days=random.randint(0, num_days-1))
                events.append({
                    'event_id': f"evt_{field.field_id}_{len(events)}",
                    'field_id': field.field_id,
                    'event_type': 'fertilizer',
                    'event_date': event_date.isoformat(),
                    'details': {
                        'fertilizer_type': random.choice(['NPK', 'Urea', 'Organic']),
                        'n_kg_per_ha': random.randint(30, 80),
                        'p_kg_per_ha': random.randint(15, 40),
                        'k_kg_per_ha': random.randint(15, 40)
                    }
                })
            
            # Irrigation (0-3 times)
            if random.random() < 0.7:  # 70% of fields get irrigation
                num_irrigation = random.randint(1, 3)
                for _ in range(num_irrigation):
                    event_date = start_date + timedelta(days=random.randint(0, num_days-1))
                    events.append({
                        'event_id': f"evt_{field.field_id}_{len(events)}",
                        'field_id': field.field_id,
                        'event_type': 'irrigation',
                        'event_date': event_date.isoformat(),
                        'details': {
                            'amount_mm': random.randint(15, 40),
                            'method': random.choice(['sprinkler', 'drip', 'flood'])
                        }
                    })
        
        return events
    
    def get_all_farms(self) -> List[Dict]:
        """Get all farm data in MongoDB format."""
        return [{
            'farm_id': farm.farm_id,
            'name': farm.name,
            'location': {
                'type': 'Point',
                'coordinates': list(farm.location)
            },
            'total_area_hectares': sum(f.area_hectares for f in self.fields if f.farm_id == farm.farm_id),
            'established_date': '2010-01-01'
        } for farm in self.farms]
    
    def get_all_fields(self) -> List[Dict]:
        """Get all field data in MongoDB format."""
        return [{
            'field_id': field.field_id,
            'farm_id': field.farm_id,
            'name': field.name,
            'boundary': {
                'type': 'Polygon',
                'coordinates': [self.generate_polygon(field.center_location, field.area_hectares)]
            },
            'area_hectares': field.area_hectares,
            'soil_type': field.soil_type,
            'soil_ph': field.soil_ph,
            'terrain': {
                'elevation_m': random.randint(50, 300),
                'slope_degrees': field.slope_degrees,
                'aspect': field.aspect
            },
            'establishment_date': '2010-01-01',
            'current_species': field.species
        } for field in self.fields]
    
    def get_all_farmers(self) -> List[Dict]:
        """Get all farmer data."""
        return self.farmers


if __name__ == "__main__":
    # Example usage
    generator = PastureDataGenerator(num_farms=5)
    
    print(f"Generated {len(generator.farms)} farms")
    print(f"Generated {len(generator.fields)} fields")
    print(f"Generated {len(generator.farmers)} farmers")
    
    # Generate sensor data for first field
    field = generator.fields[0]
    start_date = datetime(2024, 11, 1)
    sensor_data = generator.generate_sensor_data(field, start_date, num_days=7, readings_per_day=4)
    
    print(f"Generated {len(sensor_data)} sensor readings for field {field.field_id}")
    print(f"Sample reading: {sensor_data[0]}")
