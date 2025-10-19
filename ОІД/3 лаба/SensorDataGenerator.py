import random
import json
import hashlib
from datetime import datetime
from typing import Dict, Tuple
from enums import SensorType, ErrorType
from logger import get_logger

logger = get_logger(__name__)

class SensorDataGenerator:
    def __init__(self, error_rate: float = 0.2, duplicate_rate: float = 0.1):
        self.error_rate = error_rate
        self.duplicate_rate = duplicate_rate
        self.last_data = None
        self.sequence_id = 0
        
        self.ranges = {
            SensorType.TEMPERATURE: (-20, 50),
            SensorType.HUMIDITY: (0, 100),
            SensorType.PRESSURE: (900, 1100),
            SensorType.LIGHT: (0, 10000)
        }
    
    def _generate_clean_data(self, sensor_type: SensorType) -> Dict:
        self.sequence_id += 1
        min_val, max_val = self.ranges[sensor_type]
        
        data = {
            "sensor_id": f"SENSOR_{sensor_type.value.upper()}_{random.randint(1, 5)}",
            "sensor_type": sensor_type.value,
            "value": round(random.uniform(min_val, max_val), 2),
            "timestamp": datetime.now().isoformat(),
            "sequence_id": self.sequence_id,
            "unit": self._get_unit(sensor_type)
        }
        
        data["checksum"] = self._calculate_checksum(data)
        return data
    
    def _calculate_checksum(self, data: Dict) -> str:
        data_copy = {k: v for k, v in data.items() if k != "checksum"}
        data_str = json.dumps(data_copy, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:8]
    
    def _get_unit(self, sensor_type: SensorType) -> str:
        units = {
            SensorType.TEMPERATURE: "°C",
            SensorType.HUMIDITY: "%",
            SensorType.PRESSURE: "hPa",
            SensorType.LIGHT: "lux"
        }
        return units[sensor_type]
    
    def _inject_error(self, data: Dict) -> Tuple[Dict, ErrorType]:
        error_types = [
            ErrorType.CORRUPTED_DATA,
            ErrorType.OUT_OF_RANGE,
            ErrorType.MISSING_FIELD
        ]
        
        error = random.choice(error_types)
        corrupted_data = data.copy()
        
        if error == ErrorType.CORRUPTED_DATA:
            corrupted_data["checksum"] = "INVALID"
            
        elif error == ErrorType.OUT_OF_RANGE:
            corrupted_data["value"] = corrupted_data["value"] * 10
            corrupted_data["checksum"] = self._calculate_checksum(corrupted_data)
            
        elif error == ErrorType.MISSING_FIELD:
            del corrupted_data["timestamp"]
        
        return corrupted_data, error
    
    def generate(self, sensor_type: SensorType = None) -> Tuple[Dict, ErrorType]:
        if sensor_type is None:
            sensor_type = random.choice(list(SensorType))
        
        clean_data = self._generate_clean_data(sensor_type)
        
        if self.last_data and random.random() < self.duplicate_rate:
            logger.warning(f"Генерується дублікат: seq_id={self.last_data['sequence_id']}")
            return self.last_data.copy(), ErrorType.DUPLICATE
        
        if random.random() < self.error_rate:
            corrupted_data, error_type = self._inject_error(clean_data)
            logger.warning(f"Генерується помилка: {error_type.name}")
            self.last_data = clean_data
            return corrupted_data, error_type
        
        self.last_data = clean_data
        return clean_data, ErrorType.NONE
