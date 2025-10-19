import json
import hashlib
from typing import Dict, Optional, Tuple

from enums import SensorType

class DataValidator:
    def __init__(self):
        self.seen_ids = set()
        self.validation_stats = {
            "total": 0,
            "valid": 0,
            "duplicates": 0,
            "corrupted": 0,
            "out_of_range": 0,
            "missing_fields": 0
        }
    
    def validate(self, data: Dict) -> Tuple[bool, Optional[str]]:
        self.validation_stats["total"] += 1

        expected_checksum = self._calculate_checksum(data)
        if data["checksum"] != expected_checksum:
            self.validation_stats["corrupted"] += 1
            return False, f"Невірна контрольна сума: очікувано {expected_checksum}, отримано {data['checksum']}"
        
        required_fields = ["sensor_id", "sensor_type", "value", "timestamp", 
                          "sequence_id", "checksum", "unit"]
        
        for field in required_fields:
            if field not in data:
                self.validation_stats["missing_fields"] += 1
                return False, f"Відсутнє поле: {field}"
        
        record_id = f"{data['sensor_id']}_{data['sequence_id']}"
        if record_id in self.seen_ids:
            self.validation_stats["duplicates"] += 1
            return False, f"Дублікат запису: {record_id}"
        
        sensor_type = SensorType(data["sensor_type"])
        if not self._check_range(data["value"], sensor_type):
            self.validation_stats["out_of_range"] += 1
            return False, f"Значення поза допустимим діапазоном: {data['value']}"
        
        self.seen_ids.add(record_id)
        self.validation_stats["valid"] += 1
        
        return True, None
    
    def _calculate_checksum(self, data: Dict) -> str:
        data_copy = {k: v for k, v in data.items() if k != "checksum"}
        data_str = json.dumps(data_copy, sort_keys=True)
        return hashlib.md5(data_str.encode()).hexdigest()[:8]
    
    def _check_range(self, value: float, sensor_type: SensorType) -> bool:
        ranges = {
            SensorType.TEMPERATURE: (-20, 50),
            SensorType.HUMIDITY: (0, 100),
            SensorType.PRESSURE: (900, 1100),
            SensorType.LIGHT: (0, 10000)
        }
        min_val, max_val = ranges[sensor_type]
        return min_val <= value <= max_val
    
    def get_stats(self) -> Dict:
        stats = self.validation_stats.copy()
        if stats["total"] > 0:
            stats["success_rate"] = round(stats["valid"] / stats["total"] * 100, 2)
        else:
            stats["success_rate"] = 0
        return stats
