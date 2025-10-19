import random
import time
from datetime import datetime
from typing import Dict, List

from logger import get_logger

logger = get_logger(__name__)

class DataProcessor:
    def __init__(self, max_retries: int = 3, retry_delay: float = 0.5):
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.processed_data = []
        self.failed_data = []
        self.processing_stats = {
            "processed": 0,
            "failed": 0,
            "retried": 0
        }
    
    def process(self, data: Dict) -> bool:
        """
        Обробляє валідні дані з механізмом повторних спроб
        
        Повертає: успішність обробки
        """
        retries = 0
        
        while retries <= self.max_retries:
            try:
                if random.random() < 0.1 and retries < self.max_retries:
                    raise Exception("Тимчасова помилка обробки")
                
                processed = self._transform_data(data)
                self.processed_data.append(processed)
                self.processing_stats["processed"] += 1
                
                logger.info(f"✓ Оброблено: {data['sensor_id']} seq={data['sequence_id']}")
                return True
                
            except Exception as e:
                retries += 1
                self.processing_stats["retried"] += 1
                
                if retries <= self.max_retries:
                    logger.warning(f"Помилка обробки (спроба {retries}/{self.max_retries}): {e}")
                    time.sleep(self.retry_delay * retries)  # Exponential backoff
                else:
                    logger.error(f"Не вдалося обробити після {self.max_retries} спроб")
                    self.failed_data.append(data)
                    self.processing_stats["failed"] += 1
                    return False
    
    def _transform_data(self, data: Dict) -> Dict:
        return {
            "sensor_id": data["sensor_id"],
            "type": data["sensor_type"],
            "value": data["value"],
            "unit": data["unit"],
            "processed_at": datetime.now().isoformat(),
            "original_timestamp": data["timestamp"]
        }
    
    def get_processed_data(self) -> List[Dict]:
        return self.processed_data
    
    def get_failed_data(self) -> List[Dict]:
        return self.failed_data
    
    def get_stats(self) -> Dict:
        return self.processing_stats.copy()
