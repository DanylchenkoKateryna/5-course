import time
from collections import deque

from SensorDataGenerator import SensorDataGenerator
from DataValidator import DataValidator
from DataProcessor import DataProcessor
from logger import get_logger

logger = get_logger(__name__)

class FaultTolerantPipeline:
    def __init__(self, error_rate: float = 0.2, duplicate_rate: float = 0.1):
        self.generator = SensorDataGenerator(error_rate, duplicate_rate)
        self.validator = DataValidator()
        self.processor = DataProcessor()
        self.buffer = deque(maxlen=1000)
    
    def run(self, num_records: int = 100):
        logger.info(f"Запуск конвеєра для {num_records} записів")
        logger.info("=" * 60)
        
        for i in range(num_records):
            data, error_type = self.generator.generate()
            self.buffer.append(data)
            
            is_valid, error_msg = self.validator.validate(data)
            
            if is_valid:
                self.processor.process(data)
            else:
                logger.error(f"Х Невалідні дані: {error_msg}")
            
            time.sleep(0.05)
        
        logger.info("=" * 60)
        logger.info("Конвеєр завершив роботу")
        self._print_report()
    
    def _print_report(self):
        print("\n" + "=" * 60)
        print("ЗВІТ ПРО РОБОТУ КОНВЕЄРА")
        print("=" * 60)
        
        # Статистика валідації
        val_stats = self.validator.get_stats()
        print(f"\nВалідація:")
        print(f"  Всього записів: {val_stats['total']}")
        print(f"  Валідних: {val_stats['valid']} ({val_stats['success_rate']}%)")
        print(f"  Дублікатів: {val_stats['duplicates']}")
        print(f"  Пошкоджених: {val_stats['corrupted']}")
        print(f"  Поза діапазоном: {val_stats['out_of_range']}")
        print(f"  Відсутні поля: {val_stats['missing_fields']}")
        
        # Статистика обробки
        proc_stats = self.processor.get_stats()
        print(f"\nОбробка:")
        print(f"  Оброблено: {proc_stats['processed']}")
        print(f"  Помилок: {proc_stats['failed']}")
        print(f"  Повторних спроб: {proc_stats['retried']}")
        
        # Загальна ефективність
        total = val_stats['total']
        success = proc_stats['processed']
        if total > 0:
            efficiency = round(success / total * 100, 2)
            print(f"\nЗагальна ефективність: {efficiency}%")
        
        print("=" * 60)
