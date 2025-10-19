from FaultTolerantPipeline import FaultTolerantPipeline

if __name__ == "__main__":
    print("ВІДМОВОСТІЙКИЙ КОНВЕЄР ДАНИХ ДАТЧИКІВ")
    
    pipeline = FaultTolerantPipeline(error_rate=0.2, duplicate_rate=0.1)
    
    pipeline.run(num_records=50)
    
    processed = pipeline.processor.get_processed_data()
    if processed:
        print(f"\nПриклад оброблених даних (перші 3):")
        for i, record in enumerate(processed[:3], 1):
            print(f"\n  Запис {i}:")
            print(f"    Датчик: {record['sensor_id']}")
            print(f"    Тип: {record['type']}")
            print(f"    Значення: {record['value']} {record['unit']}")
            print(f"    Оброблено: {record['processed_at']}")