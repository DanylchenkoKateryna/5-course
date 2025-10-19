import pandas as pd
import time
import os

csv_file = "student_performance.csv" 
df = pd.read_csv(csv_file)

print("CSV loaded. Shape:", df.shape)
print("CSV size (MB):", os.path.getsize(csv_file)/1024/1024)

parquet_file = "student_performance.parquet"
df.to_parquet(parquet_file, engine='pyarrow', index=False, compression=None)
print("Parquet saved without compression. Size (MB):", os.path.getsize(parquet_file)/1024/1024)

parquet_file_snappy = "student_performance_snappy.parquet"
df.to_parquet(parquet_file_snappy, engine='pyarrow', index=False, compression='snappy')
print("Parquet saved with Snappy compression. Size (MB):", os.path.getsize(parquet_file_snappy)/1024/1024)

start = time.time()
df_csv = pd.read_csv(csv_file)
end = time.time()
print(f"CSV read time: {end - start:.2f} sec")

start = time.time()
df_parquet = pd.read_parquet(parquet_file_snappy)
end = time.time()
print(f"Parquet (Snappy) read time: {end - start:.2f} sec")

# --- SUM CSV ---
start = time.perf_counter()
sum_csv = df_csv['weekly_self_study_hours'].sum()
end = time.perf_counter()
print(f"Sum weekly_self_study_hours CSV: {sum_csv}, Time: {end - start:.6f} sec")

# --- SUM Parquet ---
start = time.perf_counter()
sum_parquet = df_parquet['weekly_self_study_hours'].sum()
end = time.perf_counter()
print(f"Sum weekly_self_study_hours Parquet: {sum_parquet}, Time: {end - start:.6f} sec")