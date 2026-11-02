"""Содержимое холодного слоя в MinIO:  python list_cold.py"""
import os

from minio import Minio

mc = Minio(os.getenv("MINIO_ENDPOINT", "minio:9000"), access_key=os.getenv("MINIO_USER", "minioadmin"),
           secret_key=os.getenv("MINIO_PASSWORD", "minioadmin123"), secure=False)
total = 0
for o in mc.list_objects("cold-tier", recursive=True):
    total += o.size
    print(f"s3://cold-tier/{o.object_name:<55} {o.size / 2**20:8.2f} МБ  {o.last_modified:%Y-%m-%d %H:%M}")
print(f"итого {total / 2**20:.2f} МБ")
