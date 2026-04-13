import os

# Memuat kunci rahasia Service Account Anda
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "potent-catwalk-484317-n0-1d7b5e1ab639.json"

import logging
from pathlib import Path
from google.cloud import bigquery
from google.api_core.exceptions import GoogleAPIError

# Konfigurasi Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def load_csv_to_bigquery(project_id: str, dataset_id: str, table_id: str, file_path: str) -> None:
    if not Path(file_path).exists():
        logger.error(f"File tidak ditemukan: {file_path}. Pastikan sudah di-unzip dan dipindah ke folder ini!")
        return

    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=True,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE 
    )

    logger.info(f"Memulai proses upload untuk {file_path} ke {table_ref}...")

    try:
        with open(file_path, "rb") as source_file:
            job = client.load_table_from_file(source_file, table_ref, job_config=job_config)
        job.result()  
        
        table = client.get_table(table_ref)
        logger.info(f"✅ SUKSES! Tabel {table_id} berhasil dimuat. Total Baris: {table.num_rows:,}")
    except Exception as e:
        logger.error(f"❌ Gagal memuat data: {e}")

if __name__ == "__main__":
    PROJECT_ID = "potent-catwalk-484317-n0" 
    DATASET_ID = "raw_layer"
    
    files_to_upload = [
        {"table": "application_train", "file": "application_train.csv"},
        {"table": "bureau", "file": "bureau.csv"}
    ]
    
    for item in files_to_upload:
        load_csv_to_bigquery(PROJECT_ID, DATASET_ID, item["table"], item["file"])