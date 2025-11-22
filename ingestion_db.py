import pandas as pd
import os
from sqlalchemy import create_engine
import logging
import time

# ---------- Logging Setup ----------
logging.basicConfig(
    filename="logs/ingestion_db.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

# ---------- Create SQLAlchemy Engine ----------
# Note: triple slashes = relative path; four = absolute path
engine = create_engine('sqlite:///inventory.db')

# ---------- Ingestion Function ----------
def ingest_db(df, table_name, engine):
   ''' This function will ingest the dataframe into the database table'''
    try:
        df.to_sql(table_name, con=engine, if_exists='replace', index=False)
        logging.info(f"✅ Successfully ingested table: {table_name}")
    except Exception as e:
        logging.error(f"❌ Error ingesting table {table_name}: {e}", exc_info=True)
        raise

# ---------- Bulk CSV Loader (optional utility) ----------
def load_raw_data():
    """Load all CSVs from the 'Datas' folder into the database."""
    start = time.time()
    for file in os.listdir('Datas'):
        if file.endswith('.csv'):
            try:
                df = pd.read_csv(os.path.join('Datas', file))
                logging.info(f"Ingesting {file}...")
                ingest_db(df, file[:-4], engine)
            except Exception as e:
                logging.error(f"Error loading {file}: {e}", exc_info=True)
    end = time.time()
    total_time = (end - start) / 60
    logging.info(f"-------------- Ingestion Complete --------------")
    logging.info(f"Total Time Taken: {total_time:.2f} minutes")

if __name__ == "__main__":
    load_raw_data()
