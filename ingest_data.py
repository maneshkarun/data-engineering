import argparse

from time import time

from sqlalchemy import create_engine
import pandas as pd

import os


def main(params):
    # input_path = params.input_path
    table_name = params.table_name
    username = params.username
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url

    csv_name = "output.csv"

# Check if the file already exists
    if not os.path.exists(csv_name):
        os.system(f"wget {url} -O {csv_name}")
    else:
        print(f"{csv_name} already exists. Skipping download.")



    engine = create_engine(f"postgresql://{username}:{password}@{host}:{port}/{db}")
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])

    df.head(n=0).to_sql(table_name, con=engine, if_exists='replace')

    # df.to_sql(table_name, con=engine, if_exists='append')

    while True:
        try:
            t0 = time()
            df.to_sql(params.table_name, con=engine, if_exists='append')
            t1 = time()
            print(f"Chunk loaded in {t1 - t0:.3f} seconds.")
            df = next(df_iter)
        except StopIteration:
            print("Iteration is stopped.")
            break

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest data to PostgreSQL database.")
    parser.add_argument("--url", help="URL to input data file.")
    # parser.add_argument("--input_path", help="Path to input data file.")
    parser.add_argument("--table_name", help="Name of table to be created.")
    parser.add_argument("--username", help="Username for database.")
    parser.add_argument("--password", help="Password for database.")
    parser.add_argument("--host", help="Host for database.")
    parser.add_argument("--port", help="Port for database.")
    parser.add_argument("--db", help="Database name.")
    args = parser.parse_args()

    main(args)
"""
URL="http://192.168.0.103:8000/yellow_tripdata_2019-02.csv/yellow_tripdata_2019-02.csv"
python ingest_data.py \
    --url="http://192.168.0.103:8000/yellow_tripdata_2019-02.csv/yellow_tripdata_2019-02.csv" \
    --table_name=yellow_taxi \
    --username=root \
    --password=root \
    --host=pg-database \
    --port=5431 \
    --db=ny_taxi

"""
"""
    docker build -t ingest_data:1.0 .
"""
"""
    docker run -it \
        --network pg-network \
        ingest_data:1.0 \
        --url="http://192.168.0.103:8000/yellow_tripdata_2019-02.csv/" \
        --table_name=yellow_taxi \
        --username=root \
        --password=root \
        --host=pg-database \
        --port=5431 \
        --db=ny_taxi

"""

"""
    docker run -it '
        --network pg-network '
        ingest_data:1.0 '
        --input_path="D:/Data Engineering/yellow_tripdata_2019-02.csv/yellow_tripdata_2019-02.csv" '
        --table_name=yellow_taxi '
        --username=root '
        --password=root '
        --host=pg-database '
        --port=5431 '
        --db=ny_taxi

"""