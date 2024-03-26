

import os
import logging
import berserk
import pandas as pd
import numpy as np
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from pendulum import datetime
from airflow.decorators import dag, task

S3_BUCKET = 'njr-astrosdk'


@dag(
    schedule="@daily",
    start_date=datetime(2024, 3, 15),
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["ETL", "chess", "harmony", "data-pipeline"]
) 
def data_pipeline_dag():

    @task()
    def extract_data():
        task_logger.debug("This log is at the level of DEBUG")

        lichess_token = os.getenv('LICHESS_TOKEN')
        if not lichess_token:
            raise ValueError("LICHESS_TOKEN environment variable is not set")

        session = berserk.TokenSession(lichess_token)
        client = berserk.Client(session=session)
        games_list = list(client.games.export_by_player(username='njoura'))
        df = pd.json_normalize(games_list)

        columns_to_keep = ["id", "rated", "speed", "createdAt", "lastMoveAt", "status",
                        "players.white.user.name", "players.white.rating", "players.white.ratingDiff",
                        "players.black.user.name", "players.black.rating", "players.black.ratingDiff",
                        "winner", "moves"]

        column_mapping = {
            "players.white.user.name": "white_player_username",
            "players.white.rating": "white_player_rating",
            "players.white.ratingDiff": "white_player_ratingDiff",
            "players.black.user.name": "black_player_username",
            "players.black.rating": "black_player_rating",
            "players.black.ratingDiff": "black_player_ratingDiff",
        }

        df = df[columns_to_keep].rename(columns=column_mapping)
        logging.info(f"Before cleaning: {df.shape}")

        df.replace(r'^\s*$', np.nan, regex=True, inplace=True)
        df.dropna(inplace=True)

        logging.info(f"After cleaning: {df.shape}")
        return df

    @task()
    def transform_data(df: pd.DataFrame):
        # Transform the DataFrame and save it to a CSV file
        df['new_column'] = 'constant_value'
        csv_file_path = 'transformed_data.csv'
        df.to_csv(csv_file_path, index=False)
        return csv_file_path

    @task()
    def load_to_s3(csv_file_path: str):
        hook = S3Hook('aws_conn')
        hook.load_file(
            filename=csv_file_path,
            key='load_data_to_s3/transformed_data.csv',
            bucket_name=S3_BUCKET,
            replace=True
        )

    # Correctly define task dependencies
    extract_data() >> transform_data() >> load_to_s3()
    data_pipeline_dag = data_pipeline_dag()

