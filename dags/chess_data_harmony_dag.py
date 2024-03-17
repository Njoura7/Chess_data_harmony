import json
import os 
import berserk
import pandas as pd

from pendulum import datetime
from airflow.decorators import (
    dag,
    task,
)  

@dag(
    schedule="@daily",
    start_date=datetime(2024, 3, 15),
    catchup=False,
    default_args={
        "retries": 2,  
    },
    tags=["example"],
) 
def chess_data_harmony_dag():

    @task()
    def extract_data():
        
        session = berserk.TokenSession("LICHESS_TOKEN")
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
        return df

    @task()
    def transform_data(df: pd.DataFrame):
        # Simple transformation: add a new column with a constant value
        df['new_column'] = 'constant_value'
        return df

    @task()
    def load_data(df: pd.DataFrame):
        # Dummy load: just print the DataFrame to simulate loading
        print(df.head())

    data = extract_data()
    transformed_data = transform_data(data)
    load_data(transformed_data)

chess_data_harmony_dag()
