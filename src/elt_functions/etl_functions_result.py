import os
import sys

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(parent_dir)

from src.parameters import param_setted


def process_result_csv(df_characters_comics: pd.DataFrame, df_characters: pd.DataFrame):
    # display(df_characters_comics.head())
    # display(df_characters.head())
    result = create_result_file(df_characters_comics, df_characters)
    # print(f"result: {result.columns}")
    # display(result)
    save_result_csv(result)


def create_result_file(
    df_characters_comics: pd.DataFrame, df_characters: pd.DataFrame
) -> pd.DataFrame:
    result = df_characters.merge(
        df_characters_comics, on="characters_id", how="left"
    ).rename(
        columns={
            "characters_characters_comics_available": "quantity_of_comics_appear",
            "characters_name": "Character_name",
        }
    )

    result = result.sort_values(by=["Character_name"], ascending=True)
    return result


def save_result_csv(df: pd.DataFrame):
    path = os.path.join(
        param_setted.get("output_target_folder"), "result", "technical_result.csv"
    )

    columns = ["Character_name", "quantity_of_comics_appear"]
    df[columns].to_csv(path, sep=";", index=False)
