import sys
import os

import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(parent_dir)

from src.aux_functions.data_aux_functions import (read_json, rename_columns_table,
        split_tables, update_row_dict, save_as_parquet)
from src.parameters import param_setted


def convert_save_characters_tables(path_input:str, path_output:str):
    df = read_json(path_input)
    df = rename_columns_table(df, "characters")
    
    (
    df_characters, df_characters_thumbnail, df_characters_comics,
    df_characters_series, df_characters_stories, df_characters_events,
    df_characters_urls
    ) = create_col_to_characters_tables(df)

    df_characters_comics = treatment_characters_comics(df_characters_comics)

    #create_result_file(df_characters_comics, df_characters)

    target_path = os.path.join(path_output, "result", "characters")
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    save_as_parquet(df_characters, target_path, "characters_table") 
    save_as_parquet(df_characters_thumbnail, target_path, "characters_thumbnail_table") 
    save_as_parquet(df_characters_comics, target_path, "characters_comics_table") 
    save_as_parquet(df_characters_series, target_path, "characters_series_table") 
    save_as_parquet(df_characters_stories, target_path, "characters_stories_table") 
    save_as_parquet(df_characters_events, target_path, "characters_events_table") 
    save_as_parquet(df_characters_urls, target_path, "characters_urls_table") 


    return (df_characters, df_characters_thumbnail, df_characters_comics,
    df_characters_series, df_characters_stories, df_characters_events,
    df_characters_urls)


def create_col_to_characters_tables(df:pd.DataFrame):
     col_characters = ["characters_id", "characters_name", "characters_description", "characters_modified", "characters_resourceURI",]
     col_characters_thumbnail = ['characters_thumbnail',]
     col_characters_comics = ['characters_comics']
     col_characters_series = ['characters_series', ]
     col_characters_stories = ['characters_stories',]
     col_characters_events = ['characters_events', ]
     col_characters_urls = ['characters_urls',]
 
     df_characters = split_tables(df,col_characters, "characters_id")
     df_characters_thumbnail = split_tables(df,col_characters_thumbnail, "characters_id")
     df_characters_comics = split_tables(df,col_characters_comics, "characters_id")
     df_characters_series = split_tables(df,col_characters_series, "characters_id")
     df_characters_stories = split_tables(df,col_characters_stories, "characters_id")
     df_characters_events = split_tables(df,col_characters_events, "characters_id")
     df_characters_urls = split_tables(df,col_characters_urls, "characters_id")
 
     return (df_characters, df_characters_thumbnail, df_characters_comics,
             df_characters_series, df_characters_stories, df_characters_events,
             df_characters_urls)


def treatment_characters_comics(df_characters_comics:pd.DataFrame):
    df_characters_comics.apply(lambda x: update_row_dict(x,"characters_id", "characters_comics"),axis=1)
    df_characters_comics = pd.json_normalize(df_characters_comics["characters_comics"])
    df_characters_comics = rename_columns_table(df_characters_comics, "comics", "characters_id")
    
    return df_characters_comics