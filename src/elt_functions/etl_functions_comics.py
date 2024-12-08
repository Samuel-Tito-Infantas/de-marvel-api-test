import os
import sys

import pandas as pd
from typing import Tuple

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(parent_dir)

from src.aux_functions.data_aux_functions import (
    read_json,
    rename_columns_table,
    split_tables,
    save_as_parquet,
    treatment_explode_json_tables,
)


def convert_save_comics_tables(path_input: str, path_output: str, mode: str) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    df = read_json(path_input)
    df = rename_columns_table(df, mode)

    (
        df_comics,
        df_comics_creators,
        df_comics_characters,
        df_comics_stories,
        df_comics_events,
        df_comics_images,
        df_comics_thumbnail,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_series,
        df_comics_variants,
        df_comics_collectedIssues,
        df_comics_collections,
        df_comics_textObjects,
    ) = create_col_to_comics_tables(df)

    (
        df_comics_characters,
        df_comics_creators,
        df_comics_stories,
        df_comics_events,
        df_comics_thumbnail,
        df_comics_series,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_images,
    ) = treatment_comics_tables(
        "comics_id",
        mode,
        df_comics_characters,
        df_comics_creators,
        df_comics_stories,
        df_comics_events,
        df_comics_thumbnail,
        df_comics_series,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_images,
    )

    target_path = os.path.join(path_output, "result", mode)
    print(f"target_path: {target_path}")
    if not os.path.exists(target_path):
        os.makedirs(target_path)

    save_as_parquet(df_comics, target_path, "comics_table")
    save_as_parquet(df_comics_creators, target_path, "comics_creators_table")
    save_as_parquet(df_comics_characters, target_path, "comics_characters_table")
    save_as_parquet(df_comics_stories, target_path, "comics_stories_table")
    save_as_parquet(df_comics_events, target_path, "comics_events_table")
    save_as_parquet(df_comics_images, target_path, "comics_images_table")
    save_as_parquet(df_comics_thumbnail, target_path, "comics_thumbnail_table")
    save_as_parquet(df_comics_dates, target_path, "comics_dates_table")
    save_as_parquet(df_comics_prices, target_path, "comics_prices_table")
    save_as_parquet(df_comics_urls, target_path, "comics_urls_table")
    save_as_parquet(df_comics_series, target_path, "comics_series_table")
    save_as_parquet(df_comics_variants, target_path, "comics_variants_table")
    save_as_parquet(
        df_comics_collectedIssues, target_path, "comics_collectedIssues_table"
    )
    save_as_parquet(df_comics_collections, target_path, "comics_collections_table")
    save_as_parquet(df_comics_textObjects, target_path, "comics_textObjects_table")

    return (
        df_comics,
        df_comics_creators,
        df_comics_characters,
        df_comics_stories,
        df_comics_events,
        df_comics_images,
        df_comics_thumbnail,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_series,
        df_comics_variants,
        df_comics_collectedIssues,
        df_comics_collections,
        df_comics_textObjects,
    )


def create_col_to_comics_tables(
    df: pd.DataFrame,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:
    col_comics = [
        "comics_id",
        "comics_digitalId",
        "comics_title",
        "comics_issueNumber",
        "comics_variantDescription",
        "comics_description",
        "comics_modified",
        "comics_isbn",
        "comics_upc",
        "comics_diamondCode",
        "comics_ean",
        "comics_issn",
        "comics_format",
        "comics_pageCount",
        "comics_resourceURI",
    ]
    col_comics_creators = [
        "comics_creators",
    ]
    col_comics_characters = [
        "comics_characters",
    ]
    col_comics_stories = [
        "comics_stories",
    ]
    col_comics_events = [
        "comics_events",
    ]
    col_comics_images = [
        "comics_images",
    ]
    col_comics_thumbnail = [
        "comics_thumbnail",
    ]
    col_comics_dates = [
        "comics_dates",
    ]
    col_comics_prices = [
        "comics_prices",
    ]
    col_comics_urls = [
        "comics_urls",
    ]
    col_comics_series = [
        "comics_series",
    ]
    col_comics_variants = [
        "comics_variants",
    ]

    col_comics_collectedIssues = [
        "comics_collectedIssues",
    ]
    col_comics_collections = [
        "comics_collections",
    ]
    col_comics_textObjects = [
        "comics_textObjects",
    ]

    df_comics = split_tables(df, col_comics, "comics_id")
    df_comics_creators = split_tables(df, col_comics_creators, "comics_id")
    df_comics_characters = split_tables(df, col_comics_characters, "comics_id")
    df_comics_stories = split_tables(df, col_comics_stories, "comics_id")
    df_comics_events = split_tables(df, col_comics_events, "comics_id")
    df_comics_images = split_tables(df, col_comics_images, "comics_id")
    df_comics_thumbnail = split_tables(df, col_comics_thumbnail, "comics_id")
    df_comics_dates = split_tables(df, col_comics_dates, "comics_id")
    df_comics_prices = split_tables(df, col_comics_prices, "comics_id")
    df_comics_urls = split_tables(df, col_comics_urls, "comics_id")
    df_comics_series = split_tables(df, col_comics_series, "comics_id")

    df_comics_variants = split_tables(df, col_comics_variants, "comics_id")
    df_comics_collectedIssues = split_tables(
        df, col_comics_collectedIssues, "comics_id"
    )
    df_comics_collections = split_tables(df, col_comics_collections, "comics_id")
    df_comics_textObjects = split_tables(df, col_comics_textObjects, "comics_id")

    return (
        df_comics,
        df_comics_creators,
        df_comics_characters,
        df_comics_stories,
        df_comics_events,
        df_comics_images,
        df_comics_thumbnail,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_series,
        df_comics_variants,
        df_comics_collectedIssues,
        df_comics_collections,
        df_comics_textObjects,
    )


def treatment_comics_tables(
    id_col_name: str,
    mode: str,
    df_comics_characters: pd.DataFrame,
    df_comics_creators: pd.DataFrame,
    df_comics_stories: pd.DataFrame,
    df_comics_events: pd.DataFrame,
    df_comics_thumbnail: pd.DataFrame,
    df_comics_series: pd.DataFrame,
    df_comics_dates: pd.DataFrame,
    df_comics_prices: pd.DataFrame,
    df_comics_urls: pd.DataFrame,
    df_comics_images: pd.DataFrame,
) -> Tuple[
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
    pd.DataFrame,
]:

    df_comics_characters = treatment_explode_json_tables(
        df_comics_characters, id_col_name, "comics_characters", mode
    )
    df_comics_creators = treatment_explode_json_tables(
        df_comics_creators, id_col_name, "comics_creators", mode
    )
    df_comics_stories = treatment_explode_json_tables(
        df_comics_stories, id_col_name, "comics_stories", mode
    )
    df_comics_events = treatment_explode_json_tables(
        df_comics_events, id_col_name, "comics_events", mode
    )
    df_comics_thumbnail = treatment_explode_json_tables(
        df_comics_thumbnail, id_col_name, "comics_thumbnail", mode
    )
    df_comics_series = treatment_explode_json_tables(
        df_comics_series, id_col_name, "comics_series", mode
    )

    df_comics_dates = treatment_explode_json_tables(
        df_comics_dates, id_col_name, "comics_dates", mode, "list"
    )
    df_comics_prices = treatment_explode_json_tables(
        df_comics_prices, id_col_name, "comics_prices", mode, "list"
    )
    df_comics_urls = treatment_explode_json_tables(
        df_comics_urls, id_col_name, "comics_urls", mode, "list"
    )
    df_comics_images = treatment_explode_json_tables(
        df_comics_images, id_col_name, "comics_images", mode, "list"
    )

    return (
        df_comics_characters,
        df_comics_creators,
        df_comics_stories,
        df_comics_events,
        df_comics_thumbnail,
        df_comics_series,
        df_comics_dates,
        df_comics_prices,
        df_comics_urls,
        df_comics_images,
    )
