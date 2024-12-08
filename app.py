import os

from src.parameters import param_setted
from src.aux_functions.api_request_code import request_full_data_api
from src.elt_functions.etl_functions_characters import convert_save_characters_tables
from src.elt_functions.etl_functions_result import process_result_csv


def main_function(mode: str):

    request_full_data_api(mode)

    input_path = os.path.join(param_setted.get("tmp_target_folder"), mode)
    output_path = os.path.join(param_setted.get("output_target_folder"), "result", mode)

    (
        df_characters,
        df_characters_thumbnail,
        df_characters_comics,
        df_characters_series,
        df_characters_stories,
        df_characters_events,
        df_characters_urls,
    ) = convert_save_characters_tables(input_path, output_path, mode)

    process_result_csv(df_characters_comics, df_characters)

    return


if __name__ == "__main__":

    main_function("characters")
