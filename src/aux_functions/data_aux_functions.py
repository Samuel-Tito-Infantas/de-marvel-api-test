import os
import pandas as pd


def read_json(path: str):

    files = os.listdir(path)
    full_df_list = [pd.read_json(os.path.join(path, file)) for file in files]
    df = pd.concat(full_df_list).reset_index(drop=True)
    return df


def rename_columns_table(df: pd.DataFrame, mode: str, exception_col: str = None):
    if exception_col is None:
        custom_col = [f"{mode}_{col}" for col in df.columns]

    else:
        custom_col = [
            f"{mode}_{col}" if col != exception_col else col for col in df.columns
        ]

    df.columns = custom_col
    return df


def split_tables(df: pd.DataFrame, target_columns: list, id_target: str):
    if id_target not in target_columns:
        target_columns.append(id_target)

    result = df[target_columns]
    return result


def treatment_explode_json_tables(
    df: pd.DataFrame,
    id_col_name: str,
    target_col_name: str,
    mode: str,
    how: str = "dict",
):
    df.apply(lambda x: update_row(x, id_col_name, target_col_name, how), axis=1)
    df = pd.json_normalize(df[target_col_name])
    df = rename_columns_table(df, f"{mode}_{target_col_name}", id_col_name)

    return df


def update_row(
    row: pd.core.series.Series,
    id_col_name: str,
    target_col_name: str,
    how: str = "dict",
) -> pd.core.series.Series:
    # print(f"type: {type(row)}")
    new = row[id_col_name]

    if how == "dict":
        row[target_col_name].update({id_col_name: new})

    elif how == "list":
        row[target_col_name].append({id_col_name: new})

    else:
        print("Erro on how parameters")

    return row


def save_as_parquet(df: pd.DataFrame, path: str, file_name: str):
    target_final = os.path.join(path, f"{file_name}.parquet")
    try:
        df.to_parquet(target_final)
    except:
        df = df.astype(str)
        df.to_parquet(target_final)
