import os
import sys

import requests
import hashlib
import time
import uuid
import json

from typing import Tuple, Dict, Any

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, "..", ".."))
sys.path.append(parent_dir)

from src.parameters import param_setted

PUBLIC_KEY = os.environ.get("PUBLIC_KEY")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY")
ENVIROMENT = os.environ.get("ENVIROMENT")


def request_full_data_api(mode: str):

    limit_param = 100
    offset_param = 0
    call_count = 0
    sleep_after = 5
    sleep_duration = 3


    base_url = param_setted.get("base_url", None)
    tmp_target_folder = param_setted.get("tmp_target_folder", None)

    url_word = "characters" if mode == "characters" else "comics"
    url = base_url + url_word
    # print(f"url: {url}")

    newpath = os.path.join(tmp_target_folder, f"{url_word}")

    if not os.path.exists(newpath):
        os.makedirs(newpath)

    while True:

        # print(f"limit_param: {limit_param}, offset_param:{offset_param}", end="\n")
        result = get_marvel_api(url, limit_param, offset_param)
        try:
            request_code = result.get("data", []).get("code", None)
            # print(f"request_code: {request_code}", end="\n")
        except:
            print(f"erro on request_code. Check the result:\n {result} ", end="\n")

        if request_code == 200:
            (
                request_body_offset,
                request_body_limit,
                request_body_total,
                request_body_count,
                body_result,
            ) = get_body_parameters(result)

            convert_persist_parquet(body_result, newpath)

            if request_body_offset + request_body_limit >= request_body_total:
                break

            offset_param += limit_param
            call_count += 1

            if call_count % sleep_after == 0:
                print(f"Pausing for {sleep_duration** (call_count/sleep_after)*1.7} seconds after {call_count} calls...")
                time.sleep(sleep_duration ** (call_count/sleep_after)*1.7)

        else:
            print("Stop!!!")


def get_marvel_api(
    url: str, limit_param: int = 100, offset_param: int = 0
) -> Dict[str, Any]:
    """
    TBD.

    Returns:
        dict: TBD.
    """

    if not PUBLIC_KEY or not PRIVATE_KEY:

        return {
            "status": "error",
            "message": "Missing PUBLIC_KEY or PRIVATE_KEY in environment variables.",
        }

    hash_result, ts = create_hash_md5(PRIVATE_KEY, PUBLIC_KEY)

    params = {
        "ts": ts,
        "apikey": PUBLIC_KEY,
        "hash": hash_result,
        "limit": limit_param,
        "offset": offset_param,
    }

    try:
        response = requests.get(url, params=params)

        if response.status_code == 200:
            return {
                "status": "success",
                "message": "MARVEL API is reachable.",
                "data": response.json(),
            }
        else:
            return {
                "status": "error",
                "message": f"Failed to connect to MARVEL API. Status code: {response.status_code}",
                "details": response.json(),
            }

    except Exception as e:
        return {
            "status": "error",
            "message": "An error occurred while connecting to the MARVEL API.",
            "details": str(e),
        }


def create_hash_md5(private_key: str, public_key: str) -> Tuple[str, str]:
    """
    TBD.

    Args:
        private_key (str): .
        public_key (str): .

    Returns:
        Tuple: .
    """

    ts = str(time.time())
    to_hash = ts + private_key + public_key
    hash_result = hashlib.md5(to_hash.encode()).hexdigest()

    return hash_result, ts


def get_body_parameters(result: json) -> (int, int, int, int, json):

    request_body = result.get("data", []).get("data", [])
    request_body_offset = request_body.get("offset", None)
    request_body_limit = request_body.get("limit", None)
    request_body_total = request_body.get("total", None)
    request_body_count = request_body.get("count", None)
    body_result = request_body.get("results", [])

    return (
        request_body_offset,
        request_body_limit,
        request_body_total,
        request_body_count,
        body_result,
    )


def convert_persist_parquet(body_result: json, path: str) -> None:

    json_object = json.dumps(body_result, indent=4)
    random_id = uuid_str_generator_id()
    target_path = os.path.join(path, f"record_{random_id}.json")
    with open(target_path, "w") as outfile:
        outfile.write(json_object)


def uuid_str_generator_id() -> str:
    tmp_id = uuid.uuid4()
    tmp_id = str(tmp_id)
    return tmp_id
