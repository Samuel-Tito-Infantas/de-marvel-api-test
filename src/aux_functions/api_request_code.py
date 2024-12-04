import os
import requests
import hashlib
import time
from typing import Tuple, Dict, Any

PUBLIC_KEY = os.environ.get("PUBLIC_KEY")
PRIVATE_KEY = os.environ.get("PRIVATE_KEY")
ENVIROMENT = os.environ.get("ENVIROMENT")

def get_marvel_api_status() -> Dict[str, Any]:
    """
    TBD.

    Returns:
        dict: TBD.
    """
    # Validate environment variables
    
    if not PUBLIC_KEY or not PRIVATE_KEY:

        return {
            "status": "error",
            "message": "Missing PUBLIC_KEY or PRIVATE_KEY in environment variables."
        }


    hash_result, ts = create_hash_md5(PRIVATE_KEY, PUBLIC_KEY)
    
    url = "https://gateway.marvel.com/v1/public/characters"
    
    params = {
        "ts": ts,
        "apikey": PUBLIC_KEY,
        "hash": hash_result,
        "limit": 100,
        "offset": 5,
    }
    
    try:
        response = requests.get(url, params=params)
        
        if response.status_code == 200:
            return {"status": "success", "message": "MARVEL API is reachable.", "data": response.json()}
        else:
            return {"status": "error", "message": f"Failed to connect to MARVEL API. Status code: {response.status_code}", "details": response.json()}
    
    except Exception as e:
        return {"status": "error", "message": "An error occurred while connecting to the MARVEL API.", "details": str(e)}



def create_hash_md5(private_key: str, public_key: str)-> Tuple[str,str]:
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