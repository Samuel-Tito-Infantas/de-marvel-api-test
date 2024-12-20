# Marvel API Technical Test Case

By: STI

## Table of Contents

1. Project Overview
2. Tech Stack
3. Setup Instructions
4. Data Pipeline Workflow
5. Layout
6. Testing
7. Challenges and Solutions
8. Future Improvements


## Project Overview

This project aims to connect with [MARVEL API](https://developer.marvel.com/docs) from the following endpoints:

- `/v1/public/characters`
- `/v1/public/comics`

The goal is to process a list of characters and determine the number of comics in which each character has appeared.

## Tech Stack

- Programming Language: Python
- Data Processing: Pandas, Json
- Version Contro: Git
- File formats used: Json, Parquet and CSV    

## Setup Instructions

To run the project locally, follow these steps:

1. Clone the repository:
    ```bash
    git clone https: https://github.com/Samuel-Tito-Infantas/de-marvel-api-test.git
    cd de-marvel-api-test 
    ```

2. Fill in the `.env` file with your API keys:
    ```env
    PUBLIC_KEY = 123456789
    PRIVATE_KEY = 123456789
    ENVIROMENT = LOCAL
    ```

3. Run the executor program:
    ```bash
    ./executor.sh
    ```
    This script will handle all necessary steps automatically.

## Data Pipeline Workflow

<p align="center"><img src="docs/image/Workflow.png"></p>

The workflow is desinged to make requests to the  [MARVEL API](https://developer.marvel.com/docs) and persist the response data in folder at runtime.

Each response is saved as a `JSON` file during iteration to avoid data loss.

The `JSON` files are then processed, joined, and split into separate dataframes, which are saved in Parquet format.

Finally, a custom function generates the submission file:
`output_result/result/technical_result.csv`, which contains the requested information in the following format:


| Column_name | Type |
| --------    | ------- |
| Character_name | str  |
| quantity_of_comics_appear | int |


## Layout

```
de-marvel-api-test   
|    
├── src
│   ├── aux_functions
|   |   ├── api_request_code.py
|   |   └── data_aux_functions.py
|   |   
│   ├── etl_functions
|   |   ├── etl_functions_characters.py
|   |   ├── etl_functions_comics.py
|   |   └── etl_functions_result.py
│   └── parameters.py
|   
├── tmp_output
│   ├── characters
|   |   └── ...
|   └── comics
|       └── ...
|
├── output_result
│   ├── technical_result.csv
|   └── result
|       ├── characters
|       |   └── ...
|       └── comics
|           └── ...  
├── .env
├── .flake8
├── executor.sh
├── app.py
├── README.md
├── requirements-dev.txt
├── requirements.txt
└── .gitignore
└── ...
```

## Testing [TBD]

Testing will be added in a future iteration.

## Challenges and Solutions

- __SSL Certificate Issues__:       
Handling the SSL certificate took approximately two days to resolve.

- __Complex API Strategy for__ `/v1/public/comics`:   
The large number of records made this endpoint challenging. To prevent the API from terminating connections, a sleep step was implemented to manage the request rate.

## Future Improvements

1. Integrate the code to insert data into` MySQL` or `PostgreSQL` using a `Docker image`.
2. Adapt the code to run in an `AWS environment` using services such as `Lambda`, `S3`, `Glue`, and `RDS`.
3. Implement a `CI/CD` workflow using `GitHub Actions` and `Terraform` to enhance the project's reliability and scalability.
