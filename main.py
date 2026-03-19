import json
import logging
from pathlib import Path
import duckdb
import requests
from dbt.cli.main import dbtRunner, dbtRunnerResult

# General idea: ELT pipeline using Kimball's dimensional modeling.
# Load raw data first, then transform with dbt.

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

DB_PATH  = "warehouse.duckdb"
API_URL  = "https://clinicaltrials.gov/api/v2/studies"
RAW_PATH = Path("data/raw_studies.jsonl")
DBT_PROJECT_DIR = Path("dbt_project")
OUTPUT_DIR = Path("output")

# Helper function to avoid repeating code
def _dbt_run(step: str, models: list[str]) -> None:
    runner = dbtRunner()
    result: dbtRunnerResult = runner.invoke(
        [
            "run",
            "--select", " ".join(models),
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR),
        ]
    )
    if not result.success:
        raise RuntimeError(f"dbt run failed at step '{step}' — check logs above for details")


# Downloading the entire dataset from the API takes a while, so I limited it to 10k for this test
# I used AI here to help me with the syntax and debugging.
def ingest():
    logger.info("Step 1: fetching studies from ClinicalTrials.gov API")

    RAW_PATH.parent.mkdir(exist_ok=True)

    if RAW_PATH.exists():
        logger.info("Raw file already exists, skipping fetch")
    else:
        # There is no simple way to just write the data into duckdb, so I decided to simulate a data landing zone
        # and write the json as a physical local file, then load said json into duckdb.
        total = 0
        page_token = None
        with open(RAW_PATH, "w") as f:
            # Downloading every study takes a long time, so for this example I limited it to a few
            while total < 10000:
                # 1000 is the maximum pageSize according to the docs
                params = {"format": "json", "pageSize": 1000}
                if page_token:
                    params["pageToken"] = page_token

                resp = requests.get(API_URL, params=params, timeout=30)
                # Throws exception if the HTTP response was a 4xx or 5xx
                resp.raise_for_status()

                # There is a risk of python running out of memory if I try to write to disk every study at the same time
                # So I do it once per API call
                data = resp.json()

                for study in data["studies"]:
                    f.write(json.dumps(study) + "\n")
                total += len(data["studies"])
                logger.info("Fetched %d studies so far", total)

                page_token = data.get("nextPageToken")
                if not page_token:
                    break

    con = duckdb.connect(DB_PATH)
    try:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw;")
        # duckdb has a very neat function that creates a table from a json. This is the Newline Delimited version
        con.execute(f"CREATE OR REPLACE TABLE raw.raw_studies AS SELECT * FROM read_ndjson_auto('{RAW_PATH}')")
        logger.info("Loaded raw.raw_studies from %s", RAW_PATH)
    finally:
        con.close()


def transform():
    logger.info("Step 2: running dbt transformations")

    _dbt_run("staging", ["stg_study"])

    _dbt_run("facts", ["fact_study"])

    _dbt_run("dimensions", [
        "dim_date",
        "dim_study_type",
        "dim_condition",
        "dim_intervention",
        "dim_country",
    ])

    _dbt_run("bridges", [
        "bridge_study_condition",
        "bridge_study_intervention",
        "bridge_study_country",
    ])

    # Future improvement: abstracting the _dbt_run so that it can run tests or models
    # Ran out of time to implement it, but I wanted to make it clear that I know this is kind of dirty
    # Usually I would run dbt integrated with airflow (astronomer) which makes handling tests a much cleaner affair
    runner = dbtRunner()
    result: dbtRunnerResult = runner.invoke(
        [
            "test",
            "--project-dir", str(DBT_PROJECT_DIR),
            "--profiles-dir", str(DBT_PROJECT_DIR),
        ]
    )
    if not result.success:
        raise RuntimeError("dbt tests failed — check logs above for details")

    logger.info("dbt transformations completed successfully")


def mart():
    logger.info("Step 3: running dbt mart models")

    _dbt_run("marts", [
        "trials_by_type_and_phase",
        "top_conditions",
        "trials_by_country",
    ])

    logger.info("dbt mart models completed successfully")

# This function is just to show the result of the mart functions in this test
# In a prod system the reports would be either be sent somewhere or saved as views
# (materialized or otherwise) in a datawarehouse.
def export():
    logger.info("Extra step: writting reports to CSV")

    OUTPUT_DIR.mkdir(exist_ok=True)

    tables = [
        ("main_marts", "trials_by_type_and_phase"),
        ("main_marts", "top_conditions"),
        ("main_marts", "trials_by_country"),
    ]

    con = duckdb.connect(DB_PATH)
    try:
        for schema, table in tables:
            out = OUTPUT_DIR / f"{table}.csv"
            con.execute(f"COPY (SELECT * FROM {schema}.{table}) TO '{out}' (HEADER, DELIMITER ',')")
            logger.info("Exported %s to %s", table, out)
    finally:
        con.close()


if __name__ == "__main__":
    ingest()
    transform()
    mart()
    export()