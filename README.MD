# MigX — Clinical Trials Data Pipeline

## Project Overview

```
| API | -> | Ingestion Layer | -> | Transformation Layer | -> | Marts Layer |
```

ELT pipeline that uses DuckDB to simulate a data warehouse and dbt to handle the transformations. The challenge asked to use one dataset of 4 possible choices, and I chose the ClinicalTrials.gov API to show my experience working with APIs.

I simulated a data landing zone by first writing the JSON into disk, then reading it from disk into the database. In a real production environment, DuckDB would be a data warehouse like BigQuery, and instead of writing to a local disk the data would be saved to a bucket.

---

## Architecture

Simple medallion architecture with 3 layers:

- **Staging** — raw data loaded from the source with minimal transformations
- **Intermediate** — the bulk of the transformation to generate the data models that the reports will use
- **Marts** — the actual reports: queries that generate useful information from the data

---

## Data Model

I decided to use a classic Kimball (star) schema. While the challenge asked to "design a normalized database schema", it also stated that it left architectural choices open. So I opted for a data model that is not normalized. This has the benefit that it results in less expensive joins, which translates into faster queries and lower computational costs. On the other hand, it increases the amount of data stored and therefore the storage costs. Because in cloud systems computational cost is typically much more expensive than storage cost, I considered this to be a worthy trade-off.

There are also further cost optimizations that could be made. For example, the CSV stored in a bucket would likely only be accessed once when generating the models, and it could later be moved into long-term storage which is much cheaper (a quick search returned a price of $0.0012 per GB per month). The CSV could even be deleted, but perhaps it would be preferable to keep at least the latest batch for regenerating the tables in case of an incident, legal audits, etc.

If for any reason there is a dire need to reduce data redundancy and normalize the tables as much as possible, a snowflake schema would be preferable. This would require changing the scripts in the intermediate layer and adapting the joins of some of the mart queries, so this architectural choice has a cost but it is still reversible.

---

## Limitations

I did not include orchestration as I considered it to be over-engineering for this test. In a real production system it would be absolutely necessary. A simple way to implement it would be using Airflow, integrated with dbt via Astronomer Cosmos. The Airflow instance could be run using a container in a Kubernetes cluster, or in a fully managed service like Cloud Composer.

This would also improve scalability, observability and error handling (more on that later).

To save time, instead of downloading the entire dataset I limited the download to 10k studies.

---

## Future Improvements

Apart from orchestration, the biggest thing missing is Python unit tests. I was planning on using Pytest to mock the DuckDB and dbt libraries, and write unit tests for each of the functions in main. However, I ran out of time.

Better, custom dbt tests would also be an improvement.

There is a seed file in the dbt folder that I was planning on using to enrich the `dim_country` table, but I did not have time to use it.

Each layer should have its own set of dbt tests that are run before that stage is finished, instead of all at once.

---

## AI Usage

I used Claude to write boilerplate (the dbt_project yml files, logging declarations), write basic SQL like the fact table, and to help me debug.

The design is 100% mine, covering everything from architectural choices to project structure and code organization. There is nothing in this project that I couldn't do (and indeed, in the past, have done) myself manually. It simply saved time.

---

## Time Allocation

| Phase | Time |
|---|---|
| Design | 1.5 hours (most of that was figuring out the API and reading documentation) |
| Implementation | 4 hours |
| Documentation | 1 hour |

---

## How to Run

Build and run the Docker image:

```bash
docker build -t migx .
docker run -v "${PWD}/output:/app/output" migx
```

The pipeline will download 10k studies from the API, build the warehouse, and export the mart tables as CSVs to the `output/` folder.

---

## Bonus Questions

### Scalability

The biggest bottleneck right now is at ingestion. The best way to improve this while still using the API would be to use the filtering functionality mentioned in the documentation as a way to partition the data, then parallelize the download by giving each filter to one worker, then merging the results. The documentation did not mention rate limits, but this could potentially be a problem too.

In practice a better solution would probably be to switch to the option that contains multiple CSVs that can be downloaded in bulk. This problem is embarrassingly easy to parallelize, though once again you might hit rate limits from the source website.

Obviously a proper data warehouse (be it on-site or in the cloud) would scale better than a local DuckDB database.

The current dbt models are destructive and recreate the tables every time. An incremental model would be preferable for a real production system.

### Data Quality

Custom dbt tests would help catch data quality issues automatically. Right now they are very simple and limited. It would also be preferable to run a set of tests per layer, instead of all at once.

### Compliance

There is personal data in this dataset (investigator names, emails, and phone numbers in the locations module). Legitimate Interest due to this being related to medical research might justify not anonymizing it, but there still needs to be an effective way to keep track of and delete all data related to a particular person should they request it (right to be forgotten).

### Monitoring

Should the improvements mentioned above be implemented, monitoring becomes much easier using various logging tools like Stackdriver or Airflow logs. Each stage of the pipeline should log when it starts and when it finishes. Any exception should be caught and logged. Automatic alerts should be set to detect said error logs and notify whoever owns the system (for example, with a Slack alert). The various cloud vendors offer tools to help with this (like Cloud Monitoring for GCP).

### Security

Should this run in the cloud, a service account should be set up with the minimum access necessary for the pipeline to run. The hardcoded constants should be environment (or Airflow) variables.
