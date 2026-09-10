# Business Rules Engine & ETL Pipeline on Azure

A config-driven ETL pipeline that ingests raw transaction data, applies
business validation rules loaded from an external JSON file (no code
change needed when business teams update rules), and splits records
into **clean / flagged / rejected** outputs.

## Why this project
Mirrors a real Data Engineering task: business teams define rules
("flag transactions over ₹50,000", "reject rows missing a customer ID"),
and the pipeline must apply them without a redeploy every time the
rules change.

## Stack
- **PySpark** (`src/etl_pipeline.py`) — primary implementation
- **Scala/Spark** (`src/RuleEngine.scala`) — equivalent implementation, same logic
- **Azure Databricks** — where this would run in production
- **Azure Data Lake Storage Gen2** — source/target storage
- **Azure Data Factory** — orchestration (see `adf_pipeline_templates/adf_pipeline.json`)
- **Azure Synapse Analytics** — downstream reporting sink

## Run it locally
```bash
pip install pyspark pandas pytest
python src/etl_pipeline.py \
    --input data/raw_transactions.csv \
    --rules config/business_rules.json \
    --output-dir output
```
Outputs land in `output/clean_transactions.csv`,
`output/flagged_transactions.csv`, `output/rejected_transactions.csv`.

## Run the tests
```bash
python -m pytest tests/ -v
```

## How the rules engine works
Rules live in `config/business_rules.json` — each rule has a column,
an operator (`>`, `<`, `==`, `is_null`, `in`, ...), a value, and an
action (`flag_for_review` or `reject`). `apply_business_rules()`
compiles each rule into a Spark `Column` condition at runtime, so
adding a new rule is just adding a new JSON object — no PySpark code
changes required.

## Azure deployment notes
- `src/etl_pipeline.py` becomes a Databricks notebook/job.
- `config/business_rules.json` is stored in ADLS Gen2 so business
  teams (or a lightweight admin UI) can update it independently.
- `adf_pipeline_templates/adf_pipeline.json` shows the ADF pipeline
  that validates the source file exists, triggers the Databricks job,
  copies clean data into Synapse, and posts a Teams alert if rows were
  rejected.

## What I'd extend next
- Add row-level lineage/audit logging (which rule rejected which row, when)
- Parameterize thresholds per business unit
- Add a simple Streamlit/Power BI view over the flagged output for reviewers
