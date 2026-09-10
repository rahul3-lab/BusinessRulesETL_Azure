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

## Project structure

```
.
├── data/
│   └── raw_transactions.csv        # sample raw input
├── config/
│   └── business_rules.json         # externalized rule definitions
├── src/
│   ├── etl_pipeline.py             # PySpark implementation (primary)
│   └── RuleEngine.scala            # Scala/Spark equivalent implementation
├── tests/
│   └── test_etl_pipeline.py        # pytest unit tests
├── adf_pipeline_templates/
│   └── adf_pipeline.json           # Azure Data Factory orchestration template
└── README.md
```

## Stack

- **PySpark** (`src/etl_pipeline.py`) — primary implementation
- **Scala/Spark** (`src/RuleEngine.scala`) — equivalent implementation, same logic
- **Azure Databricks** — where this would run in production
- **Azure Data Lake Storage Gen2 (ADLS Gen2)** — source/target storage
- **Azure Data Factory (ADF)** — orchestration (see `adf_pipeline_templates/adf_pipeline.json`)
- **Azure Synapse Analytics** — downstream reporting sink

## How the rules engine works

Rules live in `config/business_rules.json`. Each rule has:

| Field         | Description                                               |
|---------------|-------------------------------------------------------------|
| `rule_id`     | Unique identifier, e.g. `R001`                              |
| `description` | Human-readable explanation of the rule                      |
| `column`      | DataFrame column the rule evaluates                         |
| `operator`    | `>`, `<`, `>=`, `<=`, `==`, `is_null`, `in`                  |
| `value`       | Comparison value (number, string, list, or `null`)          |
| `action`      | `flag_for_review` or `reject`                                |

`apply_business_rules()` compiles each rule into a Spark `Column`
condition at runtime, so adding a new rule is just adding a new JSON
object — no PySpark/Scala code changes required.

Every row is tagged with:
- `triggered_rules` — array of `rule_id`s that matched
- `row_status` — `CLEAN`, `FLAGGED`, or `REJECTED`

**Precedence:** `REJECTED` > `FLAGGED` > `CLEAN`. A row that trips both
a reject rule and a flag rule ends up `REJECTED`.

### Current rules (`config/business_rules.json`)

| Rule | Description                                       | Action           |
|------|----------------------------------------------------|------------------|
| R001 | `transaction_amount` > 50,000                       | flag_for_review  |
| R002 | `customer_id` is null                               | reject           |
| R003 | `region` in `[EMBARGO_A, EMBARGO_B]`                | flag_for_review  |
| R004 | `transaction_amount` < 0                            | reject           |

## Run it locally

```bash
pip install pyspark pandas pytest

python src/etl_pipeline.py \
    --input data/raw_transactions.csv \
    --rules config/business_rules.json \
    --output-dir output
```

Outputs land in:
- `output/clean_transactions.csv`
- `output/flagged_transactions.csv`
- `output/rejected_transactions.csv`

## Run the tests

```bash
python -m pytest tests/ -v
```

Tests cover: flagging high-value transactions, rejecting null
`customer_id`s, rejecting negative amounts, clean rows staying clean,
and reject-takes-precedence-over-flag behavior.

## Azure deployment notes

- `src/etl_pipeline.py` becomes a Databricks notebook/job (see
  `notebookPath` in `adf_pipeline.json`).
- `config/business_rules.json` is stored in ADLS Gen2 so business
  teams (or a lightweight admin UI) can update it independently of
  any code deployment.
- `adf_pipeline_templates/adf_pipeline.json` defines the ADF pipeline:
  1. **Validate_Source_File_Exists** — `GetMetadata` check against the raw container
  2. **Run_Databricks_RulesEngine_Job** — triggers the Databricks notebook with `input`, `rules`, and `output-dir` parameters
  3. **Copy_Clean_Data_To_Synapse** — copies clean output into Synapse via `SqlDWSink`
  4. **Send_Alert_On_Rejected_Rows** — posts a Teams webhook alert once the job completes

## Scala equivalent

`src/RuleEngine.scala` mirrors the same rule-loading and rule-application
logic in Scala/Spark, for environments where the Databricks job is run
as a compiled JAR rather than a PySpark notebook:

```bash
spark-submit --class RuleEngine target/scala-2.12/etl-pipeline_2.12-1.0.jar \
    data/raw_transactions.csv config/business_rules.json output/
```

## What I'd extend next

- Add row-level lineage/audit logging (which rule rejected which row, when)
- Parameterize thresholds per business unit
- Add a simple Streamlit/Power BI view over the flagged output for reviewers
- Add schema validation as a rule type (e.g. required columns, data types)
- Alert routing by severity (e.g. only page on-call for reject spikes, not flags)
