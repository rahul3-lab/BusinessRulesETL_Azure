"""
Business Rules Engine & ETL Pipeline
-------------------------------------
Reads raw transaction data, applies business rules loaded from an
external JSON config (so business teams can change rules without a
code deployment), and writes clean / flagged / rejected outputs.

Locally this runs against CSV files. In Azure it is designed to run
on Azure Databricks, reading from / writing to Azure Data Lake Storage
(ADLS Gen2), and orchestrated by an Azure Data Factory pipeline
(see /adf_pipeline_templates).

Usage:
    python src/etl_pipeline.py \
        --input data/raw_transactions.csv \
        --rules config/business_rules.json \
        --output-dir output
"""

import argparse
import json
import logging
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("etl_pipeline")


def get_spark_session(app_name: str = "BusinessRulesETL") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def load_rules(rules_path: str) -> list:
    with open(rules_path, "r") as f:
        rules_config = json.load(f)
    logger.info(f"Loaded {len(rules_config['rules'])} business rules from {rules_path}")
    return rules_config["rules"]


def build_rule_condition(rule: dict):
    """Translate a JSON-configured rule into a Spark Column condition."""
    col = F.col(rule["column"])
    op = rule["operator"]
    val = rule["value"]

    if op == ">":
        return col > val
    elif op == "<":
        return col < val
    elif op == ">=":
        return col >= val
    elif op == "<=":
        return col <= val
    elif op == "==":
        return col == val
    elif op == "is_null":
        return col.isNull()
    elif op == "in":
        return col.isin(val)
    else:
        raise ValueError(f"Unsupported operator '{op}' in rule {rule['rule_id']}")


def apply_business_rules(df: DataFrame, rules: list) -> DataFrame:
    """
    Evaluate every rule against the DataFrame and tag each row with:
      - triggered_rules: array of rule_ids that matched
      - row_status: 'REJECTED' | 'FLAGGED' | 'CLEAN'
    """
    df = df.withColumn("triggered_rules", F.array())
    df = df.withColumn("row_status", F.lit("CLEAN"))

    for rule in rules:
        condition = build_rule_condition(rule)
        rule_id = rule["rule_id"]
        action = rule["action"]  # 'reject' or 'flag_for_review'

        df = df.withColumn(
            "triggered_rules",
            F.when(condition, F.array_union("triggered_rules", F.array(F.lit(rule_id))))
             .otherwise(F.col("triggered_rules"))
        )

        new_status = "REJECTED" if action == "reject" else "FLAGGED"
        # REJECTED takes precedence over FLAGGED; both take precedence over CLEAN
        df = df.withColumn(
            "row_status",
            F.when(
                condition & (F.lit(new_status) == "REJECTED"),
                F.lit("REJECTED")
            ).when(
                condition & (F.lit(new_status) == "FLAGGED") & (F.col("row_status") != "REJECTED"),
                F.lit("FLAGGED")
            ).otherwise(F.col("row_status"))
        )

    return df


def run_pipeline(input_path: str, rules_path: str, output_dir: str):
    spark = get_spark_session()

    logger.info(f"Reading raw data from {input_path}")
    raw_df = spark.read.option("header", True).option("inferSchema", True).csv(input_path)
    raw_count = raw_df.count()
    logger.info(f"Loaded {raw_count} raw rows")

    rules = load_rules(rules_path)
    tagged_df = apply_business_rules(raw_df, rules)

    clean_df = tagged_df.filter(F.col("row_status") == "CLEAN")
    flagged_df = tagged_df.filter(F.col("row_status") == "FLAGGED")
    rejected_df = tagged_df.filter(F.col("row_status") == "REJECTED")

    os.makedirs(output_dir, exist_ok=True)
    clean_df.toPandas().to_csv(f"{output_dir}/clean_transactions.csv", index=False)
    flagged_df.toPandas().to_csv(f"{output_dir}/flagged_transactions.csv", index=False)
    rejected_df.toPandas().to_csv(f"{output_dir}/rejected_transactions.csv", index=False)

    logger.info(f"Clean: {clean_df.count()} | Flagged: {flagged_df.count()} | Rejected: {rejected_df.count()}")
    logger.info(f"Outputs written to {output_dir}/")

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Business Rules ETL Pipeline")
    parser.add_argument("--input", default="data/raw_transactions.csv")
    parser.add_argument("--rules", default="config/business_rules.json")
    parser.add_argument("--output-dir", default="output")
    args = parser.parse_args()

    run_pipeline(args.input, args.rules, args.output_dir)
