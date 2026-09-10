"""
Unit tests for the business rules engine.
Run with: pytest tests/
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

import pytest
from pyspark.sql import SparkSession
from etl_pipeline import apply_business_rules, build_rule_condition


@pytest.fixture(scope="module")
def spark():
    spark = SparkSession.builder.master("local[2]").appName("test").getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_rules():
    return [
        {"rule_id": "R001", "column": "amount", "operator": ">", "value": 1000, "action": "flag_for_review"},
        {"rule_id": "R002", "column": "customer_id", "operator": "is_null", "value": None, "action": "reject"},
        {"rule_id": "R004", "column": "amount", "operator": "<", "value": 0, "action": "reject"},
    ]


def test_flags_high_value_transaction(spark, sample_rules):
    df = spark.createDataFrame([("CUST1", 5000)], ["customer_id", "amount"])
    result = apply_business_rules(df, sample_rules).collect()[0]
    assert result["row_status"] == "FLAGGED"
    assert "R001" in result["triggered_rules"]


def test_rejects_null_customer_id(spark, sample_rules):
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("amount", IntegerType(), True),
    ])
    df = spark.createDataFrame([(None, 500)], schema)
    result = apply_business_rules(df, sample_rules).collect()[0]
    assert result["row_status"] == "REJECTED"
    assert "R002" in result["triggered_rules"]


def test_rejects_negative_amount(spark, sample_rules):
    df = spark.createDataFrame([("CUST2", -50)], ["customer_id", "amount"])
    result = apply_business_rules(df, sample_rules).collect()[0]
    assert result["row_status"] == "REJECTED"
    assert "R004" in result["triggered_rules"]


def test_clean_row_stays_clean(spark, sample_rules):
    df = spark.createDataFrame([("CUST3", 100)], ["customer_id", "amount"])
    result = apply_business_rules(df, sample_rules).collect()[0]
    assert result["row_status"] == "CLEAN"
    assert result["triggered_rules"] == []


def test_reject_takes_precedence_over_flag(spark, sample_rules):
    # amount > 1000 triggers FLAG, but also test a row that is null AND high value -> REJECT wins
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType
    schema = StructType([
        StructField("customer_id", StringType(), True),
        StructField("amount", IntegerType(), True),
    ])
    df = spark.createDataFrame([(None, 5000)], schema)
    result = apply_business_rules(df, sample_rules).collect()[0]
    assert result["row_status"] == "REJECTED"
