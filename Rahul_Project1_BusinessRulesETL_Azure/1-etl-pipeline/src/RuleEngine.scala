/*
  RuleEngine.scala
  -----------------
  Scala/Spark equivalent of the business-rules ETL logic (src/etl_pipeline.py).
  Included to demonstrate the Scala + Spark skill set requested in the JD.

  Build/run (with sbt and Spark on classpath):
    spark-submit --class RuleEngine target/scala-2.12/etl-pipeline_2.12-1.0.jar \
      data/raw_transactions.csv config/business_rules.json output/
*/

import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.functions._
import play.api.libs.json._
import scala.io.Source

case class BusinessRule(rule_id: String, description: String, column: String,
                         operator: String, value: JsValue, action: String)

object RuleEngine {

  def loadRules(path: String): Seq[BusinessRule] = {
    val jsonStr = Source.fromFile(path).mkString
    val json = Json.parse(jsonStr)
    (json \ "rules").as[Seq[JsObject]].map { r =>
      BusinessRule(
        (r \ "rule_id").as[String],
        (r \ "description").as[String],
        (r \ "column").as[String],
        (r \ "operator").as[String],
        (r \ "value").getOrElse(JsNull),
        (r \ "action").as[String]
      )
    }
  }

  def buildCondition(rule: BusinessRule) = {
    val c = col(rule.column)
    rule.operator match {
      case ">"       => c > rule.value.as[Double]
      case "<"       => c < rule.value.as[Double]
      case ">="      => c >= rule.value.as[Double]
      case "<="      => c <= rule.value.as[Double]
      case "=="      => c === rule.value.toString
      case "is_null" => c.isNull
      case "in"      => c.isin(rule.value.as[Seq[String]]: _*)
      case other     => throw new IllegalArgumentException(s"Unsupported operator: $other")
    }
  }

  def applyRules(df: DataFrame, rules: Seq[BusinessRule]): DataFrame = {
    var result = df.withColumn("triggered_rules", array())
                    .withColumn("row_status", lit("CLEAN"))

    rules.foreach { rule =>
      val cond = buildCondition(rule)
      val newStatus = if (rule.action == "reject") "REJECTED" else "FLAGGED"

      result = result.withColumn(
        "triggered_rules",
        when(cond, array_union(col("triggered_rules"), array(lit(rule.rule_id))))
          .otherwise(col("triggered_rules"))
      )

      result = result.withColumn(
        "row_status",
        when(cond && lit(newStatus) === "REJECTED", lit("REJECTED"))
          .when(cond && lit(newStatus) === "FLAGGED" && col("row_status") =!= "REJECTED", lit("FLAGGED"))
          .otherwise(col("row_status"))
      )
    }
    result
  }

  def main(args: Array[String]): Unit = {
    val inputPath  = if (args.length > 0) args(0) else "data/raw_transactions.csv"
    val rulesPath  = if (args.length > 1) args(1) else "config/business_rules.json"
    val outputDir  = if (args.length > 2) args(2) else "output"

    val spark = SparkSession.builder()
      .appName("BusinessRulesETL-Scala")
      .master(sys.env.getOrElse("SPARK_MASTER", "local[*]"))
      .getOrCreate()

    val rawDf = spark.read.option("header", "true").option("inferSchema", "true").csv(inputPath)
    val rules = loadRules(rulesPath)
    val tagged = applyRules(rawDf, rules)

    tagged.filter(col("row_status") === "CLEAN").write.mode("overwrite")
      .option("header", "true").csv(s"$outputDir/clean_transactions")
    tagged.filter(col("row_status") === "FLAGGED").write.mode("overwrite")
      .option("header", "true").csv(s"$outputDir/flagged_transactions")
    tagged.filter(col("row_status") === "REJECTED").write.mode("overwrite")
      .option("header", "true").csv(s"$outputDir/rejected_transactions")

    spark.stop()
  }
}
