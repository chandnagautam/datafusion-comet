import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

val session = spark
val numRows = 10000000L

println(s"Generating $numRows rows of standard clickstream dataset...")

// We generate deterministic user activity logs/clickstream
val df = session.range(0, numRows)
  .withColumn("user_id", (rand(42) * 1000000).cast("int"))
  .withColumn("session_id", concat(lit("sess_"), md5((rand(43) * 10000000).cast("string"))))
  .withColumn("event_timestamp", (lit(1704067200L) + (rand(44) * 31536000L)).cast("timestamp"))
  .withColumn("page_path", when(rand(45) < 0.4, "/home")
    .when(rand(45) < 0.7, "/product")
    .when(rand(45) < 0.9, "/cart")
    .otherwise("/checkout"))
  .withColumn("event_type", when(rand(46) < 0.4, "view")
    .when(rand(46) < 0.7, "click")
    .when(rand(46) < 0.9, "add_to_cart")
    .otherwise("purchase"))
  .withColumn("device_type", when(rand(47) < 0.5, "mobile")
    .when(rand(47) < 0.85, "desktop")
    .otherwise("tablet"))

session.sql("CREATE DATABASE IF NOT EXISTS demo_catalog.db")

// 1. Create and write sorted table
println("Setting up sorted table: clickstream_sorted")
session.sql("DROP TABLE IF EXISTS demo_catalog.db.clickstream_sorted")
session.sql("""
  CREATE TABLE demo_catalog.db.clickstream_sorted (
    id BIGINT,
    user_id INT,
    session_id STRING,
    event_timestamp TIMESTAMP,
    page_path STRING,
    event_type STRING,
    device_type STRING
  ) USING iceberg
""")

val tableSorted = org.apache.iceberg.spark.Spark3Util.loadIcebergTable(session, "demo_catalog.db.clickstream_sorted")
tableSorted.replaceSortOrder().asc("event_timestamp").commit()

println("Writing sorted table (enforces write sort order)...")
df.write.format("iceberg").mode("append").save("demo_catalog.db.clickstream_sorted")

// 2. Create and write unsorted table
println("Setting up unsorted table: clickstream_unsorted")
session.sql("DROP TABLE IF EXISTS demo_catalog.db.clickstream_unsorted")
session.sql("""
  CREATE TABLE demo_catalog.db.clickstream_unsorted (
    id BIGINT,
    user_id INT,
    session_id STRING,
    event_timestamp TIMESTAMP,
    page_path STRING,
    event_type STRING,
    device_type STRING
  ) USING iceberg
""")

println("Writing unsorted table...")
df.write.format("iceberg").mode("append").save("demo_catalog.db.clickstream_unsorted")

println("All tables written successfully!")
sys.exit(0)
