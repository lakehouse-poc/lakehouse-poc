from databricks.connect import DatabricksSession
from pyspark.sql import functions as F

# Remote Spark Session über Databricks Connect holen
spark = DatabricksSession.builder.getOrCreate()

# kleines DataFrame bauen (läuft remote auf deinem Databricks Serverless)
df = spark.range(0, 5).withColumnRenamed("id", "Wert")

print("Hallo aus Databricks 👋")
print("Anzahl Zeilen:", df.count())
print("Rows als Liste:", df.collect())

# Optional: noch eine Transformation als Proof
df2 = df.withColumn("Wert_x2", F.col("Wert") * 2)
print("Doppelt so hoch:", df2.collect())
