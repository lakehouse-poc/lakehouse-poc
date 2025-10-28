from pyspark.sql import SparkSession

# SparkSession vom Databricks-Cluster holen
spark = SparkSession.builder.getOrCreate()

# kleines DataFrame bauen
df = spark.range(0, 5).withColumnRenamed("id", "Wert")

print("Hallo aus Databricks 👋")
print("Anzahl Zeilen:", df.count())
print("Rows als Liste:", df.collect())

