from databricks.connect import DatabricksSession

# Verbindung zu deinem Databricks Workspace herstellen
spark = DatabricksSession.builder.getOrCreate()

# Beispiel: einfache DataFrame-Operation
df = spark.range(10)
print(df.count())
