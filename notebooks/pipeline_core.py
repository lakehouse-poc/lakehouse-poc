from pyspark.sql import functions as F
from pyspark.sql import Window

#
# =========================================================
# CONFIG
# =========================================================
# Idee: Jede Tabelle bekommt ihre eigene Config.
# Später können wir das aus YAML laden, aber jetzt erstmal hardcodiert.

config_kosten = {
    "source_path"        : "abf:/mnt/datalake/input/KOSTEN_*.csv",  # Beispiel Pfad (SAS/Blob/ABF etc.)
    "raw_table"          : "layer0100.kosten_raw",
    "dim_table"          : "layer0150.dim_kostenstelle",            # SCD2-Zieltabelle
    "business_key"       : "Kostenstelle",
    "compare_columns"    : ["Bezeichnung", "Bereich"],
    "technical_columns"  : {
        "gueltig_von" : "GueltigVon",
        "gueltig_bis" : "GueltigBis",
        "is_current"  : "IsCurrent"
    }
}

#
# =========================================================
# HELPERS
# =========================================================

def with_ingest_metadata(df):
    """
    Hängt Metadaten an, damit wir wissen, wann/woher die Daten kamen.
    """
    return (
        df
        .withColumn("IngestTimestamp", F.current_timestamp())
        .withColumn("IngestSource", F.lit("blob-import"))  # später: dynamisch
    )

#
# =========================================================
# 1) LOAD RAW
# =========================================================
def load_raw(spark, source_path, raw_table):
    """
    Liest CSV (oder Parquet) ein und speichert den Rohzustand unverändert in die RAW-Zieltabelle.
    RAW = 0100 Layer (Staging / Landing / Bronze).

    - Header = True, Schema wird inferiert
    - wir hängen Ingest-Metadaten dran
    - wir speichern als Delta-Tabelle
    """
    df = (
        spark.read
             .option("header", True)
             .option("inferSchema", True)
             .csv(source_path)
    )

    df = with_ingest_metadata(df)

    # RAW als Delta persistent machen
    (
        df
        .write
        .format("delta")
        .mode("append")        # wichtig: wir hängen an, RAW ist historisch
        .saveAsTable(raw_table)
    )

    return df  # geben wir zurück, falls man gleich weiterarbeiten will


#
# =========================================================
# 2) TRANSFORM DIM
# =========================================================
def transform_dim(df_raw, business_key, compare_columns):
    """
    Nimmt die RAW-Daten und bereitet die 'aktuellen' Dimensionseinträge vor.
    Dinge wie:
    - Duplikate je Key auflösen
    - nur relevante Spalten auswählen
    - Trim / Cleanup
    - Datentyp-Korrekturen wären hier auch denkbar
    """

    wanted_cols = [business_key] + compare_columns

    df_clean = df_raw.select(*wanted_cols)

    # trim strings
    for colname in wanted_cols:
        df_clean = df_clean.withColumn(colname, F.trim(F.col(colname)))

    # wir wollen pro business_key nur den letzten Stand (z.B. anhand IngestTimestamp)
    # Achtung: df_raw muss IngestTimestamp haben -> deswegen load_raw() zurückgeben ist praktisch
    w = Window.partitionBy(business_key).orderBy(F.col("IngestTimestamp").desc())

    df_latest = (
        df_raw
        .withColumn("rn", F.row_number().over(w))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .select(*wanted_cols)
        .distinct()
    )

    return df_latest


#
# =========================================================
# 3) MERGE SCD2
# =========================================================
def merge_scd(spark,
              df_latest,
              dim_table,
              business_key,
              compare_columns,
              technical_columns):
    """
    Slowly Changing Dimension Type 2.

    Ziel:
    - Falls Key neu -> neuen aktiven Datensatz anlegen
    - Falls Key existiert, aber sich etwas geändert hat -> alten auf 'inactive' setzen, neuen aktiven Datensatz schreiben
    - Falls unverändert -> nichts tun

    Wir erwarten, dass dim_table schon als Delta-Tabelle existiert (mit History).
    Falls nicht: wir legen sie initial an.
    """

    col_valid_from = technical_columns["gueltig_von"]
    col_valid_to   = technical_columns["gueltig_bis"]
    col_is_current = technical_columns["is_current"]

    # Schritt 0: Sicherstellen, dass es die Tabelle gibt
    if not spark._jsparkSession.catalog().tableExists(dim_table):
        # Tabelle erstmalig anlegen
        init_df = (
            df_latest
            .withColumn(col_valid_from, F.current_timestamp())
            .withColumn(col_valid_to,   F.lit(None).cast("timestamp"))
            .withColumn(col_is_current, F.lit(True))
        )

        (
            init_df
            .write
            .format("delta")
            .mode("overwrite")
            .saveAsTable(dim_table)
        )
        return

    # Schritt 1: existierende Dimension laden
    df_dim = spark.table(dim_table)

    # Aktuell gültige Zeilen (IsCurrent = True)
    df_dim_current = df_dim.filter(F.col(col_is_current) == True)

    # Schritt 2: Join RAW(neueste Werte) mit aktueller Dim-Version
    join_cond = [df_latest[business_key] == df_dim_current[business_key]]
    df_join = df_latest.join(df_dim_current, join_cond, "left")

    # Schritt 3: herausfinden, was sich geändert hat
    change_exprs = []
    for c in compare_columns:
        change_exprs.append(
            (F.col(f"df_latest.{c}") != F.col(f"df_dim_current.{c}")) &
            (F.col(f"df_latest.{c}").isNotNull() | F.col(f"df_dim_current.{c}").isNotNull())
        )

    # Achtung: wir müssen die Aliase setzen, damit obiges klappt
    df_join = df_latest.alias("df_latest").join(
        df_dim_current.alias("df_dim_current"),
        on=F.col(f"df_latest.{business_key}") == F.col(f"df_dim_current.{business_key}"),
        how="left"
    )

    any_change = None
    for expr in change_exprs:
        any_change = expr if any_change is None else (any_change | expr)

    df_to_update = (
        df_join
        .withColumn("is_new_key", F.col(f"df_dim_current.{business_key}").isNull())
        .withColumn("is_changed", F.when(F.col("is_new_key") == True, F.lit(False)).otherwise(any_change))
    )

    # Split:
    df_inserts_new_keys = df_to_update.filter(F.col("is_new_key") == True)
    df_inserts_changed  = df_to_update.filter(
        (F.col("is_new_key") == False) & (F.col("is_changed") == True)
    )
    df_unchanged        = df_to_update.filter(
        (F.col("is_new_key") == False) & (F.col("is_changed") == False)
    )

    # Schritt 4: Alte Versionen der geänderten Keys schließen (IsCurrent=False, GueltigBis=now)
    keys_changed = df_inserts_changed.select(f"df_latest.{business_key}").distinct()

    if keys_changed.count() > 0:
        ts_now = F.current_timestamp()
        df_dim_updates = (
            df_dim
            .join(keys_changed,
                  df_dim[business_key] == keys_changed[f"df_latest.{business_key}"],
                  "inner")
            .filter(F.col(col_is_current) == True)
            .withColumn(col_is_current, F.lit(False))
            .withColumn(col_valid_to,   ts_now)
        )

        # wir schreiben Updates zurück mittels Merge (Delta Lake)
        # Delta Lake Merge (SQL Syntax in Python)
        df_dim_updates.createOrReplaceTempView("tmp_updates")

        spark.sql(f"""
            MERGE INTO {dim_table} AS target
            USING tmp_updates AS src
            ON target.{business_key} = src.{business_key}
               AND target.{col_is_current} = True
            WHEN MATCHED THEN UPDATE SET
                target.{col_is_current} = False,
                target.{col_valid_to}   = src.{col_valid_to}
        """)

    # Schritt 5: Neue/aktualisierte Versionen einfügen
    df_new_versions = (
        df_inserts_new_keys
        .unionByName(df_inserts_changed)
        .select(
            F.col(f"df_latest.{business_key}").alias(business_key),
            *[
                F.col(f"df_latest.{c}").alias(c)
                for c in compare_columns
            ]
        )
        .withColumn(col_valid_from, F.current_timestamp())
        .withColumn(col_valid_to,   F.lit(None).cast("timestamp"))
        .withColumn(col_is_current, F.lit(True))
    )

    if df_new_versions.count() > 0:
        (
            df_new_versions
            .write
            .format("delta")
            .mode("append")
            .saveAsTable(dim_table)
        )

    # Hinweis/Return: könnte man ein kleines dict zurückgeben zur Doku
    return {
        "unchanged"   : df_unchanged.count(),
        "new_keys"    : df_inserts_new_keys.count(),
        "updated_keys": df_inserts_changed.count()
    }


#
# =========================================================
# 4) ORCHESTRATION / JOB
# =========================================================
def run_kosten_pipeline(spark):
    """
    Das ist unser "Job". Später kann das ein Notebook-Job werden oder ein echter Job-Cluster-Task.
    """
    cfg = config_kosten

    # 1. RAW laden
    df_raw = load_raw(
        spark=spark,
        source_path=cfg["source_path"],
        raw_table=cfg["raw_table"]
    )

    # 2. aufbereiten für Dimension
    df_dim_latest = transform_dim(
        df_raw=df_raw,
        business_key=cfg["business_key"],
        compare_columns=cfg["compare_columns"]
    )

    # 3. SCD2 mergen
    result_stats = merge_scd(
        spark=spark,
        df_latest=df_dim_latest,
        dim_table=cfg["dim_table"],
        business_key=cfg["business_key"],
        compare_columns=cfg["compare_columns"],
        technical_columns=cfg["technical_columns"]
    )

    return result_stats
