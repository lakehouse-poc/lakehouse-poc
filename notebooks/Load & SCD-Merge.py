# Databricks notebook source
import io, requests, pandas as pd, fnmatch
from pyspark.sql import functions as F

def load_files_to_raw(cfg, new_files, sas_token):
    """Liest alle passenden Dateien und schreibt sie in target_raw."""
    matched = [f for f in new_files if fnmatch.fnmatch(f["name"], cfg["pattern"])]
    if not matched:
        print(f"Keine neuen Dateien für {cfg['target_raw']}")
        return False

    for blob in matched:
        file_url = f"{cfg['source_base']}{blob['name']}?{sas_token}"
        r = requests.get(file_url)
        r.raise_for_status()
        pdf = pd.read_csv(io.BytesIO(r.content), sep=';', encoding='cp1252')
        df = spark.createDataFrame(pdf)
        df.write.mode("append").saveAsTable(cfg["target_raw"])
        print(f"   ➜ {blob['name']} → {cfg['target_raw']} ({len(pdf)} Zeilen)")

    return True

def run_scd_merge(cfg):
    """Führt das SCD-Merge von raw → dim aus."""
    on_clause = " AND ".join([f"tgt.{c}=src.{c}" for c in cfg["key_columns"]])
    diff_clause = " OR ".join([f"tgt.{c}<>src.{c}" for c in cfg["compare_columns"]])
    merge_sql = f"""
    MERGE INTO {cfg['target_dim']} AS tgt
    USING {cfg['target_raw']} AS src
    ON {on_clause} AND tgt.IsCurrent = true
    WHEN MATCHED AND ({diff_clause}) THEN
      UPDATE SET tgt.EffectiveTo=current_timestamp(), tgt.IsCurrent=false
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({", ".join(cfg["key_columns"] + cfg["compare_columns"])},
              EffectiveFrom, EffectiveTo, IsCurrent)
      VALUES ({", ".join(["src."+c for c in cfg["key_columns"] + cfg["compare_columns"]])},
              current_timestamp(), null, true)
    """
    print(f"▶ Merge {cfg['target_raw']} → {cfg['target_dim']}")
    spark.sql(merge_sql)