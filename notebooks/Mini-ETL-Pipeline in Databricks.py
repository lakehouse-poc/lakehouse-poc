# Databricks notebook source
import pandas as pd
import io


defaults = {
    "encoding": "cp1252",
    "delimiter": ";",
    "quotechar": '"',        # <- gültiges Zeichen
    "quoting": 3,            # <- 3 entspricht csv.QUOTE_NONE
    "header": True,
    "line_terminator": "\r\n",
    "locale": "de-DE"
}

raw_map = [
    {"pattern": "KOSTEN_*.csv", "source_base": "https://frankstorage02.blob.core.windows.net/data/", "target_table": "kosten_raw"},
    {"pattern": "PERSONAL_*.csv", "source_base": "https://frankstorage02.blob.core.windows.net/data/", "target_table": "personal_raw"}
]

file_map = [{**defaults, **entry} for entry in raw_map]
config_df = pd.DataFrame(file_map)
display(config_df)

import requests, xml.etree.ElementTree as ET
from datetime import datetime, timezone

sas_token = "sp=rl&st=2025-10-27T17:34:19Z&se=2025-11-30T01:49:19Z&spr=https&sv=2024-11-04&sr=c&sig=rtu4zvHI%2FemJzQpMmT0yn8OpXXpnM3lDJPMZflPbpUU%3D"
container_url = "https://frankstorage02.blob.core.windows.net/data/"

list_url = f"{container_url}?restype=container&comp=list&{sas_token}"
response = requests.get(list_url)
response.raise_for_status()

root = ET.fromstring(response.text)

blob_infos = []
for blob in root.findall(".//Blob"):
    name = blob.find("Name").text
    lm_text = blob.find("Properties").find("Last-Modified").text
    last_modified = datetime.strptime(lm_text, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
    blob_infos.append({"name": name, "last_modified": last_modified})

blob_infos

scd_map = [
    {"source": "kosten_raw", "target": "dim_kostenstelle",
     "key_columns": ["Kostenstelle"], "compare_columns": ["Bezeichnung","Bereich"]},
    {"source": "personal_raw", "target": "dim_personal",
     "key_columns": ["Personalnummer"], "compare_columns": ["Name","Abteilung"]}
]

def run_scd_merge(spark, source, target, key_cols, compare_cols):
    on_clause = " AND ".join([f"tgt.{c}=src.{c}" for c in key_cols])
    diff_clause = " OR ".join([f"tgt.{c}<>src.{c}" for c in compare_cols])
    merge_sql = f'''
    MERGE INTO {target} AS tgt
    USING {source} AS src
    ON {on_clause} AND tgt.IsCurrent = true
    WHEN MATCHED AND ({diff_clause}) THEN
      UPDATE SET tgt.EffectiveTo=current_timestamp(), tgt.IsCurrent=false
    WHEN NOT MATCHED BY TARGET THEN
      INSERT ({", ".join(key_cols + compare_cols)}, EffectiveFrom, EffectiveTo, IsCurrent)
      VALUES ({", ".join(["src."+c for c in key_cols + compare_cols])}, current_timestamp(), null, true)
    '''
    spark.sql(merge_sql)

import io
import csv

import fnmatch
from pyspark.sql import functions as F

def load_files_to_raw(cfg, blob_infos, sas_token):
    pattern = cfg["pattern"]
    target_table = cfg["target_table"]

    print(f"=== Import für Pattern {pattern} → Tabelle {target_table} ===")

    # Dateien raussuchen, die zum Pattern passen
    matched_files = [b for b in blob_infos if fnmatch.fnmatch(b["name"], pattern)]

    if not matched_files:
        print("   Keine passenden Dateien gefunden.")
        return False

    for blob in matched_files:
        blob_name = blob["name"]

        file_url = f"{cfg['source_base']}{blob_name}?{sas_token}"

        r = requests.get(file_url)
        r.raise_for_status()

        pdf = pd.read_csv(
            io.BytesIO(r.content),
            sep=cfg["delimiter"],
            encoding=cfg["encoding"],
            quotechar=cfg["quotechar"],
            quoting=cfg["quoting"],
            header=0 if cfg["header"] else None
        )

        df = spark.createDataFrame(pdf)

        # hier schreiben wir wirklich in die Raw-Tabelle
        df.write.mode("append").saveAsTable(target_table)

        print(f"   ✔ {blob_name} importiert ({len(pdf)} Zeilen) in {target_table}")

    return True

for cfg in file_map:
    loaded = load_files_to_raw(cfg, blob_infos, sas_token)

    # wenn wir was in raw geladen haben, dann auch SCD-Merge fahren
    if loaded:
        # wir suchen in scd_map die passende Definition für genau diese Raw-Tabelle
        match = [m for m in scd_map if m["source"] == cfg["target_table"]]
        if match:
            m = match[0]
            run_scd_merge(
                spark,
                source=m["source"],
                target=m["target"],
                key_cols=m["key_columns"],
                compare_cols=m["compare_columns"]
            )
            print(f"   🔄 SCD Merge für {m['target']} ausgeführt")
        else:
            print("   (keine SCD-Definition für diese Raw-Tabelle gefunden)")
