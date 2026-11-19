"""Marts analytiques en Spark SQL, exportés pour Power BI.

Chaque fichier de sql/marts devient une table Gold (Parquet) et un export CSV.
Les requêtes reçoivent leurs paramètres par marqueurs nommés (:date_fin) :
aucune concaténation de chaînes dans le SQL.
"""
import logging
from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

from .config import chemin, periode

log = logging.getLogger(__name__)

SQL_DIR = Path(__file__).resolve().parents[2] / "sql" / "marts"
TABLES_GOLD = ["dim_date", "dim_client", "dim_cellule", "fait_trafic", "fait_recharge", "fait_transaction_mm"]


def enregistrer_vues(spark: SparkSession, cfg: dict) -> None:
    for table in TABLES_GOLD:
        spark.read.parquet(chemin(cfg, "gold", table)).createOrReplaceTempView(table)


def executer_sql(spark: SparkSession, fichier: Path, params: dict) -> DataFrame:
    requete = fichier.read_text(encoding="utf-8")
    args = {k: v for k, v in params.items() if f":{k}" in requete}
    return spark.sql(requete, args=args) if args else spark.sql(requete)


def build_marts(spark: SparkSession, cfg: dict) -> None:
    enregistrer_vues(spark, cfg)
    _, fin = periode(cfg)
    params = {"date_fin": (fin - timedelta(days=1)).isoformat()}

    for fichier in sorted(SQL_DIR.glob("*.sql")):
        nom = fichier.stem
        executer_sql(spark, fichier, params).write.mode("overwrite").parquet(chemin(cfg, "gold", "marts", nom))
        mart = spark.read.parquet(chemin(cfg, "gold", "marts", nom))
        # Un seul fichier CSV par mart : directement importable dans Power BI
        mart.coalesce(1).write.mode("overwrite").option("header", True).csv(chemin(cfg, "exports", nom))
        log.info("Mart %-24s %12s lignes", nom, f"{mart.count():,}")
