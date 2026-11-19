"""Couche Bronze : ingestion des sources brutes en Parquet, avec schémas explicites.

Les schémas sont déclarés plutôt qu'inférés : l'inférence relit les fichiers,
coûte cher sur de gros volumes et change silencieusement si une source dérive.
"""
import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from .config import chemin

log = logging.getLogger(__name__)


def _schema(*champs: tuple) -> StructType:
    return StructType([StructField(nom, type_(), True) for nom, type_ in champs])


SOURCES = {
    "clients": ("crm/clients", "csv", _schema(
        ("client_id", LongType), ("msisdn", StringType), ("segment", StringType), ("offre", StringType),
        ("region", StringType), ("tranche_age", StringType), ("date_activation", StringType))),
    "cellules": ("reseau/cellules", "csv", _schema(
        ("cell_id", StringType), ("region", StringType), ("technologie", StringType))),
    "cdr": ("cdr", "csv", _schema(
        ("cdr_id", LongType), ("msisdn", StringType), ("date_heure", StringType),
        ("type_evenement", StringType), ("destination", StringType), ("duree_sec", IntegerType),
        ("volume_mo", DoubleType), ("cell_id", StringType), ("montant_fcfa", DoubleType))),
    "recharges": ("recharges", "csv", _schema(
        ("recharge_id", LongType), ("msisdn", StringType), ("date_heure", StringType),
        ("montant_fcfa", LongType), ("canal", StringType))),
    "mobile_money": ("mobile_money", "json", _schema(
        ("txn_id", StringType), ("msisdn", StringType), ("date_heure", StringType),
        ("type_transaction", StringType), ("montant_fcfa", LongType), ("frais_fcfa", LongType),
        ("statut", StringType))),
}


def ingest(spark: SparkSession, cfg: dict) -> None:
    for table, (source, fmt, schema) in SOURCES.items():
        lecteur = spark.read.schema(schema)
        df = (lecteur.option("header", True).csv(chemin(cfg, "raw", source)) if fmt == "csv"
              else lecteur.json(chemin(cfg, "raw", source)))
        (df.withColumn("_source_file", F.input_file_name())
           .withColumn("_ingested_at", F.current_timestamp())
           .write.mode("overwrite").parquet(chemin(cfg, "bronze", table)))
        log.info("Bronze %-13s %12s lignes", table, f"{spark.read.parquet(chemin(cfg, 'bronze', table)).count():,}")
