"""Création de la SparkSession."""
from pyspark.sql import SparkSession


def get_spark(cfg: dict) -> SparkSession:
    return (
        SparkSession.builder.appName(cfg["app_name"])
        .master(cfg["master"])
        .config("spark.sql.shuffle.partitions", cfg["shuffle_partitions"])
        # Adaptive Query Execution : fusion des petites partitions, gestion du skew
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.skewJoin.enabled", "true")
        # Le Sénégal est en UTC toute l'année : pas d'ambiguïté d'horodatage
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
