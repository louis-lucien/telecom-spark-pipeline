"""Couche Gold : modèle en étoile (dimensions + faits) prêt pour le reporting.

    dim_date ─┐                      ┌─ dim_cellule
              ├── fait_trafic ───────┤
    dim_client┼── fait_recharge      │
              └── fait_transaction_mm

Les événements dont le MSISDN n'existe pas dans le CRM sont rattachés au membre
« Inconnu » (client_id = -1) au lieu d'être perdus : les totaux restent justes.
"""
import logging
from datetime import date

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import chemin, periode

log = logging.getLogger(__name__)

INCONNU = -1
JOURS_FR = ["Dimanche", "Lundi", "Mardi", "Mercredi", "Jeudi", "Vendredi", "Samedi"]


def _date_id(col: str = "event_date"):
    return F.date_format(col, "yyyyMMdd").cast("int").alias("date_id")


def build_dim_date(spark: SparkSession, debut: date, fin: date) -> DataFrame:
    dates = spark.sql(f"SELECT explode(sequence(DATE'{debut}', date_sub(DATE'{fin}', 1))) AS date")
    return dates.select(
        _date_id("date"),
        "date",
        F.year("date").alias("annee"),
        F.month("date").alias("mois"),
        F.weekofyear("date").alias("semaine"),
        F.dayofmonth("date").alias("jour"),
        F.element_at(F.array(*map(F.lit, JOURS_FR)), F.dayofweek("date")).alias("jour_semaine"),
        F.dayofweek("date").isin(1, 7).alias("est_weekend"),
    )


def build_dim_client(clients: DataFrame) -> DataFrame:
    dim = clients.select("client_id", "msisdn", "segment", "offre", "region", "tranche_age", "date_activation")
    inconnu = dim.sparkSession.createDataFrame(
        [(INCONNU, None, "Inconnu", "Inconnu", "Inconnue", "Inconnue", None)], dim.schema)
    return dim.unionByName(inconnu)


def build_dim_cellule(cellules: DataFrame) -> DataFrame:
    dim = cellules.select("cell_id", "region", "technologie")
    inconnue = dim.sparkSession.createDataFrame([("INCONNUE", "Inconnue", "Inconnue")], dim.schema)
    return dim.unionByName(inconnue)


def rattacher_client(df: DataFrame, dim_client: DataFrame) -> DataFrame:
    """MSISDN -> client_id par broadcast join : la petite table est copiée sur chaque
    exécuteur, la grosse table de faits n'est jamais redistribuée (pas de shuffle)."""
    cles = F.broadcast(dim_client.filter(F.col("client_id") != INCONNU).select("msisdn", "client_id"))
    return (df.join(cles, "msisdn", "left")
              .withColumn("client_id", F.coalesce("client_id", F.lit(INCONNU).cast("long"))))


def build_fait_trafic(cdr: DataFrame, dim_client: DataFrame, dim_cellule: DataFrame) -> DataFrame:
    cellules = F.broadcast(dim_cellule.select("cell_id", F.lit(True).alias("_connue")))
    return (rattacher_client(cdr, dim_client)
            .join(cellules, "cell_id", "left")
            .select(
                _date_id(), "heure", "client_id",
                F.when(F.col("_connue"), F.col("cell_id")).otherwise("INCONNUE").alias("cell_id"),
                "type_evenement", "destination", "duree_sec", "volume_mo",
                (F.col("type_evenement") == "SMS").cast("int").alias("nb_sms"),
                "montant_fcfa", "event_date"))


def build_fait_recharge(recharges: DataFrame, dim_client: DataFrame) -> DataFrame:
    return rattacher_client(recharges, dim_client).select(
        "recharge_id", _date_id(), "heure", "client_id", "canal", "montant_fcfa", "event_date")


def build_fait_transaction_mm(mm: DataFrame, dim_client: DataFrame) -> DataFrame:
    return rattacher_client(mm, dim_client).select(
        "txn_id", _date_id(), "heure", "client_id", "type_transaction",
        "montant_fcfa", "frais_fcfa", "statut", "event_date")


def build_gold(spark: SparkSession, cfg: dict) -> None:
    debut, fin = periode(cfg)
    silver = lambda t: spark.read.parquet(chemin(cfg, "silver", t))

    dim_client = build_dim_client(silver("clients")).cache()
    dim_cellule = build_dim_cellule(silver("cellules")).cache()
    tables = {
        "dim_date": build_dim_date(spark, debut, fin),
        "dim_client": dim_client,
        "dim_cellule": dim_cellule,
        "fait_trafic": build_fait_trafic(silver("cdr"), dim_client, dim_cellule),
        "fait_recharge": build_fait_recharge(silver("recharges"), dim_client),
        "fait_transaction_mm": build_fait_transaction_mm(silver("mobile_money"), dim_client),
    }
    for nom, df in tables.items():
        writer = df.write.mode("overwrite")
        if nom.startswith("fait_"):
            writer = writer.partitionBy("event_date")
        writer.parquet(chemin(cfg, "gold", nom))

        ecrit = spark.read.parquet(chemin(cfg, "gold", nom))
        if nom.startswith("fait_"):
            stats = ecrit.agg(F.count("*").alias("n"), F.sum((F.col("client_id") == INCONNU).cast("int")).alias("inc")).first()
            log.info("Gold %-20s %12s lignes (%s rattachées à « Inconnu »)", nom, f"{stats['n']:,}", f"{stats['inc']:,}")
        else:
            log.info("Gold %-20s %12s lignes", nom, f"{ecrit.count():,}")
