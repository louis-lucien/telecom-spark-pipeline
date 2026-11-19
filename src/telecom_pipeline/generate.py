"""Génération de données télécoms synthétiques, directement avec Spark.

Les lignes sont produites en parallèle à partir de spark.range : le volume se
règle dans la config, de quelques milliers à plusieurs dizaines de millions.
Des défauts de qualité sont injectés volontairement (doublons, MSISDN mal
formatés, dates corrompues, valeurs négatives) pour que la couche Silver ait
un vrai travail de nettoyage. Tarifs et répartitions sont fictifs.
"""
import logging
from datetime import datetime, timezone

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import chemin

log = logging.getLogger(__name__)

REGIONS = ["Dakar", "Thiès", "Diourbel", "Saint-Louis", "Louga", "Kaolack", "Fatick",
           "Kaffrine", "Ziguinchor", "Kolda", "Sédhiou", "Tambacounda", "Kédougou", "Matam"]
POIDS_REGIONS = [0.30, 0.13, 0.11, 0.07, 0.06, 0.07, 0.05, 0.04, 0.04, 0.04, 0.03, 0.04, 0.01, 0.01]
OFFRES = ["Prépayé Standard", "Prépayé Pass Data", "Postpayé Forfait", "Entreprise Flotte"]


def _choix(r: Column, valeurs: list, poids: list) -> Column:
    """Tire une valeur selon des poids à partir d'une colonne uniforme [0, 1)."""
    cumul, expr = 0.0, None
    for valeur, p in zip(valeurs, poids):
        cumul += p
        cond = r < cumul
        expr = F.when(cond, F.lit(valeur)) if expr is None else expr.when(cond, F.lit(valeur))
    return expr.otherwise(F.lit(valeurs[-1]))


def _aleas(df: DataFrame, seed: int, n: int) -> DataFrame:
    """Ajoute n colonnes aléatoires _r0.._r{n-1} matérialisées une seule fois par ligne."""
    for i in range(n):
        df = df.withColumn(f"_r{i}", F.rand(seed + i))
    return df


def _sans_aleas(df: DataFrame) -> DataFrame:
    return df.drop(*[c for c in df.columns if c.startswith("_r")])


def _msisdn(idx: Column) -> Column:
    """MSISDN à 9 chiffres (77/78 + 7 chiffres), déterministe depuis l'index client."""
    return F.concat(F.when(idx % 2 == 0, F.lit("77")).otherwise(F.lit("78")),
                    F.lpad((idx + 1_000_000).cast("string"), 7, "0"))


def _salir_msisdn(m: Column, r: Column) -> Column:
    """Formats courants dans les exports : +221, 00221, numéro espacé."""
    espace = F.concat_ws(" ", F.substring(m, 1, 2), F.substring(m, 3, 3),
                         F.substring(m, 6, 2), F.substring(m, 8, 2))
    return (F.when(r < 0.05, F.concat(F.lit("+221"), m))
            .when(r < 0.08, F.concat(F.lit("00221"), m))
            .when(r < 0.10, espace)
            .otherwise(m))


def _horodatage(r: Column, debut_epoch: int, jours: int, churner: Column) -> Column:
    """Instant aléatoire dans la période ; un churner n'est actif que sur les 60 premiers %."""
    secondes = r * jours * 86_400 * F.when(churner, 0.6).otherwise(1.0)
    ts = F.timestamp_seconds(F.lit(debut_epoch) + secondes.cast("long"))
    return F.date_format(ts, "yyyy-MM-dd HH:mm:ss")


def _index_client(r: Column, n_clients: int) -> Column:
    # r² concentre l'activité sur une minorité de gros consommateurs (distribution réaliste)
    return F.floor(F.pow(r, 2) * n_clients).cast("long")


def gen_clients(spark: SparkSession, g: dict, debut: str) -> DataFrame:
    df = _aleas(spark.range(g["clients"]).withColumnRenamed("id", "client_id"), g["seed"], 5)
    offre = _choix(F.col("_r0"), OFFRES, [0.55, 0.30, 0.10, 0.05])
    df = df.select(
        "client_id",
        _salir_msisdn(_msisdn(F.col("client_id")), F.col("_r4")).alias("msisdn"),
        F.when(offre.startswith("Prépayé"), "Prépayé")
         .when(offre.startswith("Postpayé"), "Postpayé").otherwise("Entreprise").alias("segment"),
        offre.alias("offre"),
        _choix(F.col("_r1"), REGIONS, POIDS_REGIONS).alias("region"),
        _choix(F.col("_r2"), ["18-24", "25-34", "35-44", "45-54", "55+"],
               [0.22, 0.31, 0.22, 0.14, 0.11]).alias("tranche_age"),
        F.date_format(F.date_sub(F.lit(debut).cast("date"), (F.col("_r3") * 2500).cast("int")),
                      "yyyy-MM-dd").alias("date_activation"),
    )
    return df.unionByName(df.sample(0.01, g["seed"]))  # 1 % de doublons (export CRM rejoué)


def gen_cellules(spark: SparkSession, g: dict) -> DataFrame:
    df = _aleas(spark.range(1, g["cellules"] + 1), g["seed"] + 100, 2)
    return df.select(
        F.concat(F.lit("CELL_"), F.lpad(F.col("id").cast("string"), 4, "0")).alias("cell_id"),
        _choix(F.col("_r0"), REGIONS, POIDS_REGIONS).alias("region"),
        _choix(F.col("_r1"), ["2G", "3G", "4G", "5G"], [0.15, 0.25, 0.55, 0.05]).alias("technologie"),
    )


def gen_cdr(spark: SparkSession, g: dict, debut_epoch: int, jours: int) -> DataFrame:
    df = _aleas(spark.range(g["cdr"]).withColumnRenamed("id", "cdr_id"), g["seed"] + 200, 8)
    idx = _index_client(F.col("_r0"), g["clients"])
    df = (
        df.withColumn("_idx", idx)
        .withColumn("type_evenement", _choix(F.col("_r1"), ["VOIX", "SMS", "DATA"], [0.45, 0.25, 0.30]))
        .withColumn("destination", F.when(
            F.col("type_evenement") != "DATA",
            _choix(F.col("_r3"), ["ONNET", "OFFNET", "INTERNATIONAL"], [0.60, 0.32, 0.08])))
        .withColumn("duree_sec", F.when(F.col("type_evenement") == "VOIX",
                                        (-F.log(1 - F.col("_r4")) * 95 + 1).cast("int")))
        .withColumn("volume_mo", F.when(F.col("type_evenement") == "DATA",
                                        F.round(-F.log(1 - F.col("_r4")) * 40, 2)))
    )
    tarif_min = (F.when(F.col("destination") == "ONNET", 50)
                 .when(F.col("destination") == "OFFNET", 75).otherwise(200))
    tarif_sms = F.when(F.col("destination") == "INTERNATIONAL", 50).otherwise(20)
    montant = (F.when(F.col("type_evenement") == "VOIX", F.col("duree_sec") / 60 * tarif_min)
               .when(F.col("type_evenement") == "SMS", tarif_sms)
               .otherwise(F.col("volume_mo") * 5))
    churner = (F.col("_idx") % 100) < g["churn_pct"]
    df = df.select(
        "cdr_id",
        # 0,3 % de MSISDN absents, 10 % de formats hétérogènes
        F.when(F.col("_r6") < 0.003, F.lit(None))
         .otherwise(_salir_msisdn(_msisdn(F.col("_idx")), F.col("_r7"))).alias("msisdn"),
        # 0,1 % de dates corrompues (horodatage remis à zéro par l'équipement)
        F.when(F.col("_r6").between(0.003, 0.004), F.lit("1970-01-01 00:00:00"))
         .otherwise(_horodatage(F.col("_r2"), debut_epoch, jours, churner)).alias("date_heure"),
        "type_evenement",
        "destination",
        # 0,2 % de durées négatives (erreur de médiation)
        F.when(F.col("_r6").between(0.004, 0.006), -F.col("duree_sec"))
         .otherwise(F.col("duree_sec")).alias("duree_sec"),
        "volume_mo",
        F.concat(F.lit("CELL_"), F.lpad((F.floor(F.col("_r5") * g["cellules"]) + 1).cast("string"), 4, "0"))
         .alias("cell_id"),
        F.round(montant, 2).alias("montant_fcfa"),
    )
    return df.unionByName(df.sample(0.005, g["seed"]))  # 0,5 % de CDR dupliqués


def gen_recharges(spark: SparkSession, g: dict, debut_epoch: int, jours: int) -> DataFrame:
    df = _aleas(spark.range(g["recharges"]).withColumnRenamed("id", "recharge_id"), g["seed"] + 300, 6)
    idx = _index_client(F.col("_r0"), g["clients"])
    churner = (idx % 100) < g["churn_pct"]
    montant = _choix(F.col("_r2"), [100, 200, 500, 1000, 2000, 5000], [0.15, 0.20, 0.30, 0.20, 0.10, 0.05])
    return df.select(
        "recharge_id",
        _salir_msisdn(_msisdn(idx), F.col("_r4")).alias("msisdn"),
        _horodatage(F.col("_r1"), debut_epoch, jours, churner).alias("date_heure"),
        F.when(F.col("_r5") < 0.002, F.lit(0)).otherwise(montant).cast("long").alias("montant_fcfa"),
        _choix(F.col("_r3"), ["USSD", "Distributeur", "Mobile Money", "Carte"],
               [0.35, 0.40, 0.20, 0.05]).alias("canal"),
    )


def gen_mobile_money(spark: SparkSession, g: dict, debut_epoch: int, jours: int) -> DataFrame:
    df = _aleas(spark.range(g["mobile_money"]), g["seed"] + 400, 6)
    idx = _index_client(F.col("_r0"), g["clients"])
    churner = (idx % 100) < g["churn_pct"]
    type_txn = _choix(F.col("_r2"), ["DEPOT", "RETRAIT", "TRANSFERT", "PAIEMENT_MARCHAND", "ACHAT_CREDIT"],
                      [0.25, 0.22, 0.28, 0.15, 0.10])
    # Montants log-normaux (médiane ~4 000 FCFA), bornés entre 100 et 2 000 000 FCFA
    df = df.withColumn("_rmontant", F.least(
        F.greatest(F.round(F.exp(F.randn(g["seed"] + 410) * 0.9 + 8.3), -1), F.lit(100)),
        F.lit(2_000_000)).cast("long"))
    montant = F.col("_rmontant")
    df = df.select(
        F.concat(F.lit("MM"), F.lpad(F.col("id").cast("string"), 10, "0")).alias("txn_id"),
        _salir_msisdn(_msisdn(idx), F.col("_r4")).alias("msisdn"),
        _horodatage(F.col("_r1"), debut_epoch, jours, churner).alias("date_heure"),
        type_txn.alias("type_transaction"),
        F.when(F.col("_r5") < 0.001, -montant).otherwise(montant).alias("montant_fcfa"),
        F.col("_r3").alias("_statut"),
    )
    df = df.withColumn("frais_fcfa", F.when(F.col("type_transaction").isin("TRANSFERT", "RETRAIT"),
                                            F.round(F.abs(F.col("montant_fcfa")) * 0.01)).otherwise(0).cast("long"))
    df = df.withColumn("statut", F.when(F.col("_statut") < 0.97, "SUCCES").otherwise("ECHEC")).drop("_statut")
    return df.unionByName(df.sample(0.005, g["seed"]))  # 0,5 % de transactions rejouées


def generate(spark: SparkSession, cfg: dict) -> None:
    g, debut, jours = cfg["generation"], cfg["periode"]["debut"], cfg["periode"]["jours"]
    if g["clients"] >= 9_000_000:
        raise ValueError("Le schéma de MSISDN supporte au plus 9 millions de clients")
    debut_epoch = int(datetime.fromisoformat(debut).replace(tzinfo=timezone.utc).timestamp())

    sorties = {
        "crm/clients": (gen_clients(spark, g, debut), "csv"),
        "reseau/cellules": (gen_cellules(spark, g), "csv"),
        "cdr": (gen_cdr(spark, g, debut_epoch, jours), "csv"),
        "recharges": (gen_recharges(spark, g, debut_epoch, jours), "csv"),
        "mobile_money": (gen_mobile_money(spark, g, debut_epoch, jours), "json"),
    }
    for nom, (df, fmt) in sorties.items():
        writer = _sans_aleas(df).write.mode("overwrite")
        if fmt == "csv":
            writer.option("header", True).csv(chemin(cfg, "raw", nom))
        else:
            writer.json(chemin(cfg, "raw", nom))
        log.info("Source générée : %s (%s)", nom, fmt)
