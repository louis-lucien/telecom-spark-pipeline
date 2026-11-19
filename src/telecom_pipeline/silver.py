"""Couche Silver : normalisation, dédoublonnage, validation et contrôle qualité.

Chaque ligne invalide est conservée dans silver/_rejets avec son motif : rien
n'est supprimé sans trace. Un rapport de qualité est produit à chaque exécution
et le pipeline s'arrête si le taux de rejet dépasse le seuil configuré.
"""
import json
import logging
from datetime import date
from pathlib import Path

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from .config import chemin, periode

log = logging.getLogger(__name__)

MSISDN_VALIDE = r"^7[05678][0-9]{7}$"
TYPES_EVENEMENT = ["VOIX", "SMS", "DATA"]
STATUTS_MM = ["SUCCES", "ECHEC"]


class QualityGateError(RuntimeError):
    """Levée quand la qualité des données ne permet pas d'alimenter le reporting."""


def normaliser_msisdn(col: Column) -> Column:
    """+221 77 123 45 67, 00221771234567, 77 123 45 67 -> 771234567."""
    chiffres = F.regexp_replace(col, r"[^0-9]", "")
    return F.regexp_replace(chiffres, r"^(00221|221)", "")


def separer_valides(df: DataFrame, regles: list[tuple[str, Column]]) -> tuple[DataFrame, DataFrame]:
    """Applique les règles dans l'ordre ; la première violée donne le motif de rejet."""
    motif = None
    for nom, invalide in regles:
        cond = F.coalesce(invalide, F.lit(False))
        motif = F.when(cond, F.lit(nom)) if motif is None else motif.when(cond, F.lit(nom))
    marque = df.withColumn("motif_rejet", motif)
    return (marque.filter(F.col("motif_rejet").isNull()).drop("motif_rejet"),
            marque.filter(F.col("motif_rejet").isNotNull()))


def _msisdn_invalide() -> Column:
    return F.col("msisdn").isNull() | ~F.col("msisdn").rlike(MSISDN_VALIDE)


def _avec_horodatage(df: DataFrame) -> DataFrame:
    ts = F.to_timestamp("date_heure", "yyyy-MM-dd HH:mm:ss")
    return (df.withColumn("ts", ts)
              .withColumn("event_date", F.to_date("ts"))
              .withColumn("heure", F.hour("ts")))


def _hors_periode(debut: date, fin: date) -> Column:
    ts = F.col("ts")
    return ts.isNull() | (ts < F.lit(debut).cast("timestamp")) | (ts >= F.lit(fin).cast("timestamp"))


def nettoyer_clients(df: DataFrame, debut: date, fin: date) -> tuple[DataFrame, DataFrame]:
    df = (df.dropDuplicates(["client_id"])
            .withColumn("msisdn", normaliser_msisdn(F.col("msisdn")))
            .withColumn("date_activation", F.to_date("date_activation")))
    return separer_valides(df, [
        ("msisdn_invalide", _msisdn_invalide()),
        ("client_id_absent", F.col("client_id").isNull()),
    ])


def nettoyer_cellules(df: DataFrame, debut: date, fin: date) -> tuple[DataFrame, DataFrame]:
    return separer_valides(df.dropDuplicates(["cell_id"]), [
        ("cell_id_absent", F.col("cell_id").isNull()),
    ])


def nettoyer_cdr(df: DataFrame, debut: date, fin: date) -> tuple[DataFrame, DataFrame]:
    df = _avec_horodatage(df.dropDuplicates(["cdr_id"])
                            .withColumn("msisdn", normaliser_msisdn(F.col("msisdn"))))
    valides, rejets = separer_valides(df, [
        ("msisdn_invalide", _msisdn_invalide()),
        ("date_hors_periode", _hors_periode(debut, fin)),
        ("type_inconnu", ~F.col("type_evenement").isin(TYPES_EVENEMENT)),
        ("duree_negative", F.col("duree_sec") < 0),
        ("volume_negatif", F.col("volume_mo") < 0),
    ])
    return valides.drop("date_heure"), rejets


def nettoyer_recharges(df: DataFrame, debut: date, fin: date) -> tuple[DataFrame, DataFrame]:
    df = _avec_horodatage(df.dropDuplicates(["recharge_id"])
                            .withColumn("msisdn", normaliser_msisdn(F.col("msisdn"))))
    valides, rejets = separer_valides(df, [
        ("msisdn_invalide", _msisdn_invalide()),
        ("date_hors_periode", _hors_periode(debut, fin)),
        ("montant_invalide", F.col("montant_fcfa").isNull() | (F.col("montant_fcfa") <= 0)),
    ])
    return valides.drop("date_heure"), rejets


def nettoyer_mobile_money(df: DataFrame, debut: date, fin: date) -> tuple[DataFrame, DataFrame]:
    df = _avec_horodatage(df.dropDuplicates(["txn_id"])
                            .withColumn("msisdn", normaliser_msisdn(F.col("msisdn"))))
    valides, rejets = separer_valides(df, [
        ("msisdn_invalide", _msisdn_invalide()),
        ("date_hors_periode", _hors_periode(debut, fin)),
        ("montant_invalide", F.col("montant_fcfa").isNull() | (F.col("montant_fcfa") <= 0)),
        ("statut_inconnu", ~F.col("statut").isin(STATUTS_MM)),
    ])
    return valides.drop("date_heure"), rejets


NETTOYAGES = {
    "clients": nettoyer_clients,
    "cellules": nettoyer_cellules,
    "cdr": nettoyer_cdr,
    "recharges": nettoyer_recharges,
    "mobile_money": nettoyer_mobile_money,
}


def build_silver(spark: SparkSession, cfg: dict) -> dict:
    debut, fin = periode(cfg)
    rapport = {}
    for table, nettoyer in NETTOYAGES.items():
        brut = spark.read.parquet(chemin(cfg, "bronze", table)).drop("_source_file", "_ingested_at")
        valides, rejets = nettoyer(brut, debut, fin)

        writer = valides.write.mode("overwrite")
        if "event_date" in valides.columns:
            writer = writer.partitionBy("event_date")  # élagage des partitions par jour en aval
        writer.parquet(chemin(cfg, "silver", table))
        rejets.write.mode("overwrite").parquet(chemin(cfg, "silver", "_rejets", table))

        n_bronze = brut.count()
        n_silver = spark.read.parquet(chemin(cfg, "silver", table)).count()
        motifs = {r["motif_rejet"]: r["count"] for r in
                  spark.read.parquet(chemin(cfg, "silver", "_rejets", table)).groupBy("motif_rejet").count().collect()}
        n_rejets = sum(motifs.values())
        rapport[table] = {
            "lignes_bronze": n_bronze,
            "doublons_supprimes": n_bronze - n_silver - n_rejets,
            "rejets": dict(sorted(motifs.items())),
            "lignes_silver": n_silver,
            "taux_rejet": round((n_bronze - n_silver) / n_bronze, 4) if n_bronze else 0.0,
        }
        log.info("Silver %-13s %12s lignes valides (%.2f %% écartées)",
                 table, f"{n_silver:,}", 100 * rapport[table]["taux_rejet"])

    ecrire_rapport(rapport, Path(cfg["paths"]["reports"]))
    controle_qualite(rapport, cfg["qualite"]["taux_rejet_max"])
    return rapport


def controle_qualite(rapport: dict, seuil: float) -> None:
    hors_seuil = {t: r["taux_rejet"] for t, r in rapport.items() if r["taux_rejet"] > seuil}
    if hors_seuil:
        raise QualityGateError(f"Taux de rejet au-delà de {seuil:.0%} : {hors_seuil}")


def ecrire_rapport(rapport: dict, dossier: Path) -> None:
    dossier.mkdir(parents=True, exist_ok=True)
    (dossier / "qualite_silver.json").write_text(json.dumps(rapport, indent=2, ensure_ascii=False), encoding="utf-8")
    lignes = ["# Rapport qualité — couche Silver", "",
              "| Table | Lignes Bronze | Doublons | Rejets (motifs) | Lignes Silver | Taux écarté |",
              "| --- | ---: | ---: | --- | ---: | ---: |"]
    for table, r in rapport.items():
        motifs = ", ".join(f"{m} : {n:,}" for m, n in r["rejets"].items()) or "—"
        lignes.append(f"| {table} | {r['lignes_bronze']:,} | {r['doublons_supprimes']:,} | {motifs} "
                      f"| {r['lignes_silver']:,} | {r['taux_rejet']:.2%} |")
    (dossier / "qualite_silver.md").write_text("\n".join(lignes) + "\n", encoding="utf-8")
