from datetime import date

import pytest
from pyspark.sql import functions as F

from telecom_pipeline.bronze import SOURCES
from telecom_pipeline.silver import QualityGateError, controle_qualite, nettoyer_cdr, normaliser_msisdn

DEBUT, FIN = date(2026, 9, 1), date(2026, 10, 1)


def cdr(spark, lignes):
    return spark.createDataFrame(lignes, SOURCES["cdr"][2])


@pytest.mark.parametrize("brut", ["+221771234567", "00221771234567", "77 123 45 67", "771234567"])
def test_normaliser_msisdn(spark, brut):
    df = spark.createDataFrame([(brut,)], ["msisdn"])
    assert df.select(normaliser_msisdn(F.col("msisdn"))).first()[0] == "771234567"


def test_nettoyer_cdr_dedoublonne_et_rejette(spark):
    df = cdr(spark, [
        (1, "+221771234567", "2026-09-10 08:00:00", "VOIX", "ONNET", 60, None, "CELL_0001", 50.0),
        (1, "+221771234567", "2026-09-10 08:00:00", "VOIX", "ONNET", 60, None, "CELL_0001", 50.0),  # doublon
        (2, None, "2026-09-10 09:00:00", "SMS", "ONNET", None, None, "CELL_0001", 20.0),
        (3, "781234567", "1970-01-01 00:00:00", "DATA", None, None, 12.5, "CELL_0002", 62.5),
        (4, "781234567", "2026-09-11 10:00:00", "VOIX", "OFFNET", -30, None, "CELL_0002", 37.5),
        (5, "781234567", "2026-09-12 23:59:59", "DATA", None, None, 3.0, "CELL_0003", 15.0),
    ])
    valides, rejets = nettoyer_cdr(df, DEBUT, FIN)

    assert sorted(r["cdr_id"] for r in valides.collect()) == [1, 5]
    assert valides.filter("cdr_id = 1").first()["msisdn"] == "771234567"
    assert valides.filter("cdr_id = 5").first()["heure"] == 23
    motifs = {r["cdr_id"]: r["motif_rejet"] for r in rejets.collect()}
    assert motifs == {2: "msisdn_invalide", 3: "date_hors_periode", 4: "duree_negative"}


def test_quality_gate_bloque_au_dela_du_seuil():
    controle_qualite({"cdr": {"taux_rejet": 0.01}}, seuil=0.05)
    with pytest.raises(QualityGateError):
        controle_qualite({"cdr": {"taux_rejet": 0.12}}, seuil=0.05)
