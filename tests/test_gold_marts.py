from datetime import date, datetime
from pathlib import Path

from telecom_pipeline.gold import (INCONNU, build_dim_cellule, build_dim_client, build_dim_date,
                                   build_fait_recharge, build_fait_trafic, build_fait_transaction_mm)
from telecom_pipeline.marts import SQL_DIR, executer_sql

DEBUT, FIN = date(2026, 9, 1), date(2026, 10, 1)


def _gold(spark):
    clients = spark.createDataFrame(
        [(1, "771111111", "Prépayé", "Prépayé Standard", "Dakar", "25-34", date(2024, 1, 1)),
         (2, "782222222", "Postpayé", "Postpayé Forfait", "Thiès", "35-44", date(2023, 6, 1))],
        "client_id long, msisdn string, segment string, offre string, region string, "
        "tranche_age string, date_activation date")
    cellules = spark.createDataFrame([("CELL_0001", "Dakar", "4G")], "cell_id string, region string, technologie string")
    evt = "msisdn string, ts timestamp, event_date date, heure int"
    cdr = spark.createDataFrame(
        [(10, "771111111", datetime(2026, 9, 29, 8), date(2026, 9, 29), 8, "VOIX", "ONNET", 120, None, "CELL_0001", 100.0),
         (11, "782222222", datetime(2026, 9, 3, 9), date(2026, 9, 3), 9, "SMS", "ONNET", None, None, "CELL_0001", 20.0),
         (12, "709999999", datetime(2026, 9, 4, 9), date(2026, 9, 4), 9, "DATA", None, None, 10.0, "CELL_9999", 50.0)],
        f"cdr_id long, {evt}, type_evenement string, destination string, duree_sec int, "
        "volume_mo double, cell_id string, montant_fcfa double")
    recharges = spark.createDataFrame(
        [(20, "771111111", datetime(2026, 9, 28, 7), date(2026, 9, 28), 7, 1000, "USSD")],
        f"recharge_id long, {evt}, montant_fcfa long, canal string")
    mm = spark.createDataFrame(
        [("MM1", "771111111", datetime(2026, 9, 30, 12), date(2026, 9, 30), 12, "TRANSFERT", 5000, 50, "SUCCES")],
        f"txn_id string, {evt}, type_transaction string, montant_fcfa long, frais_fcfa long, statut string")

    dim_client, dim_cellule = build_dim_client(clients), build_dim_cellule(cellules)
    tables = {
        "dim_date": build_dim_date(spark, DEBUT, FIN),
        "dim_client": dim_client,
        "dim_cellule": dim_cellule,
        "fait_trafic": build_fait_trafic(cdr, dim_client, dim_cellule),
        "fait_recharge": build_fait_recharge(recharges, dim_client),
        "fait_transaction_mm": build_fait_transaction_mm(mm, dim_client),
    }
    for nom, df in tables.items():
        df.createOrReplaceTempView(nom)
    return tables


def test_dim_date_couvre_la_periode(spark):
    dim = build_dim_date(spark, DEBUT, FIN)
    assert dim.count() == 30
    assert dim.filter("date_id = 20260905").first()["jour_semaine"] == "Samedi"


def test_msisdn_et_cellule_inconnus_rattaches_au_membre_inconnu(spark):
    fait = _gold(spark)["fait_trafic"]
    ligne = fait.filter("type_evenement = 'DATA'").first()
    assert ligne["client_id"] == INCONNU
    assert ligne["cell_id"] == "INCONNUE"
    assert fait.count() == 3  # aucun événement perdu


def test_vue_360_detecte_le_risque_de_churn(spark):
    _gold(spark)
    vue = executer_sql(spark, SQL_DIR / "vue_360_client.sql", {"date_fin": "2026-09-30"})
    lignes = {r["client_id"]: r for r in vue.collect()}

    assert set(lignes) == {1, 2}
    assert lignes[1]["minutes_voix"] == 2.0
    assert lignes[1]["revenu_total_fcfa"] == 150.0  # trafic 100 + frais Mobile Money 50
    assert lignes[1]["jours_inactivite"] == 0
    assert lignes[2]["jours_inactivite"] == 27
    assert lignes[2]["statut_engagement"] == "A_RISQUE"


def test_tous_les_marts_s_executent(spark):
    _gold(spark)
    for fichier in sorted(Path(SQL_DIR).glob("*.sql")):
        assert executer_sql(spark, fichier, {"date_fin": "2026-09-30"}).count() > 0, fichier.name
