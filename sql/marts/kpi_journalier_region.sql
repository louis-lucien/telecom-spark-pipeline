-- KPI quotidiens par région : trafic, revenu, recharges et Mobile Money.
-- Le trafic est rattaché à la région de la cellule (lieu d'usage) ;
-- recharges et Mobile Money à la région de résidence du client.
WITH trafic AS (
    SELECT f.date_id,
           c.region,
           COUNT(DISTINCT f.client_id)                                          AS clients_actifs,
           ROUND(SUM(CASE WHEN f.type_evenement = 'VOIX' THEN f.duree_sec END) / 60, 1) AS minutes_voix,
           SUM(f.nb_sms)                                                        AS nb_sms,
           ROUND(SUM(f.volume_mo) / 1024, 2)                                    AS data_go,
           ROUND(SUM(f.montant_fcfa), 0)                                        AS revenu_trafic_fcfa
    FROM fait_trafic f
    JOIN dim_cellule c ON c.cell_id = f.cell_id
    GROUP BY f.date_id, c.region
),
recharges AS (
    SELECT r.date_id, d.region,
           COUNT(*)            AS nb_recharges,
           SUM(r.montant_fcfa) AS montant_recharges_fcfa
    FROM fait_recharge r
    JOIN dim_client d ON d.client_id = r.client_id
    GROUP BY r.date_id, d.region
),
mobile_money AS (
    SELECT m.date_id, d.region,
           COUNT(*)                                              AS nb_transactions_mm,
           SUM(m.montant_fcfa)                                   AS volume_mm_fcfa,
           SUM(m.frais_fcfa)                                     AS frais_mm_fcfa,
           ROUND(AVG(CASE WHEN m.statut = 'ECHEC' THEN 1.0 ELSE 0 END), 4) AS taux_echec_mm
    FROM fait_transaction_mm m
    JOIN dim_client d ON d.client_id = m.client_id
    GROUP BY m.date_id, d.region
)
SELECT dt.date,
       dt.jour_semaine,
       dt.est_weekend,
       t.region,
       t.clients_actifs,
       t.minutes_voix,
       t.nb_sms,
       t.data_go,
       t.revenu_trafic_fcfa,
       COALESCE(r.nb_recharges, 0)            AS nb_recharges,
       COALESCE(r.montant_recharges_fcfa, 0)  AS montant_recharges_fcfa,
       COALESCE(m.nb_transactions_mm, 0)      AS nb_transactions_mm,
       COALESCE(m.volume_mm_fcfa, 0)          AS volume_mm_fcfa,
       COALESCE(m.frais_mm_fcfa, 0)           AS frais_mm_fcfa,
       m.taux_echec_mm
FROM trafic t
JOIN dim_date dt          ON dt.date_id = t.date_id
LEFT JOIN recharges r     ON r.date_id = t.date_id AND r.region = t.region
LEFT JOIN mobile_money m  ON m.date_id = t.date_id AND m.region = t.region
ORDER BY dt.date, t.region
