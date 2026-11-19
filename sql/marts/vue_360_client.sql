-- Vue 360 : une ligne par abonné, toutes activités confondues sur la période.
-- Sert au ciblage marketing et à la détection précoce du churn.
WITH trafic AS (
    SELECT client_id,
           COUNT(DISTINCT date_id)                                                  AS jours_actifs,
           ROUND(SUM(CASE WHEN type_evenement = 'VOIX' THEN duree_sec ELSE 0 END) / 60, 1) AS minutes_voix,
           SUM(nb_sms)                                                              AS nb_sms,
           ROUND(SUM(COALESCE(volume_mo, 0)) / 1024, 3)                             AS data_go,
           ROUND(SUM(montant_fcfa), 0)                                              AS revenu_trafic_fcfa,
           MAX(event_date)                                                          AS dernier_trafic
    FROM fait_trafic
    WHERE client_id <> -1
    GROUP BY client_id
),
cellule_principale AS (
    -- Cellule la plus utilisée par chaque abonné (fonction de fenêtrage)
    SELECT client_id, cell_id
    FROM (
        SELECT client_id, cell_id,
               ROW_NUMBER() OVER (PARTITION BY client_id ORDER BY COUNT(*) DESC, cell_id) AS rang
        FROM fait_trafic
        WHERE client_id <> -1
        GROUP BY client_id, cell_id
    )
    WHERE rang = 1
),
recharges AS (
    SELECT client_id,
           COUNT(*)          AS nb_recharges,
           SUM(montant_fcfa) AS montant_recharges_fcfa,
           MAX(event_date)   AS derniere_recharge
    FROM fait_recharge
    WHERE client_id <> -1
    GROUP BY client_id
),
mobile_money AS (
    SELECT client_id,
           COUNT(*)          AS nb_transactions_mm,
           SUM(montant_fcfa) AS volume_mm_fcfa,
           SUM(frais_fcfa)   AS frais_mm_fcfa,
           MAX(event_date)   AS derniere_transaction_mm
    FROM fait_transaction_mm
    WHERE client_id <> -1 AND statut = 'SUCCES'
    GROUP BY client_id
),
profil AS (
    SELECT d.client_id, d.msisdn, d.segment, d.offre, d.region, d.tranche_age,
           DATEDIFF(CAST(:date_fin AS DATE), d.date_activation)       AS anciennete_jours,
           COALESCE(t.jours_actifs, 0)                                AS jours_actifs,
           COALESCE(t.minutes_voix, 0)                                AS minutes_voix,
           COALESCE(t.nb_sms, 0)                                      AS nb_sms,
           COALESCE(t.data_go, 0)                                     AS data_go,
           COALESCE(r.nb_recharges, 0)                                AS nb_recharges,
           COALESCE(r.montant_recharges_fcfa, 0)                      AS montant_recharges_fcfa,
           COALESCE(m.nb_transactions_mm, 0)                          AS nb_transactions_mm,
           COALESCE(m.volume_mm_fcfa, 0)                              AS volume_mm_fcfa,
           COALESCE(t.revenu_trafic_fcfa, 0) + COALESCE(m.frais_mm_fcfa, 0) AS revenu_total_fcfa,
           cp.cell_id                                                 AS cellule_principale,
           GREATEST(t.dernier_trafic, r.derniere_recharge, m.derniere_transaction_mm) AS derniere_activite
    FROM dim_client d
    LEFT JOIN trafic t              ON t.client_id = d.client_id
    LEFT JOIN cellule_principale cp ON cp.client_id = d.client_id
    LEFT JOIN recharges r           ON r.client_id = d.client_id
    LEFT JOIN mobile_money m        ON m.client_id = d.client_id
    WHERE d.client_id <> -1
)
SELECT *,
       DATEDIFF(CAST(:date_fin AS DATE), derniere_activite) AS jours_inactivite,
       CASE
           WHEN derniere_activite IS NULL                                     THEN 'INACTIF'
           WHEN DATEDIFF(CAST(:date_fin AS DATE), derniere_activite) > 7       THEN 'A_RISQUE'
           WHEN jours_actifs < 5                                              THEN 'FAIBLE_USAGE'
           ELSE 'ACTIF'
       END AS statut_engagement,
       NTILE(10) OVER (ORDER BY revenu_total_fcfa DESC) AS decile_valeur
FROM profil
