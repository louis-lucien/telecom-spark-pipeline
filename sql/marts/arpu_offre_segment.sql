-- ARPU (revenu moyen par abonné actif) par offre et segment, sur la période.
WITH revenu_client AS (
    SELECT client_id, SUM(montant_fcfa) AS revenu_fcfa
    FROM fait_trafic
    WHERE client_id <> -1
    GROUP BY client_id
)
SELECT d.segment,
       d.offre,
       COUNT(*)                              AS abonnes_actifs,
       ROUND(SUM(rc.revenu_fcfa), 0)         AS revenu_total_fcfa,
       ROUND(AVG(rc.revenu_fcfa), 0)         AS arpu_fcfa,
       ROUND(PERCENTILE_APPROX(rc.revenu_fcfa, 0.5), 0) AS revenu_median_fcfa,
       ROUND(100 * SUM(rc.revenu_fcfa) / SUM(SUM(rc.revenu_fcfa)) OVER (), 2) AS part_revenu_pct
FROM revenu_client rc
JOIN dim_client d ON d.client_id = rc.client_id
GROUP BY d.segment, d.offre
ORDER BY revenu_total_fcfa DESC
