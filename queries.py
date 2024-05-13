hist_set_query = """
SELECT
DISTINCT skw.normalised_skw,
  metrics.result_ad_lvl1_nm as l1_category,
  metrics.result_ad_lvl2_nm as l2_category,
  SAFE_DIVIDE(SUM(skw.clicks),SUM(imp)) AS CTR
FROM
  `ek-ignite-p-g0t8.ignite_scratchbook.as_normalised_query_historical_data_RELPER_397` skw
JOIN
  `liberty-prod-rjuh.search_relevancy.fct_search_relevancy_performance__metrics` metrics
ON
  skw.normalised_skw = LOWER(metrics.search_keyword)
WHERE
  metrics.result_ad_lvl1_nm = 'Multimedia_Elektronik'
AND metrics.result_ad_lvl2_nm in ('PCs', 'TV_Video', 'Tablets_reader', 'Notebooks', 'Konsolen')
AND ARRAY_LENGTH(SPLIT(skw.normalised_skw, ' ')) = 2
AND SAFE_DIVIDE((clicks),(imp)) > 0.00084749
AND metrics.created_date_local BETWEEN '{}' AND '{}'
GROUP BY
      1,
      2,
      3
"""

curr_set_query = """
SELECT
DISTINCT skw.normalised_skw,
  metrics.result_ad_lvl1_nm as l1_category,
  metrics.result_ad_lvl2_nm as l2_category,
  SAFE_DIVIDE(SUM(skw.clicks),SUM(imp)) AS CTR
FROM
  `ek-ignite-p-g0t8.ignite_scratchbook.as_normalised_query_historical_data_RELPER_397` skw
JOIN
  `liberty-prod-rjuh.search_relevancy.fct_search_relevancy_performance__metrics` metrics
ON
  skw.normalised_skw = LOWER(metrics.search_keyword)
WHERE
  metrics.result_ad_lvl1_nm = 'Multimedia_Elektronik'
AND metrics.result_ad_lvl2_nm in ('PCs', 'TV_Video', 'Tablets_reader', 'Notebooks', 'Konsolen')
AND ARRAY_LENGTH(SPLIT(skw.normalised_skw, ' ')) = 2
AND SAFE_DIVIDE((clicks),(imp)) > 0.00084749
AND metrics.created_date_local BETWEEN '{}' AND '{}'
GROUP BY
      1,
      2,
      3
"""