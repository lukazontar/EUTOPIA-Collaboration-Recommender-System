WITH REF_STG_CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES AS (SELECT *
                                                     FROM {{ source('DATALAKE', 'CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES') }})
SELECT CERIF_RESEARCH_TOPIC_CODE
     , JSON_EXTRACT(JSON, '$.DOI')      AS ARTICLE_DOI
     , JSON_EXTRACT(JSON, '$.title')    AS ARTICLE_TITLE
     , JSON_EXTRACT(JSON, '$.abstract') AS ARTICLE_ABSTRACT
FROM REF_STG_CERIF_RESEARCH_TOPIC_TOP_N_ARTICLES