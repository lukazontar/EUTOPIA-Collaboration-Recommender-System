WITH MERGED_ORCID_SUMMARY_BY_ARTICLE
         AS (SELECT *
             FROM {{ ref('STG_API_ORCID_ARTICLE') }}
             UNION ALL
             SELECT *
             FROM {{ ref('STG_ONETIME_ORCID_ARTICLE') }})
SELECT DISTINCT ORCID_ID
              , UNIVERSITY_NAME
              , DOI
              , MAX(LAST_MODIFIED_DT) OVER (PARTITION BY DOI ORDER BY IF(LAST_MODIFIED_DT IS NULL, 0, 1) DESC) AS LAST_MODIFIED_DT
              , FIRST_VALUE(TITLE) OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC) AS TITLE
              , FIRST_VALUE(PUBLICATION_YEAR)
                            OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)        AS PUBLICATION_YEAR
              , FIRST_VALUE(PUBLICATION_MONTH)
                            OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)        AS PUBLICATION_MONTH
              , FIRST_VALUE(PUBLICATION_DAY)
                            OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)        AS PUBLICATION_DAY
              , FIRST_VALUE(TYPE) OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)  AS TYPE
              , FIRST_VALUE(URL) OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)   AS URL
              , FIRST_VALUE(JOURNAL_TITLE)
                            OVER (PARTITION BY DOI ORDER BY IF(TITLE IS NULL, 0, 1) DESC)        AS JOURNAL_TITLE
FROM MERGED_ORCID_SUMMARY_BY_ARTICLE
