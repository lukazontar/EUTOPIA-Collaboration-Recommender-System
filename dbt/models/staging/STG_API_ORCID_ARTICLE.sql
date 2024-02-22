WITH API_ORCID_ARTICLE_JSON
         AS (SELECT ORCID_ID                                                              AS ORCID_ID,
                    UNIVERSITY_NAME,
                    WORK                                                                  AS WORK_JSON,
                    IFNULL(WORK_SUMMARY, JSON_EXTRACT(WORK, '$.work-summary'))            AS WORK_SUMMARY_JSON,
                    IFNULL(EXTERNAL_ID, JSON_EXTRACT(WORK, '$.external-ids.external-id')) AS EXTERNAL_ID_JSON
             FROM {{ source ('ORCID', 'ORCID_MEMBER' )}}
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(JSON, '$.activities-summary.works.group'))) WORK
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(WORK, '$.external-ids.external-id'))) EXTERNAL_ID
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(WORK, '$.work-summary'))) WORK_SUMMARY)
        ,
     API_ORCID_SUMMARY_BY_ARTICLE
         AS (SELECT ORCID_ID
                  , UNIVERSITY_NAME
                  , CAST(TIMESTAMP_MILLIS(CAST(JSON_EXTRACT_SCALAR(WORK_JSON, '$.last-modified-date.value') AS INT64)) AS DATE) AS LAST_MODIFIED_DT
                  , JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.external-id-value')                                                AS DOI
                  , IFNULL(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.title.translated-title.value'),
                           JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.title.title.value'))                                       AS TITLE
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.publication-date.year.value')                                                        AS PUBLICATION_YEAR
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.publication-date.month.value')                                                       AS PUBLICATION_MONTH
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.publication-date.day.value')                                      AS PUBLICATION_DAY
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.type')                                                            AS TYPE
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.url.value')                                                       AS URL
                  , JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.journal-title.value')                                             AS JOURNAL_TITLE
             FROM API_ORCID_ARTICLE_JSON
             WHERE JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.external-id-type') = 'doi')
SELECT *
FROM API_ORCID_SUMMARY_BY_ARTICLE