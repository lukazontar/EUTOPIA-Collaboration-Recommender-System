WITH ONETIME_ORCID_ARTICLE_JSON
         AS (SELECT json_extract_scalar(JSON, '$.record:record.common:orcid-identifier.common:path') AS ORCID_ID,
                    UNIVERSITY_NAME,
                    IFNULL(WORK_SUMMARY, JSON_EXTRACT(WORK, '$.work:work-summary'))                  AS WORK_SUMMARY_JSON,
                    IFNULL(EXTERNAL_ID,
                           JSON_EXTRACT(WORK, '$.common:external-ids.common:external-id'))           AS EXTERNAL_ID_JSON
             FROM {{ source('ORCID', 'ONETIME_ORCID_MEMBER') }} 
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(JSON,
                                                                       '$.record:record.activities:activities-summary.activities:works.activities:group'))) WORK
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(work, '$.common:external-ids.common:external-id'))) EXTERNAL_ID
                      LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(JSON_EXTRACT(work, '$.work:work-summary'))) WORK_SUMMARY)
        ,
     ONETIME_ORCID_SUMMARY_BY_ARTICLE
         AS (SELECT ORCID_ID                                                                                               AS ORCID_ID,
                    UNIVERSITY_NAME,
                    CAST(CAST(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.common:last-modified-date') AS TIMESTAMP) AS DATE) AS LAST_MODIFIED_DT,
                    JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON,
                                        '$.common:external-id-value')                                                      AS DOI,
                    IFNULL(JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.work:title.translated-title'),
                           JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON, '$.work:title.common:title'))                            AS TITLE,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.common:publication-date.common:year')                                           AS PUBLICATION_YEAR,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.common:publication-date.common:month')                                          AS PUBLICATION_MONTH,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.common:publication-date.common:day')                                            AS PUBLICATION_DAY,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.work:type')                                                                     AS TYPE,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.common:url')                                                                    AS URL,
                    JSON_EXTRACT_SCALAR(WORK_SUMMARY_JSON,
                                        '$.work:journal-title')                                                            AS JOURNAL_TITLE
             FROM ONETIME_ORCID_ARTICLE_JSON
             WHERE JSON_EXTRACT_SCALAR(EXTERNAL_ID_JSON, '$.common:external-id-type') = 'doi')
 SELECT *
 FROM ONETIME_ORCID_SUMMARY_BY_ARTICLE