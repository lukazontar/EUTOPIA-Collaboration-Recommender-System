WITH REF_CROSSREF_HISTORIC_ARTICLE_PROCESSED AS (SELECT *
                                                 FROM {{ source('DATALAKE', 'CROSSREF_HISTORIC_ARTICLE_PROCESSED') }}
                                                 WHERE IS_EUTOPIA_AFFILIATED_INSTITUTION)
SELECT A.ARTICLE_DOI,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.DOI')             AS REFERENCE_ARTICLE_DOI,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.ISSN')            AS REFERENCE_ARTICLE_ISSN,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.issue')           AS REFERENCE_ARTICLE_ISSUE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.key')             AS REFERENCE_ARTICLE_KEY,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.series-title')    AS REFERENCE_ARTICLE_SERIES_TITLE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.isbn-type')       AS REFERENCE_ARTICLE_ISBN_TYPE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.doi-asserted-by') AS REFERENCE_ARTICLE_DOI_ASSERTED_BY,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.first-page')      AS REFERENCE_ARTICLE_FIRST_PAGE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.ISBN')            AS REFERENCE_ARTICLE_ISBN,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.component')       AS REFERENCE_ARTICLE_COMPONENT,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.article-title')   AS REFERENCE_ARTICLE_ARTICLE_TITLE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.volume-title')    AS REFERENCE_ARTICLE_VOLUME_TITLE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.volume')          AS REFERENCE_ARTICLE_VOLUME,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.author')          AS REFERENCE_ARTICLE_AUTHOR,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.year')            AS REFERENCE_ARTICLE_YEAR,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.unstructured')    AS REFERENCE_ARTICLE_UNSTRUCTURED,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.edition')         AS REFERENCE_ARTICLE_EDITION,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.journal-title')   AS REFERENCE_ARTICLE_JOURNAL_TITLE,
       JSON_EXTRACT_SCALAR(REFERENCE, '$.issn-type')       AS REFERENCE_ARTICLE_ISSN_TYPE
FROM REF_CROSSREF_HISTORIC_ARTICLE_PROCESSED A
         LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(A.ARTICLE_REFERENCE), '$')) REFERENCE
                   ON TRUE