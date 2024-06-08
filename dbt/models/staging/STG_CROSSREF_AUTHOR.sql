WITH REF_STG_CROSSREF_DOI_METADATA AS (SELECT *
                                       FROM {{ source('DATALAKE', 'CROSSREF_HISTORIC_ARTICLE_PROCESSED') }})
SELECT AUTHOR_SID,
       AUTHOR_FULL_NAME,
       -- Extract the actual ORCID ID from the ORCID field (e.g. input: "http://orcid.org/0000-0001-5000-8578")
       CASE
           WHEN AUTHOR_ORCID_ID IS NOT NULL THEN
               REGEXP_EXTRACT(JSON_EXTRACT_SCALAR(AUTHOR_ORCID_ID), '([0-9]{4}-[0-9]{4}-[0-9]{4}-[0-9]{4})')
           ELSE
               NULL
           END AS AUTHOR_ORCID_ID
FROM REF_STG_CROSSREF_DOI_METADATA
