CREATE OR REPLACE VIEW `collaboration-recommender`.DATALAKE.V_REFERENCE_ARTICLE AS
SELECT JSON_EXTRACT_SCALAR(REFERENCE, '$.DOI') AS REFERENCE_ARTICLE_DOI
FROM `collaboration-recommender`.DATALAKE.CROSSREF_HISTORIC_ARTICLE_PROCESSED A
         INNER JOIN `collaboration-recommender`.DATALAKE.V_ARTICLE E
                    ON A.ARTICLE_SID = E.ARTICLE_SID
         LEFT JOIN UNNEST(JSON_EXTRACT_ARRAY(PARSE_JSON(A.ARTICLE_REFERENCE), '$')) REFERENCE
                   ON TRUE
WHERE JSON_EXTRACT_SCALAR(REFERENCE, '$.DOI') IS NOT NULL
GROUP BY REFERENCE_ARTICLE_DOI;