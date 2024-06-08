CREATE OR REPLACE VIEW DATALAKE.V_AUTHOR AS
SELECT DISTINCT A.AUTHOR_SID
              , A.AUTHOR_FULL_NAME
              , JSON_EXTRACT_SCALAR(A.AUTHOR_ORCID_ID) AS AUTHOR_ORCID_ID
FROM `collaboration-recommender`.DATALAKE.CROSSREF_HISTORIC_ARTICLE_PROCESSED A
         INNER JOIN `collaboration-recommender`.DATALAKE.V_ARTICLE E
                    ON A.ARTICLE_SID = E.ARTICLE_SID;
