CREATE OR REPLACE VIEW `collaboration-recommender`.DATALAKE.V_ARTICLE AS
SELECT DISTINCT ARTICLE_SID
              , ARTICLE_DOI
FROM `collaboration-recommender`.DATALAKE.CROSSREF_HISTORIC_ARTICLE_PROCESSED
WHERE IS_EUTOPIA_AFFILIATED_INSTITUTION;