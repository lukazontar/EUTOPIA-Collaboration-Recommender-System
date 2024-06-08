CREATE OR REPLACE TABLE `collaboration-recommender`.DATALAKE.CROSSREF_HISTORIC_ARTICLE_PROCESSED
(
    ARTICLE_DOI                       STRING,
    AUTHOR_FULL_NAME                  STRING,
    AUTHOR_ORCID_ID                   STRING,
    ORIGINAL_AFFILIATION_NAME         STRING,
    ARTICLE_URL                       STRING,
    ARTICLE_FUNDER                    STRING,
    ARTICLE_INSTITUTION               STRING,
    ARTICLE_PUBLISHER                 STRING,
    ARTICLE_TITLE                     STRING,
    ARTICLE_SHORT_TITLE               STRING,
    ARTICLE_SUBTITLE                  STRING,
    ARTICLE_ORIGINAL_TITLE            STRING,
    ARTICLE_CONTAINER_TITLE           STRING,
    ARTICLE_SHORT_CONTAINER_TITLE     STRING,
    ARTICLE_ABSTRACT                  STRING,
    ARTICLE_REFERENCE                 STRING,
    ARTICLE_EST_PUBLISH_DT            DATE,
    INDEXED_DT                        DATE,
    AUTHOR_SID                        STRING,
    ARTICLE_SID                       STRING,
    INSTITUTION_SID                   STRING,
    IS_EUTOPIA_AFFILIATED_INSTITUTION BOOLEAN
);