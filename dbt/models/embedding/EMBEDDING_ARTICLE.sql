WITH REF_STG_CROSSREF_ARTICLE AS (SELECT * FROM {{ ref('STG_CROSSREF_ARTICLE') }})
SELECT DISTINCT ARTICLE_DOI
              , CONCAT('query:',
                       '\nTitle:', IFNULL(ARTICLE_TITLE, '/'),
                       '\nShort Title:', IFNULL(ARTICLE_SHORT_TITLE, '/'),
                       '\nSubtitle:', IFNULL(ARTICLE_SUBTITLE, '/'),
                       '\nAbstract:', IFNULL(ARTICLE_ABSTRACT, '/')
                ) AS EMBEDDING_INPUT

              , CONCAT(CASE WHEN ARTICLE_TITLE IS NOT NULL THEN CONCAT(ARTICLE_TITLE, '\n') ELSE '' END,
                       CASE
                           WHEN ARTICLE_SHORT_TITLE IS NOT NULL THEN CONCAT(ARTICLE_SHORT_TITLE, '\n')
                           ELSE '' END,
                       CASE
                           WHEN ARTICLE_SUBTITLE IS NOT NULL THEN CONCAT(ARTICLE_SUBTITLE, '\n')
                           ELSE '' END,
                       CASE
                           WHEN ARTICLE_ABSTRACT IS NOT NULL THEN CONCAT(ARTICLE_ABSTRACT, '\n')
                           ELSE '' END
                ) AS LANGUAGE_INPUT
FROM REF_STG_CROSSREF_ARTICLE