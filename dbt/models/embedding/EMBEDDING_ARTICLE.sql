WITH REF_STG_CROSSREF_ARTICLE AS (SELECT * FROM {{ ref('STG_CROSSREF_ARTICLE') }})
SELECT DISTINCT ARTICLE_DOI
              , CONCAT('query:',
                       '\nTitle:', IFNULL(ARTICLE_TITLE, '/'),
                       '\nShort Title:', IFNULL(ARTICLE_SHORT_TITLE, '/'),
                       '\nSubtitle:', IFNULL(ARTICLE_SUBTITLE, '/'),
                       '\nAbstract:', IFNULL(ARTICLE_ABSTRACT, '/')
                ) AS EMBEDDING_INPUT
FROM REF_STG_CROSSREF_ARTICLE