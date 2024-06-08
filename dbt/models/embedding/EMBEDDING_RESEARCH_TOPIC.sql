WITH REF_STG_CERIF_RESEARCH_TOPIC AS (SELECT * FROM {{ ref('STG_CERIF_RESEARCH_TOPIC') }})
SELECT DISTINCT RESEARCH_TOPIC_CODE,
                CONCAT('query:',
                       '\nResearch Branch:', IFNULL(RESEARCH_BRANCH_NAME, '/'),
                       '\nResearch Subbranch:', IFNULL(RESEARCH_SUBBRANCH_NAME, '/'),
                       '\nResearch Topic:', IFNULL(RESEARCH_TOPIC_NAME, '/')
                ) AS EMBEDDING_INPUT
FROM REF_STG_CERIF_RESEARCH_TOPIC