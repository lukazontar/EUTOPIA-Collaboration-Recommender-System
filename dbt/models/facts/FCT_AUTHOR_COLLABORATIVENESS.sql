WITH REF_AUTHOR_COLLABORATIVENESS AS (SELECT *
                                      FROM {{ source('ANALYTICS', 'AUTHOR_COLLABORATIVENESS') }})
SELECT AUTHOR_SID,
       PUBLICATIONS                        AS PUBLICATION_COUNT,
       EXTERNAL_COLLABORATIONS             AS EXTERNAL_COLLABORATION_COUNT,
       COLLABORATION_RATE,
       COLLABORATION_RATE_PERCENTILE,
       IS_AUTHOR_MORE_COLLABORATIVE,
       DATALAKE.UDF_MD5_HASH([AUTHOR_SID]) AS PK_AUTHOR_COLLABORATIVENESS
FROM REF_AUTHOR_COLLABORATIVENESS