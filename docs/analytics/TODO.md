1. Lots of author name variations in the data. Need to standardize them.

```sql
SELECT A.AUTHOR_SID,
       A.AUTHOR_FULL_NAME,
       A.AUTHOR_ORCID_ID,
       COUNT(DISTINCT F.ARTICLE_SID)
FROM DBT_DEV.DIM_AUTHOR A
         INNER JOIN DBT_DEV.FCT_COLLABORATION F
                    ON A.AUTHOR_SID = F.AUTHOR_SID
WHERE A.AUTHOR_SID IN (
                       'd4a041e54a2bd63a1e766f346449740a',
                       '0517f1ec4ceacc10a83f7d1897643225',
                       'd38541d77a35bc8771399976b35c9ca4',
                       'a0a8ffafd9158668b0192698833f4053',
                       'c58d69427ed1fed58ff550a3c11786ac'
    )
GROUP BY A.AUTHOR_SID,
         A.AUTHOR_FULL_NAME,
         A.AUTHOR_ORCID_ID
```