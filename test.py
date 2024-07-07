from google.cloud import bigquery

client = bigquery.Client()
query = """
select count(1) from `collaboration-recommender`.DBT_DEV.DIM_INSTITUTION
"""
df = client.query(query).result().to_dataframe()

print("The query data:" + str(df))
