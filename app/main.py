from enum import StrEnum
from fastapi import FastAPI, Response
import uvicorn
import os
from influxdb_client_3 import InfluxDBClient3
from mangum import Mangum
from fastapi.middleware.cors import CORSMiddleware


root_path = os.getenv('ENV', default='dev')

app = FastAPI(root_path=f'/{root_path}')

app.add_middleware(
    CORSMiddleware,
    allow_origins=['*'],
    allow_methods=["*"],
    allow_headers=["*"],
)

client = InfluxDBClient3(token="H3UlwR2chi7ZcxeEvFlwsqVEgkoUNdq7eSTIyhs6utc-4yJxygSHAvaETKupFcILfs6wOFeywcSHF0g-WF1-4A==",
                         host="https://eu-central-1-1.aws.cloud2.influxdata.com",
                         org="patronitedb",
                         database="snapshot/autogen")


class RankingType(StrEnum):
    monthly_revenue = "monthly_revenue"
    number_of_patrons = "number_of_patrons"
    total_revenue = "total_revenue"


def query_top_authors_from_influxdb(criteria: RankingType = RankingType.monthly_revenue, tag: str = None, offset: int = 0,limit: int = 10):
    if tag:
        query = f"SELECT t2.* FROM (SELECT url, MAX(time) as recent_time FROM creators WHERE tags LIKE '%{tag}%' AND time >= now() - interval '7 days' GROUP BY url) t1 JOIN creators t2 ON t1.url = t2.url AND t1.recent_time = t2.time ORDER BY t2.{criteria} DESC OFFSET {offset} LIMIT {limit};"
    else: 
        query = f"SELECT t2.* FROM (SELECT url, MAX(time) as recent_time FROM creators WHERE time >= now() - interval '7 days' GROUP BY url) t1 JOIN creators t2 ON t1.url = t2.url AND t1.recent_time = t2.time ORDER BY t2.{criteria} DESC OFFSET {offset} LIMIT {limit};"

    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()

def query_tags():
    query = """
    SELECT DISTINCT * FROM (SELECT DISTINCT split_part(tags, ',', 1) AS tag FROM creators
    UNION
    SELECT DISTINCT split_part(tags, ',', 2) AS tag FROM creators
    UNION
    SELECT DISTINCT split_part(tags, ',', 3) AS tag FROM creators) WHERE length(tag) > 0ORDER by tag 
    """
    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()


def query_authors(tag: str = None):
    if tag:
        query = f"SELECT url, last_value(name ORDER BY time) AS name FROM creators WHERE tags LIKE '%{tag}%' AND time >= now() - interval '7 days' GROUP BY url ;"
    else: 
        query = f"SELECT url, last_value(name ORDER BY time) AS name FROM creators WHERE time >= now() - interval '7 days' GROUP BY url ORDER BY name;"

    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()



@app.get("/top_authors")
def get_top_authors(criteria: RankingType = RankingType.monthly_revenue, tag: str | None = None, offset: int = 0, limit: int = 10):
    df = query_top_authors_from_influxdb(criteria, tag, offset, limit)
    return Response(df.to_json(orient="records"), media_type="application/json")


@app.get("/metadata/tags")
def get_tags():
    df = query_tags()
    return list(df['tag'].values)


@app.get("/metadata/authors")
def get_authors(tag: str | None = None):
    df = query_authors(tag)
    return Response(df.to_json(orient="records"), media_type="application/json")

# The magic that allows the integration with AWS Lambda
handler = Mangum(
    app, 
    lifespan="off", 
    api_gateway_base_path=root_path
)


if __name__ == "__main__":
    uvicorn.run(app, port=8000)