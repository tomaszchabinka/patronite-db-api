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
    name = "name"
    monthly_revenue = "monthly_revenue"
    number_of_patrons = "number_of_patrons"
    total_revenue = "total_revenue"
    tags = "tags"

class OrderType(StrEnum):
    desc = "desc"
    asc = "asc"

class Filter(object):
    id: str
    value: list


def query_top_authors_from_influxdb(
        criteria: RankingType = RankingType.monthly_revenue, 
        offset: int = 0,
        limit: int = 10, 
        order: OrderType = OrderType.desc,
        tags: str = None,
        min_total_revenue: int = None,
        max_total_revenue: int = None,
        min_monthly_revenue: int = None,
        max_monthly_revenue: int = None,
        min_number_of_patrons: int = None,
        max_number_of_patrons: int = None,
        ):
    queryString = f"SELECT t2.* FROM (SELECT url, MAX(time) as recent_time FROM creators WHERE"
    if tags:
        queryString += f" (find_in_set(split_part(tags, ',', 1), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 2), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 3), '{tags}') = 1) AND"
    if min_total_revenue:
        queryString += f" total_revenue >= {min_total_revenue} AND"
    if max_total_revenue:
        queryString += f" total_revenue <= {max_total_revenue} AND"
    if min_monthly_revenue:
        queryString += f" monthly_revenue >= {min_monthly_revenue} AND"
    if max_monthly_revenue:
        queryString += f" monthly_revenue <= {max_monthly_revenue} AND"
    if min_number_of_patrons:
        queryString += f" number_of_patrons >= {min_number_of_patrons} AND"
    if max_number_of_patrons:
        queryString += f" number_of_patrons <= {max_number_of_patrons} AND"
    queryString += f" time >= now() - interval '7 days' GROUP BY url) t1 JOIN creators t2 ON t1.url = t2.url AND t1.recent_time = t2.time ORDER BY t2.{criteria} {order} OFFSET {offset} LIMIT {limit};"
    query = f"{queryString}"
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


def query_authors(tags: str = None):
    if tags:
        query = f"SELECT url, last_value(name ORDER BY time) AS name FROM creators WHERE (find_in_set(split_part(tags, ',', 1), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 2), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 3), '{tags}') = 1) AND time >= now() - interval '7 days' GROUP BY url ;"
    else: 
        query = f"SELECT url, last_value(name ORDER BY time) AS name FROM creators WHERE time >= now() - interval '7 days' GROUP BY url ORDER BY name;"

    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()

def query_row_count(
        tags: str = None,
        min_total_revenue: int = None,
        max_total_revenue: int = None,
        min_monthly_revenue: int = None,
        max_monthly_revenue: int = None,
        min_number_of_patrons: int = None,
        max_number_of_patrons: int = None,
        ):
    queryString = f"SELECT last_value(name ORDER BY time) AS name FROM creators WHERE"
    if tags:
        queryString += f" (find_in_set(split_part(tags, ',', 1), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 2), '{tags}') = 1 OR find_in_set(split_part(tags, ',', 3), '{tags}') = 1) AND"
    if min_total_revenue:
        queryString += f" total_revenue >= {min_total_revenue} AND"
    if max_total_revenue:
        queryString += f" total_revenue <= {max_total_revenue} AND"
    if min_monthly_revenue:
        queryString += f" monthly_revenue >= {min_monthly_revenue} AND"
    if max_monthly_revenue:
        queryString += f" monthly_revenue <= {max_monthly_revenue} AND"
    if min_number_of_patrons:
        queryString += f" number_of_patrons >= {min_number_of_patrons} AND"
    if max_number_of_patrons:
        queryString += f" number_of_patrons <= {max_number_of_patrons} AND"
    queryString += f" time >= now() - interval '7 days' GROUP BY name ORDER BY name;"
    query = f"{queryString}"
    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()

def query_min_max():
    query = f"""
    SELECT 'total_revenue' as name, MAX(total_revenue) as max FROM (SELECT last_value(total_revenue ORDER BY time) AS total_revenue FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
    UNION
    SELECT 'number_of_patrons' as name, MAX(number_of_patrons) as max FROM (SELECT last_value(number_of_patrons ORDER BY time) AS number_of_patrons FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
    UNION
    SELECT 'monthly_revenue' as name, MAX(monthly_revenue) as max FROM (SELECT last_value(monthly_revenue ORDER BY time) AS monthly_revenue FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
    """
    reader = client.query(query=query, mode="reader", language="sql")
    table = reader.read_all()
    return table.to_pandas()

@app.get("/top_authors")
def get_top_authors(
    criteria: RankingType = RankingType.monthly_revenue,
    offset: int = 0,
    limit: int = 10,
    order: OrderType = OrderType.desc,
    tags: str | None = None,
    min_total_revenue: int | None = None,
    max_total_revenue: int | None = None,
    min_monthly_revenue: int = None,
    max_monthly_revenue: int = None,
    min_number_of_patrons: int = None,
    max_number_of_patrons: int = None,
    ):
    df = query_top_authors_from_influxdb(
        criteria,
        offset,
        limit,
        order,
        tags,
        min_total_revenue,
        max_total_revenue,
        min_monthly_revenue,
        max_monthly_revenue,
        min_number_of_patrons,
        max_number_of_patrons,
    )
    return Response(df.to_json(orient="records"), media_type="application/json")


@app.get("/metadata/tags")
def get_tags():
    df = query_tags()
    return list(df['tag'].values)


@app.get("/metadata/authors")
def get_authors(tags: str | None = None):
    df = query_authors(tags)
    return Response(df.to_json(orient="records"), media_type="application/json")

@app.get("/metadata/row_count")
def get_row_count(
        tags: str = None,
        min_total_revenue: int = None,
        max_total_revenue: int = None,
        min_monthly_revenue: int = None,
        max_monthly_revenue: int = None,
        min_number_of_patrons: int = None,
        max_number_of_patrons: int = None,
        ):
    df = query_row_count(
        tags,
        min_total_revenue,
        max_total_revenue,
        min_monthly_revenue,
        max_monthly_revenue,
        min_number_of_patrons,
        max_number_of_patrons,
        )
    return len(list(df["name"].values))

@app.get("/metadata/min_max")
def get_min_max():
    df = query_min_max()
    return Response(df.to_json(orient="records"), media_type="application/json")

# The magic that allows the integration with AWS Lambda
handler = Mangum(
    app, 
    lifespan="off", 
    api_gateway_base_path=root_path
)


if __name__ == "__main__":
    uvicorn.run(app, port=8000)


# SELECT 'total_revenue' as name, MAX(total_revenue) as max FROM (SELECT last_value(total_revenue ORDER BY time) AS total_revenue FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
# UNION
# SELECT 'number_of_patrons' as name, MAX(number_of_patrons) as max FROM (SELECT last_value(number_of_patrons ORDER BY time) AS number_of_patrons FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
# UNION
# SELECT 'monthly_revenue' as name, MAX(monthly_revenue) as max FROM (SELECT last_value(monthly_revenue ORDER BY time) AS monthly_revenue FROM creators WHERE time >= now() - interval '7 days' GROUP BY name)
# UNION