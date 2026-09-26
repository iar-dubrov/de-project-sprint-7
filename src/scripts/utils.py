import datetime

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.window import Window

# таймзоны для городов из geo.csv (в самом файле их нет, а spark понимает только названия вида Australia/....)
CITY_TZ = {
    "Sydney": "Australia/Sydney",
    "Newcastle": "Australia/Sydney",
    "Wollongong": "Australia/Sydney",
    "Maitland": "Australia/Sydney",
    "Canberra": "Australia/Canberra",
    "Melbourne": "Australia/Melbourne",
    "Cranbourne": "Australia/Melbourne",
    "Geelong": "Australia/Melbourne",
    "Ballarat": "Australia/Melbourne",
    "Bendigo": "Australia/Melbourne",
    "Brisbane": "Australia/Brisbane",
    "Gold Coast": "Australia/Brisbane",
    "Ipswich": "Australia/Brisbane",
    "Toowoomba": "Australia/Brisbane",
    "Townsville": "Australia/Brisbane",
    "Cairns": "Australia/Brisbane",
    "Mackay": "Australia/Brisbane",
    "Rockhampton": "Australia/Brisbane",
    "Perth": "Australia/Perth",
    "Bunbury": "Australia/Perth",
    "Adelaide": "Australia/Adelaide",
    "Hobart": "Australia/Hobart",
    "Launceston": "Australia/Hobart",
    "Darwin": "Australia/Darwin",
}


def get_spark(app_name):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.session.timeZone", "UTC") \
        .getOrCreate()


def read_events(spark, events_path, date, depth):
    events = spark.read.parquet(events_path).where(F.col("date") <= date)
    if int(depth) > 0:
        start = datetime.datetime.strptime(date, "%Y-%m-%d") - datetime.timedelta(days=int(depth) - 1)
        events = events.where(F.col("date") >= start.strftime("%Y-%m-%d"))
    return events


def event_time():
    # у каналов время в datetime, у личных сообщений в message_ts
    # в message_ts бывает не тот год, поэтому дату берем из партиции, а из поля только время
    time_str = F.coalesce(F.col("event.datetime"), F.col("event.message_ts"))
    return F.to_timestamp(F.concat_ws(" ", F.col("date").cast("string"), F.substring(time_str, 12, 8)))


def read_messages(events):
    return events \
        .where((F.col("event_type") == "message") & F.col("lat").isNotNull()) \
        .select(
            F.col("event.message_from").alias("user_id"),
            F.col("event.message_id").alias("message_id"),
            F.col("date"),
            event_time().alias("event_time"),
            F.col("lat"),
            F.col("lon"),
        ) \
        .withColumn("row_id", F.monotonically_increasing_id())  # message_id бывает не уникальный


def read_cities(spark, geo_path):
    cities = spark.read.csv(geo_path, sep=";", header=True) \
        .select(
            F.col("id").cast("int").alias("zone_id"),
            F.col("city"),
            F.regexp_replace("lat", ",", ".").cast("double").alias("city_lat"),
            F.regexp_replace("lng", ",", ".").cast("double").alias("city_lon"),
        )
    tz_map = F.create_map(*[F.lit(x) for pair in CITY_TZ.items() for x in pair])
    return cities.withColumn("timezone", F.coalesce(tz_map[F.col("city")], F.lit("Australia/Sydney")))


def distance(lat1, lon1, lat2, lon2):
    lat1 = F.radians(lat1)
    lat2 = F.radians(lat2)
    lon1 = F.radians(lon1)
    lon2 = F.radians(lon2)
    a = F.pow(F.sin((lat2 - lat1) / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin((lon2 - lon1) / 2), 2)
    return 2 * 6371 * F.asin(F.sqrt(a))


def add_city(messages, cities):
    w = Window.partitionBy("row_id").orderBy("dist")
    return messages \
        .crossJoin(F.broadcast(cities)) \
        .withColumn("dist", distance(F.col("lat"), F.col("lon"), F.col("city_lat"), F.col("city_lon"))) \
        .withColumn("rn", F.row_number().over(w)) \
        .where("rn = 1") \
        .drop("rn", "dist", "city_lat", "city_lon")


def last_message(messages):
    w = Window.partitionBy("user_id").orderBy(F.col("event_time").desc())
    return messages \
        .withColumn("rn", F.row_number().over(w)) \
        .where("rn = 1") \
        .drop("rn")
