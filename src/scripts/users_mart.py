import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from utils import get_spark, read_events, read_messages, read_cities, add_city, last_message

# сколько дней надо прожить в городе, чтобы он считался домашним
HOME_DAYS = 27


def get_act_city(messages):
    # актуальный город - город последнего сообщения
    return last_message(messages).select(
        "user_id",
        F.col("city").alias("act_city"),
        F.from_utc_timestamp(F.col("event_time"), F.col("timezone")).alias("local_time"),
    )


def get_visits(messages):
    w = Window.partitionBy("user_id").orderBy("event_time", "row_id")
    return messages \
        .withColumn("prev_city", F.lag("city").over(w)) \
        .withColumn("new_visit", F.when(F.col("prev_city").isNull() | (F.col("prev_city") != F.col("city")), 1).otherwise(0)) \
        .withColumn("visit_num", F.sum("new_visit").over(w)) \
        .groupBy("user_id", "visit_num", "city") \
        .agg(F.min("date").alias("start_date"), F.max("date").alias("end_date"))


def get_home_city(visits):
    w = Window.partitionBy("user_id").orderBy(F.col("start_date").desc())
    return visits \
        .where(F.datediff("end_date", "start_date") >= HOME_DAYS) \
        .withColumn("rn", F.row_number().over(w)) \
        .where("rn = 1") \
        .select("user_id", F.col("city").alias("home_city"))


def get_travels(visits):
    w = Window.partitionBy("user_id").orderBy("visit_num").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    return visits \
        .withColumn("travel_array", F.collect_list("city").over(w)) \
        .groupBy("user_id") \
        .agg(F.first("travel_array").alias("travel_array")) \
        .withColumn("travel_count", F.size("travel_array"))


def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    output_path = sys.argv[5]

    spark = get_spark(f"users_mart_{date}")

    events = read_events(spark, events_path, date, depth)
    cities = read_cities(spark, geo_path)
    messages = add_city(read_messages(events), cities).cache()

    visits = get_visits(messages).cache()

    result = get_act_city(messages) \
        .join(get_home_city(visits), "user_id", "left") \
        .join(get_travels(visits), "user_id", "left") \
        .select("user_id", "act_city", "home_city", "travel_count", "travel_array", "local_time")

    result.write.mode("overwrite").parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()
