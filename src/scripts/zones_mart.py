import sys

import pyspark.sql.functions as F
from pyspark.sql.window import Window

from utils import get_spark, read_events, read_messages, read_cities, add_city, last_message

EVENT_TYPES = ["message", "reaction", "subscription", "user"]


def get_all_events(events, messages):
    # сообщения уже с городом
    msg = messages.select("user_id", "date", F.lit("message").alias("event_type"), "zone_id")

    # регистрация - первое сообщение пользователя
    w = Window.partitionBy("user_id").orderBy("event_time")
    reg = messages \
        .withColumn("rn", F.row_number().over(w)) \
        .where("rn = 1") \
        .select("user_id", "date", F.lit("user").alias("event_type"), "zone_id")

    user_zone = last_message(messages).select("user_id", "zone_id")

    reactions = events \
        .where(F.col("event_type") == "reaction") \
        .select(F.col("event.reaction_from").cast("long").alias("user_id"), "date", "event_type")

    subscriptions = events \
        .where(F.col("event_type") == "subscription") \
        .select(F.col("event.user").cast("long").alias("user_id"), "date", "event_type")

    others = reactions.union(subscriptions).join(user_zone, "user_id", "inner")

    return msg.unionByName(reg).unionByName(others) \
        .withColumn("month", F.trunc("date", "month")) \
        .withColumn("week", F.trunc("date", "week"))


def count_events(all_events, group_cols, prefix):
    result = all_events.groupBy(group_cols).pivot("event_type", EVENT_TYPES).count()
    for t in EVENT_TYPES:
        result = result.withColumnRenamed(t, f"{prefix}_{t}")
    return result


def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    output_path = sys.argv[5]

    spark = get_spark(f"zones_mart_{date}")

    events = read_events(spark, events_path, date, depth)
    cities = read_cities(spark, geo_path)
    messages = add_city(read_messages(events), cities).cache()

    all_events = get_all_events(events, messages)

    week = count_events(all_events, ["month", "week", "zone_id"], "week")
    month = count_events(all_events, ["month", "zone_id"], "month")

    result = week.join(month, ["month", "zone_id"], "left") \
        .select(
            "month", "week", "zone_id",
            "week_message", "week_reaction", "week_subscription", "week_user",
            "month_message", "month_reaction", "month_subscription", "month_user",
        ) \
        .fillna(0)

    result.write.mode("overwrite").parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()
