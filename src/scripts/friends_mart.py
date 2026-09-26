import sys

import pyspark.sql.functions as F

from utils import get_spark, read_events, read_messages, read_cities, add_city, last_message, distance


def get_user_positions(messages):
    # где пользователь был в последний раз
    return last_message(messages).select("user_id", "lat", "lon", "zone_id", "timezone")


def get_subscriptions(events):
    return events \
        .where(F.col("event_type") == "subscription") \
        .select(
            F.col("event.user").cast("long").alias("user_id"),
            F.col("event.subscription_channel").alias("channel_id"),
        ) \
        .distinct()


def get_contacts(events):
    # все, кто хоть раз писал друг другу
    return events \
        .where((F.col("event_type") == "message") & F.col("event.message_to").isNotNull()) \
        .select(
            F.least("event.message_from", "event.message_to").alias("user_left"),
            F.greatest("event.message_from", "event.message_to").alias("user_right"),
        ) \
        .distinct()


def main():
    date = sys.argv[1]
    depth = sys.argv[2]
    events_path = sys.argv[3]
    geo_path = sys.argv[4]
    output_path = sys.argv[5]

    spark = get_spark(f"friends_mart_{date}")

    events = read_events(spark, events_path, date, depth)
    cities = read_cities(spark, geo_path)
    messages = add_city(read_messages(events), cities)

    positions = get_user_positions(messages).cache()

    subs = get_subscriptions(events).join(positions, "user_id", "inner").cache()

    left = subs.select(
        "channel_id", "zone_id",
        F.col("user_id").alias("user_left"), F.col("lat").alias("lat_left"),
        F.col("lon").alias("lon_left"), "timezone",
    )
    right = subs.select(
        "channel_id", "zone_id",
        F.col("user_id").alias("user_right"), F.col("lat").alias("lat_right"),
        F.col("lon").alias("lon_right"),
    )

    pairs = left.join(right, ["channel_id", "zone_id"]) \
        .where(F.col("user_left") < F.col("user_right")) \
        .where(distance(F.col("lat_left"), F.col("lon_left"), F.col("lat_right"), F.col("lon_right")) <= 1) \
        .select("user_left", "user_right", "zone_id", "timezone") \
        .dropDuplicates(["user_left", "user_right"])

    result = pairs.join(get_contacts(events), ["user_left", "user_right"], "left_anti") \
        .withColumn("processed_dttm", F.current_timestamp()) \
        .withColumn("local_time", F.from_utc_timestamp(F.col("processed_dttm"), F.col("timezone"))) \
        .select("user_left", "user_right", "processed_dttm", "zone_id", "local_time")

    result.write.mode("overwrite").parquet(f"{output_path}/date={date}")


if __name__ == "__main__":
    main()
