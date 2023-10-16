import os
import pyspark
import argparse

if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--deposits", help="Deposits parquet path", default="./output/deposits.parquet"
    )

    parser.add_argument(
        "--events", help="Events parquet path", default="./output/events.parquet"
    )

    parser.add_argument(
        "--withdrawals",
        help="Withdrawals parquet path",
        default="./output/withdrawals.parquet",
    )

    parser.add_argument(
        "--users", help="Users parquet path", default="./output/users.parquet"
    )

    parser.add_argument("--output", help="CSV output dir", default="./output/csv/")

    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder.master("local[4]")
        .appName("ingest data")
        .getOrCreate()
    )

    deposits = spark.read.parquet(args.deposits)
    deposits.createOrReplaceTempView("deposits")

    withdrawals = spark.read.parquet(args.withdrawals)
    withdrawals.createOrReplaceTempView("withdrawals")

    events = spark.read.parquet(args.events)
    events.createOrReplaceTempView("events")

    users = spark.read.parquet(args.users)
    users.createOrReplaceTempView("users")

    # How many users were active on a given day (they made a deposit or withdrawal)
    sql_stmt = """
    WITH all_data AS (
        SELECT event_timestamp, user_id FROM withdrawals
        UNION ALL
        SELECT event_timestamp, user_id FROM deposits
    )SELECT
        COUNT(DISTINCT user_id) qty
    FROM
        all_data
    WHERE
        date_trunc('day', event_timestamp) = '2020-01-18'

    """
    # using pandas to get a single CSV file, not a partitioned dir
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(os.path.join(args.output, "first_query.csv"), index=False)

    # Identify users haven't made a deposit
    sql_stmt = """
    SELECT * FROM users
    WHERE NOT EXISTS (SELECT 1 FROM deposits WHERE deposits.user_id = users.user_id)

    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(os.path.join(args.output, "users_with_no_deposits.csv"), index=False)

    # Identify on a given day which users have made more than 5 deposits historically
    sql_stmt = """
    SELECT
        user_id
    FROM
        deposits
    WHERE
        date_trunc('day', event_timestamp) <= '2020-01-18'
    GROUP BY
        user_id
    HAVING COUNT(user_id) > 5

    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(
        os.path.join(args.output, "user_with_more_than_five_deposits.csv"), index=False
    )

    # When was the last time a user a user made a login
    sql_stmt = """
    SELECT
        user_id, max(event_timestamp) last_login_date
    FROM
        events
    WHERE
        event_name = 'login'
        AND user_id = 'f471223d1a1614b58a7dc45c9d01df19'
    GROUP BY
        user_id

    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(os.path.join(args.output, "user_last_login.csv"), index=False)

    # How many times a user has made a login between two dates
    sql_stmt = """
    SELECT
        count(user_id) qty
    FROM
        events
    WHERE
        event_name = 'login'
        AND user_id = 'f471223d1a1614b58a7dc45c9d01df19'
        AND date_trunc('day', event_timestamp) BETWEEN '2020-01-12' AND '2021-05-12'
    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(os.path.join(args.output, "user_login_between_dates.csv"), index=False)

    # Number of unique currencies deposited on a given day
    sql_stmt = """
    SELECT
        count(DISTINCT currency) qty
    FROM
        deposits
    WHERE
        date_trunc('day', event_timestamp) = '2020-05-12'
    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(
        os.path.join(
            args.output, "number_of_unique_currencies_deposited_on_a_given_day.csv"
        ),
        index=False,
    )

    # Number of unique currencies withdrew on a given day
    sql_stmt = """
    SELECT
        count(DISTINCT currency) qty
    FROM
        withdrawals
    WHERE
        date_trunc('day', event_timestamp) = '2020-05-12'
    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(
        os.path.join(
            args.output, "number_of_unique_currencies_withdrew_on_a_given_day.csv"
        ),
        index=False,
    )

    # Total amount deposited of a given currency on a given day
    sql_stmt = """
    SELECT
        sum(amount) total_deposited_amount
    FROM
        deposits
    WHERE
        date_trunc('day', event_timestamp) = '2020-05-12'
        AND currency = 'mxn'
    """
    df_ = spark.sql(sql_stmt).toPandas()
    df_.to_csv(
        os.path.join(args.output, "total_amount_one_currency_one_day.csv"), index=False
    )
