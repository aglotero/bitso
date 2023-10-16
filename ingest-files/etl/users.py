import pyspark
import argparse
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
)


def load_csv(spark, path):
    schema = StructType(
        [
            StructField("user_id", StringType(), False),
        ]
    )

    df_csv = spark.read.options(header=True).schema(schema).csv(path)

    return df_csv


def write_parquet(df, path, mode="overwrite"):
    df.write.mode(mode).parquet(path)


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input", help="CSV path", default="./input/user_id_sample_data.csv"
    )

    parser.add_argument(
        "--output", help="Parquet path", default="./output/users.parquet"
    )

    parser.add_argument(
        "--writingmode",
        help="Parquet writing mode (append/overwrite).",
        default="overwrite",
    )

    args = parser.parse_args()

    spark = (
        pyspark.sql.SparkSession.builder.master("local[4]")
        .appName("ingest data")
        .getOrCreate()
    )

    df = load_csv(spark, args.input)
    write_parquet(df, args.output, args.writingmode)
