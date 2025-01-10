if __name__ == "__main__":
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("demo").getOrCreate()

    df = spark.createDataFrame(
        [
            ("sue", 32),
            ("li", 3),
            ("bob", 75),
            ("heo", 13),
        ],
        ["first_name", "age"],
    )

    df.show()