import argparse
import time
import os

def q01(spark, load_table):
    load_table("lineitem")
    sql_lineitem = spark.sql(
        """select
                l_returnflag,
                l_linestatus,
                sum(l_quantity) as sum_qty,
                sum(l_extendedprice) as sum_base_price,
                sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
                sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
                avg(l_quantity) as avg_qty,
                avg(l_extendedprice) as avg_price,
                avg(l_discount) as avg_disc,
                count(*) as count_order
            from
                lineitem
            where
                l_shipdate <= date '1998-12-01' - interval '90' day
            group by
                l_returnflag,
                l_linestatus
            order by
                l_returnflag,
                l_linestatus"""
    )
    return sql_lineitem.toPandas()


if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "parquet_path",
        default="s3a://eventual-dev-benchmarking-fixtures/uncompressed-smaller-rg/tpch-dbgen/1000_0/512/parquet/"
    )
    args = parser.parse_args()
    print(f"Using Parquet files at: {args.parquet_path}")

    def load_table(tbl: str):
        df = spark.read.parquet(os.path.join(args.parquet_path, tbl))
        df.createOrReplaceTempView(tbl)

    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("tpch-q1").getOrCreate()

    start = time.time()
    results = q01(spark, load_table)
    print(f"Finished Q01 in {time.time() - start:.2f}s")

    print(results)
