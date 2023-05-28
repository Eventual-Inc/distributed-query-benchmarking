import os
import time

import argparse
from pyspark.sql import SparkSession
    
    
###
# Queries
###

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


def q02(spark, load_table):
    load_table("region")
    load_table("nation")
    load_table("supplier")
    load_table("part")
    load_table("partsupp")
    SIZE = 15
    TYPE = "BRASS"
    REGION = "EUROPE"
    total = spark.sql(
        f"""select
                s_acctbal,
                s_name,
                n_name,
                p_partkey,
                p_mfgr,
                s_address,
                s_phone,
                s_comment
            from
                part,
                supplier,
                partsupp,
                nation,
                region
            where
                p_partkey = ps_partkey
                and s_suppkey = ps_suppkey
                and p_size = {SIZE}
                and p_type like '%{TYPE}'
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = '{REGION}'
                and ps_supplycost = (
                    select
                    min(ps_supplycost)
                    from
                    partsupp, supplier,
                    nation, region
                    where
                    p_partkey = ps_partkey
                    and s_suppkey = ps_suppkey
                    and s_nationkey = n_nationkey
                    and n_regionkey = r_regionkey
                    and r_name = '{REGION}'
                    )
            order by
                s_acctbal desc,
                n_name,
                s_name,
                p_partkey"""
    )

    return total.toPandas()


def q03(spark, load_table):
    load_table("customer")
    load_table("orders")
    load_table("lineitem")
    total = spark.sql(
        """select
            l_orderkey,
            sum(l_extendedprice * (1 - l_discount)) as revenue,
            o_orderdate,
            o_shippriority
        from
            customer,
            orders,
            lineitem
        where
            c_mktsegment = 'HOUSEHOLD'
            and c_custkey = o_custkey
            and l_orderkey = o_orderkey
            and o_orderdate < date '1995-03-04'
            and l_shipdate > date '1995-03-04'
        group by
            l_orderkey,
            o_orderdate,
            o_shippriority
        order by
            revenue desc,
            o_orderdate
        limit 10"""
    )

    return total.toPandas()


def q04(spark, load_table):
    load_table("orders")
    load_table("lineitem")
    total = spark.sql(
        """select
                o_orderpriority,
                count(*) as order_count
            from
                orders
            where
                o_orderdate >= date '1993-08-01'
                and o_orderdate < date '1993-08-01' + interval '3' month
                and exists (
                    select
                        *
                    from
                        lineitem
                    where
                        l_orderkey = o_orderkey
                        and l_commitdate < l_receiptdate
                )
            group by
                o_orderpriority
            order by
                o_orderpriority"""
    )

    return total.toPandas()


def q05(spark, load_table):
    load_table("customer")
    load_table("orders")
    load_table("lineitem")
    load_table("supplier")
    load_table("nation")
    load_table("region")
    total = spark.sql(
        """select
                n_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue
            from
                customer,
                orders,
                lineitem,
                supplier,
                nation,
                region
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and l_suppkey = s_suppkey
                and c_nationkey = s_nationkey
                and s_nationkey = n_nationkey
                and n_regionkey = r_regionkey
                and r_name = 'ASIA'
                and o_orderdate >= date '1996-01-01'
                and o_orderdate < date '1996-01-01' + interval '1' year
            group by
                n_name
            order by
                revenue desc"""
    )

    return total.toPandas()


def q06(spark, load_table):
    load_table("lineitem")
    sql_lineitem = spark.sql(
        """select
                sum(l_extendedprice * l_discount) as revenue
            from
                lineitem
            where
                l_shipdate >= date '1996-01-01'
                and l_shipdate < date '1996-01-01' + interval '1' year
                and l_discount between .08 and .1
                and l_quantity < 24"""
    )
    return sql_lineitem.toPandas()


def q07(spark, load_table):
    load_table("customer")
    load_table("orders")
    load_table("lineitem")
    load_table("supplier")
    load_table("nation")
    NATION1 = "FRANCE"
    NATION2 = "GERMANY"
    total = spark.sql(
        f"""select
                supp_nation,
                cust_nation,
                l_year, sum(volume) as revenue
            from (
                select
                    n1.n_name as supp_nation,
                    n2.n_name as cust_nation,
                    extract(year from l_shipdate) as l_year,
                    l_extendedprice * (1 - l_discount) as volume
                from
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2
                where
                    s_suppkey = l_suppkey
                    and o_orderkey = l_orderkey
                    and c_custkey = o_custkey
                    and s_nationkey = n1.n_nationkey
                    and c_nationkey = n2.n_nationkey
                    and (
                    (n1.n_name = '{NATION1}' and n2.n_name = '{NATION2}')
                    or (n1.n_name = '{NATION2}' and n2.n_name = '{NATION1}')
                    )
                    and l_shipdate between date '1995-01-01' and date '1996-12-31'
                ) as shipping
            group by
                supp_nation,
                cust_nation,
                l_year
            order by
                supp_nation,
                cust_nation,
                l_year"""
    )
    return total.toPandas()


def q08(spark, load_table):
    load_table("part")
    load_table("supplier")
    load_table("lineitem")
    load_table("orders")
    load_table("customer")
    load_table("nation")
    load_table("region")
    NATION = "BRAZIL"
    REGION = "AMERICA"
    TYPE = "ECONOMY ANODIZED STEEL"
    total = spark.sql(
        f"""select
                o_year,
                sum(case
                    when nation = '{NATION}'
                    then volume
                    else 0
                end) / sum(volume) as mkt_share
            from (
                select
                    extract(year from o_orderdate) as o_year,
                    l_extendedprice * (1-l_discount) as volume,
                    n2.n_name as nation
                from
                    part,
                    supplier,
                    lineitem,
                    orders,
                    customer,
                    nation n1,
                    nation n2,
                    region
                where
                    p_partkey = l_partkey
                    and s_suppkey = l_suppkey
                    and l_orderkey = o_orderkey
                    and o_custkey = c_custkey
                    and c_nationkey = n1.n_nationkey
                    and n1.n_regionkey = r_regionkey
                    and r_name = '{REGION}'
                    and s_nationkey = n2.n_nationkey
                    and o_orderdate between date '1995-01-01' and date '1996-12-31'
                    and p_type = '{TYPE}'
                ) as all_nations
            group by
                o_year
            order by
                o_year"""
    )
    return total.toPandas()


def q09(spark, load_table):
    load_table("part")
    load_table("supplier")
    load_table("lineitem")
    load_table("partsupp")
    load_table("orders")
    load_table("nation")
    total = spark.sql(
        """select
                nation,
                o_year,
                sum(amount) as sum_profit
            from
                (
                    select
                        n_name as nation,
                        year(o_orderdate) as o_year,
                        l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount
                    from
                        part,
                        supplier,
                        lineitem,
                        partsupp,
                        orders,
                        nation
                    where
                        s_suppkey = l_suppkey
                        and ps_suppkey = l_suppkey
                        and ps_partkey = l_partkey
                        and p_partkey = l_partkey
                        and o_orderkey = l_orderkey
                        and s_nationkey = n_nationkey
                        and p_name like '%ghost%'
                ) as profit
            group by
                nation,
                o_year
            order by
                nation,
                o_year desc"""
    )
    return total.toPandas()


def q10(spark, load_table):
    load_table("customer")
    load_table("orders")
    load_table("lineitem")
    load_table("nation")
    total = spark.sql(
        """select
                c_custkey,
                c_name,
                sum(l_extendedprice * (1 - l_discount)) as revenue,
                c_acctbal,
                n_name,
                c_address,
                c_phone,
                c_comment
            from
                customer,
                orders,
                lineitem,
                nation
            where
                c_custkey = o_custkey
                and l_orderkey = o_orderkey
                and o_orderdate >= date '1994-11-01'
                and o_orderdate < date '1994-11-01' + interval '3' month
                and l_returnflag = 'R'
                and c_nationkey = n_nationkey
            group by
                c_custkey,
                c_name,
                c_acctbal,
                c_phone,
                n_name,
                c_address,
                c_comment
            order by
                revenue desc
            limit 20"""
    )
    return total.toPandas()


def run(spark, q: int, data_folder):

    def load_table(tbl: str):
        df = spark.read.parquet(os.path.join(data_folder, tbl))
        df.createOrReplaceTempView(tbl)

    func = globals()[f"q{q:02}"]
    return func(spark, load_table)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "parquet_path",
        nargs="?",
        default="s3://eventual-dev-benchmarking-fixtures/uncompressed/tpch-dbgen/1000_0/512/parquet/"
    )
    args = parser.parse_args()
    print(f"Using Parquet files at: {args.parquet_path}")

    for tpch_qnum in range(1, 11):
        for attempt in range(2):
            print(f"========== Starting benchmarks for Q{tpch_qnum}, attempt {attempt} ==========\n")
            spark = SparkSession.builder.appName(f"TPCH-Q{tpch_qnum}-attempt-{attempt}").getOrCreate()
            start = time.time()
            print(run(spark, tpch_qnum, args.parquet_path))
            print(f"--- walltime: {time.time() - start}s ---")
            print(f"========== Finished benchmarks for Q{tpch_qnum}, attempt {attempt} ==========\n")
