import daft
import datetime

from typing import Callable

GetDFFunc = Callable[[str], daft.DataFrame]

def q1(get_df: GetDFFunc) -> daft.DataFrame:
    lineitem = get_df("lineitem")

    discounted_price = daft.col("L_EXTENDEDPRICE") * (1 - daft.col("L_DISCOUNT"))
    taxed_discounted_price = discounted_price * (1 + daft.col("L_TAX"))
    daft_df = (
        lineitem.where(daft.col("L_SHIPDATE") <= datetime.date(1998, 9, 2))
        .groupby(daft.col("L_RETURNFLAG"), daft.col("L_LINESTATUS"))
        .agg(
            [
                (daft.col("L_QUANTITY").alias("sum_qty"), "sum"),
                (daft.col("L_EXTENDEDPRICE").alias("sum_base_price"), "sum"),
                (discounted_price.alias("sum_disc_price"), "sum"),
                (taxed_discounted_price.alias("sum_charge"), "sum"),
                (daft.col("L_QUANTITY").alias("avg_qty"), "mean"),
                (daft.col("L_EXTENDEDPRICE").alias("avg_price"), "mean"),
                (daft.col("L_DISCOUNT").alias("avg_disc"), "mean"),
                (daft.col("L_QUANTITY").alias("count_order"), "count"),
            ]
        )
        .sort(["L_RETURNFLAG", "L_LINESTATUS"])
    )
    return daft_df


def q2(get_df: GetDFFunc) -> daft.DataFrame:
    region = get_df("region")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    part = get_df("part")

    europe = (
        region.where(daft.col("R_NAME") == "EUROPE")
        .join(nation, left_on=daft.col("R_REGIONKEY"), right_on=daft.col("N_REGIONKEY"))
        .join(supplier, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("S_NATIONKEY"))
        .join(partsupp, left_on=daft.col("S_SUPPKEY"), right_on=daft.col("PS_SUPPKEY"))
    )

    brass = part.where((daft.col("P_SIZE") == 15) & daft.col("P_TYPE").str.endswith("BRASS")).join(
        europe,
        left_on=daft.col("P_PARTKEY"),
        right_on=daft.col("PS_PARTKEY"),
    )
    min_cost = brass.groupby(daft.col("P_PARTKEY")).agg(
        [
            (daft.col("PS_SUPPLYCOST").alias("min"), "min"),
        ]
    )

    daft_df = (
        brass.join(min_cost, on=daft.col("P_PARTKEY"))
        .where(daft.col("PS_SUPPLYCOST") == daft.col("min"))
        .select(
            daft.col("S_ACCTBAL"),
            daft.col("S_NAME"),
            daft.col("N_NAME"),
            daft.col("P_PARTKEY"),
            daft.col("P_MFGR"),
            daft.col("S_ADDRESS"),
            daft.col("S_PHONE"),
            daft.col("S_COMMENT"),
        )
        .sort(by=["S_ACCTBAL", "N_NAME", "S_NAME", "P_PARTKEY"], desc=[True, False, False, False])
        .limit(100)
    )
    return daft_df


def q3(get_df: GetDFFunc) -> daft.DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    customer = get_df("customer").where(daft.col("C_MKTSEGMENT") == "BUILDING")
    orders = get_df("orders").where(daft.col("O_ORDERDATE") < datetime.date(1995, 3, 15))
    lineitem = get_df("lineitem").where(daft.col("L_SHIPDATE") > datetime.date(1995, 3, 15))

    daft_df = (
        customer.join(orders, left_on=daft.col("C_CUSTKEY"), right_on=daft.col("O_CUSTKEY"))
        .select(daft.col("O_ORDERKEY"), daft.col("O_ORDERDATE"), daft.col("O_SHIPPRIORITY"))
        .join(lineitem, left_on=daft.col("O_ORDERKEY"), right_on=daft.col("L_ORDERKEY"))
        .select(
            daft.col("O_ORDERKEY"),
            decrease(daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT")).alias("volume"),
            daft.col("O_ORDERDATE"),
            daft.col("O_SHIPPRIORITY"),
        )
        .groupby(daft.col("O_ORDERKEY"), daft.col("O_ORDERDATE"), daft.col("O_SHIPPRIORITY"))
        .agg([(daft.col("volume").alias("revenue"), "sum")])
        .sort(by=["revenue", "O_ORDERDATE"], desc=[True, False])
        .limit(10)
        .select("O_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY")
    )
    return daft_df


def q4(get_df: GetDFFunc) -> daft.DataFrame:
    orders = get_df("orders")
    lineitems = get_df("lineitem")

    orders = orders.where(
        (daft.col("O_ORDERDATE") >= datetime.date(1993, 7, 1)) & (daft.col("O_ORDERDATE") < datetime.date(1993, 10, 1))
    )

    lineitems = lineitems.where(daft.col("L_COMMITDATE") < daft.col("L_RECEIPTDATE")).select(daft.col("L_ORDERKEY")).distinct()

    daft_df = (
        lineitems.join(orders, left_on=daft.col("L_ORDERKEY"), right_on=daft.col("O_ORDERKEY"))
        .groupby(daft.col("O_ORDERPRIORITY"))
        .agg([(daft.col("L_ORDERKEY").alias("order_count"), "count")])
        .sort(daft.col("O_ORDERPRIORITY"))
    )
    return daft_df


def q5(get_df: GetDFFunc) -> daft.DataFrame:
    orders = get_df("orders").where(
        (daft.col("O_ORDERDATE") >= datetime.date(1994, 1, 1)) & (daft.col("O_ORDERDATE") < datetime.date(1995, 1, 1))
    )
    region = get_df("region").where(daft.col("R_NAME") == "ASIA")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    daft_df = (
        region.join(nation, left_on=daft.col("R_REGIONKEY"), right_on=daft.col("N_REGIONKEY"))
        .join(supplier, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("S_NATIONKEY"))
        .join(lineitem, left_on=daft.col("S_SUPPKEY"), right_on=daft.col("L_SUPPKEY"))
        .select(daft.col("N_NAME"), daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT"), daft.col("L_ORDERKEY"), daft.col("N_NATIONKEY"))
        .join(orders, left_on=daft.col("L_ORDERKEY"), right_on=daft.col("O_ORDERKEY"))
        .join(customer, left_on=[daft.col("O_CUSTKEY"), daft.col("N_NATIONKEY")], right_on=[daft.col("C_CUSTKEY"), daft.col("C_NATIONKEY")])
        .select(daft.col("N_NAME"), (daft.col("L_EXTENDEDPRICE") * (1 - daft.col("L_DISCOUNT"))).alias("value"))
        .groupby(daft.col("N_NAME"))
        .agg([(daft.col("value").alias("revenue"), "sum")])
        .sort(daft.col("revenue"), desc=True)
    )
    return daft_df


def q6(get_df: GetDFFunc) -> daft.DataFrame:
    lineitem = get_df("lineitem")
    daft_df = lineitem.where(
        (daft.col("L_SHIPDATE") >= datetime.date(1994, 1, 1))
        & (daft.col("L_SHIPDATE") < datetime.date(1995, 1, 1))
        & (daft.col("L_DISCOUNT") >= 0.05)
        & (daft.col("L_DISCOUNT") <= 0.07)
        & (daft.col("L_QUANTITY") < 24)
    ).sum(daft.col("L_EXTENDEDPRICE") * daft.col("L_DISCOUNT"))
    return daft_df


def q7(get_df: GetDFFunc) -> daft.DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(
        (daft.col("L_SHIPDATE") >= datetime.date(1995, 1, 1)) & (daft.col("L_SHIPDATE") <= datetime.date(1996, 12, 31))
    )
    nation = get_df("nation").where((daft.col("N_NAME") == "FRANCE") | (daft.col("N_NAME") == "GERMANY"))
    supplier = get_df("supplier")
    customer = get_df("customer")
    orders = get_df("orders")

    supNation = (
        nation.join(supplier, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("S_NATIONKEY"))
        .join(lineitem, left_on=daft.col("S_SUPPKEY"), right_on=daft.col("L_SUPPKEY"))
        .select(
            daft.col("N_NAME").alias("supp_nation"),
            daft.col("L_ORDERKEY"),
            daft.col("L_EXTENDEDPRICE"),
            daft.col("L_DISCOUNT"),
            daft.col("L_SHIPDATE"),
        )
    )

    daft_df = (
        nation.join(customer, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("C_NATIONKEY"))
        .join(orders, left_on=daft.col("C_CUSTKEY"), right_on=daft.col("O_CUSTKEY"))
        .select(daft.col("N_NAME").alias("cust_nation"), daft.col("O_ORDERKEY"))
        .join(supNation, left_on=daft.col("O_ORDERKEY"), right_on=daft.col("L_ORDERKEY"))
        .where(
            ((daft.col("supp_nation") == "FRANCE") & (daft.col("cust_nation") == "GERMANY"))
            | ((daft.col("supp_nation") == "GERMANY") & (daft.col("cust_nation") == "FRANCE"))
        )
        .select(
            daft.col("supp_nation"),
            daft.col("cust_nation"),
            daft.col("L_SHIPDATE").dt.year().alias("l_year"),
            decrease(daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT")).alias("volume"),
        )
        .groupby(daft.col("supp_nation"), daft.col("cust_nation"), daft.col("l_year"))
        .agg([(daft.col("volume").alias("revenue"), "sum")])
        .sort(by=["supp_nation", "cust_nation", "l_year"])
    )
    return daft_df


def q8(get_df: GetDFFunc) -> daft.DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    region = get_df("region").where(daft.col("R_NAME") == "AMERICA")
    orders = get_df("orders").where(
        (daft.col("O_ORDERDATE") <= datetime.date(1996, 12, 31)) & (daft.col("O_ORDERDATE") >= datetime.date(1995, 1, 1))
    )
    part = get_df("part").where(daft.col("P_TYPE") == "ECONOMY ANODIZED STEEL")
    nation = get_df("nation")
    supplier = get_df("supplier")
    lineitem = get_df("lineitem")
    customer = get_df("customer")

    nat = nation.join(supplier, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("S_NATIONKEY"))

    line = (
        lineitem.select(
            daft.col("L_PARTKEY"),
            daft.col("L_SUPPKEY"),
            daft.col("L_ORDERKEY"),
            decrease(daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT")).alias("volume"),
        )
        .join(part, left_on=daft.col("L_PARTKEY"), right_on=daft.col("P_PARTKEY"))
        .join(nat, left_on=daft.col("L_SUPPKEY"), right_on=daft.col("S_SUPPKEY"))
    )

    daft_df = (
        nation.join(region, left_on=daft.col("N_REGIONKEY"), right_on=daft.col("R_REGIONKEY"))
        .select(daft.col("N_NATIONKEY"))
        .join(customer, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("C_NATIONKEY"))
        .select(daft.col("C_CUSTKEY"))
        .join(orders, left_on=daft.col("C_CUSTKEY"), right_on=daft.col("O_CUSTKEY"))
        .select(daft.col("O_ORDERKEY"), daft.col("O_ORDERDATE"))
        .join(line, left_on=daft.col("O_ORDERKEY"), right_on=daft.col("L_ORDERKEY"))
        .select(
            daft.col("O_ORDERDATE").dt.year().alias("o_year"),
            daft.col("volume"),
            (daft.col("N_NAME") == "BRAZIL").if_else(daft.col("volume"), 0.0).alias("case_volume"),
        )
        .groupby(daft.col("o_year"))
        .agg([(daft.col("case_volume").alias("case_volume_sum"), "sum"), (daft.col("volume").alias("volume_sum"), "sum")])
        .select(daft.col("o_year"), daft.col("case_volume_sum") / daft.col("volume_sum"))
        .sort(daft.col("o_year"))
    )

    return daft_df


def q9(get_df: GetDFFunc) -> daft.DataFrame:
    lineitem = get_df("lineitem")
    part = get_df("part")
    nation = get_df("nation")
    supplier = get_df("supplier")
    partsupp = get_df("partsupp")
    orders = get_df("orders")

    def expr(x, y, v, w):
        return x * (1 - y) - (v * w)

    linepart = part.where(daft.col("P_NAME").str.contains("green")).join(
        lineitem, left_on=daft.col("P_PARTKEY"), right_on=daft.col("L_PARTKEY")
    )
    natsup = nation.join(supplier, left_on=daft.col("N_NATIONKEY"), right_on=daft.col("S_NATIONKEY"))

    daft_df = (
        linepart.join(natsup, left_on=daft.col("L_SUPPKEY"), right_on=daft.col("S_SUPPKEY"))
        .join(partsupp, left_on=[daft.col("L_SUPPKEY"), daft.col("P_PARTKEY")], right_on=[daft.col("PS_SUPPKEY"), daft.col("PS_PARTKEY")])
        .join(orders, left_on=daft.col("L_ORDERKEY"), right_on=daft.col("O_ORDERKEY"))
        .select(
            daft.col("N_NAME"),
            daft.col("O_ORDERDATE").dt.year().alias("o_year"),
            expr(daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT"), daft.col("PS_SUPPLYCOST"), daft.col("L_QUANTITY")).alias("amount"),
        )
        .groupby(daft.col("N_NAME"), daft.col("o_year"))
        .agg([(daft.col("amount"), "sum")])
        .sort(by=["N_NAME", "o_year"], desc=[False, True])
    )

    return daft_df


def q10(get_df: GetDFFunc) -> daft.DataFrame:
    def decrease(x, y):
        return x * (1 - y)

    lineitem = get_df("lineitem").where(daft.col("L_RETURNFLAG") == "R")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

    daft_df = (
        orders.where(
            (daft.col("O_ORDERDATE") < datetime.date(1994, 1, 1)) & (daft.col("O_ORDERDATE") >= datetime.date(1993, 10, 1))
        )
        .join(customer, left_on=daft.col("O_CUSTKEY"), right_on=daft.col("C_CUSTKEY"))
        .join(nation, left_on=daft.col("C_NATIONKEY"), right_on=daft.col("N_NATIONKEY"))
        .join(lineitem, left_on=daft.col("O_ORDERKEY"), right_on=daft.col("L_ORDERKEY"))
        .select(
            daft.col("O_CUSTKEY"),
            daft.col("C_NAME"),
            decrease(daft.col("L_EXTENDEDPRICE"), daft.col("L_DISCOUNT")).alias("volume"),
            daft.col("C_ACCTBAL"),
            daft.col("N_NAME"),
            daft.col("C_ADDRESS"),
            daft.col("C_PHONE"),
            daft.col("C_COMMENT"),
        )
        .groupby(
            daft.col("O_CUSTKEY"),
            daft.col("C_NAME"),
            daft.col("C_ACCTBAL"),
            daft.col("C_PHONE"),
            daft.col("N_NAME"),
            daft.col("C_ADDRESS"),
            daft.col("C_COMMENT"),
        )
        .agg([(daft.col("volume").alias("revenue"), "sum")])
        .sort(daft.col("revenue"), desc=True)
        .select(
            daft.col("O_CUSTKEY"),
            daft.col("C_NAME"),
            daft.col("revenue"),
            daft.col("C_ACCTBAL"),
            daft.col("N_NAME"),
            daft.col("C_ADDRESS"),
            daft.col("C_PHONE"),
            daft.col("C_COMMENT"),
        )
        .limit(20)
    )

    return daft_df
