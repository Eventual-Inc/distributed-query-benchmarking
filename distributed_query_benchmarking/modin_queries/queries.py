###
# Adapted from: https://github.com/pola-rs/tpch/tree/main/modin_queries
# and https://github.com/xprobe-inc/benchmarks/blob/main/tpch/modin/queries.py
###

from datetime import date
import datetime

Q_NUM = 1


def q1(get_df):
    lineitem = get_df("lineitem")

    VAR1 = date(1998, 9, 2)

    lineitem_filtered = lineitem.loc[
        :,
        [
            "L_QUANTITY",
            "L_EXTENDEDPRICE",
            "L_DISCOUNT",
            "L_TAX",
            "L_RETURNFLAG",
            "L_LINESTATUS",
            "L_SHIPDATE",
            "L_ORDERKEY",
        ],
    ]
    sel = lineitem_filtered["L_SHIPDATE"] <= VAR1
    lineitem_filtered = lineitem_filtered[sel]
    lineitem_filtered["sum_qty"] = lineitem_filtered["L_QUANTITY"]
    lineitem_filtered["sum_base_price"] = lineitem_filtered["L_EXTENDEDPRICE"]
    lineitem_filtered["avg_qty"] = lineitem_filtered["L_QUANTITY"]
    lineitem_filtered["avg_price"] = lineitem_filtered["L_EXTENDEDPRICE"]
    lineitem_filtered["sum_disc_price"] = lineitem_filtered["L_EXTENDEDPRICE"] * (
        1 - lineitem_filtered["L_DISCOUNT"]
    )
    lineitem_filtered["sum_charge"] = (
        lineitem_filtered["L_EXTENDEDPRICE"]
        * (1 - lineitem_filtered["L_DISCOUNT"])
        * (1 + lineitem_filtered["L_TAX"])
    )
    lineitem_filtered["avg_disc"] = lineitem_filtered["L_DISCOUNT"]
    lineitem_filtered["count_order"] = lineitem_filtered["L_ORDERKEY"]
    gb = lineitem_filtered.groupby(["L_RETURNFLAG", "L_LINESTATUS"])

    total = gb.agg(
        {
            "sum_qty": "sum",
            "sum_base_price": "sum",
            "sum_disc_price": "sum",
            "sum_charge": "sum",
            "avg_qty": "mean",
            "avg_price": "mean",
            "avg_disc": "mean",
            "count_order": "count",
        }
    )

    result_df = total.reset_index().sort_values(["L_RETURNFLAG", "L_LINESTATUS"])

    return result_df


def q2(get_df):

    var1 = 15
    var2 = "BRASS"
    var3 = "EUROPE"

    region_ds = get_df("region")
    nation_ds = get_df("nation")
    supplier_ds = get_df("supplier")
    part_ds = get_df("part")
    part_supp_ds = get_df("part_supp")

    nation_filtered = nation_ds.loc[:, ["N_NATIONKEY", "N_NAME", "N_REGIONKEY"]]
    region_filtered = region_ds[(region_ds["R_NAME"] == var3)]
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    r_n_merged = nation_filtered.merge(
        region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner"
    )
    r_n_merged = r_n_merged.loc[:, ["N_NATIONKEY", "N_NAME"]]
    supplier_filtered = supplier_ds.loc[
        :,
        [
            "S_SUPPKEY",
            "S_NAME",
            "S_ADDRESS",
            "S_NATIONKEY",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
        ],
    ]
    s_r_n_merged = r_n_merged.merge(
        supplier_filtered,
        left_on="N_NATIONKEY",
        right_on="S_NATIONKEY",
        how="inner",
    )
    s_r_n_merged = s_r_n_merged.loc[
        :,
        [
            "N_NAME",
            "S_SUPPKEY",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
        ],
    ]
    partsupp_filtered = part_supp_ds.loc[
        :, ["PS_PARTKEY", "PS_SUPPKEY", "PS_SUPPLYCOST"]
    ]
    ps_s_r_n_merged = s_r_n_merged.merge(
        partsupp_filtered, left_on="S_SUPPKEY", right_on="PS_SUPPKEY", how="inner"
    )
    ps_s_r_n_merged = ps_s_r_n_merged.loc[
        :,
        [
            "N_NAME",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
            "PS_PARTKEY",
            "PS_SUPPLYCOST",
        ],
    ]
    part_filtered = part_ds.loc[:, ["P_PARTKEY", "P_MFGR", "P_SIZE", "P_TYPE"]]
    part_filtered = part_filtered[
        (part_filtered["P_SIZE"] == var1)
        & (part_filtered["P_TYPE"].str.endswith(var2))
    ]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY", "P_MFGR"]]
    merged_df = part_filtered.merge(
        ps_s_r_n_merged, left_on="P_PARTKEY", right_on="PS_PARTKEY", how="inner"
    )
    merged_df = merged_df.loc[
        :,
        [
            "N_NAME",
            "S_NAME",
            "S_ADDRESS",
            "S_PHONE",
            "S_ACCTBAL",
            "S_COMMENT",
            "PS_SUPPLYCOST",
            "P_PARTKEY",
            "P_MFGR",
        ],
    ]
    min_values = merged_df.groupby("P_PARTKEY", as_index=False)[
        "PS_SUPPLYCOST"
    ].min()
    min_values.columns = ["P_PARTKEY_CPY", "MIN_SUPPLYCOST"]
    merged_df = merged_df.merge(
        min_values,
        left_on=["P_PARTKEY", "PS_SUPPLYCOST"],
        right_on=["P_PARTKEY_CPY", "MIN_SUPPLYCOST"],
        how="inner",
    )
    result_df = merged_df.loc[
        :,
        [
            "S_ACCTBAL",
            "S_NAME",
            "N_NAME",
            "P_PARTKEY",
            "P_MFGR",
            "S_ADDRESS",
            "S_PHONE",
            "S_COMMENT",
        ],
    ]
    result_df = result_df.sort_values(
        by=[
            "S_ACCTBAL",
            "N_NAME",
            "S_NAME",
            "P_PARTKEY",
        ],
        ascending=[
            False,
            True,
            True,
            True,
        ],
    )[:100]

    return result_df


def q3(get_df):

    var1 = var2 = datetime.datetime.strptime("1995-03-15", "%Y-%m-%d")
    var3 = "BUILDING"

    customer_ds = get_df("customer")
    line_item_ds = get_df("line_item")
    orders_ds = get_df("orders")

    lineitem_filtered = line_item_ds.loc[
        :, ["L_ORDERKEY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
    ]
    orders_filtered = orders_ds.loc[
        :, ["O_ORDERKEY", "O_CUSTKEY", "O_ORDERDATE", "O_SHIPPRIORITY"]
    ]
    customer_filtered = customer_ds.loc[:, ["C_MKTSEGMENT", "C_CUSTKEY"]]
    lsel = lineitem_filtered.L_SHIPDATE > var1
    osel = orders_filtered.O_ORDERDATE < var2
    csel = customer_filtered.C_MKTSEGMENT == var3
    flineitem = lineitem_filtered[lsel]
    forders = orders_filtered[osel]
    fcustomer = customer_filtered[csel]
    jn1 = fcustomer.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn2 = jn1.merge(flineitem, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn2["revenue"] = jn2.L_EXTENDEDPRICE * (1 - jn2.L_DISCOUNT)

    total = (
        jn2.groupby(
            ["L_ORDERKEY", "O_ORDERDATE", "O_SHIPPRIORITY"], as_index=False
        )["revenue"]
        .sum()
        .sort_values(["revenue"], ascending=False)
    )
    result_df = total[:10].loc[
        :, ["L_ORDERKEY", "revenue", "O_ORDERDATE", "O_SHIPPRIORITY"]
    ]
    return result_df

def q4(get_df):

    date1 = datetime.datetime.strptime("1993-10-01", "%Y-%m-%d")
    date2 = datetime.datetime.strptime("1993-07-01", "%Y-%m-%d")

    line_item_ds = get_df("line_item")
    orders_ds = get_df("orders")

    lsel = line_item_ds.L_COMMITDATE < line_item_ds.L_RECEIPTDATE
    osel = (orders_ds.O_ORDERDATE < date1) & (orders_ds.O_ORDERDATE >= date2)
    flineitem = line_item_ds[lsel]
    forders = orders_ds[osel]
    jn = forders[forders["O_ORDERKEY"].isin(flineitem["L_ORDERKEY"])]
    result_df = (
        jn.groupby("O_ORDERPRIORITY", as_index=False)["O_ORDERKEY"]
        .count()
        .sort_values(["O_ORDERPRIORITY"])
        .rename(columns={"O_ORDERKEY": "order_count"})
    )
    return result_df

def q5(get_df):

    date1 = datetime.datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d")

    region_ds = get_df("region")
    nation_ds = get_df("nation")
    customer_ds = get_df("customer")
    line_item_ds = get_df("line_item")
    orders_ds = get_df("orders")
    supplier_ds = get_df("supplier")

    rsel = region_ds.R_NAME == "ASIA"
    osel = (orders_ds.O_ORDERDATE >= date1) & (orders_ds.O_ORDERDATE < date2)
    forders = orders_ds[osel]
    fregion = region_ds[rsel]
    jn1 = fregion.merge(nation_ds, left_on="R_REGIONKEY", right_on="N_REGIONKEY")
    jn2 = jn1.merge(customer_ds, left_on="N_NATIONKEY", right_on="C_NATIONKEY")
    jn3 = jn2.merge(forders, left_on="C_CUSTKEY", right_on="O_CUSTKEY")
    jn4 = jn3.merge(line_item_ds, left_on="O_ORDERKEY", right_on="L_ORDERKEY")
    jn5 = supplier_ds.merge(
        jn4,
        left_on=["S_SUPPKEY", "S_NATIONKEY"],
        right_on=["L_SUPPKEY", "N_NATIONKEY"],
    )
    jn5["revenue"] = jn5.L_EXTENDEDPRICE * (1.0 - jn5.L_DISCOUNT)
    gb = jn5.groupby("N_NAME", as_index=False)["revenue"].sum()
    result_df = gb.sort_values("revenue", ascending=False)
    return result_df

def q6(get_df):

    date1 = datetime.datetime.strptime("1994-01-01", "%Y-%m-%d")
    date2 = datetime.datetime.strptime("1995-01-01", "%Y-%m-%d")
    var3 = 24

    line_item_ds = get_df("line_item")

    lineitem_filtered = line_item_ds.loc[
        :, ["L_QUANTITY", "L_EXTENDEDPRICE", "L_DISCOUNT", "L_SHIPDATE"]
    ]
    sel = (
        (lineitem_filtered.L_SHIPDATE >= date1)
        & (lineitem_filtered.L_SHIPDATE < date2)
        & (lineitem_filtered.L_DISCOUNT >= 0.05)
        & (lineitem_filtered.L_DISCOUNT <= 0.07)
        & (lineitem_filtered.L_QUANTITY < var3)
    )

    flineitem = lineitem_filtered[sel]
    result_value = (flineitem.L_EXTENDEDPRICE * flineitem.L_DISCOUNT).sum()
    result_df = pd.DataFrame({"revenue": [result_value]})
    return result_df

def q7(get_df):
    nation_ds = get_df("nation")
    customer_ds = get_df("customer")
    line_item_ds = get_df("line_item")
    orders_ds = get_df("orders")
    supplier_ds = get_df("supplier")

    lineitem_filtered = line_item_ds[
        (line_item_ds["L_SHIPDATE"] >= datetime.strptime("1995-01-01", "%Y-%m-%d"))
        & (line_item_ds["L_SHIPDATE"] < datetime.strptime("1997-01-01", "%Y-%m-%d"))
    ]
    lineitem_filtered["l_year"] = lineitem_filtered["L_SHIPDATE"].dt.year
    lineitem_filtered["revenue"] = lineitem_filtered["L_EXTENDEDPRICE"] * (
        1.0 - lineitem_filtered["L_DISCOUNT"]
    )
    lineitem_filtered = lineitem_filtered.loc[
        :, ["L_ORDERKEY", "L_SUPPKEY", "l_year", "revenue"]
    ]
    supplier_filtered = supplier_ds.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    orders_filtered = orders_ds.loc[:, ["O_ORDERKEY", "O_CUSTKEY"]]
    customer_filtered = customer_ds.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    n1 = nation_ds[(nation_ds["N_NAME"] == "FRANCE")].loc[
        :, ["N_NATIONKEY", "N_NAME"]
    ]
    n2 = nation_ds[(nation_ds["N_NAME"] == "GERMANY")].loc[
        :, ["N_NATIONKEY", "N_NAME"]
    ]

    # ----- do nation 1 -----
    N1_C = customer_filtered.merge(
        n1, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    N1_C = N1_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(
        columns={"N_NAME": "cust_nation"}
    )
    N1_C_O = N1_C.merge(
        orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="inner"
    )
    N1_C_O = N1_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    N2_S = supplier_filtered.merge(
        n2, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    N2_S = N2_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(
        columns={"N_NAME": "supp_nation"}
    )
    N2_S_L = N2_S.merge(
        lineitem_filtered, left_on="S_SUPPKEY", right_on="L_SUPPKEY", how="inner"
    )
    N2_S_L = N2_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total1 = N1_C_O.merge(
        N2_S_L, left_on="O_ORDERKEY", right_on="L_ORDERKEY", how="inner"
    )
    total1 = total1.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # ----- do nation 2 ----- (same as nation 1 section but with nation 2)
    N2_C = customer_filtered.merge(
        n2, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    N2_C = N2_C.drop(columns=["C_NATIONKEY", "N_NATIONKEY"]).rename(
        columns={"N_NAME": "cust_nation"}
    )
    N2_C_O = N2_C.merge(
        orders_filtered, left_on="C_CUSTKEY", right_on="O_CUSTKEY", how="inner"
    )
    N2_C_O = N2_C_O.drop(columns=["C_CUSTKEY", "O_CUSTKEY"])

    N1_S = supplier_filtered.merge(
        n1, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    N1_S = N1_S.drop(columns=["S_NATIONKEY", "N_NATIONKEY"]).rename(
        columns={"N_NAME": "supp_nation"}
    )
    N1_S_L = N1_S.merge(
        lineitem_filtered, left_on="S_SUPPKEY", right_on="L_SUPPKEY", how="inner"
    )
    N1_S_L = N1_S_L.drop(columns=["S_SUPPKEY", "L_SUPPKEY"])

    total2 = N2_C_O.merge(
        N1_S_L, left_on="O_ORDERKEY", right_on="L_ORDERKEY", how="inner"
    )
    total2 = total2.drop(columns=["O_ORDERKEY", "L_ORDERKEY"])

    # concat results
    total = pd.concat([total1, total2])
    result_df = (
        total.groupby(["supp_nation", "cust_nation", "l_year"])
        .revenue.agg("sum")
        .reset_index()
    )
    result_df.columns = ["supp_nation", "cust_nation", "l_year", "revenue"]

    result_df = result_df.sort_values(
        by=["supp_nation", "cust_nation", "l_year"],
        ascending=[
            True,
            True,
            True,
        ],
    )
    return result_df


def q8(get_df):
    part = get_df("part")
    lineitem = get_df("lineitem")
    orders = get_df("orders")
    customer = get_df("customer")
    supplier = get_df("supplier")
    nation = get_df("nation")
    region = get_df("region")

    part_filtered = part[(part["P_TYPE"] == "ECONOMY ANODIZED STEEL")]
    part_filtered = part_filtered.loc[:, ["P_PARTKEY"]]
    lineitem_filtered = lineitem.loc[:, ["L_PARTKEY", "L_SUPPKEY", "L_ORDERKEY"]]
    lineitem_filtered["VOLUME"] = lineitem["L_EXTENDEDPRICE"] * (
        1.0 - lineitem["L_DISCOUNT"]
    )
    total = part_filtered.merge(
        lineitem_filtered, left_on="P_PARTKEY", right_on="L_PARTKEY", how="inner"
    )
    total = total.loc[:, ["L_SUPPKEY", "L_ORDERKEY", "VOLUME"]]
    supplier_filtered = supplier.loc[:, ["S_SUPPKEY", "S_NATIONKEY"]]
    total = total.merge(
        supplier_filtered, left_on="L_SUPPKEY", right_on="S_SUPPKEY", how="inner"
    )
    total = total.loc[:, ["L_ORDERKEY", "VOLUME", "S_NATIONKEY"]]
    orders_filtered = orders[
        (orders["O_ORDERDATE"] >= pd.Timestamp("1995-01-01"))
        & (orders["O_ORDERDATE"] < pd.Timestamp("1997-01-01"))
    ]
    orders_filtered["O_YEAR"] = orders_filtered["O_ORDERDATE"].apply(lambda x: x.year)
    orders_filtered = orders_filtered.loc[:, ["O_ORDERKEY", "O_CUSTKEY", "O_YEAR"]]
    total = total.merge(
        orders_filtered, left_on="L_ORDERKEY", right_on="O_ORDERKEY", how="inner"
    )
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_CUSTKEY", "O_YEAR"]]
    customer_filtered = customer.loc[:, ["C_CUSTKEY", "C_NATIONKEY"]]
    total = total.merge(
        customer_filtered, left_on="O_CUSTKEY", right_on="C_CUSTKEY", how="inner"
    )
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "C_NATIONKEY"]]
    n1_filtered = nation.loc[:, ["N_NATIONKEY", "N_REGIONKEY"]]
    n2_filtered = nation.loc[:, ["N_NATIONKEY", "N_NAME"]].rename(
        columns={"N_NAME": "NATION"}
    )
    total = total.merge(
        n1_filtered, left_on="C_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    total = total.loc[:, ["VOLUME", "S_NATIONKEY", "O_YEAR", "N_REGIONKEY"]]
    total = total.merge(
        n2_filtered, left_on="S_NATIONKEY", right_on="N_NATIONKEY", how="inner"
    )
    total = total.loc[:, ["VOLUME", "O_YEAR", "N_REGIONKEY", "NATION"]]
    region_filtered = region[(region["R_NAME"] == "AMERICA")]
    region_filtered = region_filtered.loc[:, ["R_REGIONKEY"]]
    total = total.merge(
        region_filtered, left_on="N_REGIONKEY", right_on="R_REGIONKEY", how="inner"
    )
    total = total.loc[:, ["VOLUME", "O_YEAR", "NATION"]]

    def udf(df):
        demonimator = df["VOLUME"].sum()
        df = df[df["NATION"] == "BRAZIL"]
        numerator = df["VOLUME"].sum()
        return numerator / demonimator

    # modin returns empty column with as_index=false
    total = total.groupby("O_YEAR").apply(udf).reset_index()
    total.columns = ["O_YEAR", "MKT_SHARE"]
    total = total.sort_values(
        by=[
            "O_YEAR",
        ],
        ascending=[
            True,
        ],
    )
    return total


def q9(get_df):
    part = get_df("part")
    partsupp = get_df("partsupp")
    lineitem = get_df("lineitem")
    orders = get_df("orders")
    supplier = get_df("supplier")
    nation = get_df("nation")

    psel = part.P_NAME.str.contains("green")
    fpart = part[psel]
    jn1 = lineitem.merge(fpart, left_on="L_PARTKEY", right_on="P_PARTKEY")
    jn2 = jn1.merge(supplier, left_on="L_SUPPKEY", right_on="S_SUPPKEY")
    jn3 = jn2.merge(nation, left_on="S_NATIONKEY", right_on="N_NATIONKEY")
    jn4 = partsupp.merge(
        jn3, left_on=["PS_PARTKEY", "PS_SUPPKEY"], right_on=["L_PARTKEY", "L_SUPPKEY"]
    )
    jn5 = jn4.merge(orders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn5["TMP"] = jn5.L_EXTENDEDPRICE * (1 - jn5.L_DISCOUNT) - (
        (1 * jn5.PS_SUPPLYCOST) * jn5.L_QUANTITY
    )
    jn5["O_YEAR"] = jn5.O_ORDERDATE.apply(lambda x: x.year)
    gb = jn5.groupby(["N_NAME", "O_YEAR"], as_index=False)["TMP"].sum()
    total = gb.sort_values(["N_NAME", "O_YEAR"], ascending=[True, False])
    return total


def q10(get_df):
    lineitem = get_df("lineitem")
    orders = get_df("orders")
    nation = get_df("nation")
    customer = get_df("customer")

    date1 = pd.Timestamp("1993-10-01")
    date2 = pd.Timestamp("1994-01-01")
    osel = (orders.O_ORDERDATE >= date1) & (orders.O_ORDERDATE < date2)
    lsel = lineitem.L_RETURNFLAG == "R"
    forders = orders[osel]
    flineitem = lineitem[lsel]
    jn1 = flineitem.merge(forders, left_on="L_ORDERKEY", right_on="O_ORDERKEY")
    jn2 = jn1.merge(customer, left_on="O_CUSTKEY", right_on="C_CUSTKEY")
    jn3 = jn2.merge(nation, left_on="C_NATIONKEY", right_on="N_NATIONKEY")
    jn3["TMP"] = jn3.L_EXTENDEDPRICE * (1.0 - jn3.L_DISCOUNT)
    gb = jn3.groupby(
        [
            "C_CUSTKEY",
            "C_NAME",
            "C_ACCTBAL",
            "C_PHONE",
            "N_NAME",
            "C_ADDRESS",
            "C_COMMENT",
        ],
        as_index=False,
    )["TMP"].sum()
    total = gb.sort_values("TMP", ascending=False)
    total = total.head(20)
    return total