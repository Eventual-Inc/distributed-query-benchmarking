###
# Adapted from: https://github.com/pola-rs/tpch/tree/main/spark_queries
###


Q1_SQL = """
select
    L_RETURNFLAG,
    L_LINESTATUS,
    sum(L_QUANTITY) as sum_qty,
    sum(L_EXTENDEDPRICE) as sum_base_price,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as sum_disc_price,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as sum_charge,
    avg(L_QUANTITY) as avg_qty,
    avg(L_EXTENDEDPRICE) as avg_price,
    avg(L_DISCOUNT) as avg_disc,
    count(*) as count_order
from
    lineitem
where
    date(L_SHIPDATE) <= date('1998-09-02')
group by
    L_RETURNFLAG,
    L_LINESTATUS
order by
    L_RETURNFLAG,
    L_LINESTATUS
"""

Q2_SQL = """
select
    S_ACCTBAL,
    S_NAME,
    N_NAME,
    P_PARTKEY,
    P_MFGR,
    S_ADDRESS,
    S_PHONE,
    S_COMMENT
from
    part,
    supplier,
    partsupp,
    nation,
    region
where
    P_PARTKEY = PS_PARTKEY
    and S_SUPPKEY = PS_SUPPKEY
    and P_SIZE = 15
    and P_TYPE like '%BRASS'
    and S_NATIONKEY = N_NATIONKEY
    and N_REGIONKEY = R_REGIONKEY
    and R_NAME = 'EUROPE'
    and PS_SUPPLYCOST = (
        select
            min(PS_SUPPLYCOST)
        from
            partsupp,
            supplier,
            nation,
            region
        where
            P_PARTKEY = PS_PARTKEY
            and S_SUPPKEY = PS_SUPPKEY
            and S_NATIONKEY = N_NATIONKEY
            and N_REGIONKEY = R_REGIONKEY
            and R_NAME = 'EUROPE'
    )
order by
    S_ACCTBAL desc,
    N_NAME,
    S_NAME,
    P_PARTKEY
limit 100
"""

Q3_SQL = """
select
    L_ORDERKEY,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
    date(O_ORDERDATE),
    O_SHIPPRIORITY
from
    customer,
    orders,
    lineitem
where
    C_MKTSEGMENT = 'BUILDING'
    and C_CUSTKEY = O_CUSTKEY
    and L_ORDERKEY = O_ORDERKEY
    and O_ORDERDATE < date '1995-03-15'
    and L_SHIPDATE > date '1995-03-15'
group by
    L_ORDERKEY,
    O_ORDERDATE,
    O_SHIPPRIORITY
order by
    revenue desc,
    O_ORDERDATE
limit 10
"""

Q4_SQL = """
select
    O_ORDERPRIORITY,
    count(*) as order_count
from
    orders
where
    O_ORDERDATE >= date '1993-07-01'
    and O_ORDERDATE < date '1993-07-01' + interval '3' month
    and exists (
        select
            *
        from
            lineitem
        where
            L_ORDERKEY = O_ORDERKEY
            and L_COMMITDATE < L_RECEIPTDATE
    )
group by
    O_ORDERPRIORITY
order by
    O_ORDERPRIORITY
"""

Q5_SQL = """
select
    N_NAME,
    sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
from
    customer,
    orders,
    lineitem,
    supplier,
    nation,
    region
where
    C_CUSTKEY = O_CUSTKEY
    and L_ORDERKEY = O_ORDERKEY
    and L_SUPPKEY = S_SUPPKEY
    and C_NATIONKEY = S_NATIONKEY
    and S_NATIONKEY = N_NATIONKEY
    and N_REGIONKEY = R_REGIONKEY
    and R_NAME = 'ASIA'
    and O_ORDERDATE >= date '1994-01-01'
    and O_ORDERDATE < date '1994-01-01' + interval '1' year
group by
    N_NAME
order by
    revenue desc
"""

Q6_SQL = """
select
    sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
from
    lineitem
where
    L_SHIPDATE >= date '1994-01-01'
    and L_SHIPDATE < date '1994-01-01' + interval '1' year
    and L_DISCOUNT between .06 - 0.01 and .06 + 0.01
    and L_QUANTITY < 24
"""

Q7_SQL = """
select
    supp_nation,
    cust_nation,
    l_year,
    sum(volume) as revenue
from
    (
        select
            n1.N_NAME as supp_nation,
            n2.N_NAME as cust_nation,
            year(L_SHIPDATE) as l_year,
            L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volume
        from
            supplier,
            lineitem,
            orders,
            customer,
            nation n1,
            nation n2
        where
            S_SUPPKEY = L_SUPPKEY
            and O_ORDERKEY = L_ORDERKEY
            and C_CUSTKEY = O_CUSTKEY
            and S_NATIONKEY = n1.N_NATIONKEY
            and C_NATIONKEY = n2.N_NATIONKEY
            and (
                (n1.N_NAME = 'FRANCE' and n2.N_NAME = 'GERMANY')
                or (n1.N_NAME = 'GERMANY' and n2.N_NAME = 'FRANCE')
            )
            and date(L_SHIPDATE) between date '1995-01-01' and date '1996-12-31'
    ) as shipping
group by
    supp_nation,
    cust_nation,
    l_year
order by
    supp_nation,
    cust_nation,
    l_year
"""

def _load_tables(tables: list[str], load_table):
    for tbl in tables:
        load_table(tbl)


def q1(spark, load_table):
    _load_tables(["lineitem"], load_table)
    result = spark.sql(Q1_SQL)
    result.show()
    return result

def q2(spark, load_table):
    _load_tables(["region", "nation", "supplier", "part", "part_supp"], load_table)
    result = spark.sql(Q2_SQL)
    result.show()
    return result

def q3(spark, load_table):
    _load_tables(["customer",  "orders",  "line_item"], load_table) 
    result = spark.sql(Q3_SQL)
    result.show()
    return result

def q4(spark, load_table):
    _load_tables(["orders",  "line_item"], load_table) 
    result = spark.sql(Q4_SQL)
    result.show()
    return result

def q5(spark, load_table):
    _load_tables(["customer",  "orders",  "line_item",  "supplier",  "nation",  "region"], load_table) 
    result = spark.sql(Q5_SQL)
    result.show()
    return result

def q6(spark, load_table):
    _load_tables(["line_item"], load_table) 
    result = spark.sql(Q6_SQL)
    result.show()
    return result

def q7(spark, load_table):
    _load_tables(["customer",  "orders",  "line_item",  "supplier",  "nation"], load_table) 
    result = spark.sql(Q7_SQL)
    result.show()
    return result
