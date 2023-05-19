import argparse
import pathlib

SCHEMA = {
    "part": [
        "P_PARTKEY",
        "P_NAME",
        "P_MFGR",
        "P_BRAND",
        "P_TYPE",
        "P_SIZE",
        "P_CONTAINER",
        "P_RETAILPRICE",
        "P_COMMENT",
    ],
    "partsupp": [
        "PS_PARTKEY",
        "PS_SUPPKEY",
        "PS_AVAILQTY",
        "PS_SUPPLYCOST",
        "PS_COMMENT",
    ],
    "supplier": [
        "S_SUPPKEY",
        "S_NAME",
        "S_ADDRESS",
        "S_NATIONKEY",
        "S_PHONE",
        "S_ACCTBAL",
        "S_COMMENT",
    ],
    "customer": [
        "C_CUSTKEY",
        "C_NAME",
        "C_ADDRESS",
        "C_NATIONKEY",
        "C_PHONE",
        "C_ACCTBAL",
        "C_MKTSEGMENT",
        "C_COMMENT",
    ],
    "orders": [
        "O_ORDERKEY",
        "O_CUSTKEY",
        "O_ORDERSTATUS",
        "O_TOTALPRICE",
        "O_ORDERDATE",
        "O_ORDERPRIORITY",
        "O_CLERK",
        "O_SHIPPRIORITY",
        "O_COMMENT",
    ],
    "lineitem": [
        "L_ORDERKEY",
        "L_PARTKEY",
        "L_SUPPKEY",
        "L_LINENUMBER",
        "L_QUANTITY",
        "L_EXTENDEDPRICE",
        "L_DISCOUNT",
        "L_TAX",
        "L_RETURNFLAG",
        "L_LINESTATUS",
        "L_SHIPDATE",
        "L_COMMITDATE",
        "L_RECEIPTDATE",
        "L_SHIPINSTRUCT",
        "L_SHIPMODE",
        "L_COMMENT",
    ],
    "nation": [
        "N_NATIONKEY",
        "N_NAME",
        "N_REGIONKEY",
        "N_COMMENT",
    ],
    "region": [
        "R_REGIONKEY",
        "R_NAME",
        "R_COMMENT",
    ],
}


def main(filepath: str):
    s = pathlib.Path(filepath).read_text()
    for tbl in SCHEMA:
        for column in SCHEMA[tbl]:
            s = s.replace(column.lower(), column)
    pathlib.Path(filepath).write_text(s)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("filepath")
    args = parser.parse_args()

    main(args.filepath)
