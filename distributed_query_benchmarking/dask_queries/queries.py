###
# Adapted from: https://github.com/pola-rs/tpch/tree/main/dask_queries
###

import dask.dataframe as dd
from datetime import date, datetime


def q1(get_df):
    lineitem = get_df("lineitem")

    VAR1 = datetime(year=1998, month=9, day=2)

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
    sel = dd.to_datetime(lineitem_filtered.L_SHIPDATE) <= VAR1
    lineitem_filtered = lineitem_filtered[sel].copy()
    lineitem_filtered["sum_qty"] = lineitem_filtered.L_QUANTITY
    lineitem_filtered["sum_base_price"] = lineitem_filtered.L_EXTENDEDPRICE
    lineitem_filtered["avg_qty"] = lineitem_filtered.L_QUANTITY
    lineitem_filtered["avg_price"] = lineitem_filtered.L_EXTENDEDPRICE
    lineitem_filtered["sum_disc_price"] = lineitem_filtered.L_EXTENDEDPRICE * (
        1 - lineitem_filtered.L_DISCOUNT
    )
    lineitem_filtered["sum_charge"] = (
        lineitem_filtered.L_EXTENDEDPRICE
        * (1 - lineitem_filtered.L_DISCOUNT)
        * (1 + lineitem_filtered.L_TAX)
    )
    lineitem_filtered["avg_disc"] = lineitem_filtered.L_DISCOUNT
    lineitem_filtered["count_order"] = lineitem_filtered.L_ORDERKEY
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

    result_df = (
        total.compute().reset_index().sort_values(["L_RETURNFLAG", "L_LINESTATUS"])
    )

    return result_df
