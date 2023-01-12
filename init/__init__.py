from altair.utils.data import limit_rows
from tqdm.notebook import tqdm, trange
import pyreadstat
from IPython.display import display
from pprint import pprint
import connectorx as cx
import random
from sqlalchemy import create_engine
from sklearn import preprocessing
from scipy.stats.mstats import winsorize
from scipy import stats
from pyspark.sql.window import Window
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark import pandas as ps
import pyspark.sql.functions as F
from pandas import to_numeric as tonum
from pandas import to_datetime as todate
from pandas import read_stata as rdta
from pandas import read_sql as rsql
from pandas import read_parquet as rpq
from pandas import read_csv as rcsv
from pandas import json_normalize
from loguru import logger
from icecream import ic
from rapidfuzz import fuzz
from clickhouse_driver import Client as ch_client
import wrds
import statsmodels.api as sm
import seaborn as sns
import requests
import pyarrow.parquet as pq
import pyarrow as pa
import pyarrow.csv
import psycopg2
import polars as p
from polars import col as c
import plotly.express as px
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

p.Config.set_tbl_rows(50)
p.Config.set_tbl_cols(100)
p.Config.set_fmt_str_lengths(200)

plt.style.use("tableau-colorblind10")
import duckdb as dk
import argparse
import csv
import fileinput
import hashlib
import io
import itertools
import json
import os
import pickle
import re
import socket
import sqlite3
import string
import sys
import tempfile
import time
import urllib
import uuid
import warnings
from collections import *
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timedelta
from glob import glob
from io import StringIO
from multiprocessing import Pool
from pathlib import Path

import altair as alt

alt.data_transformers.disable_max_rows()

ch_clt = ch_client("localhost")


def rch(query, df_type="pandas", cache="memory", force_read=False):
    hash = hashlib.md5(query.encode()).hexdigest()
    if df_type == "pandas":
        if os.path.exists(f"/tmp/{hash}") and not force_read:
            logger.info("use cached dataset")
            return rpq(f"/tmp/{hash}")
        x, y = ch_clt.execute(query, with_column_types=True)
        df = pd.DataFrame(x, columns=[c[0] for c in y])
        df.to_parquet(f"/tmp/{hash}")
        return df
    if df_type == "spark" or df_type == "spandas":
        logger.info(f"use {df_type}")
        if os.path.exists(f"/tmp/{hash}") and not force_read:
            logger.info("use cached dataset")
            if df_type == "spandas":
                sp.set_option("compute.default_index_type", "distributed")
                return sp.read_parquet(f"/tmp/{hash}")
            else:
                spark = spark_init()
                return spark.read.parquet(f"/tmp/{hash}")
        logger.info("cached dataset not found, run the query")
        # https://markelic.de/how-to-access-your-clickhouse-database-with-spark-in-python/
        # https://github.com/housepower/ClickHouse-Native-JDBC
        if (
            os.system(
                f"""clickhouse-client --query "{query} format Parquet" > /tmp/{hash} """.encode()
            )
            == 0
        ):
            if df_type == "spandas":
                sp.set_option("compute.default_index_type", "distributed")
                return sp.read_parquet(f"/tmp/{hash}")
            else:
                spark = spark_init()
                return spark.read.parquet(f"/tmp/{hash}")
        else:
            raise ValueError("query failed !! ")
    raise ValueError("must return pandas or spark dataframe")


def rcp(query, cache="memory", force_read=False):

    """
    read clichouse to polars
    """
    hash = hashlib.md5(query.encode()).hexdigest()
    if os.path.exists(f"/tmp/{hash}") and (not force_read):
        logger.info(f"use cached dataset {hash}")
        df = p.read_parquet(f"/tmp/{hash}")
        return df
    logger.info(f"cached dataset not used")
    df = p.read_sql(
        query,
        "mysql://default:@localhost:9004",
        protocol="text",
    )
    bytes = io.BytesIO()
    df.write_csv(bytes)
    df = pcsv(bytes.getvalue())
    if not force_read:
        logger.info(f"generating cache data {hash}")
        df.write_parquet(f"/tmp/{hash}", use_pyarrow=True)
    return df


def to_clickhouse(df, table):
    client = ch_client("localhost")
    return client.execute(f"INSERT INTO {table} VALUES", df.to_dicts())


def read(f, **kwargs):
    p = Path(f)
    if p.suffix == ".csv":
        try:
            logger.info("trying read csv with arrow")
            return pyarrow.csv.read_csv(f).to_pandas()
        except:
            logger.info("fall back to pandas")
            return rcsv(f, **kwargs)
    if p.suffix == ".dta":
        return rdta(f, **kwargs)
    if (
        p.suffix == ".pq"
        or p.suffix == ".pa"
        or p.suffix == ".parquet"
        or p.suffix == ".parq"
    ):
        return rpq(f, **kwargs)
    if f.suffix == ".sas7bdat":
        return pyreadstat.read_sas7bdat(f)[0]


def modify_polars_dtype(df):
    return df.with_columns(
        [
            p.col(p.UInt8).cast(p.Int64).keep_name(),
            p.col(p.UInt16).cast(p.Int64).keep_name(),
            p.col(p.UInt32).cast(p.Int64).keep_name(),
            p.col(p.UInt64).cast(p.Int64).keep_name(),
            p.col(p.Int16).cast(p.Int64).keep_name(),
            p.col(p.Int32).cast(p.Int64).keep_name(),
            p.col(p.Float32).cast(p.Float64).keep_name(),
        ]
    )


def pcsv(f):
    return p.read_csv(f, null_values=[""], parse_dates=True, infer_schema_length=None)


def read_p(f, **kwargs):
    f = Path(f)
    if f.suffix == ".csv":
        return p.read_csv(
            f, null_values=[""], parse_dates=True, infer_schema_length=None
        )
    if f.suffix == ".dta":
        df = p.from_pandas(rdta(f, **kwargs))
        return modify_polars_dtype(dfname(df))
    if (
        f.suffix == ".pq"
        or f.suffix == ".pa"
        or f.suffix == ".parquet"
        or f.suffix == ".parq"
    ):
        return modify_polars_dtype(
            dfname(p.read_parquet(f, use_pyarrow=True, **kwargs))
        )
    if f.suffix == ".sas7bdat":
        df = p.from_pandas(pd.read_sas(f, encoding="latin1"))
        return modify_polars_dtype(dfname(df))


sns.set()
sys.setrecursionlimit(100000000)

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 200)
pd.set_option("display.float_format", lambda x: "%.2f" % x)
pd.set_option("use_inf_as_na", True)
pd.options.display.max_colwidth = 100
pd.options.mode.chained_assignment = None  # default='warn'

DROPBOX = "/mnt/dd/Dropbox/"
if socket.gethostname() == "tr":
    from pandarallel import pandarallel

    wrdscon = "postgresql://leo@localhost:5432/wrds"
    factset = "postgresql://leo@localhost:5432/factset"
    con = create_engine("postgresql://leo@localhost:5432/leo")
    othercon = create_engine("postgresql://leo@localhost:5432/other")
    tmpcon = create_engine("postgresql://leo@localhost:5432/tmp")
else:
    wrdscon = "postgresql://leo: @129.94.138.180:5432/wrds"
    con = create_engine("postgresql://leo: @129.94.138.180:5432/leo")
    othercon = create_engine("postgresql://leo: @129.94.138.180:5432/other")
    tmpcon = create_engine("postgresql://leo: @129.94.138.180:5432/tmp")


def check_uniq(df, l):
    if isinstance(df, p.internals.dataframe.DataFrame):
        dup = df.groupby(l).count().filter(c("count") > 1)
        return df.join(dup, on=l)
    return df.groupby(l).size()[lambda s: s > 1]


wrdspath = Path("/mnt/nas/wrds_dataset/")


def rwrds(query, parse_dates=None):
    if (
        re.search(r".pq$", query)
        or re.search(r".pa$", query)
        or re.search(r".parquet$", query)
    ):
        return pd.read_parquet(f"/mnt/da/Dropbox/wrds_dataset/{query}")
    elif "select " in query or "SELECT " in query:
        try:
            return cx.read_sql("postgresql://leo@localhost:5432/wrds", query)
        except:
            return pd.read_sql(
                query, create_engine("postgresql://leo@localhost:5432/wrds")
            )

    elif len(re.findall(r"\w+", query)) == 1:
        return cx.read_sql(wrdscon, f"""select * from {query}""")


def read_other(query, parse_dates=None):
    df = pd.read_sql(query, othercon, parse_dates=parse_dates)
    return df


def genfyear(s, errors="raise"):
    if s.dtype == "<M8[ns]":
        return s.map(lambda x: x.year if x.month > 6 else x.year - 1)
    else:
        s = pd.to_datetime(s, errors=errors)
        return s.map(lambda x: x.year if x.month > 6 else x.year - 1)


def genfyear_spark(df, datecol):
    return df.withColumn(
        "fyear",
        F.when(F.month(datecol) > 6, F.year(datecol)).otherwise(F.year(datecol) - 1),
    )


def to_spark(df):
    uu = uuid.uuid1().hex
    f = f"/home/leo/tmp/{uu}"
    df.to_parquet(f, index=False)
    return spark.read.parquet(f)


def to_pandas(df):
    uu = uuid.uuid1().hex
    f = f"/home/leo/tmp/{uu}"
    df.write.parquet(f)
    return pd.read_parquet(f)


def spark_init():
    return (
        SparkSession.builder.config("spark.driver.memory", "200g")
        .config("spark.executor.memory", "150g")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.local.dir", "/home/leo/tmp")
        .config("spark.driver.maxResultSize", "0")
        .getOrCreate()
    )


def extract_table_names(query):
    """Extract table names from an SQL query."""
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(
        r"(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)", query, re.IGNORECASE
    )
    tables = [tbl for block in tables_blocks for tbl in re.findall(r"\w+", block)]
    return set(tables)


def winsor(df, varlist, *, by=None, limits=0.01):
    if by:
        for var in varlist:
            for bys in sorted(df[by].unique()):
                try:
                    winsorize(
                        df[df[by] == bys][var],
                        limits=limits,
                        nan_policy="omit",
                        inplace=True,
                    )
                except:
                    logger.warning(var, bys, " ---- failed")
    else:
        for var in varlist:
            winsorize(
                df[var],
                limits=limits,
                nan_policy="omit",
                inplace=True,
            )
    return df


def chk_val(s):
    return s.value_counts().sort_index()


def lagged(df, var, *, i, t, n=1):
    assert len(check_uniq(df, [i, t])) == 0
    du = df[[i, t, var]]
    du[t] = du[t] + n
    du = du.rename(columns={var: f"{var}_lag{n}"})
    return df.merge(du, how="left")


def tosql(df, table, con, schema=None, index=False):
    import io

    df.head(0).to_sql(
        table, con, schema=schema, if_exists="replace", index=index
    )  # truncates the table
    print("created schema ...")
    conn = con.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep="\t", header=False, index=index)
    output.seek(0)
    print("start to copy ...")
    cur.copy_from(output, table, null="")  # null values become ''
    conn.commit()
    print("finished")


def quarter_end(s):
    s = pd.to_datetime(s)
    qrt = s.dt.quarter
    end = {1: "-03-31", 2: "-06-30", 3: "-09-30", 4: "-12-31"}
    end = qrt.map(end)
    return pd.to_datetime(s.dt.year.astype(str) + end)


def cartesian(df1, df2=pd.DataFrame(range(20), columns=["plus"])):
    return (
        df1.assign(__key=1).merge(df2.assign(__key=1), on="__key").drop("__key", axis=1)
    )


def dfname(df):
    if isinstance(df, p.internals.dataframe.DataFrame):
        logger.info("Polars dataframe")
        return modify_polars_dtype(
            df.rename(
                {c: "_".join(re.findall("\w+", str(c).lower())) for c in df.columns}
            )
        )

    logger.info("Pandas dataframe")
    return df.rename(
        {c: "_".join(re.findall("\w+", str(c).lower())) for c in df.columns},
        axis=1,
    )


def spark_sql(sql, folder=None):
    import os

    tables = extract_table_names(sql)
    for tb in tables:
        if folder:
            path = os.path.join(folder, tb)
            spark.read.parquet(path).createOrReplaceTempView(tb)
        else:
            spark.createDataFrame(eval(tb)).createOrReplaceTempView(tb)
    return spark.sql(sql)


def ssql(sql):
    tables = extract_table_names(sql)
    for tb in tables:
        spark.createDataFrame(eval(tb)).createOrReplaceTempView(tb)
    return spark.sql(sql).toPandas()


def spark_pq(file, **kwargs):
    if "spark" not in vars():
        spark = spark_init()
    return spark.read.parquet(file)


def spark_csv(file, **kwargs):
    if "spark" not in vars():
        spark = spark_init()
    return spark.read.csv(
        file, inferSchema=True, enforceSchema=False, header=True, **kwargs
    )


def reg_tb(path, name=None):
    if name:
        tb = name
    else:
        tb = os.path.basename(path)
    spark.read.parquet(path).createOrReplaceTempView(tb)


def spark_pg(query=None, db="wrds"):
    if "spark" not in vars():
        spark = spark_init()
    return (
        spark.read.format("jdbc")
        .option("url", f"jdbc:postgresql://localhost:5432/{db}")
        .option("query", query)
        .load()
    )


def cosine(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def rpq_sql(query):
    from moz_sql_parser import parse

    d = parse(query)
    if isinstance(d["select"], list):
        columns = [c["value"] for c in d["select"]]
    else:
        columns = list(d["select"]["value"])
    if "*" in columns:
        columns = None
    df = rpq(d["from"], columns=columns)
    nc = dict()
    for c in d["select"]:
        if "name" in c:
            nc[c["value"]] = c["name"]
    return df.rename(nc, axis=1)


def writer(v, filename):
    with open(filename, "w") as f:
        wr = csv.writer(f)
        for x, y in v:
            wr.writerow([x, y])


def savepickle(var, filename):
    with open(filename, "wb") as f:
        return pickle.dump(var, f)


def loadpickle(filename):
    with open(filename, "rb") as f:
        return pickle.load(f)


def sedfile(string, filename):
    sta = string.split("/")
    if len(sta) == 4:
        _, input, output, oper = sta
    elif len(sta) == 3:
        _, input, oper = sta
    else:
        raise ValueError("cannot parse the input")

    if oper == "g":
        for line in fileinput.input(filename, inplace=1):
            if re.search(input, line):
                line = re.sub(input, output, line)
            sys.stdout.write(line)
    if oper == "d":
        for line in fileinput.input(filename, inplace=1):
            if re.search(input, line):
                continue
            sys.stdout.write(line)


def wget(url, fn=None):
    if not fn:
        fn = url.split("/")[-1]
    urllib.request.urlretrieve(url, fn)


def peekwrds(table):
    return rwrds(f"""select * from {table} limit 10""")


def create_table(df, table, ob, date_cols=[], load_data=True):
    df_to_load = df.copy()
    ds = df_to_load.dtypes.to_dict()
    for date_col in date_cols:
        df_to_load[date_col] = pd.to_datetime(df_to_load[date_col])
    with open("/tmp/tesdsfsad.txt", "w") as f:
        print(f"CREATE TABLE IF NOT EXISTS {table} (", file=f)
        for d in ds:
            if d == ob or d in ob.replace("(", "").replace(")", "").split(","):
                if "int" in str(ds[d]) or "Int" in str(ds[d]):
                    print(d, "Int64", ",", file=f)
                elif "float" in str(ds[d]):
                    print(d, "Float64", ",", file=f)
                elif "bool" in str(ds[d]):
                    print(d, "Boolean", ",", file=f)
                elif "datetime64[ns]" in str(ds[d]):
                    print(d, "Int32", ",", file=f)
                else:
                    print(d, "String", ",", file=f)
                continue
            if "int" in str(ds[d]) or "Int" in str(ds[d]):
                print(d, "Int64", " NULL,", file=f)
            elif "float" in str(ds[d]):
                print(d, "Float64", " NULL,", file=f)
            elif "bool" in str(ds[d]):
                print(d, "Boolean", " NULL,", file=f)
            elif "datetime64[ns]" in str(ds[d]):
                print(d, "Int32", " NULL,", file=f)
            else:
                print(d, "String", " NULL,", file=f)

        print(f"PRIMARY KEY {ob}", file=f)
        print(") engine=MergeTree()", file=f)
    #         print(')',file=f)
    with open("/tmp/tesdsfsad.txt") as f:
        c = ch_client("localhost")
        if c.execute(f"exists {table}")[0][0]:
            logger.warning("Table exists, dropping")
        c.execute(f"drop table if exists {table}")
        c.execute(f.read())
    for d in ds:
        if "datetime64[ns]" in str(ds[d]):
            df_to_load[d] = (
                pd.to_numeric(df_to_load[d].dt.strftime("%Y%m%d"))
                .astype("Int32")
                .astype(object)
                .replace(np.nan, None)
            )
            continue
        if "Int" in str(ds[d]) or "int" in str(ds[d]):
            df_to_load[d] = (
                df_to_load[d].astype("Int64").astype(object).replace(np.nan, None)
            )
            continue
        df_to_load[d] = df_to_load[d].astype(object).replace(np.nan, None)
    df_to_load.info()
    if load_data:
        return to_clickhouse(df_to_load, table)


def cleantype(df):
    df_to_load = df.copy()
    for c, t in df_to_load.dtypes.to_dict().items():
        if str(t) == "float64":
            try:
                df_to_load[c] = df_to_load[c].astype("Int64")
            except:
                pass
            continue
        if str(t) == "object":
            try:
                df_to_load[c] = todate(df_to_load[c])
            except:
                try:
                    df_to_load[c] = tonum(df_to_load[c])
                    try:
                        df_to_load[c] = df_to_load[c].astype("Int64")
                    except:
                        pass
                except:
                    df_to_load[c] = df_to_load[c].astype("string")
    return df_to_load


def info(ds, u=[]):
    if type(ds) == p.internals.dataframe.DataFrame:
        if u:
            display(check_uniq(ds, u))
        display(ds.describe())
        display(ds.select([p.all().null_count() / p.count()]))
        return ds

    return ds.info(show_counts=True, verbose=True)


def read_str(file, encoding="latin1", as_whole=False):
    with open(file, encoding=encoding) as f:
        if as_whole:
            return f.read()
        return [x.strip() for x in f.readlines()]


class csv_writer:
    def __init__(self, fn, title):
        self.fn = fn
        self.title = title

    def __enter__(self):
        self.fn = open(self.fn, "w")
        self.wr = csv.writer(self.fn)
        self.wr.writerow(self.title)
        return self

    def write(self, row):
        self.wr.writerow(row)

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.fn.close()


class pq_writer:
    def __init__(self, fn, schema):
        self.fn = fn
        self.schema = schema

    def __enter__(self):
        self.wr = pq.ParquetWriter(self.fn, schema=self.schema)
        return self

    def write(self, table):
        self.wr.write_table(pa.Table.from_pandas(table, preserve_index=False))

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.wr.close()


def rduck(query, db=""):
    if db:
        db = dk.connect(db, read_only=True)
        return dfname(db.execute(query).df())
    return dk.query(query).df()


def expand_ts(df, i, t, delta=1):
    df = df.sort_values([i, t])
    _min = df.groupby(i)[t].min()
    _max = df.groupby(i)[t].max()
    delta_i = (_max - _min).rename("_t").reset_index()
    a = _min.reset_index().merge(delta_i)
    res = []
    for r in a.values:
        for _t in np.arange(0, r[2], delta):
            res.append([r[0], r[1] + _t])
    return pd.DataFrame(res, columns=[i, t])


def stdize(s):
    return (s - s.mean()) / s.std()


# a few commands to use output


def wpar(s, x):
    x = Path(x)
    out = x.with_suffix(".pq")
    if isinstance(s, p.internals.dataframe.DataFrame):
        s.write_parquet(out, compression="snappy")
        return out, s.shape
    if isinstance(s, pd.core.series.Series):
        s.to_frame().to_parquet(out, index=False)
    else:
        s.to_parquet(out, index=False)
    print(out)


def wsta(s, x):
    x = Path(x).with_suffix(".dta")
    if isinstance(s, pd.core.series.Series):
        s.to_frame().to_stata(x, write_index=False, version=None)
        print(x)
    elif isinstance(s, pd.core.frame.DataFrame):
        s.to_stata(x, write_index=False, version=None)
        print(x)
    elif isinstance(s, p.internals.dataframe.DataFrame):
        s.to_pandas().to_stata(x, write_index=False, version=None)
        print(x)


def wcsv(s, x):
    x = Path(x)
    if isinstance(s, pd.core.series.Series):
        s.to_frame().to_csv(x.with_suffix(".csv"), index=False)
    else:
        s.to_csv(x.with_suffix(".csv"), index=False)


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def sasdate(df, date=None):
    if isinstance(df, p.internals.dataframe.DataFrame):
        return df.with_column(
            (p.date(1960, 1, 1) + p.duration(days=c(date))).alias(date)
        )
    else:
        return np.datetime64("1960-01-01") + pd.Timedelta(s, "d")


def vc(df, g):
    return df.groupby(g).agg(p.count()).sort("count", reverse=True)


def create_ch(df, table, ob, date_cols=[], load_data=True):
    with open("/tmp/tesdsfsad.txt", "w") as f:
        print(f"CREATE TABLE IF NOT EXISTS {table} (", file=f)
        ds = df.schema
        for d in ds:
            if d == ob or d in ob.replace("(", "").replace(")", "").split(","):
                if "Int" in str(ds[d]):
                    print(d, "Int64", ",", file=f)
                elif "Float" in str(ds[d]):
                    print(d, "Float64", ",", file=f)
                elif "Bool" in str(ds[d]):
                    print(d, "Boolean", ",", file=f)
                elif "Date" in str(ds[d]):
                    print(d, "Int32", ",", file=f)
                else:
                    print(d, "String", ",", file=f)
                continue
            if "Int" in str(ds[d]):
                print(d, "Int64", " NULL,", file=f)
            elif "Float" in str(ds[d]):
                print(d, "Float64", " NULL,", file=f)
            elif "Bool" in str(ds[d]):
                print(d, "Boolean", " NULL,", file=f)
            elif "Date" in str(ds[d]):
                print(d, "Int32", " NULL,", file=f)
            else:
                print(d, "String", " NULL,", file=f)

        print(f"PRIMARY KEY {ob}", file=f)
        print(") engine=MergeTree()", file=f)
    #         print(')',file=f)
    with open("/tmp/tesdsfsad.txt") as f:
        c = ch_client("localhost")
        if c.execute(f"exists {table}")[0][0]:
            logger.warning("Table exists, dropping")
        c.execute(f"drop table if exists {table}")
        c.execute(f.read())
    for d in ds:
        if "Date" in str(ds[d]):
            df = df.with_column(
                p.col(d).dt.year() * 10000
                + p.col(d).dt.month() * 100
                + p.col(d).dt.day()
            )
            continue
    if load_data:
        print("start to load")
        return to_clickhouse(df, table)


def convert_inf(df):
    df = modify_polars_dtype(df).with_columns(
        p.when(p.col(p.Float64).is_infinite())
        .then(None)
        .otherwise(p.col(p.Float64))
        .keep_name()
    )

    return df.fill_nan(None)


def winsor_float(df, limits=(0.01, 0.99)):
    df = convert_inf(df)
    return df.with_columns(
        [
            p.when(p.col(p.Float64) > p.col(p.Float64).quantile(limits[1]))
            .then(p.col(p.Float64).quantile(limits[1]))
            .otherwise(p.col(p.Float64))
        ]
    ).with_columns(
        [
            p.when(p.col(p.Float64) < p.col(p.Float64).quantile(limits[0]))
            .then(p.col(p.Float64).quantile(limits[0]))
            .otherwise(p.col(p.Float64))
        ]
    )
