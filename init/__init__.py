import csv
import fileinput
from loguru import logger
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
from datetime import datetime, timedelta
from glob import glob
from io import StringIO
from multiprocessing import Pool
from pathlib import Path
from clickhouse_driver import Client as ch_client
import pyarrow.parquet as pq
import pyarrow as pa
import duckdb as dk
import polars as p

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import psycopg2
import pyspark.sql.functions as F
import requests
import seaborn as sns
import statsmodels.api as sm
import wrds
from fuzzywuzzy import fuzz
from pandas import json_normalize
from pandas import read_csv as rcsv
from pandas import read_parquet as rpq
from pandas import read_sql as rsql
from pandas import read_stata as rdta
from pandas import to_datetime as todate
from pandas import to_numeric as tonum
from pyspark import *
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
from scipy import stats
from scipy.stats.mstats import winsorize
from sklearn import preprocessing
from sqlalchemy import create_engine
from tqdm import tqdm, trange

import plotly.express as px
import hashlib

ch_clt = ch_client("localhost")


def rch(query, df_type="pandas", cache="memory"):
    if df_type == "pandas":
        x, y = ch_clt.execute(query, with_column_types=True)
        return pd.DataFrame(x, columns=[c[0] for c in y])
    if df_type == "spark" or df_type == "polars":
        # https://markelic.de/how-to-access-your-clickhouse-database-with-spark-in-python/
        # https://github.com/housepower/ClickHouse-Native-JDBC
        hash = hashlib.md5(query.encode()).hexdigest()
        if os.path.exists(f"/tmp/{hash}"):
            logger.info("use cached dataset")
            return spark.read.parquet(f"/tmp/{hash}")
        logger.info("cached dataset not found, run the query")
        if (
            os.system(
                f"""clickhouse-client --query "{query} format Parquet" > /tmp/{hash}
                """.encode()
            )
            == 0
        ):
            if df_type == "polars":
                return p.read_parquet(f"/tmp/{hash}")
            else:
                return spark.read.parquet(f"/tmp/{hash}")
        else:
            raise ValueError("query failed !! ")
    raise ValueError("must return pandas or spark dataframe")


def to_clickhouse(df, table):
    client = ch_client("localhost")
    return client.execute(f"INSERT INTO {table} VALUES", df.to_dict(orient="records"))


def read(f, **kwargs):
    if re.search("\.csv$", f):
        try:
            logger.info("trying read csv with arrow")
            return pa.csv.read_csv(f, **kwargs).to_pandas()
        except:
            logger.info("fall back to pandas")
            return rcsv(f, **kwargs)
    if re.search("\.dta$", f):
        return rdta(f, **kwargs)
    if re.search("(\.pq|\.parquet|\.par)$", f):
        return rpq(f, **kwargs)
    if re.search("\.sas7bdat$", f):
        return pd.read_sas(f, encoding="latin1", **kwargs)


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

    wrdscon = create_engine("postgresql://leo@localhost:5432/wrds")
    con = create_engine("postgresql://leo@localhost:5432/leo")
    othercon = create_engine("postgresql://leo@localhost:5432/other")
    tmpcon = create_engine("postgresql://leo@localhost:5432/tmp")
else:
    wrdscon = create_engine("postgresql://leo: @129.94.138.139:5432/wrds")
    con = create_engine("postgresql://leo: @129.94.138.139:5432/leo")
    othercon = create_engine("postgresql://leo: @129.94.138.139:5432/other")
    tmpcon = create_engine("postgresql://leo: @129.94.138.139:5432/tmp")


def check_uniq(df, l):
    return df.groupby(l).size()[lambda s: s > 1]


wrdspath = Path("/mnt/db/wrds_dataset/")


def rwrds(query, parse_dates=None):
    if (
        re.search(r".pq$", query)
        or re.search(r".pa$", query)
        or re.search(r".parquet$", query)
    ):
        return pd.read_parquet(f"/mnt/da/Dropbox/wrds_dataset/{query}")
    elif "select " in query or "SELECT " in query:
        return pd.read_sql(query, wrdscon, parse_dates=parse_dates)
    elif len(re.findall(r"\w+", query)) == 1:
        return pd.read_sql(
            f"""select * from {query}""", wrdscon, parse_dates=parse_dates
        )


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


def initspark():
    global spark
    spark = SparkSession.builder.getOrCreate()
    return spark


def extract_table_names(query):
    """Extract table names from an SQL query."""
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(
        r"(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)", query, re.IGNORECASE
    )
    tables = [tbl for block in tables_blocks for tbl in re.findall(r"\w+", block)]
    return set(tables)


def winsor(df, varlist, *, by=None, limit=0.01):
    if by:
        for var in varlist:
            for bys in sorted(df[by].unique()):
                try:
                    df.loc[(df[var].notnull()) & (df[by] == bys), var] = winsorize(
                        df.loc[(df[var].notnull()) & (df[by] == bys), var], limits=limit
                    )
                except:
                    print(var, bys, " ---- failed")
    else:
        for var in varlist:
            print(var)
            df.loc[(df[var].notnull()), var] = winsorize(
                df.loc[(df[var].notnull()), var], limits=limit
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
    df.columns = [x.lower() for x in list(df)]
    for c in list(df):
        df.rename({c: "_".join(re.findall("\w+", c))}, axis=1, inplace=True)
    return df


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
    return spark.read.parquet(file)


def spark_csv(file, **kwargs):
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
                .replace([np.nan], [None])
            )
            continue
        if "Int" in str(ds[d]) or "int" in str(ds[d]):
            df_to_load[d] = df_to_load[d].astype("Int64").replace([np.nan], [None])
            continue
        df_to_load[d] = df_to_load[d].replace([np.nan], [None])
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
                    df_to_load[c] = df_to_load[c].astype(str)
    return df_to_load


def info(ds):
    return ds.info(show_counts=True, verbose=True)


def read_str(file, encoding="latin1", as_whole=False):
    with open(file, encoding=encoding) as f:
        if as_whole:
            return f.read()
        return f.readlines()


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
        self.wr.write_table(pa.Table.from_pandas(table))

    def __exit__(self, exc_type, exc_value, exc_tb):
        self.wr.close()


def rduck(query, db=""):
    if db:
        db = dk.connect(db, read_only=True)
        return dfname(db.execute(query).df())
    return dk.query(query).df()
