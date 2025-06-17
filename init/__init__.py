import dbm
import random
import shelve
import shutil
import subprocess
from pprint import pprint

import connectorx as cx
import hvplot
import matplotlib as mpl
import matplotlib.pyplot as plt

# plotting defaults
from joblib import dump, load
from pandarallel import pandarallel

import numpy as np
import pandas as pd
import plotly.express as px
import polars as p
import psycopg2
import pyarrow as pa
import pyarrow.csv
import pyarrow.parquet as pq
import pyreadstat
import pyspark
import pyspark.sql.functions as F
import requests
import seaborn as sns
import statsmodels.api as sm
import wrds
from altair.utils.data import limit_rows
from clickhouse_driver import Client as ch_client
from icecream import ic
from IPython.display import display
from loguru import logger
from pandas import json_normalize
from pandas import read_csv as rcsv
from pandas import read_parquet as rpq
from pandas import read_sql as rsql
from pandas import read_stata as rdta
from pandas import to_datetime as todate
from pandas import to_numeric as tonum
from polars import col as c
from pyspark.sql import *
from pyspark.sql.functions import col
from pyspark.sql.types import *
from pyspark.sql.window import Window
from rapidfuzz import fuzz
from scipy import stats
from scipy.stats.mstats import winsorize
from sklearn import preprocessing
from sqlalchemy import create_engine
from tqdm.notebook import tqdm, trange

p.Config.set_tbl_rows(50)
p.Config.set_tbl_cols(100)
p.Config.set_fmt_str_lengths(200)

plt.style.use("tableau-colorblind10")
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
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, datetime, timedelta
from glob import glob
from io import StringIO
import multiprocessing
from multiprocessing import Pool
from pathlib import Path

import duckdb as dk

multiprocessing.set_start_method("spawn")

ch_clt = ch_client("localhost")
pd.set_option("use_inf_as_na", True)


@p.api.register_dataframe_namespace("w")
class Write:
    def __init__(self, df: p.DataFrame):
        self._df = df

    def pq(self, outfile, validate=[]):
        if validate:
            assert (
                self._df.group_by(validate).count().filter(c("count") > 1).is_empty()
            ), "Not Unique"
        out = Path(outfile).with_suffix(".pq")
        print(out, self._df.shape)
        df = convert_inf(self._df)
        if df.shape[1] > 2:
            df.sort(df.columns[:2]).write_parquet(out)
        else:
            df.sort(df.columns[0]).write_parquet(out)

    def dta(self, outfile, validate=[]):
        if validate:
            assert (
                self._df.group_by(validate).count().filter(c("count") > 1).is_empty()
            ), "Not Unique"
        out = Path(outfile).with_suffix(".dta")
        print(out, self._df.shape)
        convert_inf(self._df).to_pandas().to_stata(out, write_index=False)

    def csv(self, outfile, validate=[]):
        if validate:
            assert (
                self._df.group_by(validate).count().filter(c("count") > 1).is_empty()
            ), "Not Unique"
        out = Path(outfile).with_suffix(".csv")
        print(out, self._df.shape)
        df = convert_inf(self._df)
        if df.shape[1] > 2:
            df.sort(df.columns[:2]).write_csv(out)
        else:
            df.sort(df.columns[0]).write_csv(out)

    def excel(self, outfile):
        out = Path(outfile).with_suffix(".xlsx")
        print(out, self._df.shape)
        convert_inf(self._df).to_pandas().to_excel(out, index=False)


@p.api.register_dataframe_namespace("a")
class A:
    def __init__(self, df: p.DataFrame):
        self._df = df

    def uq(self, on):
        return self._df.group_by(on).count().filter(c("count") > 1)

    def vc(self, vars):
        return vc(self._df, vars)

    def fyear(self, vars):
        return self._df.with_columns(
            p.when(c(vars).dt.month() < 7)
            .then((c(vars).dt.year() - 1).cast(int))
            .otherwise(c(vars).dt.year().cast(int))
        )

    def sasdate(self, vars):
        cols = [vars] if isinstance(vars, str) else vars
        for col in cols:
            self._df = self._df.with_columns(
                (p.date(1960, 1, 1) + p.duration(days=c(col))).alias(col)
            )
        return self._df

    def ms(self):
        return self._df.select([p.all().null_count() * 100 / p.count()])

    def convert_inf(self):
        return convert_inf(self._df)

    def info(self):
        return info(self._df)

    def _join(self, df1, df2, on=[], how="inner"):
        keys = list(set(df1.columns) & set(df2.columns))
        if not on:
            on = keys
        print(f"common column: {keys}, Joining on {on}")
        count = self._df.group_by(on).count()
        print(
            f"""{(count.filter(c("count")>1).shape[0]*100 / count.shape[0]):.2f}% Duplicates on df_left"""
        )
        count = df2.group_by(on).count()
        print(
            f"""{(count.filter(c("count")>1).shape[0]*100 / count.shape[0]):.2f}% Duplicates on df_right"""
        )
        return self._df.join(df2, on=on, how=how, coalesce=True)

    def join(self, df2: p.DataFrame, on=[]):
        return self._join(self._df, df2, on=on)

    def left_join(self, df2: p.DataFrame, on=[]):
        return self._join(self._df, df2, on=on, how="left")

    def full_join(self, df2: p.DataFrame, on=[]):
        return self._join(self._df, df2, on=on, how="full")

    def u(self, keys):
        return self._df.group_by(keys).count().filter(c("count") > 1).sort("count")

    def winsor_float(self, limits=(0.01, 0.99)):
        df = convert_inf(self._df)
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


def rch(query, df_type="pandas", cache=None, reload=False):
    hash = hashlib.md5(query.encode()).hexdigest()
    if cache == "memory":
        path = f"/tmp/{hash}"
    else:
        path = f"/home/leo/tmp/{hash}"
    if df_type == "pandas":
        if os.path.exists(path) and not reload:
            logger.info("use cached dataset")
            return rpq(path)
        x, y = ch_clt.execute(query, with_column_types=True)
        df = pd.DataFrame(x, columns=[c[0] for c in y])
        df.to_parquet(path)
        return df
    if df_type == "spark" or df_type == "spandas":
        logger.info(f"use {df_type}")
        if os.path.exists(path) and not reload:
            logger.info("use cached dataset")
            if df_type == "spandas":
                sp.set_option("compute.default_index_type", "distributed")
                return sp.read_parquet(path)
            else:
                spark = spark_init()
                return spark.read.parquet(path)
        logger.info("cached dataset not found, run the query")
        # https://markelic.de/how-to-access-your-clickhouse-database-with-spark-in-python/
        # https://github.com/housepower/ClickHouse-Native-JDBC
        if (
            os.system(
                f"""clickhouse-client --query "{query} format Parquet" > {path} """.encode()
            )
            == 0
        ):
            if df_type == "spandas":
                sp.set_option("compute.default_index_type", "distributed")
                return sp.read_parquet(path)
            else:
                spark = spark_init()
                return spark.read.parquet(path)
        else:
            raise ValueError("query failed !! ")
    raise ValueError("must return pandas or spark dataframe")


def rcp(query, cache="memory", reload=False):
    """
    read clickhouse to polars
    """
    hash = hashlib.md5(query.encode()).hexdigest()
    path = f"/home/leo/tmp/{hash}"
    if reload or (not os.path.exists(path)):
        logger.info(f"generating cache data {hash}")
        subprocess.run(
            f"""clickhouse-client -q "{query} format CSVWithNames" > {path}""",
            shell=True,
        )
        df = p.read_csv(
            path,
            infer_schema_length=None,
            null_values=["", r"\N", "nan"],
            try_parse_dates=True,
        )
        df.write_parquet(path, use_pyarrow=True)
        return df
    else:
        logger.info(f"use cached dataset {hash}")
        return p.read_parquet(path)


def rcd(query):
    """
    read duckdb to polars
    """
    with dk.connect("/home/leo/da/Dropbox/toolbox/d.duckdb", read_only=True) as con:
        return con.sql(query).pl()


def rcs(query, cache="memory", reload=False):
    """
    read clichouse to Spark
    """
    hash = hashlib.md5(query.encode()).hexdigest()
    path = f"/home/leo/tmp/{hash}"
    if reload or (not os.path.exists(path)):
        logger.info(f"generating cache data {hash}")
        subprocess.run(
            f"""clickhouse-client -q "{query} format CSVWithNames" > {path}""",
            shell=True,
        )
        df = p.read_csv(path, infer_schema_length=100_000, null_values=["", r"\N"])
        df.write_parquet(path, use_pyarrow=True)
    else:
        logger.info(f"use cached dataset {hash}")
    spark = spark_init()
    return spark.read.parquet(path)


def to_clickhouse(df, table, partition=None):
    client = ch_client("localhost")
    if partition:
        for ds in tqdm(df.partition_by(partition)):
            client.execute(f"INSERT INTO {table} VALUES", ds.to_dicts())
    else:
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
    if p.suffix == ".sas7bdat":
        return pyreadstat.read_sas7bdat(f)[0]


def modify_polars_dtype(df):
    return df.with_columns(
        [
            p.col(p.UInt8).cast(p.Int64).name.keep(),
            p.col(p.UInt16).cast(p.Int64).name.keep(),
            p.col(p.UInt32).cast(p.Int64).name.keep(),
            p.col(p.UInt64).cast(p.Int64).name.keep(),
            p.col(p.Int16).cast(p.Int64).name.keep(),
            p.col(p.Int32).cast(p.Int64).name.keep(),
            p.col(p.Float32).cast(p.Float64).name.keep(),
        ]
    )


def pcsv(f):
    return p.read_csv(
        f, null_values=[""], try_parse_dates=True, infer_schema_length=None
    )


def read_p(f, normalize=True):
    f = Path(f)
    if f.suffix == ".csv":
        return dfname(
            p.read_csv(
                f, null_values=[""], try_parse_dates=True, infer_schema_length=None
            )
        )
    if f.suffix == ".tsv":
        return dfname(
            p.read_csv(
                f,
                null_values=[""],
                try_parse_dates=True,
                infer_schema_length=None,
                separator="\t",
            )
        )
    if f.suffix == ".dta":
        df = p.from_pandas(rdta(f))
        return modify_polars_dtype(dfname(df))
    if (
        f.suffix == ".pq"
        or f.suffix == ".pa"
        or f.suffix == ".parquet"
        or f.suffix == ".parq"
    ):
        return modify_polars_dtype(dfname(p.read_parquet(f)))
    if f.suffix == ".sas7bdat":
        df = p.from_pandas(pd.read_sas(f, encoding="latin1"))
        return modify_polars_dtype(dfname(df))
    if f.suffix == ".xlsx":
        return dfname(p.read_excel(f))


sns.set()
sys.setrecursionlimit(100000000)

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_colwidth", 200)
pd.set_option("display.max_rows", 200)
pd.set_option("display.float_format", lambda x: "%.2f" % x)
pd.set_option("use_inf_as_na", True)
pd.options.mode.chained_assignment = None  # default='warn'

DROPBOX = "/mnt/da/Dropbox/"

wrdscon = "postgresql://leo@localhost:5432/wrds"
factset = "postgresql://leo@localhost:5432/factset"

con = create_engine("postgresql://leo@localhost:5432/factset")


def check_uniq(df, l):
    if isinstance(df, p.DataFrame):
        dup = df.group_by(l).count().filter(c("count") > 1)
        return df.join(dup, on=l).sort(l)
    return df.groupby(l).size()[lambda s: s > 1]


wrdspath = Path("/mnt/nas/wrds_dataset/")


def rwrds(query, df_type="pandas"):
    if df_type == "pandas":
        reader = pd
        sql = pd.read_sql
    elif df_type == "polars":
        reader = p
        sql = p.read_database_uri
    else:
        raise ValueError("must be pandas or polars df")

    if (
        re.search(r".pq$", query)
        or re.search(r".pa$", query)
        or re.search(r".parquet$", query)
    ):
        return reader.read_parquet(f"/mnt/da/Dropbox/wrds_dataset/{query}")
    elif "select " in query or "SELECT " in query:
        return sql(query, "postgresql://leo@localhost:5432/wrds")

    elif len(re.findall(r"\w+", query)) == 1:
        return sql(f"""select * from {query}""", wrdscon)


def pwrds(query):
    if (
        re.search(r".pq$", query)
        or re.search(r".pa$", query)
        or re.search(r".parquet$", query)
    ):
        return p.read_parquet(f"/mnt/da/Dropbox/wrds_dataset/{query}")
    elif "select " in query or "SELECT " in query:
        return p.read_database(query, "postgresql://leo@localhost:5432/wrds")
    elif len(re.findall(r"\w+", query)) == 1:
        return p.read_database(wrdscon, f"""select * from {query}""")


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


def spark_init(mem=50):
    print(f"{mem}g")
    return (
        SparkSession.builder.config("spark.driver.memory", f"{mem}g")
        .config("spark.executor.memory", f"{int(mem/10)}g")
        .config("spark.sql.parquet.compression.codec", "zstd")
        .config("spark.local.dir", f"{os.environ['HOME']}/tmp")
        .config("spark.driver.maxResultSize", "0")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .config("spark.sql.execution.arrow.pyspark.fallback.enabled", "false")
        .getOrCreate()
    )


def spark2polars(spark_df):
    return p.from_arrow(pa.Table.from_batches(spark_df._collect_as_arrow()))


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
    if isinstance(df, p.DataFrame):
        # logger.info("Polars dataframe")
        return modify_polars_dtype(
            df.rename(
                {c: "_".join(re.findall(r"\w+", str(c).lower())) for c in df.columns}
            )
        )

    elif isinstance(df, pyspark.sql.dataframe.DataFrame):
        logger.info("spark dataframe")
        for column in df.columns:
            df = df.withColumnRenamed(
                column, "_".join(re.findall(r"\w+", str(column).lower()))
            )
        return df

    elif isinstance(df, pd.core.frame.DataFrame):
        logger.info("Pandas dataframe")
        return df.rename(
            {c: "_".join(re.findall(r"\w+", str(c).lower())) for c in df.columns},
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
        global spark
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


def load_dbm(filename, key):
    with dbm.open(filename, "r") as f:
        return f[key].decode()


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
    if type(ds) == p.DataFrame:
        if u:
            display(check_uniq(ds, u))
        display(ds.describe())
        display(ds.select([p.all().null_count() / p.len() * 100]))
        return ds

    return ds.info(show_counts=True, verbose=True)


def read_str(file, encoding="latin1"):
    with open(file, encoding=encoding) as f:
        return f.read().splitlines()


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
        return dfname(db.execute(query).pl())
    return dk.query(query).pl()


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
    return pd.DataFrame(res, columns=[i, t]).sort_values([i, t])


def stdize(s):
    return (s - s.mean()) / s.std()


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def sasdate(df, date=None):
    if isinstance(df, p.DataFrame):
        return df.with_columns(
            (p.date(1960, 1, 1) + p.duration(days=c(date))).alias(date)
        )
    else:
        return np.datetime64("1960-01-01") + pd.to_timedelta(df[date], "d")


def vc(df, g):
    return (
        df.group_by(g)
        .agg(p.count())
        .filter(c("count") > 1)
        .sort(["count"], descending=True)
    )


def create_ch(df, table, ob, date_cols=[], load_data=True, partition=None):
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
            df = df.with_columns(
                p.col(d).dt.year() * 10000
                + p.col(d).dt.month() * 100
                + p.col(d).dt.day()
            )
            continue
    if load_data:
        print("start to load")
        print(f"loaded {to_clickhouse(df, table, partition)}")
    return df


def convert_inf(df):
    df = modify_polars_dtype(df).with_columns(
        p.when(p.col(p.Float64).is_infinite())
        .then(None)
        .otherwise(p.col(p.Float64))
        .name.keep()
    )


    return df.fill_nan(None)


def update_dbm(dbfile, key, value):
    """
    Update a key in a dbm file
    Pass dbfile, key, value as arguments
    """
    with dbm.open(dbfile, "c") as db:
        db[key] = value
        print("Key: ", key)
        print("Value: ", value)


def grep(s, file, shift=0):
    d = read_str(file)
    for i, r in enumerate(d):
        if s in r:
            yield d[i + shift]


def egrep(s, file):
    regex = re.compile(s)
    return [r for r in read_str(file) if regex.search(r)]


# plt.rcParams['axes.edgecolor'] = 'black'
# plt.rcParams['axes.labelcolor'] = 'black'
# plt.rcParams['xtick.color'] = 'black'
# plt.rcParams['ytick.color'] = 'black'
plt.rcParams["figure.facecolor"] = "white"
plt.rcParams["axes.facecolor"] = "white"
plt.rcParams["grid.color"] = "grey"
plt.rcParams["grid.linestyle"] = "-"
plt.rcParams["grid.linewidth"] = 0.5
