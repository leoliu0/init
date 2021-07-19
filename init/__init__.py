import csv
import fileinput
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
from tqdm import tqdm

import plotly.express as px

sns.set()
sys.setrecursionlimit(100000000)

pd.set_option("display.max_columns", 100)
pd.set_option("display.max_rows", 200)
pd.set_option('display.float_format', lambda x: '%.2f' % x)
pd.set_option('use_inf_as_na', True)
pd.options.display.max_colwidth = 100
pd.options.mode.chained_assignment = None  # default='warn'

DROPBOX = '/mnt/dd/Dropbox/'
if socket.gethostname() == 'tr':
    from pandarallel import pandarallel
    wrdscon = create_engine('postgresql://leo@localhost:5432/wrds')
    con = create_engine('postgresql://leo@localhost:5432/leo')
    othercon = create_engine('postgresql://leo@localhost:5432/other')
    tmpcon = create_engine('postgresql://leo@localhost:5432/tmp')
else:
    wrdscon = create_engine('postgresql://leo: @129.94.138.139:5432/wrds')
    con = create_engine('postgresql://leo: @129.94.138.139:5432/leo')
    othercon = create_engine('postgresql://leo: @129.94.138.139:5432/other')
    tmpcon = create_engine('postgresql://leo: @129.94.138.139:5432/tmp')


def check_uniq(df, l):
    return df.groupby(l).size()[lambda s: s > 1]


wrdspath = Path('/mnt/db/wrds_dataset/')


def rwrds(query, parse_dates=None):
    if re.search(r'.pq$', query) or re.search(r'.pa$', query) or re.search(
            r'.parquet$', query):
        return pd.read_parquet(f'/mnt/da/Dropbox/wrds_dataset/{query}')
    elif 'select ' in query or 'SELECT ' in query:
        return pd.read_sql(query, wrdscon, parse_dates=parse_dates)
    elif len(re.findall(r'\w+', query)) == 1:
        return pd.read_sql(f'''select * from {query}''',
                           wrdscon,
                           parse_dates=parse_dates)


def read_other(query, parse_dates=None):
    df = pd.read_sql(query, othercon, parse_dates=parse_dates)
    return df


def genfyear(s):
    if s.dtype == '<M8[ns]':
        return s.map(lambda x: x.year if x.month > 6 else x.year - 1)
    else:
        s = pd.to_datetime(s)
        return s.map(lambda x: x.year if x.month > 6 else x.year - 1)


def to_spark(df):
    uu = uuid.uuid1().hex
    f = f'/home/leo/tmp/{uu}'
    df.to_parquet(f, index=False)
    return spark.read.parquet(f)


def to_pandas(df):
    uu = uuid.uuid1().hex
    f = f'/home/leo/tmp/{uu}'
    df.write.parquet(f)
    return pd.read_parquet(f)


def initspark():
    return SparkSession.builder.getOrCreate()


def extract_table_names(query):
    """ Extract table names from an SQL query. """
    # a good old fashioned regex. turns out this worked better than actually parsing the code
    tables_blocks = re.findall(r'(?:FROM|JOIN)\s+(\w+(?:\s*,\s*\w+)*)', query,
                               re.IGNORECASE)
    tables = [
        tbl for block in tables_blocks for tbl in re.findall(r'\w+', block)
    ]
    return set(tables)


def winsor(df, varlist, *, by=None, limit=0.01):
    if by:
        for var in varlist:
            for bys in sorted(df[by].unique()):
                try:
                    df.loc[(df[var].notnull()) & (df[by] == bys),
                           var] = winsorize(df.loc[(df[var].notnull()) &
                                                   (df[by] == bys), var],
                                            limits=limit)
                except:
                    print(var, bys, ' ---- failed')
    else:
        for var in varlist:
            print(var)
            df.loc[(df[var].notnull()),
                   var] = winsorize(df.loc[(df[var].notnull()), var],
                                    limits=limit)
    return df


def chk_val(s):
    return s.value_counts().sort_index()


def lagged(df, var, *, i, t, n=1):
    assert len(check_uniq(df, [i, t])) == 0
    du = df[[i, t, var]]
    du[t] = du[t] + n
    du = du.rename(columns={var: f'{var}_lag{n}'})
    return df.merge(du, how='left')


def tosql(df, table, con, schema=None, index=False):
    import io
    df.head(0).to_sql(table,
                      con,
                      schema=schema,
                      if_exists='replace',
                      index=index)  #truncates the table
    print('created schema ...')
    conn = con.raw_connection()
    cur = conn.cursor()
    output = io.StringIO()
    df.to_csv(output, sep='\t', header=False, index=index)
    output.seek(0)
    print('start to copy ...')
    cur.copy_from(output, table, null="")  # null values become ''
    conn.commit()
    print('finished')


def quarter_end(s):
    s = pd.to_datetime(s)
    qrt = s.dt.quarter
    end = {1: '-03-31', 2: '-06-30', 3: '-09-30', 4: '-12-31'}
    end = qrt.map(end)
    return pd.to_datetime(s.dt.year.astype(str) + end)


def cartesian(df1, df2=pd.DataFrame(range(20), columns=['plus'])):
    return df1.assign(__key=1).merge(df2.assign(__key=1),
                                     on='__key').drop('__key', axis=1)


def dfname(df):
    df.columns = [x.lower() for x in list(df)]
    for c in list(df):
        df.rename({c: '_'.join(re.findall('\w+', c))}, axis=1, inplace=True)
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
    return spark.read.csv(file,
                          inferSchema=True,
                          enforceSchema=False,
                          header=True,
                          **kwargs)


def reg_tb(path, name=None):
    if name:
        tb = name
    else:
        tb = os.path.basename(path)
    spark.read.parquet(path).createOrReplaceTempView(tb)


def spark_pg(query=None, db='wrds'):
    return spark.read.format("jdbc") \
        .option("url", f"jdbc:postgresql://localhost:5432/{db}") \
        .option('query',query).load()


def cosine(a, b):
    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))


def rpq_sql(query):
    from moz_sql_parser import parse
    d = parse(query)
    if isinstance(d['select'], list):
        columns = [c['value'] for c in d['select']]
    else:
        columns = list(d['select']['value'])
    if '*' in columns:
        columns = None
    df = rpq(d['from'], columns=columns)
    nc = dict()
    for c in d['select']:
        if 'name' in c:
            nc[c['value']] = c['name']
    return df.rename(nc, axis=1)


def writer(v, filename):
    with open(filename, 'w') as f:
        wr = csv.writer(f)
        for x, y in v:
            wr.writerow([x, y])


def savepickle(var, filename):
    with open(filename, 'wb') as f:
        return pickle.dump(var, f)


def loadpickle(filename):
    with open(filename, 'rb') as f:
        return pickle.load(f)


def sedfile(string, filename):
    sta = string.split('/')
    if len(sta) == 4:
        _, input, output, oper = sta
    elif len(sta) == 3:
        _, input, oper = sta
    else:
        raise ValueError("cannot parse the input")

    if oper == 'g':
        for line in fileinput.input(filename, inplace=1):
            if re.search(input, line):
                line = re.sub(input, output, line)
            sys.stdout.write(line)
    if oper == 'd':
        for line in fileinput.input(filename, inplace=1):
            if re.search(input, line):
                continue
            sys.stdout.write(line)


def wget(url, fn=None):
    if not fn:
        fn = url.split('/')[-1]
    urllib.request.urlretrieve(url, fn)


def peekwrds(table):
    return rwrds(f'''select * from {table} limit 10''')
