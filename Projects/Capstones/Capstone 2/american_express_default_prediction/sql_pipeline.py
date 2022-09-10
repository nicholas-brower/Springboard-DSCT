import pyodbc
from contextlib import closing
from typing import Callable
import re
import datetime
import numpy as np
import pandas as pd
from sql_db_connection import CONNECTION_STRING as SQL_CNXN_STR
from type_maps import py_to_np

TRAIN_TABLE = 'train_data'
TEST_TABLE = 'test_data'
TRAIN_LABELS_TABLE = 'train_labels'
TARGET_FEATURE_COL = 'target'
NUMERIC_AGGS = [
    'mean', 'min', 'max', 'range', 'sum', 'count', 'first', 'last', 
    'delta_mean', 'delta_max', 'delta_min', 'delta_pd', 'span'
]

def sql_exec_callable(
        statement: str, action: Callable, 
        cnxn_str: str=SQL_CNXN_STR) -> object:
    '''Create a connection to a SQL database. Create a cursor object and pass
    it as argument to a given callable action. Return the result of calling
    this action
    
    Arguments
    statement
        A sql statement as a string
    action
        A callable action
    cnxn_str
        Sql connection string. Default = SQL_CNXN_STR, a constant.
    
    Returns
        The result of calling action(crsr.execute(statement))
    '''
    with closing(pyodbc.connect(
        SQL_CNXN_STR, autocommit=False, readonly=True
            )) as cnxn:
        with closing(cnxn.cursor()) as crsr:
            return action(crsr.execute(statement))
def query_result(crsr: pyodbc.Cursor) -> dict:
    desc = crsr.description
    if not desc:
        return {}
    cols = [_[0] for _ in desc]
    dtypes = [py_to_np[_[1].__name__] for _ in desc]
    rows = crsr.fetchall()
    if not any(rows):
        return {}
    col_data = {
        cols[i]: np.array([row[i] for row in rows], dtype=dtypes[i])
        for i in range(len(desc))
    }
    return pd.DataFrame(col_data)
def sql_query(statement: str) -> pd.DataFrame:
    return (sql_exec_callable(statement, query_result))
def sql_scalar(statement: str) -> object:
    return sql_query(statement).iloc[0, 0]
def check_db(schema: str) -> str:
    return (sql_query(
        f'''SELECT CATALOG_NAME FROM INFORMATION_SCHEMA.SCHEMATA
        WHERE SCHEMA_NAME='{schema}';'''
        ).CATALOG_NAME.values[0])
def sql_exec_w(statement: str, cnxn_str=SQL_CNXN_STR) -> None:
    '''Create a write-enabled connection to a SQL database. Create a cursor
    object and use it to execute a statement.
    '''
    with closing(pyodbc.connect(
        SQL_CNXN_STR, autocommit=False, readonly=False
            )) as cnxn:
        with closing(cnxn.cursor()) as crsr:
            crsr.execute(statement)
def null_pct(col: str, id_col: str, table: str, id_count: int=0) -> float:
    if not id_count:
        id_count = sql_scalar(f'SELECT COUNT(DISTINCT {id_col} FROM {table};')
    null_count = sql_scalar(
        f'''SELECT CAST(COUNT({id_col}) AS FLOAT) AS null_count
        FROM 
        (
            SELECT {id_col}, COUNT({col}) AS {col}_count 
            FROM {table} GROUP BY {id_col}
        ) subq WHERE {col}_count=0;'''
    )
    return null_count / id_count
def target_labels(
        id_col: str, labels_table: str=TRAIN_LABELS_TABLE, 
        target_col: str=TARGET_FEATURE_COL) -> pd.DataFrame:
    return sql_query(
        f'SELECT {id_col}, {target_col} FROM {labels_table} ORDER BY {id_col};'
    )
def is_null_corr(
        col: str, id_col: str, table: str=TRAIN_TABLE, 
        labels_table: str=TRAIN_LABELS_TABLE, 
        target_col: str=TARGET_FEATURE_COL) -> float:
    col_is_null = sql_query(
        f'''
        SELECT 
            t.{id_col},
            t.{target_col},
            CASE 
                WHEN s.{col}_count > 0 THEN 0 ELSE 1
            END AS is_null
        FROM {labels_table} t
            JOIN
            (
                SELECT {id_col}, COUNT({col}) AS {col}_count
                FROM {table} GROUP BY {id_col}
            ) s ON t.{id_col} = s.{id_col}
        ;'''
    )
    return col_ .is_null.corr(col_is_null.target)   
def numeric_view_cols(
        col: str, agg_names: list[str]=NUMERIC_AGGS)-> dict[str:str]:
    return {agg_name: f'{col}_{agg_name}' for agg_name in NUMERIC_AGGS}
def numeric_aggs_corr(
        col: str, id_col: str, agg_view: str='', table: str=TRAIN_TABLE, 
        labels_table: str=TRAIN_LABELS_TABLE, 
        target_col: str=TARGET_FEATURE_COL, 
        agg_names:list[str]=NUMERIC_AGGS) -> dict[str:float]:
    if not agg_view:
        agg_view = f'{table}_{col}_agg'
    agg_data = sql_query(
        f'''SELECT t.{id_col}, t.{target_col}, a.*
        FROM {labels_table} t LEFT JOIN {agg_view} a
            ON t.{id_col} = a.{id_col};'''
    )
    if not any(agg_data):
        return {}
    agg_corr = {}
    for agg_name, col_name in numeric_view_cols(col, agg_names).items():
        agg_sub = agg_data[[id_col, col_name, target_col]].dropna()
        if not any(agg_sub):
            agg_cor[agg_name] = None
        else:
            agg_corr[agg_name] = agg_sub[col_name].corr(agg_sub[target_col])
    return agg_corr
def scalar_median(col: str, table: str, subquery: str='') -> float:
    if not subquery:
        subquery = f'SELECT {col} FROM {table}'
    return sql_scalar(
        f'''
        SELECT DISTINCT PERCENTILE_CONT(0.5)
            WITHIN GROUP(ORDER BY {col}) OVER()
        FROM ({subquery}) sub_q;'''
    )
def train_col_summary(
        col: str, id_col: str, target_categories: dict[str:int],
        agg_view: str='', table: str=TRAIN_TABLE, 
        target_col: str=TARGET_FEATURE_COL, 
        labels_table: str=TRAIN_LABELS_TABLE, 
        agg_names: list[str]=NUMERIC_AGGS,
        aggs_to_summarize:[list[str]]=[],) -> dict[str: float]:
    col_summary = {}
    if not aggs_to_summarize:
        aggs_to_summarize = ['mean']  
    if not agg_view:
        agg_view = f'{table}_{col}_agg'
    agg_alias = [
        (name, agg) for name, agg in zip(
            'min max mean'.split(), 'MIN MAX AVG'.split()
        )
    ]
    unweighted_overall = sql_query(
        f'SELECT ' + ', '.join([
            f'{agg}({col}) AS {col}_{name}'
            for name, agg in agg_alias
        ]) + f' FROM {table};'
    )
    unweighted_overall_median = scalar_median(col, table)
    unweighted_by_target = sql_query(
        f'SELECT ' + ', '.join([
            f'{agg}(t.{col}) AS {col}_{name}'
            for name, agg in agg_alias
        ]) + f', l.{target_col} ' + f'''
        FROM {table} t JOIN {labels_table} l
        ON t.{id_col} = l.{id_col}
        GROUP BY l.{target_col};'''
    )
    unw_medians_by_target = {
        label: scalar_median(col, table, subquery=(
            f'''
            SELECT t.{col}
            FROM {table} t JOIN {labels_table} l ON t.{id_col} = l.{id_col}
            WHERE l.{target_col} = {target_val}'''
        )) for label, target_val in target_categories.items()
    }
    col_summary['unweighted'] = {}
    col_summary['unweighted']['overall'] = {
        name: unweighted_overall[f'{col}_{name}'].values[0]
        for name, agg in agg_alias
    }
    col_summary['unweighted']['overall']['median'] = unweighted_overall_median
    col_summary['unweighted']['by target'] = {
        label: {
            name: 
            unweighted_by_target[
                unweighted_by_target[target_col]==target_val
            ][f'{col}_{name}'].values[0]
            for name, agg in agg_alias
        } for label, target_val in target_categories.items()
    }
    for label, median_val in unw_medians_by_target.items():
        col_summary['unweighted']['by target'][label]['median'] = median_val
    col_summary['weighted'] = {}
    weighted_by_target = {
        label: sql_query(
            f'''
            SELECT ''' + '\n' + ', \n'.join(
                f'{agg}(a.{col}_{suffix}) AS {col}_{suffix}_{name}'
                for name, agg in agg_alias for suffix in aggs_to_summarize
            ) + f'''
            FROM {agg_view} a JOIN {labels_table} t
            ON a.{id_col} = t.{id_col}
            WHERE t.{target_col} = {target_val}
            GROUP BY t.{target_col};'''
        ) for label, target_val in target_categories.items()
    }
    weighted_medians_by_t = {
        label: {
            suffix:
            scalar_median(f'{col}_{suffix}', agg_view, subquery=(
                f'''
                SELECT a.{col}_{suffix} FROM {agg_view} a
                JOIN {labels_table} l ON a.{id_col} = l.{id_col}
                WHERE l.{target_col} = {target_val}'''
            )) for suffix in aggs_to_summarize
        } for label, target_val in target_categories.items()
    }
    col_summary['weighted']['by target'] = {
        label: {
            suffix: {
                name: {
                    weighted_by_target[label][f'{col}_{suffix}_{name}'].values[0]
                } for name, aggs in agg_alias
            } for suffix in aggs_to_summarize
        } for label, target_val in target_categories.items()
    }
    for label, d in weighted_medians_by_t.items():
        for suffix, median in d.items():
            col_summary['weighted']['by target'][label][suffix]['median'] = (
                median
            )
    return col_summary
def cat_subfeature_agg(
        cat_feature: str, id_col: str, target_categories: dict[str:int]={},
        table: str=TRAIN_TABLE, view: str='', 
        target_col: str=TARGET_FEATURE_COL,
        labels_table: str=TRAIN_LABELS_TABLE, 
        correlate: bool=True) -> pd.DataFrame:
    aggs = {}
    if not view:
        view = f'{table}_{cat_feature}_cat'
    cat_data = sql_query(f'SELECT * FROM {view}')
    subfeatures = [
        column for column in cat_data.columns 
        if column!=id_col and not column.endswith('count')
    ]
    for subfeature in subfeatures:
        aggs[subfeature] = {'summary': {'overall': {
            'min': cat_data[subfeature].min(),
            'max': cat_data[subfeature].max(),
            'mean': cat_data[subfeature].mean(),
            'median': cat_data[subfeature].median()
        }}}
    if correlate:
        cat_data = sql_query(
            f'''
            SELECT v.*, t.{target_col} 
            FROM {view} v JOIN {labels_table} t
            ON v.{id_col} = t.{id_col}
            '''
        )
        target_categories = {
            label: cat_data[target_col]==target_val
            for label, target_val in target_categories.items()
        }
        by_target = {
            subfeature: {
                label: {
                    'min': cat_data[mask][subfeature].min(),
                    'max': cat_data[mask][subfeature].max(),
                    'mean': cat_data[mask][subfeature].mean(),
                    'median': cat_data[mask][subfeature].median()
                } for label, mask in target_categories.items()
            } for subfeature in subfeatures
        }
        for subfeature in subfeatures:
            aggs[subfeature]['correlation with target'] = (
                cat_data[subfeature].corr(cat_data[target_col])
            )
            aggs[subfeature]['summary']['by target'] = (
                by_target[subfeature]
            )
    return aggs

def median_data_height(col: str, id_col: str, table: str=TRAIN_TABLE)->float:
    return sql_scalar(f'''
        WITH count_by_id AS
        (
            SELECT DISTINCT t.{id_col}, ISNULL(c.col_count, 0) AS col_count
            FROM
            {table} t LEFT JOIN (
                SELECT {id_col}, COUNT({col}) AS col_count
                FROM {table}
                GROUP BY {id_col}
            ) c ON t.{id_col} = c.{id_col}
        )
        SELECT DISTINCT PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY col_count) OVER()
        FROM count_by_id;'''
    )
    
def col_percentiles(
        col: str, id_col: str, percentiles: list[int],
        table: str=TRAIN_TABLE) -> dict[int:float]:
    ptiles = [
        f'    PERCENTILE_CONT({p/100}) '
        + f'WITHIN GROUP(ORDER BY {col}) OVER() AS p_{i}'
        for i, p in enumerate(percentiles)
    ]
    percentile_q = f'''
    SELECT DISTINCT
    ''' + ',\n'.join(ptiles) + f'\n    FROM {table};'
    vals = sql_query(percentile_q)
    return {
        percentile: vals[f'p_{i}'].values[0]
        for i, percentile in enumerate(percentiles)
    }
    
        
        