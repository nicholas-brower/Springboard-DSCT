from os import path, PathLike, makedirs
if not path.exists('data/working/sql/'):
    os.makedirs('data/working/sql/')


def csv_create_table(
        table_name: str, header: list[str],
        col_opts: dict[str:list[str]], db_name: str,
        schema: str='dbo') -> str:
    '''Generate a CREATE TABLE statement for a given csv header, column
    dictionary specying types and options, database name, schema name, and
    table name.
    '''
    create_stmt = f'''
    USE [{db_name}]
    GO
    
    SET ANSI_NULLS ON
    GO

    SET QUOTED_IDENTIFIER ON
    GO
    
    CREATE TABLE [{schema}].[{table_name}] (
'''
    cols = ',\n'.join(
        f'        [{col}] {" ".join(col_opts[col])}' for col in header
    )
    create_stmt = create_stmt + cols 
    return '\n'.join(_[4:] for _ in  create_stmt.split('\n')) + '\n);'
'''
functions from table_creation_funcs
'''


def timeseries_feature_aggregates(
        id_col: str, datetime_col: str, col: str, col_table: str, 
        target_col: str='', target_table: str='', span: bool=True,
        delta: bool=True, delta_pd: bool=True, create_table: bool=False,
        create_view: bool=False) -> str:
    '''Generate sql statement(s) as a string to select aggregates of a given
    feature from a given table, grouped by a unique identifier.
    
    Arguments
    id_col
        A string specifying the name of the column containing the identifier
        by which timeseries data will be grouped before calculating 
        aggregates.
    datetime_col
        A string specifying the name of the column containing the date and/or
        time at which each observation in col is measured.
    col
        A string specifying the name of the column containing the data to 
        perform calculations on
    col_table
        A string specifying the name of the table containing the timeseries
        data
    target_col
        A string specifying the name of the column containing target feature
        classifications
    target_table
        A string specifying the name of the table containing the target 
        feature. Assumes col_table can join target_table using id_col.
    span
        A boolean specifying whether the timeseries span per id is included 
        in the resultant statements. Default=True
    delta
        A boolean specifying whether the col change between observations
        is calcualted and included in the resultant statements. Default=True
    delta_pd
        A boolean specifying whether the col change per day is
        calculated and included in the resultant statements. Default=True
    create_table
        A boolean specying whether CREATE TABLE and INSERT INTO statements
        are generated.
    create_view
        A boolean specifying whether the resultant statements should be
        organized to create a view.
    '''
    _t_ = int(bool(target_col))
    if target_table:
        u_ids_table = f'{target_table}'
    else:
        u_ids_table = f'{col_table}'
    if create_table:
        create_stmt = (
            f'''
CREATE TABLE {col_table}_{col}_aggs (
	{id_col} VARCHAR(80),
    {col}_mean FLOAT, 
    {col}_min FLOAT,
    {col}_max FLOAT, 
    {col}_range FLOAT,
    {col}_sum FLOAT,
    {col}_count FLOAT,
    {col}_first FLOAT, 
    {col}_last FLOAT,
    {col}_delta_mean FLOAT,
    {col}_delta_max FLOAT,
    {col}_delta_min FLOAT,
    {col}_delta_pd FLOAT,
    {col}_span INT{_t_*','}
    {_t_ * f'{target_col} INT'}
);'''
        )
    if create_view:
        create_stmt = f'CREATE VIEW {col_table}_{col}_agg AS'
    else:
        create_stmt = ''
    insert_stmt = (
    '' + create_table * f'''
INSERT INTO {col_table}_{col}_aggs(
	{id_col},
    {col}_mean, 
    {col}_min,
    {col}_max, 
    {col}_range,
    {col}_sum,
    {col}_count,
    {col}_first, 
    {col}_last,
    {col}_delta_mean,
    {col}_delta_max,
    {col}_delta_min,
    {col}_delta_pd,
    {col}_span{_t_*','}
    {_t_ * f'{target_col} INT'}
)'''
    )
    return(
        f'''
{create_stmt}
WITH 
u_ids AS
(
    SELECT DISTINCT {id_col}{_t_*f', {target_col}'} FROM {u_ids_table}
),
subset AS
(
    SELECT
        td.{id_col}, td.{datetime_col}, td.{col} {_t_* f', tl.{target_col}'}
    FROM
        {col_table} td 
        {_t_*f'JOIN {target_table} tl ON td.{id_col} = tl.{id_col}'}
),
first_last AS
(
    SELECT 
        {id_col}, 
        MIN({datetime_col}) AS first_dt, 
        MAX({datetime_col}) AS last_dt
    FROM subset
    WHERE {col} IS NOT NULL
    GROUP BY {id_col}
),
first_{col} AS
(
    SELECT
        f.{id_col}, s.{col} AS {col}_first
    FROM
        first_last f 
        JOIN subset s 
            ON f.{id_col} = s.{id_col} 
            AND f.first_dt = s.{datetime_col}
),
last_{col} AS
(
    SELECT
        f.{id_col}, s.{col} AS {col}_last
    FROM
        first_last f 
        JOIN subset s 
            ON f.{id_col} = s.{id_col} 
            AND f.last_dt = s.{datetime_col}
),
{col}_span AS
(
    SELECT
        {id_col},
        DATEDIFF(DAY, first_dt, last_dt) AS {col}_span
    FROM
        first_last
),
{col}_minus_lag AS
(
    SELECT
        s.{id_col}, 
        s.{datetime_col},
        CAST(DATEDIFF(
            DAY, 
            LAG(s.{datetime_col}, 1) OVER(PARTITION BY s.{id_col} ORDER BY s.{datetime_col}),
            s.{datetime_col}
        ) AS FLOAT) AS date_delta,
        s.{col},
        s.{col} - LAG(s.{col}, 1) OVER(
                PARTITION BY s.{id_col} 
                ORDER BY s.{datetime_col}
            ) AS {col}_delta
    FROM
        subset s
),
{col}_delta AS
(
    SELECT
        m.{id_col},
        m.{datetime_col},
        m.date_delta,
        m.{col}_delta
    FROM
        {col}_minus_lag m
        JOIN first_last f
            ON m.{id_col} = f.{id_col}
    WHERE
        m.{datetime_col} > f.first_dt
),
{col}_delta_per_day AS
(
    SELECT
        {id_col},
        {col}_delta / date_delta AS {col}_delta_per_day
    FROM
        {col}_delta
),
{col}_delta_pd_mean AS
(
    SELECT
        {id_col},
        AVG({col}_delta_per_day) AS {col}_delta_pd
    FROM
        {col}_delta_per_day
    GROUP BY
        {id_col}
),      
{col}_delta_aggs AS
(
    SELECT
        {id_col},
        AVG({col}_delta) AS {col}_delta_mean,
        MAX({col}_delta) AS {col}_delta_max,
        MIN({col}_delta) AS {col}_delta_min
    FROM
        {col}_delta
    GROUP BY
        {id_col}
),
aggs AS
(
    SELECT
        {id_col}, 
        AVG({col}) AS {col}_mean,
        MIN({col}) AS {col}_min, 
        MAX({col}) AS {col}_max, 
        SUM({col}) AS {col}_sum,
        COUNT({col}) AS {col}_count{_t_*f','}
        {_t_*f'{target_col}'}
    FROM
        subset
    GROUP BY
        {id_col}{_t_*f', {target_col}'}
),
vals AS
(
    SELECT
        a.{id_col},
        a.{col}_mean,
        a.{col}_min, 
        a.{col}_max, 
        a.{col}_sum,
        a.{col}_max - a.{col}_min AS {col}_range,
        a.{col}_count,
        f.{col}_first,
        l.{col}_last,
        d.{col}_delta_mean,
        d.{col}_delta_max,
        d.{col}_delta_min,
        pd.{col}_delta_pd,
        cs.{col}_span
    FROM
        aggs a
        LEFT JOIN first_{col} f
            ON a.{id_col} = f.{id_col}
        LEFT JOIN last_{col} l
            ON a.{id_col} = l.{id_col}
        LEFT JOIN {col}_delta_aggs d
            ON  a.{id_col} = d.{id_col}
        LEFT JOIN {col}_delta_pd_mean pd
            ON a.{id_col} = pd.{id_col}
        LEFT JOIN {col}_span cs
            ON a.{id_col} = cs.{id_col}
)
{insert_stmt}
SELECT
    u.{id_col},
    v.{col}_mean, 
    v.{col}_min,
    v.{col}_max, 
    v.{col}_range,
    v.{col}_sum,
    ISNULL(v.{col}_count, 0) AS {col}_count,
    v.{col}_first, 
    v.{col}_last,
    v.{col}_delta_mean,
    v.{col}_delta_max,
    v.{col}_delta_min,
    v.{col}_delta_pd,
    v.{col}_span{_t_*f','}
    {_t_*f'u.{target_col}'}
FROM 
    u_ids u 
    LEFT JOIN vals v
        ON u.{id_col} = v.{id_col}
;'''
    )

def categorical_subfeatures(
        id_col: str, col: str, table: str,
        categories: dict[int: str], create_table: bool=False,
        insert_to: str='', update_to: str='', create_view: bool=False) -> str:
    '''Return a string to generate subfeatures for each category in a
    categorical feature recorded as time series data. Subfeatures contain the
    count of observations where the parent feature has the subfeature's value,
    divided by the total number of observations recorded for that individual.
    
    
    Arguments:
    id_col
        A string containing the name of a column used as a unique identifier
    col
        A string representing the column name of a categorical feature
    table
        A string containing the name of a table
    create
        A boolean - if True, generate CREATE TABLE and INSERT INTO statements
        to create and populate a new table with one column per subfeature.
        Default = False. Incompatible with insert_to and update_to.
    insert_to
        An optional string providing the name of a compatible table in which 
        categorical subfeatures should be inserted. Default = ''. Incompatible
        with update_to.
    update_to
        An optional string providing the name of a compatible table to be
        updated with the subfeature percentages generated in the select
        statement. Default = ''.
    create_view
        A boolean - if True, generate a CREATE VIEW statement.
    '''
    u_ids = f'''
WITH
u_ids AS
(
    SELECT DISTINCT {id_col} FROM {table}
),'''
    total_counts = (
        f'''total_counts AS
(
    SELECT {id_col}, CAST(COUNT({col}) AS FLOAT) AS {col}_count
    FROM {table}
    GROUP BY {id_col}
),'''
    )
    category_ctes = ', \n'.join([
        f'''{col}_{cat_key}_ AS
(
    SELECT {id_col}, CAST(COUNT({col}) AS FLOAT) AS {col}_{cat_key}
    FROM {table}
    WHERE {col} = {cat_value}
    GROUP BY {id_col}
)'''
        for cat_key, cat_value in categories.items()
    ]) + ','
#
#
    cat_pct_columns = ',\n'.join([
        f'''        CASE
            WHEN {col}_{cat_key} IS NULL THEN 0
            ELSE {col}_{cat_key}_.{col}_{cat_key} / tc.{col}_count
        END AS {col}_{cat_key}'''
        for cat_key in categories.keys()
    ])
    cat_pct_tables = '\n'.join([
        f'''        LEFT JOIN {col}_{cat_key}_ 
            ON tc.{id_col} = {col}_{cat_key}_.{id_col}'''
        for cat_key in categories.keys()
    ])
#
#
    cat_pct = (
        f'''cat_pct AS
(
    SELECT
        tc.{id_col},
        tc.{col}_count,
{cat_pct_columns}
    FROM
        total_counts tc
{cat_pct_tables}
)'''
    )
#
#
    select_columns = ', \n'.join([
        f'    ISNULL(c.{col}_{cat_key}, 0) AS {col}_{cat_key}'
        for cat_key in categories.keys()
    ])
#
#
    select_stmt = f'''
SELECT
    u.{id_col},
    c.{col}_count,
{select_columns}
FROM
    u_ids u
    LEFT JOIN cat_pct c
        ON u.{id_col} = c.{id_col}'''
#
#
    create_cols = (
        f'''
    {id_col} VARCHAR(80),
    {col}_count FLOAT,''' + '\n'
        + ',\n'.join([
            f'{col}_{cat_key} FLOAT'
            for cat_key in categories.keys()
        ])
        + '\n'
    )
#
#
    if create_table:
        create_stmt = f'''
CREATE TABLE {table}_{col}_cat ({create_cols});'''
    elif create_view:
        create_stmt = f'CREATE VIEW {table}_{col}_cat AS'
    else:
        create_stmt = ''
#
#
    if create_table:
        insert_table = f'{table}_{col}_cat'
    else:
        insert_table = insert_to
    insert_stmt = (
        f'''
INSERT INTO {insert_table} (
    {id_col},
    {col}_count,''' + '\n'
        + ',\n'.join([
            f'    {col}_{cat_key}'
            for cat_key in categories.keys()
        ])
        + '''
)'''
    )
#
#
    if update_to:
        select_stmt = '\n    '.join(select_stmt.split('\n'))
        select_stmt = (
            f''',
sel_stmt AS
(
{select_stmt}
),
upd_tbl AS
(
    SELECT * FROM {update_to}
)
UPDATE tbl
    SET
        ''' + ',\n        '.join([
            f'tbl.{col}_{cat_key} = s.{col}_{cat_key}'
            for cat_key in categories.keys()
            ]) + '''
    FROM
        upd_tbl tbl JOIN sel_stmt s
        ON tbl.{id_col} = s.{id_col}
'''
        )
    _c_ = any([bool(create_table), bool(create_view)])
    _i_ = any([bool(create_table), bool(insert_to)])
    output = (
        f'''
{_c_ * create_stmt}
{u_ids}
{total_counts}
{category_ctes}
{cat_pct}
{_i_ * insert_stmt}
{select_stmt}
;
'''
    )
    return(output)

def feature_delta(col: str, datetime_col: str,  id_col: str, table:str) -> str:
    '''Return a sql statement to select the change in value of a feature
    over time.
    '''
    return f'''
WITH
first AS
(
    SELECT
        {id_col},
        MIN({datetime_col} AS {col}_first
    FROM
        {table}
    WHERE
        {col} IS NOT NULL
)
SELECT
    t.{id_col},
    t.{datetime_col},
    t.{col} - LAG(t.{col}, 1) OVER(
            PARTITION BY t.{id_col} 
            ORDER BY t.{datetime_col}
        ) AS {col}_delta
FROM
    {table} t JOIN first f 
        ON t.{id_col} = f.{id_col} AND t.{datetime_col} > f.{col}_first
'''


