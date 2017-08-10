#!/usr/bin/env python
"""
stuff
"""


from enum import Enum, auto
import psycopg2
import yaml


def connect():
    """."""
    return psycopg2.connect("dbname=warehouse user=postgres password=postgres")

class DisplayType(Enum):
    STRING = auto()
    TIMESTAMP = auto()
    DATE = auto()
    INT = auto()
    MONEY = auto()
    PERCENT = auto()
    DECIMAL = auto()

class FilterType(Enum):
    STRING = auto()
    TIMESTAMP = auto()
    DATE = auto()
    DECIMAL = auto()
    INT = auto()

class FilterOperator(Enum):
    EQUALS = auto()
    SUBSTRING = auto()
    GREATER = auto()
    LESS = auto()
    GREATEREQUAL = auto()
    LESSEQUAL = auto()
    BETWEEN = auto()


class AggregationType(Enum):
    GROUPBY = auto()
    SUM = auto()
    MAX = auto()
    AFTERGROUPBY = auto()
    DIV = auto()


class Column:
    def __init__(self, sql, filter_type=FilterType.STRING, display_type=DisplayType.STRING, aggregation_type = AggregationType.MAX):
        self.sql = sql
        self.filter_type = filter_type
        self.display_type = display_type
        self.is_range_type = filter_type in [FilterType.TIMESTAMP, FilterType.DATE, FilterType.DECIMAL, FilterType.INT]
        self.aggregation_type = aggregation_type

class Dimension:
    def __init__(self, name, base_table, required_cols, join_alias, cols):
        self.name = name
        self.base_table = base_table
        self.required_cols = required_cols
        self.join_alias = join_alias
        self.cols = cols
class Measure:
    def __init__(self, name, required_cols, cols):
        self.name = name
        self.required_cols = required_cols
        self.cols = cols
class FactTable:
    def __init__(self, table, col_names):
        self.join_alias = 'fact'
        self.table = table
        self.col_names = col_names
class DimensionTable:
    def __init__(self, table, col_names):
        self.join_alias = table
        self.table = table
        self.col_names = col_names


class Report:
    def __init__(self, name, cols, filters):
        self.name = name
        self.cols = cols
        self.filters = filters

class Filter:
    def __init__(self, name, filter_operator = FilterOperator.EQUALS, filter_value = None, filter_min = None, filter_max = None):
        self.name = name
        self.filter_operator = filter_operator
        self.filter_value = filter_value
        self.filter_min = filter_min
        self.filter_max = filter_max

def build_column(key, value):
    return Column(value['sql'], FilterType[value['filter_type']], DisplayType[value['display_type']], AggregationType[value['aggregation_type']])

def load_file(file_name):
    out = {}
    warehouse = {}

    with open(file_name, 'r') as file:
        out = yaml.load(file)
    warehouse["fact_tables"] = []
    for curr in out["reporting"]["tables"]["facts"]:
        for key, val in curr.items():
            warehouse["fact_tables"].append(FactTable(key, val['col_names']))
    warehouse["dimension_tables"] = []
    for curr in out["reporting"]["tables"]["dimensions"]:
        for key, val in curr.items():    
            warehouse["dimension_tables"] .append(DimensionTable(key, val['col_names']))
    warehouse["reports"] = []
    for curr in out["reporting"]["reports"]:
        for key, val in curr.items():
            filters = []
            if 'filters' in val:
                for curr_filter in val['filters']:
                    for fkey, fval in curr_filter.items():
                        filter_value = fval.get('value')
                        filter_min = fval.get('min')
                        filter_max = fval.get('max')
                        filters.append(Filter(fkey, FilterOperator[fval['filter_operator']], filter_value, filter_min, filter_max))
            warehouse["reports"] .append(Report(key, val['cols'], filters))
    warehouse['measures'] = []
    for curr in out['reporting']['measures']:
        for key, val in curr.items():
            cols = {}
            for curr_col in val['cols']:
                for col_key, col_value in curr_col.items():
                    cols[col_key] = build_column(col_key, col_value)
            warehouse['measures'].append(Measure(key, val['required_cols'], cols))
    warehouse['dimensions'] = []
    for curr in out['reporting']['dimensions']:
        for key, val in curr.items():
            cols = {}
            for curr_col in val['cols']:
                for col_key, col_value in curr_col.items():
                    cols[col_key] = build_column(col_key, col_value)
            warehouse['dimensions'].append(Dimension(key, val.get('base_table'), val['required_cols'], val.get('join_alias', 'fact'), cols))
    return warehouse



base_template = """
{cte}
SELECT
    {cols}
FROM
    {from}
    {joins}
WHERE
    1=1
    {where}
{group}
{having}
{order}
{limit}
"""

def lookup_columns(warehouse, column_name):
    cols = []
    if not '.' in column_name:
        cols = [x.required_cols for x in warehouse['measures'] if x.name == column_name] + \
            [x.required_cols for x in warehouse['dimensions'] if x.name == column_name]
    else:
        (dimension, dot, actual_name) = column_name.rpartition('.')
        cols = [x.required_cols for x in warehouse['dimensions'] if x.name == dimension]
    return sum(cols, [])

def get_dimension_def(warehouse, dim_col):
    (dimension, dot, actual_name) = dim_col.rpartition('.')
    dim = [x for x in warehouse['dimensions'] if x.name == dimension][0]
    return dim

def get_measure_def(warehouse, measure_col):
    mes = [x for x in warehouse['measures'] if x.name == measure_col][0]
    return mes


def get_base_fact(warehouse, report):
    fact_output_cols = [lookup_columns(warehouse, x) for x in report.cols]
    # remove dupes
    fact_cols = set(sum(fact_output_cols, []))
    for fact in warehouse['fact_tables']:
        if len(fact_cols) == len(fact_cols & set(fact.col_names)):
            return fact
    return None

def get_dimensions(warehouse, report):
    dims = [get_dimension_def(warehouse, x) for x in report.cols if '.' in x]
    return dims
def get_measure_col(warehouse, measure, column):
    sql_def = measure.cols[column]
    sql = ''
    if sql_def.aggregation_type == AggregationType.DIV:
        sql = 'SUM({})/NULLIF(SUM({}), 0) AS {}'.format(sql_def.sql[0], sql_def.sql[1], column)
    elif sql_def.aggregation_type == AggregationType.MAX:
        sql = 'MAX({}) AS {}'.format(sql_def.sql, column)
    elif sql_def.aggregation_type == AggregationType.SUM:
        sql = 'SUM({}) AS {}'.format(sql_def.sql, column)
    else:
        sql = '{} AS {}'.format(sql_def.sql, column)
    return sql

def get_dimension_col(warehouse, dimension, actual_column):
    sql_def = dimension.cols[actual_column]
    sql = ''
    # TODO aggregation types handling
    sql = '{} AS {}'.format(sql_def.sql, actual_column)
    return sql

def get_dimension_groupby(warehouse, dimension, actual_column):
    sql_def = dimension.cols[actual_column]
    group_by = sql_def.sql
    return group_by



def get_group_cols(warehouse, column):
    groupby = ''
    if '.' in column:
        (dimension, dot, actual_name) = column.rpartition('.')
        return get_dimension_groupby(warehouse, get_dimension_def(warehouse, column), actual_name)
    elif len([x for x in warehouse['dimensions'] if column in x.cols]) > 0:
        return get_dimension_groupby(warehouse, [x for x in warehouse['dimensions'] if column in x.cols][0], column)
    return groupby

def get_col(warehouse, column):
    if '.' in column:
        (dimension, dot, actual_name) = column.rpartition('.')
        return get_dimension_col(warehouse, get_dimension_def(warehouse, column), actual_name)
    elif len([x for x in warehouse['dimensions'] if column in x.cols]) > 0:
        return get_dimension_col(warehouse, [x for x in warehouse['dimensions'] if column in x.cols][0], column)
    else:
        return get_measure_col(warehouse, get_measure_def(warehouse, column), column)

def build_query(warehouse, report):
    fact = get_base_fact(warehouse, report)
    cte = ''
    cols = ',\n    '.join([get_col(warehouse, x) for x in report.cols])
    from_cl = fact.table + ' fact'

    joins = ''
    for dim in get_dimensions(warehouse, report):
        joins = joins + 'INNER JOIN ' + dim.base_table + ' ' + dim.join_alias + ' USING (' + ', '.join(dim.required_cols) + ')\n    '

    where = ''
    group_by = [get_group_cols(warehouse, x) for x in report.cols]
    group_by = [x for x in group_by if x != '']
    group = ''
    if len(group_by) > 0:
        group = 'GROUP BY ' + ', '.join(group_by)
    having = ''
    order = ''
    limit = ''

    val_dict = {"cte": cte, "cols": cols, "from": from_cl, "joins": joins, "where": where, "group": group, "having": having, "order": order, "limit": limit}
    return {"query": base_template.format(**val_dict), "params": {}}


def main():
    warehouse = load_file('testconfig.yaml')
    for report in warehouse['reports']:
        q = build_query(warehouse, report)
        print(q['query'])
    

if __name__ == "__main__":
    main()
