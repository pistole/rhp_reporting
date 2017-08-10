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
    NOTEQUALS = auto()
    NOTSUBSTRING = auto()


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

class Warehouse:
    def __init__(self, warehouse_dict):
        self.fact_tables = warehouse_dict['fact_tables']
        self.dimension_tables = warehouse_dict['fact_tables']
        self.measures = warehouse_dict['measures']
        self.dimensions = warehouse_dict['dimensions']
        self.reports = warehouse_dict['reports']

def build_column(key, value):
    return Column(value['sql'], FilterType[value['filter_type']], DisplayType[value['display_type']], AggregationType[value['aggregation_type']])

def load_file(file_name: str) -> Warehouse:
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
    return Warehouse(warehouse)



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

def lookup_columns(warehouse: Warehouse, column_name: str):
    cols = []
    if not '.' in column_name:
        cols = [x.required_cols for x in warehouse.measures if x.name == column_name] + \
            [x.required_cols for x in warehouse.dimensions if x.name == column_name]
    else:
        (dimension, dot, actual_name) = column_name.rpartition('.')
        cols = [x.required_cols for x in warehouse.dimensions if x.name == dimension]
    return sum(cols, [])

def get_dimension_def(warehouse: Warehouse, dim_col: str):
    (dimension, dot, actual_name) = dim_col.rpartition('.')
    dim = None
    if dimension == '':
        dim = [x for x in warehouse.dimensions if dim_col in x.cols][0]
        if dim.cols[dim_col].sql.startswith("fact."):
          dim = None
    else:
        dim = [x for x in warehouse.dimensions if x.name == dimension][0]
    return dim

def get_measure_def(warehouse: Warehouse, measure_col:str ):
    mes = [x for x in warehouse.measures if x.name == measure_col][0]
    return mes


def get_base_fact(warehouse: Warehouse, report: Report):
    fact_output_cols = [lookup_columns(warehouse, x) for x in report.cols]
    # remove dupes
    fact_cols = set(sum(fact_output_cols, []))
    for fact in warehouse.fact_tables:
        if len(fact_cols) == len(fact_cols & set(fact.col_names)):
            return fact
    return None

def get_dimensions(warehouse: Warehouse, report: Report):
    dims = [get_dimension_def(warehouse, x) for x in report.cols if '.' in x] + \
        [get_dimension_def(warehouse, x.name) for x in report.filters]
    return set([x for x in dims if x and x.base_table])
def get_measure_col(warehouse: Warehouse, measure: Measure, column: str):
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

def get_dimension_col(warehouse: Warehouse, dimension: Dimension, actual_column: str):
    sql_def = dimension.cols[actual_column]
    sql = ''
    # TODO aggregation types handling
    sql = '{} AS {}'.format(sql_def.sql, actual_column)
    return sql

def get_dimension_groupby(warehouse: Warehouse, dimension: Dimension, actual_column: str):
    sql_def = dimension.cols[actual_column]
    group_by = sql_def.sql
    return group_by



def get_group_cols(warehouse, column):
    groupby = ''
    if '.' in column:
        (dimension, dot, actual_name) = column.rpartition('.')
        return get_dimension_groupby(warehouse, get_dimension_def(warehouse, column), actual_name)
    elif len([x for x in warehouse.dimensions if column in x.cols]) > 0:
        return get_dimension_groupby(warehouse, [x for x in warehouse.dimensions if column in x.cols][0], column)
    return groupby

def get_col(warehouse, column):
    if '.' in column:
        (dimension, dot, actual_name) = column.rpartition('.')
        return get_dimension_col(warehouse, get_dimension_def(warehouse, column), actual_name)
    elif len([x for x in warehouse.dimensions if column in x.cols]) > 0:
        return get_dimension_col(warehouse, [x for x in warehouse.dimensions if column in x.cols][0], column)
    else:
        return get_measure_col(warehouse, get_measure_def(warehouse, column), column)


def lookup_operator(filter_operator, value):
    filter_operator_map = {
        FilterOperator.EQUALS: '=',
        FilterOperator.GREATER: '>',
        FilterOperator.GREATEREQUAL: '>=',
        FilterOperator.LESS: '<',
        FilterOperator.LESSEQUAL: '<=',
        FilterOperator.SUBSTRING: 'ILIKE',
        FilterOperator.NOTSUBSTRING: 'NOT ILIKE',
        FilterOperator.NOTEQUALS: ' != '
    }

    filter_operator_map_none = {
        FilterOperator.EQUALS: ' IS ',
        FilterOperator.SUBSTRING: ' IS ',
        FilterOperator.NOTSUBSTRING: 'IS NOT ',
        FilterOperator.NOTEQUALS: ' IS NOT '
    }
    if not value:
        return filter_operator_map_none[filter_operator]
    return filter_operator_map[filter_operator]

def get_where_and_params(warehouse, report, prefix=''):
    if not report.filters:
        return ('', {})
    if prefix !='' and not prefix.endswith('.'):
        prefix = prefix + '.'
    params = {}
    sql = ''
    grouped_filters = {}
    for curr_filter in report.filters:
        key = curr_filter.name + '.' + str(curr_filter.filter_operator)
        if not key in grouped_filters:
            grouped_filters[key] = []
        grouped_filters[key].append(curr_filter)
    for filter_name, filter_list in grouped_filters.items():
        i=0
        sql = sql + 'AND ('
        for curr_filter in filter_list:
            if i > 0:
                sql = sql + ' OR '
            base_name = prefix + curr_filter.name +'.' + str(curr_filter.filter_operator)
            col_name = get_col(warehouse, curr_filter.name).rpartition(' AS ')[0]
            if curr_filter.filter_operator is FilterOperator.BETWEEN:
                param_name_min = base_name + str(i)
                i = i +1
                params[param_name_min] = curr_filter.filter_min
                param_name_max = base_name + str(i)
                i = i +1
                params[param_name_max] = curr_filter.filter_max
                sql = sql + col_name + ' BETWEEN  %('+ param_name_min +')s  AND %('+param_name_max+')s '
            elif curr_filter.filter_operator is FilterOperator.SUBSTRING:
                param_name = base_name + str(i)
                i = i +1
                params[param_name] = curr_filter.filter_value
                param_name_decorated = '\'%%\' || %('+ param_name +')s || \'%%\''
                if not curr_filter.filter_value:
                    param_name_decorated = param_name
                sql = sql + col_name + ' ' + lookup_operator(curr_filter.filter_operator, curr_filter.filter_value) + ' ' +param_name_decorated
            else:
                param_name = base_name + str(i)
                i = i +1
                params[param_name] = curr_filter.filter_value
                sql = sql + col_name + ' ' + lookup_operator(curr_filter.filter_operator, curr_filter.filter_value) + ' %('+ param_name +')s '


        sql = sql + ')\n'
    return (sql, params)

def build_query(warehouse: Warehouse, report: Report):
    schema = 'reporting.'
    fact = get_base_fact(warehouse, report)
    cte = ''
    cols = ',\n    '.join([get_col(warehouse, x) for x in report.cols])
    from_cl = schema + fact.table + ' fact'



    joins = ''
    dims = get_dimensions(warehouse, report)
    if dims:
        for dim in dims:
            joins = joins + 'INNER JOIN ' + schema + dim.base_table + ' ' + dim.join_alias + ' USING (' + ', '.join(dim.required_cols) + ')\n    '

    (where, params) = get_where_and_params(warehouse, report)
    
    group_by = [get_group_cols(warehouse, x) for x in report.cols]
    group_by = [x for x in group_by if x != '']
    group = ''
    if group_by:
        group = 'GROUP BY ' + ', '.join(set(group_by))
    having = ''
    order = ''
    limit = ''

    val_dict = {"cte": cte, "cols": cols, "from": from_cl, "joins": joins, "where": where, "group": group, "having": having, "order": order, "limit": limit}
    return {"query": base_template.format(**val_dict), "params": params}


def main():
    warehouse = load_file('testconfig.yaml')
    for report in warehouse.reports:
        q = build_query(warehouse, report)
        print(q['query'])
        print(q['params'])

if __name__ == "__main__":
    main()
