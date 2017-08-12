#!/usr/bin/env python
"""
stuff
"""

from typing import List
from enum import Enum, auto
from dateutil import parser
import heapq
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
        self.join_alias = None
        self.base_table = None
class FactTable:
    def __init__(self, table, col_names, table_group = None):
        self.join_alias = 'fact'
        self.table = table
        self.col_names = col_names
        self.table_group = table_group
class DimensionTable:
    def __init__(self, table, col_names, table_group = None):
        self.join_alias = table
        self.table = table
        self.col_names = col_names
        self.table_group = table_group


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
        self.dimension_tables = warehouse_dict['dimension_tables']
        self.measures = warehouse_dict['measures']
        self.dimensions = warehouse_dict['dimensions']
        self.reports = warehouse_dict['reports']

    def definitions(self):
        return self.measures + self.dimensions

    def find_compatible_tables(self, required_cols):
        # remove dupes
        cols = sum(required_cols, [])
        prefix_cols = [x for x in cols if '.' in x]
        prefix = None
        if prefix_cols:
            # only consider the first prefix for now
            (prefix, _, _) = prefix_cols[0].rpartition('.')
            cols = [x if not '.' in x else x.rpartition('.')[2] for x in cols]
        fact_cols = set(cols)
        candidates = []
        for table in self.fact_tables + self.dimension_tables:
            if len(fact_cols) == len(fact_cols & set(table.col_names)) and (prefix is None or table.table_group == prefix):
                heapq.heappush(candidates, (len(table.col_names), table))
        if candidates:
            return heapq.heappop(candidates)[1]
        return None




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
            warehouse["fact_tables"].append(FactTable(key, val['col_names'], val.get('table_group', None)))
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
    entry = get_entry(warehouse, column_name)
    sql_def = get_column_from_entry(warehouse, entry, column_name)
    # we need to pass the prefix for measure columns through so that
    # we can pick the correct fact based on the hinting
    if '.' in column_name and is_measure(sql_def):
        (prefix, _, _) = column_name.rpartition('.')
        return [prefix+'.'+ x for x in entry.required_cols]
    return entry.required_cols



def get_dimension_def(warehouse: Warehouse, dim_col: str) -> Dimension:
    (dimension, dot, actual_name) = dim_col.rpartition('.')
    dim = get_entry(warehouse, dim_col)
    if dimension == '':
        # this is used for group by, so don't include this dimension if it references the original table
        if dim.cols[dim_col].sql.startswith("fact."):
          dim = None
    return dim

def is_measure(sql_def: Column):
    return sql_def.aggregation_type not in [AggregationType.AFTERGROUPBY, AggregationType.GROUPBY]

def is_dimension(sql_def: Column):
    return not is_measure(sql_def)


def get_base_fact(warehouse: Warehouse, report: Report) -> FactTable:
    raw_cols = report.cols + [x.name for x in report.filters] + sum([x.required_cols for x in get_joins(warehouse, report)],[])
    fact_output_cols = [lookup_columns(warehouse, x) for x in raw_cols]
    return warehouse.find_compatible_tables(fact_output_cols)

def get_joins(warehouse: Warehouse, report: Report):
    dims = [get_dimension_def(warehouse, x) for x in report.cols if '.' in x] + \
        [get_dimension_def(warehouse, x.name) for x in report.filters]
    return set([x for x in dims if x and x.base_table])

def get_measure_sql(warehouse: Warehouse, sql_def: Column, column: str) -> str:
    sql = ''
    actual_column = column
    if '.' in column:
        (_,_,actual_column) = column.rpartition('.')
    if sql_def.aggregation_type == AggregationType.DIV:
        sql = 'SUM({})/NULLIF(SUM({}), 0) AS {}'.format(sql_def.sql[0], sql_def.sql[1], actual_column)
    elif sql_def.aggregation_type == AggregationType.MAX:
        sql = 'MAX({}) AS {}'.format(sql_def.sql, actual_column)
    elif sql_def.aggregation_type == AggregationType.SUM:
        sql = 'SUM({}) AS {}'.format(sql_def.sql, actual_column)
    else:
        sql = '{} AS {}'.format(sql_def.sql, actual_column)
    return sql

def get_dimension_sql(warehouse: Warehouse, sql_def: Column, column: str) -> str:
    sql = ''
    actual_column = column
    if '.' in column:
        (_,_,actual_column) = column.rpartition('.')
    # TODO aggregation types handling
    sql = '{} AS {}'.format(sql_def.sql, actual_column)
    return sql

def get_filter_type(warehouse: Warehouse, column: str) -> FilterType:
    return get_column_object(warehouse, column).filter_type

def get_entry(warehouse: Warehouse, column: str):
    if ('.' in column):
        (prefix, _, actual_col) = column.rpartition('.')
        dims = [y for y in [x for x in warehouse.dimensions if x.name == prefix] if actual_col in y.cols]
        if dims:
            return dims[0]
        column = actual_col
    defs = [y for y in [x for x in warehouse.definitions()] if column in y.cols]
    if defs:
        return defs[0]
    return None

def get_column_from_entry(warehouse: Warehouse, entry, column:str) -> Column:
    if entry:
        if ('.' in column):
            (_,_,actual) = column.rpartition('.')
            return entry.cols[actual]
        return entry.cols[column]
    return None


def get_column_object(warehouse: Warehouse, column: str) -> Column:
    return get_column_from_entry(warehouse, get_entry(warehouse, column), column)
            
def get_group_cols(warehouse: Warehouse, column: str) -> str:
    sql_def = get_column_object(warehouse, column)
    if sql_def.aggregation_type in [AggregationType.AFTERGROUPBY, AggregationType.GROUPBY]:
        return sql_def.sql
    return ''

def get_col_sql(warehouse: Warehouse, column: str) -> str:
    sql_def = get_column_object(warehouse, column)
    if is_measure(sql_def):
        return get_measure_sql(warehouse, sql_def, column)
    else:
        return get_dimension_sql(warehouse, sql_def, column)

def lookup_operator(filter_operator: FilterOperator, value) -> str:
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

def get_filter_value(filter_type, value):
    filter_type_map = {
        FilterType.DATE: lambda x: parser.parse(x),
        FilterType.TIMESTAMP: lambda x: parser.parse(x),
        FilterType.DECIMAL: lambda x: Decimal(x),
        FilterType.INT: lambda x: int(x),
    }

    # pass nulls through un-casted
    if not value:
        return value
    if (filter_type in filter_type_map):
        return filter_type_map[filter_type](value)
    return value

def get_where_and_params(warehouse: Warehouse, report: Report, prefix=''):
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
            filter_type = get_filter_type(warehouse, curr_filter.name)
            if i > 0:
                sql = sql + ' OR '
            base_name = prefix + curr_filter.name +'.' + str(curr_filter.filter_operator)
            col_name = get_col_sql(warehouse, curr_filter.name).rpartition(' AS ')[0]
            if curr_filter.filter_operator is FilterOperator.BETWEEN:
                param_name_min = base_name + str(i)
                i = i +1
                params[param_name_min] = get_filter_value(filter_type, curr_filter.filter_min)
                param_name_max = base_name + str(i)
                i = i +1
                params[param_name_max] = get_filter_value(filter_type, curr_filter.filter_max)
                sql = sql + col_name + ' BETWEEN  %('+ param_name_min +')s  AND %('+param_name_max+')s '
            elif curr_filter.filter_operator is FilterOperator.SUBSTRING:
                param_name = base_name + str(i)
                i = i +1
                params[param_name] = get_filter_value(filter_type, curr_filter.filter_value)
                param_name_decorated = '\'%%\' || %('+ param_name +')s || \'%%\''
                if not curr_filter.filter_value:
                    param_name_decorated = param_name
                sql = sql + col_name + ' ' + lookup_operator(curr_filter.filter_operator, curr_filter.filter_value) + ' ' +param_name_decorated
            else:
                param_name = base_name + str(i)
                i = i +1
                params[param_name] = get_filter_value(filter_type, curr_filter.filter_value)
                sql = sql + col_name + ' ' + lookup_operator(curr_filter.filter_operator, curr_filter.filter_value) + ' %('+ param_name +')s '


        sql = sql + ')\n'
    return (sql, params)


def get_fact_prefixes(warehouse, report):
    raw_cols = report.cols + [x.name for x in report.filters] + sum([x.required_cols for x in get_joins(warehouse, report)],[])
    fact_output_cols = sum([lookup_columns(warehouse, x) for x in raw_cols], [])
    return set([x.rpartition('.')[0] for x in fact_output_cols if '.' in x])


def has_multiple_facts(warehouse: Warehouse, report: Report):
    return len(get_fact_prefixes(warehouse, report)) > 1   


def build_multifact_query(warehouse: Warehouse, report: Report):
    prefixes = get_fact_prefixes(warehouse, report)
    raw_cols = report.cols + [x.name for x in report.filters] + sum([x.required_cols for x in get_joins(warehouse, report)],[])
    cols = sum([lookup_columns(warehouse, x) for x in raw_cols],[])

    shared_cols = [x for x in cols if '.' not in x]
    dim_cols = []
    prefixed_cols = {}

    for x in report.cols:
        if '.' in x:            
            (prefix,_,_) = x.rpartition('.')
            if (prefix not in prefixes):
                dim_cols.append(x)
                continue
            if prefix not in prefixed_cols:
                prefixed_cols[prefix] = []
            prefixed_cols[prefix].append(x)
    shared_cols = list(set(shared_cols))
    i = 0;
    ctes = {}
    first_fact = None
    cte_dims = []
    for key in prefixed_cols:
        cte_name = "cte_{}".format(i)
        cte_cols = shared_cols + prefixed_cols[key]
        cte_report = Report(cte_name, cte_cols, report.filters)
        ctes[cte_name] = build_query(warehouse, cte_report, True, cte_name)
        if i == 0:
            fact = FactTable(cte_name, cte_cols, key)
            first_fact = fact
        else:
            dim = DimensionTable(cte_name, cte_cols, key)
            cte_dims.append(dim)
        i+=1

    warehouse_dict = {
        'fact_tables': [first_fact],
        'dimension_tables': warehouse.dimension_tables, 
        'measures': warehouse.measures, 
        'dimensions': warehouse.dimensions, 
        'reports': warehouse.reports
    }
    report_warehouse = Warehouse(warehouse_dict)

    schema = 'reporting.'
    cte = 'WITH \n'  + ',\n'.join(['{} as ({})'.format(x, ctes[x]['query']) for x in ctes])

    all_cols = []
    all_cols = [x.replace(first_fact.table_group, 'fact') + ' AS ' + x.replace('.', '_') for x in first_fact.col_names if x in report.cols]
    for dim in cte_dims:
        all_cols = all_cols + [x.replace(dim.table_group, dim.table) + ' AS ' + x.replace('.', '_') for x in dim.col_names if x.startswith(dim.table_group)]
    all_cols = all_cols + [get_col_sql(warehouse, x) for x in dim_cols]
    cols = ',\n    '.join(all_cols)
    from_cl = first_fact.table + ' fact'




    joins = ''
    for dim_table in cte_dims:
        joins = joins + ' INNER JOIN ' + dim_table.table + ' ' + dim_table.join_alias + ' USING (' + ', '.join(shared_cols) + ')\n    '
    dims = get_joins(report_warehouse, report)
    if dims:
        for dim in dims:
            joins = joins + ' INNER JOIN ' + schema + dim.base_table + ' ' + dim.join_alias + ' USING (' + ', '.join(dim.required_cols) + ')\n    '

    (where, params) = get_where_and_params(warehouse, report, '')
    for key in ctes:
        params.update(ctes[key])    
    group = ''
    having = ''
    order = ''
    limit = ''

    val_dict = {"cte": cte, "cols": cols, "from": from_cl, "joins": joins, "where": where, "group": group, "having": having, "order": order, "limit": limit}
    return {"query": base_template.format(**val_dict), "params": params}


def build_query(warehouse: Warehouse, report: Report, is_subquery=False, param_prefix=''):

    if not is_subquery and has_multiple_facts(warehouse, report):
        return build_multifact_query(warehouse, report)
    if (has_multiple_facts(warehouse, report)):
        print(get_fact_prefixes(warehouse, report))
        print(report.cols)
        return None
    schema = 'reporting.'
    fact = get_base_fact(warehouse, report)
    cte = ''
    cols = ',\n    '.join([get_col_sql(warehouse, x) for x in report.cols])
    from_cl = schema + fact.table + ' fact'




    joins = ''
    dims = get_joins(warehouse, report)
    if dims:
        for dim in dims:
            joins = joins + 'INNER JOIN ' + schema + dim.base_table + ' ' + dim.join_alias + ' USING (' + ', '.join(dim.required_cols) + ')\n    '

    (where, params) = get_where_and_params(warehouse, report, param_prefix)
    
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
