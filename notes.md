# Objects needed
* Low level query wrapper
    * CTEs
    * LIMIT
    * ORDER
* Filters
    * IN
* Report
    * Columns
        * unnamespaced measures and dimension data namespaced by table
    * Filters
* Table
    * Available Columns
    * Type (fact/dimension)
* Definition
    * Dimension
        * Base Table
        * required cols
        * range_type
        * group by (? might be deduced from required cols)
        * filter_type (timestamp, int, decimal, string, etc)
        * display_type (money, percentage, int, decimal, string, etc)
        * join alias
        * sql
            * "display name" : sql 
    * Measures
        * required cols
        * sql
            * "display name" : sql
        * filter_type
        * display_type
        * aggregation_type (? sum, max, min, divide?)
# Done 2017-08-11
    * dimension filters
    * automatic joins
    * join deduplication
    * unit test harness
    * is null
    * add secondary fact tables to sample schema
    * data generation for sample schema
    * filtering on dimensions requires their joins
    * warehouse class
    * pick least granular table that has all our required cols (rollups)
    * dimensions as facts v1
    * properly cast filter types before sending to the db driver
    * lookup metadata for each col in a consistent way
    * figure out measure naming wrt to unrelated fact tables (fact groups?)
        * namespaced measures, I think
        * fact table groups
            * eg, label all the orderline tables as order, then refer to quantity as order.quantity
# Todos 2017-08-11
    * separate warehouse introspection code from sql generation code
    * Table base class
    * Entry base class (need better name)
    * Query builder class
    * type hinting (In progress)
        * breaks syntax highlighting is I typehint using List[x], etc
    * having clauses (measure filters)
    * use dimensions as facts (handle table aliases correctly)
    * more tests around filtering and joins
    * composite key joins
    * memoize metadata lookups
    * investigate other postgresql drivers (psycopg2 does not do server-side parameter binding which bothers me)
    * custom aggregation types
    * in clause filters
        * group by filter id and then by filter operator and *then* we can get our list
    * custom  names for report columns
    * ctes / multi fact table joins
        * recursive query generation
        * handle facts at different granularities
    * date dimensions hierarchy?
    * role playing dimensions (ordered_product_id, replacement_product_id?)
    * multi-fact measures (sum(fact.total_cost) - sum(fact2.inventory_cost))
    * handle re-aggregation of division metrics (split into components and only divide in outermost query)
    * paging / sorting (separate paging query? composite keys?)
    * redudant code cleanup
    * aftergroupby joins / selects (fact sum/filter/group by to final granularity in cte then join dimensions with no group by)
    * use psycopg2's sql manipulation api
    * add secondary fact and remaining dimensions to yaml file
    * schema introspection to build skeleton warehouse
        * write yaml file
    * separate files
    * __init__ files
    * docs
    * pylinting
    * error handling
    * better package name
    * reporting objects rending based on display type logic
    * introspect tables and generate data that way
        * some things will need rules though
            * eg end_date after start_date, date ranges, cardinality of dimensions and facts, complex relationships between dimensions (discount requres a campaign_id, ship_date only if order status is shipped etc)

# Questions

