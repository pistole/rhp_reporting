# Objects needed
* Low level query wrapper
    * CTEs
    * SELECT
    * JOINS
    * WHERE
    * GROUP BY
    * HAVING
    * LIMIT
* Filters
    * equals, substring, >=, <, between, not equals, IS NULL
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
# Done 2017-08-10
* dimension filters
* filtering on dimensions requires their joins
* join deduplication
* unit test
* is null
* add secondary fact tables to sample schema
* data generation for sample schema
* warehouse class
# Todos 2017-08-09
* type hinting (In progress)
* figure out measure naming wrt to unrelated fact tables (fact groups?)
* properly cast filter types before sending to the db driver
* investigate other postgresql drivers (psycopg2 does not do server-side parameter binding which bothers me)
* lookup metadata for each col once
* having clauses (measure filters)
* custom aggregation types
* in clause filters
    * group by filter id and then by filter operator and *then* we can get our list
* custom names for report columns
* ctes / multi fact table joins
* use dimensions as facts (handle table aliases correctly)
* date dimensions hierarchy?
* role playing dimensions (ordered_product_id, replacement_product_id?)
* pick least granular table that has all our required cols (rollups)
* handle facts at different granularities
* multi-fact measures (sum(fact.total_cost) - sum(fact2.inventory_cost))
* handle re-aggregation of division metrics (split into components and only divide in outermost query)
* paging / sorting (separate paging query? composite keys?)
* redudant code cleanup
* aftergroupby joins / selects (fact sum/filter/group by to final granularity in cte then join dimensions with no group by)
* use psycopg2's sql manipulation api
* add secondary fact and remaining dimensions to yaml file
* schema introspection to build skeleton warehouse
* separate files
* generator class
* __init__ files
* docs
* pylinting
* error handling
* better package name
* introspect tables and generate data that way
    * some things will need rules though
        * eg end_date after start_date, date ranges, cardinality of dimensions and facts, complex relationships between dimensions (discount requres a campaign_id, ship_date only if order status is shipped etc)

# Questions

## How do I handle selecting the correct facts if there are shared dimensions / metric names across fact tables (eg total_cost on orderline and inventoryline)
* Hinting
    * How do I make this work with aggregation
    * How do I make this work with multiple facts?
    * namespacing
        * similar to dimensions, add a short-name to the fact?
            * multiple facts with the same short-name? fact groups?