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
# Todos 2017-08-09
* filters
* ctes / multi fact table joins
* require required cols from filters and join dimensions when finding facts
* use dimensions as facts
* date dimensions hierarchy?
* role playing dimensions
* pick least granular table that has all our required cols (rollups)
* handle facts at different granularities
* multi-fact measures
* paging / sorting
* redudant code cleanup
* aftergroupby joins / selects
* lookup metadata for each col once
* use psychopg2's sql manipulation api