
/*
CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.FactOrderLine_Base AS
SELECT
    order_line_id,
    product_id,
    order_date,
    ship_date,
    customer_id,
    campaign_id,
    order_status_id,
    quantity,
    total_discount,
    total_price,
    total_cost
FROM
     reporting.FactOrderLine;
*/


CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.FactOrderLine_Product AS
SELECT
    product_id,
    SUM(quantity) AS quantity,
    SUM(total_discount) AS total_discount,
    SUM(total_price) AS total_price,
    SUM(total_cost) AS total_cost
FROM
     reporting.FactOrderLine
GROUP BY product_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.FactOrderLine_Customer AS
SELECT
    customer_id,
    SUM(quantity) AS quantity,
    SUM(total_discount) AS total_discount,
    SUM(total_price) AS total_price,
    SUM(total_cost) AS total_cost
FROM
     reporting.FactOrderLine
GROUP BY customer_id;

CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.FactOrderLine_Campaign AS
SELECT
    campaign_id,
    SUM(quantity) AS quantity,
    SUM(total_discount) AS total_discount,
    SUM(total_price) AS total_price,
    SUM(total_cost) AS total_cost
FROM
     reporting.FactOrderLine
GROUP BY campaign_id;


CREATE MATERIALIZED VIEW IF NOT EXISTS reporting.FactOrderLine_OrderStatus AS
SELECT
    order_status_id,
    SUM(quantity) AS quantity,
    SUM(total_discount) AS total_discount,
    SUM(total_price) AS total_price,
    SUM(total_cost) AS total_cost
FROM
     reporting.FactOrderLine
GROUP BY order_status_id;
