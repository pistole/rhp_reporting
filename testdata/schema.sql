DROP SCHEMA IF EXISTS reporting cascade;
CREATE SCHEMA IF NOT EXISTS reporting;

CREATE TABLE IF NOT EXISTS reporting.DimCustomer
(
    customer_id SERIAL PRIMARY KEY,
    name text,
    address1 text,
    address2 text,
    state text,
    zip text,
    phone text,
    email text
);

CREATE TABLE IF NOT EXISTS reporting.DimProduct
(
    product_id SERIAL PRIMARY KEY,
    name text,
    curr_price decimal(12,2) -- probably would live in separate pricing tables
    -- more attributes
);


CREATE TABLE IF NOT EXISTS reporting.DimCampaign
(
    campaign_id SERIAL PRIMARY KEY,
    name text,
    start_time timestamptz,
    end_time timestamptz,
    description text
    -- etc
);

CREATE TABLE IF NOT EXISTS reporting.DimOrderStatus
(
    order_status_id INT PRIMARY KEY,
    name text
);
INSERT INTO reporting.DimOrderStatus
VALUES

    (1, 'Pending'),
    (2, 'Backorderd'),
    (3, 'Picking'),
    (4, 'Ready for Shipment'),
    (5, 'Shipped'),
    (6, 'Cancelled'),
    (7, 'Hold: Contact Customer'),
    (8, 'Hold: Payment Failed'),
    (9, 'Other')
;



CREATE TABLE IF NOT EXISTS reporting.FactOrderLine
(
    order_line_id SERIAL PRIMARY KEY,
    product_id int not null REFERENCES reporting.DimProduct(product_id),
    order_date timestamptz not null,
    ship_date timestamptz null DEFAULT null,
    customer_id int not null REFERENCES reporting.DimCustomer(customer_id),
    campaign_id int null REFERENCES reporting.DimCampaign(campaign_id),
    order_status_id int not null DEFAULT 1 REFERENCES reporting.DimOrderStatus(order_status_id),
    quantity int not null DEFAULT 1,
    total_discount decimal(12,2) NOT NULL DEFAULT 0,
    total_price decimal(12,2) NOT NULL,
    total_cost decimal(12,2) NOT NULL
);