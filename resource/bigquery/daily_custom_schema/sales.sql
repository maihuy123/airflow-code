Create table if not exists `{{ project }}.{{ dataset }}.{{ jobs }}_sales` (
    sale_id INT64,
    store_id INT64,
    product_id INT64,
    sale_date DATE,
    quantity_sold INT64,
    sale_amount FLOAT64
)
cluster by sale_id


