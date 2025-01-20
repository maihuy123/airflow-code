create table if not exists `{{ project }}.{{ dataset }}.{{ jobs }}_shipment` (
    shipment_id INT64,
    order_id INT64,
    product_id INT64,
    shipment_date DATE,
)