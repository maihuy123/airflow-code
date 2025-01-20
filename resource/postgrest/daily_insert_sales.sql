SELECT sale_id, store_id, product_id, sale_date, quantity_sold, sale_amount
	FROM public.sales
    WHERE sale_date = '{execute_date}'
    
    