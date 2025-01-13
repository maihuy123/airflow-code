import psycopg2 as db
import pandas as pd
 
conn_string = 'dbname=shopping user=admin host=localhost password=admin'
conn = db.connect(conn_string)
df = pd.read_sql('select * from sales limit 100', conn)
print(df.head(100))