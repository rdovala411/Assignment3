import os
import time
import boto3
from flask import Flask


AWS_REGION         = "us-east-1"               
ATHENA_DATABASE    = "orders_db"               
ATHENA_TABLE       = "processed"               
S3_OUTPUT_LOCATION = "s3://3awsassignment/enriched/"  


app = Flask(__name__)
athena = boto3.client('athena', region_name=AWS_REGION)
s3     = boto3.client('s3', region_name=AWS_REGION)

queries_to_run = [
    {
        "title": "1. Total Sales by Customer",
        "query": f"""
            SELECT customer, SUM(amount) AS total_sales
            FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
            GROUP BY customer
            ORDER BY total_sales DESC
        """
    },
    {
        "title": "2. Monthly Order Volume and Revenue",
        "query": f"""
            SELECT date_trunc('month', CAST(orderdate AS DATE)) AS order_month,
                   COUNT(orderid) AS number_of_orders,
                   ROUND(SUM(amount),2) AS monthly_revenue
            FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
            GROUP BY date_trunc('month', CAST(orderdate AS DATE))
            ORDER BY order_month
        """
    },
    {
        "title": "3. Order Status Dashboard",
        "query": f"""
            SELECT status,
                   COUNT(orderid) AS order_count,
                   ROUND(SUM(amount),2) AS total_amount
            FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
            WHERE status IN ('shipped','confirmed')
            GROUP BY status
            ORDER BY order_count DESC
        """
    },
    {
        "title": "4. Average Order Value (AOV) per Customer",
        "query": f"""
            SELECT customer,
                   ROUND(AVG(amount),2) AS avg_order_value
            FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
            GROUP BY customer
            ORDER BY avg_order_value DESC
        """
    },
    {
        "title": "5. Top 10 Largest Orders in February 2025",
        "query": f"""
            SELECT orderid,
                   customer,
                   orderdate,
                   amount
            FROM "{ATHENA_DATABASE}"."{ATHENA_TABLE}"
            WHERE CAST(orderdate AS DATE) BETWEEN DATE '2025-02-01' AND DATE '2025-02-28'
            ORDER BY amount DESC
            LIMIT 10
        """
    },
]

def run_athena_query(sql):
    """Run Athena query, wait till completion, then return header + results"""
    resp = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={'Database': ATHENA_DATABASE},
        ResultConfiguration={'OutputLocation': S3_OUTPUT_LOCATION}
    )
    exec_id = resp['QueryExecutionId']

    # wait for completion
    while True:
        desc = athena.get_query_execution(QueryExecutionId=exec_id)
        state = desc['QueryExecution']['Status']['State']
        if state in ('SUCCEEDED','FAILED','CANCELLED'):
            break
        time.sleep(1)

    if state != 'SUCCEEDED':
        err = desc['QueryExecution']['Status'].get('StateChangeReason','Unknown error')
        return None, f"Query {exec_id} failed: {err}"

    # get S3 path for results
    out_loc = desc['QueryExecution']['ResultConfiguration']['OutputLocation']
    # parse bucket/key
    bucket_key = out_loc.replace('s3://','').split('/',1)
    bucket = bucket_key[0]
    key    = bucket_key[1]

    obj  = s3.get_object(Bucket=bucket, Key=key)
    data = obj['Body'].read().decode('utf-8').splitlines()
    header = [h.strip('"') for h in data[0].split(',')]
    rows   = [[c.strip('"') for c in line.split(',')] for line in data[1:]]
    return header, rows

@app.route('/')
def index():
    html = """<html><head><title>Athena Orders Dashboard</title>
              <style>
                body { font-family: sans-serif; margin: 2em; background:#f4f4f9; }
                h1 { color:#333; }
                table { border-collapse: collapse; width:90%; margin-top:20px; background:#fff; }
                th, td { border:1px solid #ccc; padding:10px; text-align:left; }
                th { background:#007bff; color:#fff; }
                tr:nth-child(even){background:#f2f2f2;}
              </style>
              </head><body>"""
    html += "<h1>📊 Athena Orders Dashboard</h1>"

    for item in queries_to_run:
        html += f"<h2>{item['title']}</h2>"
        header, rows_or_error = run_athena_query(item['query'])
        if header:
            html += "<table><thead><tr>"
            for col in header:
                html += f"<th>{col}</th>"
            html += "</tr></thead><tbody>"
            for row in rows_or_error:
                html += "<tr>"
                for cell in row:
                    html += f"<td>{cell}</td>"
                html += "</tr>"
            html += "</tbody></table>"
        else:
            html += f"<p style='color:red;'><strong>Error:</strong> {rows_or_error}</p>"

    html += "</body></html>"
    return html

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
