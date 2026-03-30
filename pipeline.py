from apache_beam.io.gcp.pubsub import ReadFromPubSub
import json
import apache_beam as beam
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
from apache_beam.options.pipeline_options import StandardOptions, PipelineOptions

PROJECT_ID   = "gcp-pipeline-theo"
SUBSCRIPTION = f"projects/{PROJECT_ID}/subscriptions/orders-raw-sub"
TABLE        = f"{PROJECT_ID}:ecommerce.orders_clean"

def flatten_items(commande):
    for item in commande["items"]:
        yield {
            "order_id":    commande["order_id"],
            "order_date":  commande["timestamp"][:10],
            "user_id":     commande["user_id"],
            "country":     commande["country"],
            "device":      commande["device"],
            "product_id":  item["product_id"],
            "product_name": item["name"],
            "qty":         item["qty"],
            "unit_price":  item["price"],
            "total_line":  round(item["qty"] * item["price"], 2),
        }

SCHEMA={
    "fields": [
        {"name": "order_id",      "type": "STRING",    "mode": "REQUIRED"},
        {"name": "order_date",    "type": "DATE",      "mode": "NULLABLE"},
        {"name": "user_id",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "country",       "type": "STRING",    "mode": "NULLABLE"},
        {"name": "device",        "type": "STRING",    "mode": "NULLABLE"},
        {"name": "product_id",    "type": "STRING",    "mode": "NULLABLE"},
        {"name": "product_name",  "type": "STRING",    "mode": "NULLABLE"},
        {"name": "qty",           "type": "INTEGER",   "mode": "NULLABLE"},
        {"name": "unit_price",    "type": "FLOAT",     "mode": "NULLABLE"},
        {"name": "total_line",    "type": "FLOAT",     "mode": "NULLABLE"},
    ]
}

def run():
    opts = PipelineOptions([
    f"--project={PROJECT_ID}",
    "--runner=DirectRunner",
    "--streaming",])
    opts.view_as(StandardOptions).streaming = True

    with beam.Pipeline(options=opts) as p:
        (
            p
            | "Read"    >> 
              ReadFromPubSub(subscription=SUBSCRIPTION)
            | "Parse"   >> beam.Map(lambda x: json.loads(x.decode("utf-8")))
            | "Flatten" >> beam.FlatMap(flatten_items)
            | "Write"   >> WriteToBigQuery(
                table=TABLE,
                schema=SCHEMA,
                write_disposition=BigQueryDisposition.WRITE_APPEND,
                create_disposition=BigQueryDisposition.CREATE_IF_NEEDED,
            )
        )

if __name__ == "__main__":
    run()

