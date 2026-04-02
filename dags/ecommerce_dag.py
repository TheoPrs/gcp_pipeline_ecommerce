from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime
from airflow.operators.python import BranchPythonOperator,PythonOperator
from google.cloud import bigquery
from airflow.operators.dummy import DummyOperator


PROJECT_ID = "gcp-pipeline-theo"
DATASET    = "ecommerce"
LOCATION   = "EU"

default_args = {
    "owner": "theo",
    "retries": 1,
    "start_date": datetime(2024, 1, 1),
}

DAILY_REVENUE_SQL = """
CREATE OR REPLACE TABLE `{project}.{dataset}.daily_revenue` AS
SELECT
    order_date,
    country,
    product_name,
    COUNT(DISTINCT order_id)  AS nb_commandes,
    SUM(qty)                  AS nb_articles,
    ROUND(SUM(total_line), 2) AS ca_total
FROM `{project}.{dataset}.orders_clean`
GROUP BY 1, 2, 3
ORDER BY order_date DESC, ca_total DESC
""".format(project=PROJECT_ID, dataset=DATASET)

CUSTOMER_SEGMENTS_SQL = """
CREATE OR REPLACE TABLE `{project}.{dataset}.customer_segments` AS
WITH commandes AS (
    SELECT user_id, order_id, SUM(total_line) AS total_commande
    FROM `gcp-pipeline-theo.ecommerce.orders_clean`
    GROUP BY user_id, order_id
),
aggregated AS (
    SELECT user_id,count(distinct order_id) as nb_commandes, round(sum(total_commande),2) as ca_total, round(avg(total_commande),2) as panier_moyen
    FROM commandes
    GROUP BY user_id

)
SELECT
    user_id,
    nb_commandes,
    ca_total,
    panier_moyen,
    CASE     
        WHEN nb_commandes >= 5 AND panier_moyen > 150 THEN 'VIP'
        WHEN nb_commandes >= 3 THEN 'Régulier'
        WHEN nb_commandes = 1 THEN 'Nouveau'
        ELSE 'Occasionnel'
    END AS segment
FROM aggregated
""".format(project=PROJECT_ID, dataset=DATASET)

def alert():
    print("Anomalie détectée sur le CA du jour !")

def no_alert():
    print("Aucune anomalie sur le chiffre d'affaire d'aujourd'hui. ")

def check_anomaly():
    client = bigquery.Client(project="gcp-pipeline-theo")
    query = """
        SELECT
    AVG(ca_total) AS ca_moyen_7j,
    (SELECT SUM(ca_total) FROM `gcp-pipeline-theo.ecommerce.daily_revenue` WHERE order_date = CURRENT_DATE()) AS ca_aujourd_hui
FROM `gcp-pipeline-theo.ecommerce.daily_revenue`
WHERE order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    """
    result = client.query(query).result()
    row = list(result)[0]
    ca_moyen = row["ca_moyen_7j"]
    ca_aujourd_hui = row["ca_aujourd_hui"]
    if ca_aujourd_hui is None or not (ca_moyen * 0.5 < ca_aujourd_hui < ca_moyen * 1.5):
        return "no_alert"
    else:
        return "log_alert"

with DAG(
    dag_id="ecommerce_pipeline",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    
) as dag:
   
    compute_revenue = BigQueryInsertJobOperator(
          task_id="compute_daily_revenue",
          configuration={
              "query": {
                  "query": DAILY_REVENUE_SQL,
                  "useLegacySql": False,
              }
          },
          location=LOCATION,
      )
    
    customer_segments = BigQueryInsertJobOperator(
        task_id="customer_segments",
        configuration={
            "query":{
                "query" : CUSTOMER_SEGMENTS_SQL,
                "useLegacySql": False,
            }
        },
        location=LOCATION,
    )
    
    check_ca_anomaly = BranchPythonOperator(
          task_id="check_ca_anomaly",
        python_callable=check_anomaly,
    )

    log_alert = PythonOperator(
        task_id="log_alert",
        python_callable=alert,
    )

    no_alert = DummyOperator(
        task_id="no_alert"
    )

    compute_revenue >> [customer_segments, check_ca_anomaly]
    check_ca_anomaly >> [log_alert, no_alert]