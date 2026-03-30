# GCP Data Pipeline — E-commerce

Projet perso pour apprendre GCP et le streaming de données.
Pipeline qui simule des commandes e-commerce en temps réel,
les transforme avec Apache Beam, les charge dans BigQuery et les affiche sur Looker Studio.

## Stack

- Python –> génération des orders via un script et pub sur le topic
- Pub/Sub –> bus de message 
- Apache Beam (DirectRunner) –> transformation et chargement dans BigQuery
- BigQuery –> stockage et analyse
- Airflow (Docker) –> orchestration quotidienne via DAG pour créer des tables dérivées
- Looker Studio –> dashboard

## Dashboard live

https://lookerstudio.google.com/s/vhnVNyaIJMw

## Structure du projet
```
gcp-pipeline-learning/
  simulate_order.py    # Générateur de commandes aléatoires
  pipeline.py          # Pipeline Beam : Pub/Sub et ingestion BigQuery
  requirements.txt     # Dépendances Python
  README.md
```

## Lancer en local
```bash
pip install -r requirements.txt
gcloud auth application-default login
python simulate_order.py   # terminal 1
python pipeline.py         # terminal 2
```

## Modèle de données

**orders_clean** — 1 ligne par article commandé
| Colonne | Type | Description |
|---|---|---|
| order_id | STRING | Identifiant unique de la commande |
| order_date | DATE | Date de la commande |
| user_id | STRING | Identifiant client |
| country | STRING | Pays (FR, DE, ES, IT) |
| device | STRING | Device (mobile, desktop) |
| product_id | STRING | Identifiant produit |
| product_name | STRING | Nom du produit |
| qty | INTEGER | Quantité commandée |
| unit_price | FLOAT | Prix unitaire |
| total_line | FLOAT | Total de la ligne (qty x unit_price) |

