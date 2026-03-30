# GCP Data Pipeline — E-commerce

Pipeline de données en temps réel sur Google Cloud Platform, construit de A à Z.

## Architecture
```
simulate_order.py → Pub/Sub → Apache Beam → BigQuery → Airflow → Looker Studio
```

## Services GCP utilisés

- **Pub/Sub** — bus de messages, ingestion des commandes en temps réel
- **Apache Beam (DirectRunner)** — transformation et chargement dans BigQuery
- **BigQuery** — stockage et analyse des données
- **Cloud Composer / Airflow** — orchestration du pipeline quotidien
- **Looker Studio** — dashboard de visualisation

## Structure du projet
```
gcp-pipeline-learning/
├── simulate_order.py    # Générateur de commandes aléatoires
├── pipeline.py          # Pipeline Beam : Pub/Sub → BigQuery
├── requirements.txt     # Dépendances Python
└── README.md
```

## Lancer le projet

### 1. Installer les dépendances
```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

### 2. Authentification GCP
```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project TON_PROJECT_ID
```

### 3. Lancer le simulateur
```bash
python simulate_order.py
```

### 4. Lancer le pipeline
```bash
python pipeline.py
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


## Dashboard Looker Studio

Visualisation en temps réel des données de la pipeline :
https://lookerstudio.google.com/s/vhnVNyaIJMw
