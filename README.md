Project Structure: -

wikipedia-anomaly-pipeline/
├── producer/
│   └── wiki_producer.py
├── databricks/
│   ├── 01_bronze_ingestion.py
│   ├── 02_silver_features.py
│   ├── 03_gold_aggregates.py
│   └── 04_anomaly_model.py
├── dashboard/
│   └── app.py
├── requirements.txt
└── README.md