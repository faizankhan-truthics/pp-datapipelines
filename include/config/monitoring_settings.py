from airflow.models import Variable

GCP_CONN_ID = Variable.get("GCP_CONN_ID")
BQ_PROJECT_ID = Variable.get("GCP_PROJECT_ID")
BQ_MONITORING_DATASET = Variable.get("BQ_MONITORING_DATASET")
API_REQUEST_LOG_TABLE = f"{BQ_PROJECT_ID}.{BQ_MONITORING_DATASET}.api_requests"
