# Build a docker image for Airflow 2.9.3 with the necessary dependencies with JAVA and PySpark
build-docker:
	docker build -t myairflow:latest .

# Full clean deployment of the ecosystem, Airflow, and MinIO buckets with the sample_events.csv data
deploy-ecosystem-airflow-buckets:
	make deploy-ecosystem
	make deploy-airflow
	make create-minio-buckets

# Shutdown of the deployed services
down-ecosystem-airflow:
	make down-airflow
	make down-ecosystem

# Shutdown and cleans all of the deployed services and volumes
down-all:
	make down-airflow-volumes
	make down-ecosystem-volumes

# Post-exec of minio to create the necessary buckets and upload sample data
create-minio-buckets:
	docker exec -i minio mc alias set myminio http://localhost:9000 minio minio123
	docker exec -i minio mc mb -p myminio/landing || true
	docker exec -i minio mc mb -p myminio/lakehouse || true
	docker cp ../data/sample_events.csv minio:/tmp/sample_events.csv
	docker exec -i minio mc cp /tmp/sample_events.csv myminio/landing/sample_events.csv

# Deploy and bring up Airflow
deploy-airflow:
	docker-compose -f docker-compose-airflow.yml up -d

# Deploy and bring up the ecosystem (MinIO, Kafka, Zookeeper & Metabase)
deploy-ecosystem:
	docker-compose -f docker-compose-ecosystem.yml up -d

# Shutdown Airflow services
down-airflow:
	docker-compose -f docker-compose-airflow.yml down

# Shutdown Ecosystem services
down-ecosystem:
	docker-compose -f docker-compose-ecosystem.yml down

# Shutdown Airflow services and remove volumes
down-airflow-volumes:
	docker-compose -f docker-compose-airflow.yml down -v

# Shutdown Ecosystem services and remove volumes
down-ecosystem-volumes:
	docker-compose -f docker-compose-ecosystem.yml down -v
