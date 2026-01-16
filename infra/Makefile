# Makefile for managing the deployment of Case services
# Build a docker image for Airflow 2.9.3 with the necessary dependencies with JAVA and PySpark
build-docker:
	docker build -t myairflow:latest .

# Full clean deployment of the ecosystem, Airflow, and MinIO buckets with the sample_events.csv data
deploy-all:
	make deploy-ecosystem
	make deploy-airflow
	make create-minio-buckets
	make deploy-clickhouse
	make airflow-clickhouse-connection
	
# Shutdown and cleans all of the deployed services and volumes
clean-all:
	make down-airflow-volumes
	make down-ecosystem-volumes
	make down-clickhouse-volumes

# Shutdown of the deployed services
shutdown-all:
	make down-airflow
	make down-ecosystem
	make down-clickhouse

# Post-exec of minio to create the necessary buckets and upload sample data
create-minio-buckets:
	docker exec -i minio mc alias set myminio http://localhost:9000 minio minio123
	docker exec -i minio mc mb -p myminio/landing || true
	docker exec -i minio mc mb -p myminio/lakehouse || true
	docker cp ../data/sample_events_1.csv minio:/tmp/sample_events_1.csv
	docker cp ../data/sample_events_2.csv minio:/tmp/sample_events_2.csv
	docker cp ../data/sample_events_3.csv minio:/tmp/sample_events_3.csv
	docker exec -i minio mc cp /tmp/sample_events_1.csv myminio/landing/sample_events_1.csv
	docker exec -i minio mc cp /tmp/sample_events_2.csv myminio/landing/sample_events_2.csv
	docker exec -i minio mc cp /tmp/sample_events_3.csv myminio/landing/sample_events_3.csv

# Add ClickHouse connection to Airflow
airflow-clickhouse-connection:
	docker exec -i airflow-webserver airflow connections add 'clickhouse_connection' --conn-uri 'sqlite://default:default@clickhouse:9000' || true

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

# Deploy and bring up ClickHouse
deploy-clickhouse:
	docker-compose -f docker-compose-clickhouse.yml up -d

# Shutdown ClickHouse services
down-clickhouse:
	docker-compose -f docker-compose-clickhouse.yml down

# Shutdown ClickHouse services and remove volumes
down-clickhouse-volumes:
	docker-compose -f docker-compose-clickhouse.yml down -v