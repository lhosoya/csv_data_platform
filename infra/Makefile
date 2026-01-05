.PHONY: help deploy-all helm-add-repos deploy-minio deploy-kafka deploy-airflow delete-all port-forward status logs clean docker-kafka-up docker-kafka-down docker-kafka-logs docker-kafka-ps

NAMESPACE ?= default
HELM := helm
KUBECTL := kubectl
DOCKER_COMPOSE := docker-compose





# Helm repository URLs
MINIO_REPO := https://charts.min.io/
BITNAMI_REPO := https://charts.bitnami.com/bitnami
AIRFLOW_REPO := https://airflow.apache.org
TRINO_REPO := https://trinodb.github.io/charts

help:
	@echo "Available targets:"
	@echo "  make helm-add-repos      - Add Helm repositories"
	@echo "  make deploy-all          - Deploy all services (ZooKeeper, Kafka, MinIO, Airflow)"
	@echo "  make deploy-minio        - Deploy MinIO via Helm"
	@echo "  make deploy-hive         - Deploy Hive Metastore via Helm"
	@echo "  make deploy-trino        - Deploy Trino via Helm (with Hive and MinIO integration)"
	@echo "  make deploy-airflow      - Deploy Airflow via Helm"
	@echo "  make delete-all          - Delete all Helm releases"
	@echo "  make port-forward        - Port forward services for local access"
	@echo "  make status              - Check status of all releases"
	@echo "  make logs                - View logs from pods"
	@echo "  make docker-metabase-up  - Start Metabase in Docker"
	@echo "  make docker-metabase-down- Stop Metabase in Docker"
	@echo "  make docker-metabase-ps  - Show status of Docker Metabase container"
	@echo "  make docker-metabase-logs- View Docker Metabase logs"
	@echo "  make clean               - Delete all resources"
	@echo "  make docker-kafka-up     - Start standalone Kafka in Docker"
	@echo "  make docker-kafka-down   - Stop standalone Kafka in Docker"
	@echo "  make docker-kafka-ps     - Show status of Docker Kafka containers"
	@echo "  make docker-kafka-logs   - View Docker Kafka logs"
	@echo "  make config-minio		  - Configure MinIO buckets and policies"

deploy-all: helm-add-repos deploy-minio deploy-airflow docker-kafka-up
	@echo "✓ All services deployed successfully!"

helm-add-repos:
	@echo "Adding Helm repositories..."
	$(HELM) repo add minio $(MINIO_REPO) --force-update
	$(HELM) repo add bitnami $(BITNAMI_REPO) --force-update
	$(HELM) repo add apache-airflow $(AIRFLOW_REPO) --force-update
	$(HELM) repo add trino $(TRINO_REPO) --force-update

# Deploy Hive Metastore to Kubernetes
deploy-hive: helm-add-repos
	@echo "Deploying Hive Metastore via Helm..."
	$(HELM) upgrade --install hive-metastore bitnami/hive --namespace default --create-namespace --set hiveMetastore.enabled=true --set hiveServer.enabled=false --set hiveMetastore.ports.tcp=9083 --wait
	@echo "✓ Hive Metastore deployed"

# Deploy Trino to Kubernetes with Hive and MinIO integration
deploy-trino: helm-add-repos
	@echo "Deploying Trino via Helm..."
	$(HELM) upgrade --install trino trino/trino --namespace default --create-namespace --values ../trino-values.yaml --wait
	@echo "✓ Trino deployed"
	$(HELM) repo update
	@echo "✓ Helm repositories added"


# Create k8s minio deployment
# Get the deployment name and access to retrieve secrets kubectl get pods --namespace default -l "release=minio" -o jsonpath="{.items[0].metadata.name}"
# Use either powershell or unix, git bash does not support /bin/bash or /bin/sh
deploy-minio: helm-add-repos
	@echo "Deploying MinIO via Helm..."
	$(HELM) install --namespace $(NAMESPACE) \
		--set resources.requests.memory=512Mi \
		--set replicas=1 \
		--set persistence.enabled=true \
		--set mode=standalone \
		--set rootUser=admin,rootPassword=admin123 \
		minio minio/minio --wait
	@echo "✓ MinIO deployed"

configure-minio:
	@echo "Configuring MinIO buckets and policies..."
	@POD_NAME=$$(kubectl get pods --namespace default -l 'release=minio' -o jsonpath='{.items[0].metadata.name}'); \
	echo "Using Pod: $$POD_NAME"; \
	kubectl exec -n default -ti "$$POD_NAME" -- /bin/bash -c "mc alias set myminio http://localhost:9000 admin admin123"; \
	kubectl exec -n default -ti "$$POD_NAME" -- /bin/bash -c "mc admin accesskey create myminio admin --access-key acceskey123 --secret-key secretkey123";
	@echo "✓ MinIO configured"

deploy-airflow: helm-add-repos
	@echo "Deploying Airflow via Helm..."
	$(HELM) upgrade --install airflow apache-airflow/airflow --namespace airflow --create-namespace \
		--set postgresql.image.repository=bitnami/postgresql \
		--set postgresql.image.tag=latest \
		--wait
	@echo "✓ Airflow deployed"

delete-all:
	@echo "Deleting all Helm releases..."
	$(HELM) uninstall airflow --namespace=airflow --ignore-not-found
	$(HELM) uninstall minio --namespace=$(NAMESPACE) --ignore-not-found
	@echo "✓ All releases uninstalled"

status:
	@echo "=== Helm Releases Status ==="
	$(HELM) list --namespace=$(NAMESPACE)
	@echo "\n=== Pods Status ==="
	$(KUBECTL) get pods --namespace=$(NAMESPACE)
	@echo "\n=== Services Status ==="
	$(KUBECTL) get svc --namespace=$(NAMESPACE)
	@echo "\n=== PersistentVolumeClaims Status ==="
	$(KUBECTL) get pvc --namespace=$(NAMESPACE)

logs:
	@echo "\n=== MinIO Logs ==="
	$(KUBECTL) logs -l app.kubernetes.io/name=minio -f --namespace=$(NAMESPACE) --tail=50 2>/dev/null || echo "MinIO not running"
	@echo "\n=== Kafka Logs ==="
	$(KUBECTL) logs -l app.kubernetes.io/name=kafka -f --namespace=$(NAMESPACE) --tail=50 2>/dev/null || echo "Kafka not running"
	@echo "\n=== Airflow Webserver Logs ==="
	$(KUBECTL) logs -l component=webserver -f --namespace=$(NAMESPACE) --tail=50 2>/dev/null || echo "Airflow Webserver not running"
	@echo "\n=== Airflow Scheduler Logs ==="
	$(KUBECTL) logs -l component=scheduler -f --namespace=$(NAMESPACE) --tail=50 2>/dev/null || echo "Airflow Scheduler not running"

port-forward:
	@echo "Starting port forwarding..."
	@echo "Opening ports:"
	@echo "  MinIO API: http://localhost:9000"
	@echo "  MinIO Console: http://localhost:9001"
	@echo "  Kafka: localhost:9092"
	@echo "  Airflow: http://localhost:8080"
	@echo "Press Ctrl+C to stop port forwarding"
	@$(KUBECTL) port-forward -n $(NAMESPACE) svc/minio 9000:9000 &
	@$(KUBECTL) port-forward -n $(NAMESPACE) svc/minio-console 9001:9001 &
	@$(KUBECTL) port-forward -n $(NAMESPACE) svc/airflow-webserver 8080:8080 &
	@wait

clean: delete-all docker-kafka-down
	@echo "Cleanup complete!"

# Docker Compose Metabase Standalone targets
docker-metabase-up:
	@echo "Starting Metabase in Docker..."
	docker-compose -f $(DOCKER_COMPOSE_METABASE_FILE) up -d
	@echo "✓ Metabase started successfully!"
	@echo "  Metabase: http://localhost:3000"

docker-metabase-down:
	@echo "Stopping Metabase in Docker..."
	docker-compose -f $(DOCKER_COMPOSE_METABASE_FILE) down
	@echo "✓ Metabase stopped"

docker-metabase-ps:
	@echo "=== Docker Metabase Status ==="
	docker-compose -f $(DOCKER_COMPOSE_METABASE_FILE) ps

docker-metabase-logs:
	@echo "=== Docker Metabase Logs ==="
	docker-compose -f $(DOCKER_COMPOSE_METABASE_FILE) logs -f

# Docker Compose Kafka Standalone targets
docker-kafka-up:
	@echo "Starting standalone Kafka in Docker..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) up -d
	@echo "✓ Kafka started successfully!"
	@echo "  Kafka external access: localhost:9094"
	@echo "  ZooKeeper: localhost:2181"

docker-kafka-down:
	@echo "Stopping standalone Kafka in Docker..."
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) down
	@echo "✓ Kafka stopped"

docker-kafka-ps:
	@echo "=== Docker Kafka Status ==="
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) ps

docker-kafka-logs:
	@echo "=== Docker Kafka Logs ==="
	$(DOCKER_COMPOSE) -f $(DOCKER_COMPOSE_FILE) logs -f

.DEFAULT_GOAL := help
