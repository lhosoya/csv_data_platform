CSV data platform


# Install make
# Install helm
# Install docker
# Install Lens
# Install docker-compose



# Deploy ZooKeeper first (dependency)
kubectl apply -f zookeeper-deployment.yaml -f zookeeper-service.yaml

# Deploy Kafka
kubectl apply -f kafka-deployment.yaml -f kafka-service.yaml

# Deploy MinIO
kubectl apply -f minio-pvc.yaml -f minio-deployment.yaml -f minio-service.yaml

# Deploy Airflow (requires PostgreSQL running)
kubectl apply -f airflow-rbac.yaml -f airflow-configmap.yaml -f airflow-deployment.yaml -f airflow-service.yaml