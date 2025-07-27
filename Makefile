TERRAFORM_DIR = infra/terraform

init-minio:
	@echo "Initializing Terraform for MinIO..."
	terraform -chdir=$(TERRAFORM_DIR) init

plan-minio:
	@echo "Creating execution plan..."
	terraform -chdir=$(TERRAFORM_DIR) plan \
	  -var="use_minio=true" \
	  -var="minio_endpoint=http://localhost:9000"

apply-minio:
	@echo "Creating MinIO bucket..."
	terraform -chdir=$(TERRAFORM_DIR) apply -auto-approve \
	  -var="use_minio=true" \
	  -var="minio_endpoint=http://localhost:9000"

destroy-minio:
	@echo "Destroying MinIO bucket..."
	terraform -chdir=$(TERRAFORM_DIR) destroy -auto-approve \
	  -var="use_minio=true" \
	  -var="minio_endpoint=http://localhost:9000"

start-minio:
	docker-compose up -d minio
	sleep 10  # Wait for MinIO to initialize

setup-minio: start-minio init-minio apply-minio

test-s3:
	docker-compose build s3-tester
	docker-compose run --rm s3-tester

clean:
	docker-compose down -v
	make destroy-minio