help:
	@echo "  make infra-build 				Build docker image for terraform & init"
	@echo "  make infra-run command=apply   		Run init / plan / apply / destroy in Terraform using Docker"
	@echo "  make init        				Build and init Airflow"
	@echo "  make up          				Start airflow"
	@echo "  make down        				Stop airflow"

infra-init:
	docker build -t nyc_taxi/terraform -f Dockerfile.terraform --build-arg $(shell grep -F -- "GCP_CREDENTIALS_PATH" .env) . 
	docker run --rm -it -v $(shell pwd)/terraform:/data -w /data --env-file .env nyc_taxi/terraform init

infra-run:
	docker run --rm -it -v $(shell pwd)/terraform:/data -w /data --env-file .env nyc_taxi/terraform $(command)


init:
	docker-compose build
	docker-compose up airflow-init

up:
	docker-compose up

down:
	docker-compose down