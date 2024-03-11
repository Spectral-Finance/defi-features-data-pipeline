SHELL := /bin/bash
REPOSITORY_NAME =
AWS_ACCOUNT_ID =
AWS_REGION = 
DOMAIN_NAME = 
DOMAIN_OWNER = 
LIBRARY_NAME= 
SDL_REPOSITORY_NAME = 
SDL_PACKAGE_NAME = 
CODEARTIFACT_USER =
OS_NAME := `uname -s | tr A-Z a-z`
PROJECT_NAME=
IMAGE_NAME = ${PROJECT_NAME}

# Requires docker to be installed in your OS
login_ecs_docker_registry:
	aws ecr get-login-password --region us-east-2 | \
	docker login --username AWS --password-stdin ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com

build_image:
	docker build --pull --no-cache --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME} -f devops/ecs/Dockerfile .

# Push the docker image to ECR
build_and_push_image_to_ecr_dev:
	docker build --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME} -f devops/ecs/Dockerfile . \
	&& docker tag data-lakehouse/${IMAGE_NAME}:latest ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-dev:latest \
	&& docker push ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-dev:latest

build_and_push_image_to_ecr_prod:
	docker build --platform=linux/amd64 --build-arg CODEARTIFACT_TOKEN=`aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} \
	--domain-owner ${DOMAIN_OWNER} --query authorizationToken --output text` \
	-t data-lakehouse/${IMAGE_NAME} -f devops/ecs/Dockerfile .
	docker tag data-lakehouse/${IMAGE_NAME}:latest ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-prod:latest
	docker push ${DOMAIN_OWNER}.dkr.ecr.us-east-2.amazonaws.com/data-lakehouse/${IMAGE_NAME}-prod:latest

# Configures poetry to enable it to access the appropriate AWS's Codeartifact repository
conf_poetry_aws_repository:
	poetry config repositories.${SDL_REPOSITORY_NAME} https://finance-${AWS_ACCOUNT_ID}.d.codeartifact.${AWS_REGION}.amazonaws.com/pypi/${SDL_REPOSITORY_NAME}/
	poetry config http-basic.${SDL_REPOSITORY_NAME} ${CODEARTIFACT_USER} `aws codeartifact get-authorization-token --domain ${DOMAIN_NAME} --domain-owner ${AWS_ACCOUNT_ID} --query authorizationToken --output text`

# Requires Poetry>=1.2.2
# Execute the 'conf_poetry_aws_repository' before this one
poetry_add_latest_spectral_libs:
	poetry source add spectral-data-repository --secondary \
		https://finance-${AWS_ACCOUNT_ID}.d.codeartifact.${AWS_REGION}.amazonaws.com/pypi/${SDL_REPOSITORY_NAME}/simple/
	poetry add ${SDL_PACKAGE_NAME}==`aws codeartifact list-package-versions --domain ${DOMAIN_NAME} --repository ${SDL_REPOSITORY_NAME} \
		--package ${SDL_PACKAGE_NAME} --format pypi --status Published --sort-by PUBLISHED_TIME \
		| jq -r '.versions[0].version'` \
		 --source ${SDL_REPOSITORY_NAME}

# Delete the poetry virtual environment to run the project locally
delete_local_env:
	rm -rf `poetry env info -p`
	@echo "Virtual environment deleted"

setup_airflow_locally:
	cd devops/airflow && docker-compose up -d && ./config/setup_aws_connection.sh

create_ecs_stack_dev:
	cd devops/ecs/infrastructure && \
	terraform workspace select -or-create dev && \
	terraform apply

create_ecs_stack_prod:
	cd devops/ecs/infrastructure && \
	terraform init && \
	terraform workspace select prod && \
	terraform apply
