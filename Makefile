APP_NAME='pubsub'
DATABASE_DOCKER_TAG='messagedb_database'
DATABASE_DOCKER_NAME='messagedb'
PORT=5433

build_db:
	docker build -t ${DATABASE_DOCKER_TAG} .

run_db:
	docker run -p ${PORT}:5432 --rm -P --name ${DATABASE_DOCKER_NAME} ${DATABASE_DOCKER_TAG}

run_localstack:
	localstack start
