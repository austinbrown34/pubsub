APP_NAME=pubsub
DATABASE_DOCKER_TAG=messagedb_database
DATABASE_DOCKER_NAME=messaged'
DATABASE=message_store
USER=message_store
PORT=5433
POSTGRES_USER=postgres
DATABASE_SCHEMA=message_store

build_db:
	docker build -t ${DATABASE_DOCKER_TAG} .

run_db:
	docker run -p ${PORT}:5432 --rm -P --name ${DATABASE_DOCKER_NAME} ${DATABASE_DOCKER_TAG}

clear_messages:
	psql -h localhost -p ${PORT} -d ${DATABASE} -U ${USER} -c "TRUNCATE message_store.messages RESTART IDENTITY;"

grant_db_permissions:
	psql -h localhost -p ${PORT} -d ${DATABASE} -U ${POSTGRES_USER} -c "ALTER ROLE ${USER} WITH SUPERUSER;"

run_localstack:
	localstack start
