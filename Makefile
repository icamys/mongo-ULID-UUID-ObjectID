MONGO_VERSION?="6.0.5"
MONGO_INIT_TIME?="5"
MONGO_INITDB_ROOT_USERNAME?="root"
MONGO_INITDB_ROOT_PASSWORD?="root"
MONGO_CONTAINER_NAME?="perftest-mongo"
GO_VERSION := $(shell go version)

.PHONY: run
run:
	@echo "> Stopping MongoDB container if present..."
	@docker stop ${MONGO_CONTAINER_NAME} || true
	@echo ""
	@echo "> Removing MongoDB container if present..."
	@docker rm ${MONGO_CONTAINER_NAME} || true
	@echo ""
	@echo "> Starting MongoDB..."
	@docker run \
		--env MONGO_INITDB_ROOT_USERNAME=${MONGO_INITDB_ROOT_USERNAME} \
		--env MONGO_INITDB_ROOT_PASSWORD=${MONGO_INITDB_ROOT_PASSWORD} \
		--name ${MONGO_CONTAINER_NAME} -p "27017:27017" -d mongo:${MONGO_VERSION}
	@echo "> Waiting for ${MONGO_INIT_TIME} seconds for Mongo to initialize..."
	@sleep ${MONGO_INIT_TIME}
	@echo ""
	@echo "> Environment info"
	@echo "- ${GO_VERSION}"
	@echo "- Mongo version ${MONGO_VERSION}"
	@echo ""
	@echo "> Running the test..."
	@echo ""
	@MONGO_URI="mongodb://${MONGO_INITDB_ROOT_USERNAME}:${MONGO_INITDB_ROOT_PASSWORD}@localhost:27017" \
		go run perftest.go tests.go printer.go
