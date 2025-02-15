environment:
	docker-compose up -d

doris_schema:
	mysql -h 127.0.0.1 -P 9030 -u root < ./migration/doris/test_db.sql

test:
	export FE_NODES=127.0.0.1:8030 && \
	export BE_NODES=127.0.0.1:8040 && \
	export USERNAME=root && \
	go clean -testcache && \
	go test -v ./...