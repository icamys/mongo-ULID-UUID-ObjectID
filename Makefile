.PHONY: run
run:
	docker compose  up --build --abort-on-container-exit --exit-code-from perftest || true
	docker system prune --force --volumes --filter 'label=com.docker.compose.env=perftest'
