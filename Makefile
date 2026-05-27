.PHONY: clone
clone: test/resonate-test

test/resonate-test:
	@if [ -d test/resonate-test ]; then \
		echo "test/resonate-test already exists, skipping clone"; \
	else \
		git clone git@github.com:resonatehq/resonate-test.git test/resonate-test; \
	fi

.PHONY: test
test: clone
	docker compose -f test/docker-compose.yml up --build \
		--abort-on-container-exit \
		--exit-code-from testbed

.PHONY: clean
clean:
	docker compose -f docker-compose.yaml down -v --remove-orphans
	docker compose -f docker-compose.test.yml --profile all down -v --remove-orphans
	docker compose -f test/docker-compose.yml down -v --remove-orphans
	docker network rm resonate 2>/dev/null || true
