.PHONY: help install test test-cov docker-build docker-run docker-test clean

help:
	@echo "Kickstarter ETL Pipeline - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  install      - Install dependencies"
	@echo "  test         - Run tests"
	@echo "  test-cov     - Run tests with coverage"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run ETL in Docker"
	@echo "  docker-test  - Run tests in Docker"
	@echo "  clean        - Remove generated files"

install:
	pip install -r requirements-dev.txt

test:
	pytest tests/ -v

test-cov:
	pytest tests/ -v --cov=src --cov-report=term-missing

docker-build:
	docker-compose build

docker-run:
	docker-compose up etl

docker-test:
	docker-compose run etl-test

clean:
	rm -rf __pycache__ .pytest_cache .coverage htmlcov
	rm -rf tests/__pycache__ src/__pycache__
	rm -f data/*.db data/test_warehouse.db
	find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
