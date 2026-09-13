COMPOSE = docker compose
RUN     = $(COMPOSE) run --rm ancora

.DEFAULT_GOAL := help
.PHONY: help build data run test lint shell clean

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	 | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

build:  ## Build the image (Spark + Delta + DuckDB, jars pre-fetched)
	$(COMPOSE) build

data:  ## Download the UCI Online Retail dataset into data/raw (never committed)
	@mkdir -p data
	$(RUN) python -m ancora.download

run:  ## Run bronze → silver → gold → serving. Optional: SNAPSHOT=2011-12-09
	@mkdir -p data
	$(RUN) python -m ancora run $(if $(SNAPSHOT),--snapshot-date $(SNAPSHOT),)

test:  ## Run the test suite inside the image (no network, no dataset needed)
	@mkdir -p data
	$(RUN) python -m pytest -q

lint:  ## Static checks
	$(RUN) python -m ruff check .
	$(RUN) python -m ruff format --check .

shell:  ## Open a shell in the container
	$(RUN) bash

clean:  ## Remove every layer of the lakehouse (keeps data/raw)
	rm -rf data/bronze data/silver data/gold data/serving
