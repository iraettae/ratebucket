.DEFAULT_GOAL := help
.PHONY: help install lint format typecheck test check core-purity secrets bench prove clean

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

install: ## Sync the dev environment from uv.lock
	uv sync --locked --all-extras

lint: ## Ruff lint + format check
	uv run ruff check .
	uv run ruff format --check --diff .

format: ## Ruff format, in place
	uv run ruff check --fix .
	uv run ruff format .

typecheck: ## mypy --strict (paths come from [tool.mypy] files, so it stays whole-program)
	uv run mypy

test: ## Run the test suite
	uv run pytest -q

core-purity: ## The core must not know the word. Zero matches required.
	@if grep -rniE --include='*.py' 'openai|prompt|model' src/ratebucket/core/ ; then \
		echo ""; \
		echo "FAIL: src/ratebucket/core/ mentions the domain."; \
		echo "The core arbitrates resources. It does not know what an LLM is."; \
		exit 1; \
	else \
		echo "core purity: OK, zero domain words in src/ratebucket/core/"; \
	fi

secrets: ## Scan the FULL git history, not just the working tree
	gitleaks git --no-banner --redact .

check: lint typecheck core-purity test ## Everything CI runs

bench-backfill: ## Measure conservative backfilling vs strict FIFO on a mixed workload
	uv run python bench/backfill_delta.py

bench: ## Fake provider + four engines, same 429 scenario. No keys, no money, no internet.
	uv run --with aiometer --with tenacity --with aiohttp --with tiktoken --with litellm \
	       --with matplotlib python bench/run.py

prove: ## Show that the proof-tests go red when their fix is removed
	@echo "not until stage 3"; exit 1

clean: ## Remove caches and build artifacts
	rm -rf build dist .mypy_cache .pytest_cache .ruff_cache .hypothesis htmlcov .coverage
	find . -name '__pycache__' -type d -prune -exec rm -rf {} +
