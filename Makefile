# Makefile for Pokémon TCG Dataset Pipeline

BASE_DIR=./data
SCRIPT=pokemon-tcg-pricing/scripts/create_data/tcgcsv_pokemon_pipeline.py

# Calculate yesterday in YYYY-MM-DD format
YESTERDAY=$(shell date -d "yesterday" +%Y-%m-%d)

# Run the pipeline for the entire history of data
full-history:
	uv run python $(SCRIPT) \
		--start-date 2024-02-08 \
		--end-date $(shell date +%Y-%m-%d) \
		--interval 1 \
		--base-dir $(BASE_DIR)

# Run the pipeline for the entire history of data AND rebuild the full file
full-history-full:
	uv run python $(SCRIPT) \
		--start-date 2024-02-08 \
		--end-date $(shell date +%Y-%m-%d) \
		--interval 1 \
		--base-dir $(BASE_DIR) \
		--full-file

# Run the pipeline for just yesterday
yesterday:
	uv run python $(SCRIPT) \
		--start-date $(YESTERDAY) \
		--end-date $(YESTERDAY) \
		--interval 1 \
		--base-dir $(BASE_DIR)

# Run the pipeline for just yesterday AND update the full file
yesterday-full:
	uv run python $(SCRIPT) \
		--start-date $(YESTERDAY) \
		--end-date $(YESTERDAY) \
		--interval 1 \
		--base-dir $(BASE_DIR) \
		--full-file
