# Pokémon TCG Price Dataset Pipeline

This repository contains a Python pipeline that builds and maintains a **master Pokémon TCG dataset** from [tcgcsv.com](https://tcgcsv.com), which provides daily price archives from TCGPlayer.

The pipeline:
1. **Ingests** daily archives of card prices.  
2. **Extracts & normalizes** raw JSON into partitioned Parquet files.  
3. **Fetches metadata** (products & groups) via the TCGCSV API.  
4. **Merges** prices + products + groups into a curated dataset partitioned by day.  
5. **Optionally builds a full dataset** in a single Parquet file (incrementally appended).  
6. **Cleans up** temporary files and prunes raw archives to save space.  

---

## ⚙️ Installation

This project uses [**uv**](https://docs.astral.sh/uv/) for dependency management.

```bash
# Clone the repository
git clone https://github.com/<your-username>/pokemon-tcg-pricing.git
cd pokemon-tcg-pricing

# Install dependencies into a virtual environment
uv sync
```

---

## 🚀 Usage

Run the pipeline from the repo root:

```bash
uv run python scripts/tcgcsv_pokemon_pipeline.py     --start-date 2024-02-08     --end-date 2025-09-28     --interval 1     --base-dir ./data     --full-file
```

### Arguments
- `--start-date` → first date to harvest (YYYY-MM-DD).  
- `--end-date` → last date to harvest (YYYY-MM-DD).  
- `--interval` → spacing between dates (1 = daily, 7 = weekly).  
- `--base-dir` → output directory (default `./pokemon-tcg-pricing/data`).  
- `--keep-extracted` → if set, don’t delete extracted raw archives.  
- `--full-file` → also generate/append to one big merged Parquet file.  

### Typical Daily Run
For a daily update (most common case):

```bash
uv run python scripts/tcgcsv_pokemon_pipeline.py     --start-date $(date -d "yesterday" +%Y-%m-%d)     --end-date $(date -d "yesterday" +%Y-%m-%d)     --interval 1     --base-dir ./data     --full-file
```

## 🛠️ Makefile Shortcuts

This repository includes a **Makefile** so you don’t have to type long commands each time.

### Full History (Feb 8, 2024 → Today)
```bash
make full-history
```
Runs the pipeline for the entire history of TCGCSV data and updates the `daily/` partitions.

```bash
make full-history-full
```
Same as above, but also rebuilds the combined `pokemon_prices_with_full_features.parquet`.

---

### Daily Update (Yesterday Only)
```bash
make yesterday
```
Fetches just yesterday’s archive and updates `daily/`.

```bash
make yesterday-full
```
Same as above, but also appends/rebuilds the full parquet file.

---

These commands assume:
- `uv` is installed and configured.
- You’re running from the repository root.

---

## 🧹 Storage Management

- Raw `daily_prices/` → automatically pruned to keep only the **last 7 days**.  
- Temporary `archives/` and `extracted/` → always deleted after each run.  
- Curated `daily/` → permanent, partitioned dataset (use this for analysis).  
- Full file (`pokemon_prices_with_full_features.parquet`) → permanent convenience file.  

---

## 📊 Dataset Summary

- **Rows**: ~23M (as of Sept 2025).  
- **Columns**: 31 (numeric + categorical).  
- **Applications**: market trend forecasting, rarity classification, collectible asset analysis.  

---

## 📝 .gitignore Example

```gitignore
# Ignore all pipeline outputs
data/
!data_sample/

# Python cruft
__pycache__/
*.pyc
*.pyo

# Virtual environments
.venv/
```

---

## 📂 Directory Layout

```text
pokemon-tcg-pricing/
│
├── scripts/
│   └── tcgcsv_pokemon_pipeline.py     # main pipeline script
│
└── data/ (created at runtime)
    ├── archives/                      # temporary downloaded archives (auto-cleaned)
    ├── extracted/                     # temporary extraction dirs (auto-cleaned)
    ├── daily_prices/                  # raw per-day prices (keeps last 7 days only)
    ├── daily/                         # curated dataset, partitioned by day
    ├── groups.parquet                 # cached group metadata
    ├── products.parquet               # cached product metadata
    └── pokemon_prices_with_full_features.parquet   # optional combined file
```
