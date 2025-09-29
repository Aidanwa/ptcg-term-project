import argparse
import pandas as pd
from pathlib import Path

def inspect_dataset(base_dir: Path, limit: int = 5, show_summary: bool = True):
    file = base_dir / "pokemon_prices_with_full_features.parquet"
    if not file.exists():
        print(f"[ERROR] File not found: {file}")
        return

    print(f"Opening dataset: {file}")

    # Load the full parquet into a DataFrame
    df = pd.read_parquet(file, engine="pyarrow")

    print("\n=== Dataset Overview ===")
    print(f"Total rows: {len(df):,}")
    print(f"Total columns: {len(df.columns)}")
    print(f"Columns: {', '.join(df.columns)}")

    print("\n=== Sample rows ===")
    print(df.head(limit))

    if show_summary and "date" in df.columns:
        print("\n=== Row counts per date (first 20 days) ===")
        print(df.groupby("date").size().head(20))
        print("\n=== Row counts per date (last 20 days) ===")
        print(df.groupby("date").size().tail(20))
        print(f"\nUnique dates: {df['date'].nunique()}")
        
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--base-dir", default="./pokemon-tcg-pricing/data", help="Base directory with parquet file")
    ap.add_argument("--limit", type=int, default=5, help="How many rows to preview")
    ap.add_argument("--no-summary", action="store_true", help="Disable daily summary")
    args = ap.parse_args()

    base_dir = Path(args.base_dir)
    inspect_dataset(base_dir, args.limit, show_summary=not args.no_summary)

if __name__ == "__main__":
    main()
