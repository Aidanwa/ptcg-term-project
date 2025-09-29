"""
TCGCSV Pokémon TCG Master Dataset Builder
-----------------------------------------

This script builds and updates a master Pokémon TCG dataset from tcgcsv archives.

Features:
- Stage 1: Daily price harvesting (stored in `daily_prices/`, tracked by processed_days.txt).
- Stage 2: Product & group metadata (stored in `products.parquet` / `groups.parquet`).
- Stage 3: Merge: combines prices + products + groups, partitioned by date in `daily/`.
- Stage 4: (Optional) Full dataset: generates one combined file `pokemon_prices_with_full_features.parquet`.
- Automatically prunes unused columns to save space (PRUNE_COLS).
- Cleans up temporary `archives/` and `extracted/` folders at the end of each run.

Directory layout:
    base_dir/
        archives/              (temp download archives, cleaned after run)
        extracted/             (temp extraction, cleaned after run)
        daily_prices/          (raw prices parquet, partitioned by date)
        daily/                 (final merged dataset, partitioned by date)
        groups.parquet         (cached groups metadata)
        products.parquet       (cached products metadata)
        pokemon_prices_with_full_features.parquet (optional, with --full-file)

Usage:
    python tcgcsv_pokemon_pipeline.py \
        --start-date 2024-02-08 \
        --end-date 2025-09-27 \
        --interval 7 \
        --base-dir ./pokemon-tcg-pricing/data \
        [--keep-extracted] \
        [--full-file]

Arguments:
    --start-date     First date to harvest (YYYY-MM-DD).
    --end-date       Last date to harvest (YYYY-MM-DD).
    --interval       Interval in days (default=1). Example: 7 means weekly.
    --base-dir       Base output directory (default=./pokemon-tcg-pricing/data).
    --keep-extracted If set, extracted raw archives are kept instead of deleted.
    --full-file      If set, also generate one big merged parquet file.
"""
from __future__ import annotations

import argparse
import re
import shutil
import json
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter, Retry
import py7zr
from tqdm import tqdm

ARCHIVE_URL_TMPL = "https://tcgcsv.com/archive/tcgplayer/prices-{YYYY}-{MM}-{DD}.ppmd.7z"
GROUPS_URL = "https://tcgcsv.com/tcgplayer/3/groups"
PRODUCTS_URL_TMPL = "https://tcgcsv.com/tcgplayer/3/{group_id}/products"
CATEGORY_ID = "3"  # Pokémon

PRUNE_COLS = {
    "url", "image_url", "image_count", "modified_on_x",
    "presale_is_presale", "presale_released_on", "presale_note",
    "modified_on_y", "extended_data", "attack_3", "attack_4", "attack_5",
}

# ----------------------------
# Helpers
# ----------------------------

def to_snake(s: str) -> str:
    """Convert a string to snake_case."""
    s = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", s)
    s = re.sub(r"[^0-9a-zA-Z]+", "_", s)
    return s.strip("_").lower()

def make_session() -> requests.Session:
    """Create a requests session with retry logic."""
    s = requests.Session()
    retries = Retry(total=5, backoff_factor=1.2, status_forcelist=[429, 500, 502, 503, 504])
    s.mount("http://", HTTPAdapter(max_retries=retries))
    s.mount("https://", HTTPAdapter(max_retries=retries))
    return s

def ensure_dir(p: Path) -> None:
    """Ensure a directory exists."""
    p.mkdir(parents=True, exist_ok=True)

# ----------------------------
# Tracking
# ----------------------------

def load_processed_days(base_dir: Path) -> set[str]:
    f = base_dir / "processed_days.txt"
    return {line.strip() for line in f.read_text().splitlines()} if f.exists() else set()

def mark_day_processed(base_dir: Path, d: date) -> None:
    with open(base_dir / "processed_days.txt", "a") as f:
        f.write(f"{d:%Y-%m-%d}\n")

def load_processed_products(base_dir: Path) -> set[str]:
    f = base_dir / "processed_products.txt"
    return {line.strip() for line in f.read_text().splitlines()} if f.exists() else set()

def mark_product_processed(base_dir: Path, gid: str) -> None:
    with open(base_dir / "processed_products.txt", "a") as f:
        f.write(f"{gid}\n")

# ----------------------------
# Archive download & extraction
# ----------------------------

def archive_url_for(d: date) -> str:
    return ARCHIVE_URL_TMPL.replace("{YYYY}", f"{d.year:04d}") \
        .replace("{MM}", f"{d.month:02d}").replace("{DD}", f"{d.day:02d}")

def download_archive(sess: requests.Session, d: date, archives_dir: Path) -> Optional[Path]:
    url = archive_url_for(d)
    out = archives_dir / f"prices-{d:%Y-%m-%d}.ppmd.7z"
    if out.exists() and out.stat().st_size > 0:
        return out
    r = sess.get(url, stream=True, timeout=60)
    if r.status_code != 200:
        return None
    with open(out, "wb") as f:
        for chunk in r.iter_content(chunk_size=1024 * 1024):
            if chunk:
                f.write(chunk)
    return out

def extract_archive(archive_path: Path, extract_root: Path) -> Optional[Path]:
    m = re.search(r"prices-(\d{4}-\d{2}-\d{2})\.ppmd\.7z$", archive_path.name)
    if not m:
        return None
    day = m.group(1)
    day_root = extract_root / day
    if day_root.exists() and any(day_root.iterdir()):
        return day_root
    tmp_dir = day_root / "tmp"
    ensure_dir(tmp_dir)
    with py7zr.SevenZipFile(archive_path, mode="r") as z:
        z.extractall(path=tmp_dir)
    inner = None
    for p in tmp_dir.rglob(CATEGORY_ID):
        if p.is_dir():
            inner = p
            break
    if not inner:
        shutil.rmtree(tmp_dir)
        return None
    ensure_dir(day_root)
    dest = day_root / CATEGORY_ID
    if dest.exists():
        shutil.rmtree(dest)
    shutil.move(str(inner), str(dest))
    shutil.rmtree(tmp_dir)
    return day_root

# ----------------------------
# Price harvesting
# ----------------------------

def harvest_prices_for_day(day_root: Path) -> pd.DataFrame:
    date_str = day_root.name
    cat_path = day_root / CATEGORY_ID
    if not cat_path.exists():
        return pd.DataFrame()
    rows = []
    for group_dir in cat_path.iterdir():
        if not group_dir.is_dir():
            continue
        group_id = group_dir.name
        prices_file = group_dir / "prices"
        if not prices_file.exists():
            continue
        try:
            with open(prices_file, "r", encoding="utf-8") as f:
                data = json.load(f)
            results = data.get("results", [])
            if not results:
                continue
            df = pd.DataFrame(results)
        except Exception as e:
            print(f"[ERROR] Could not parse {prices_file}: {e}")
            continue
        df.columns = [to_snake(c) for c in df.columns]
        df["date"] = date_str
        df["groupid"] = group_id
        rows.append(df)
    valid = [df for df in rows if not df.empty and not df.isna().all().all()]
    return pd.concat(valid, ignore_index=True) if valid else pd.DataFrame()

def build_prices(sess, base_dir: Path, start_date: date, end_date: date, step: int, keep_extracted: bool):
    archives_dir, extract_dir, daily_dir = base_dir/"archives", base_dir/"extracted", base_dir/"daily_prices"
    ensure_dir(archives_dir); ensure_dir(extract_dir); ensure_dir(daily_dir)
    processed_days = load_processed_days(base_dir)
    all_frames, days_done = [], 0
    d = start_date
    skip_count = 0
    new_days = []
    while d <= end_date:
        if f"{d:%Y-%m-%d}" in processed_days:
            skip_count += 1
            d += timedelta(days=step)
            continue

        if skip_count > 0:
            print(f"[SKIP] {skip_count} day(s) already processed (up to {d - timedelta(days=step)})")
            skip_count = 0
        a = download_archive(sess, d, archives_dir)
        if not a:
            print(f"[SKIP] {d}: archive not found")
            d += timedelta(days=step)
            continue
        day_root = extract_archive(a, extract_dir)
        if not day_root:
            print(f"[SKIP] {d}: extraction failed or no Pokémon data")
            d += timedelta(days=step)
            continue
        df = harvest_prices_for_day(day_root)
        if df.empty:
            print(f"[SKIP] {d}: no Pokémon data")
            d += timedelta(days=step)
            continue
        df = df.drop(columns=[c for c in df.columns if c in PRUNE_COLS], errors="ignore")
        out_path = daily_dir / f"date={d:%Y-%m-%d}" / "part.parquet"
        ensure_dir(out_path.parent)
        df.to_parquet(out_path, compression="zstd", index=False)
        mark_day_processed(base_dir, d)
        new_days.append(f"{d:%Y-%m-%d}")
        if not keep_extracted:
            shutil.rmtree(day_root, ignore_errors=True)
        all_frames.append(df)
        days_done += 1
        print(f"[OK] {d}: {len(df):,} rows, {len(df.columns)} cols")
        d += timedelta(days=step)

    if skip_count > 0:
        print(f"[SKIP] {skip_count} final day(s) already processed (ending {end_date})")

    print(f"Finished {days_done} new days processed.")
    return pd.concat(all_frames, ignore_index=True) if all_frames else pd.DataFrame(), new_days

def load_all_prices(base_dir: Path) -> pd.DataFrame:
    processed_days = load_processed_days(base_dir)
    daily_dir = base_dir / "daily_prices"
    daily_files = [daily_dir / f"date={d}" / "part.parquet" for d in processed_days]
    existing_files = [f for f in daily_files if f.exists()]
    if not existing_files:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in existing_files], ignore_index=True)

# ----------------------------
# Groups & Products
# ----------------------------

def fetch_groups(sess: requests.Session) -> pd.DataFrame:
    r = sess.get(GROUPS_URL, timeout=60)
    resp = r.json()
    data = resp.get("results", resp.get("data", resp))
    if isinstance(data, dict):
        data = data.get("results", data.get("data", [data]))
    if not isinstance(data, list):
        raise ValueError(f"Unexpected groups response format: {type(data)}")
    df = pd.DataFrame.from_records(data)
    df.columns = [to_snake(c) for c in df.columns]
    if "name" in df.columns:
        df = df.rename(columns={"name": "set_name"})
    if "published_on" in df.columns:
        df = df.rename(columns={"published_on": "release_date"})
    return df

def fetch_products_for_group(sess: requests.Session, group_id: str) -> pd.DataFrame:
    r = sess.get(PRODUCTS_URL_TMPL.format(group_id=group_id), timeout=120)
    resp = r.json()
    data = resp.get("results", resp.get("data", resp))
    rows = []
    for prod in data:
        flat = {to_snake(k): v for k, v in prod.items() if k not in ("presaleInfo", "extendedData")}
        presale = prod.get("presaleInfo", {})
        for k, v in presale.items():
            flat[to_snake(f"presale_{k}")] = v
        attacks_seen = 0
        for ext in prod.get("extendedData", []):
            key, val = to_snake(ext.get("name", "")), ext.get("value")
            if not key:
                continue
            if key.startswith("attack"):
                attacks_seen += 1
                if attacks_seen > 2:
                    continue
                key = f"attack_{attacks_seen}"
            flat[key] = val
        rows.append(flat)
    df = pd.DataFrame(rows)
    return df.drop(columns=[c for c in df.columns if c in PRUNE_COLS], errors="ignore")

def build_products_and_groups(sess, base_dir: Path):
    groups_file, products_file = base_dir/"groups.parquet", base_dir/"products.parquet"
    if groups_file.exists():
        groups_df = pd.read_parquet(groups_file)
    else:
        groups_df = fetch_groups(sess)
        groups_df.to_parquet(groups_file, compression="zstd", index=False)
    processed_products = load_processed_products(base_dir)
    all_products = []
    skip_count = 0
    last_gid = None

    for gid in groups_df["group_id"].astype(str).unique():
        if gid in processed_products and products_file.exists():
            skip_count += 1
            last_gid = gid
            continue

        if skip_count > 0:
            print(f"[SKIP] {skip_count} group(s) already processed (up to {last_gid})")
            skip_count = 0

        try:
            df = fetch_products_for_group(sess, gid)
            if not df.empty:
                df["group_id"] = gid
                all_products.append(df)
                mark_product_processed(base_dir, gid)
                print(f"[OK] Products fetched for group {gid}: {len(df)}")
        except Exception as e:
            print(f"[ERROR] Products fetch failed for group {gid}: {e}")

    # Flush final block
    if skip_count > 0:
        print(f"[SKIP] {skip_count} group(s) already processed (last {last_gid})")
    if all_products:
        new_products_df = pd.concat(all_products, ignore_index=True)
        if products_file.exists():
            old_products_df = pd.read_parquet(products_file)
            products_df = pd.concat([old_products_df, new_products_df], ignore_index=True)
        else:
            products_df = new_products_df
        products_df.to_parquet(products_file, compression="zstd", index=False)
    else:
        products_df = pd.read_parquet(products_file) if products_file.exists() else pd.DataFrame()
    groups_df = groups_df.rename(columns={"group_id": "groupid"})
    products_df = products_df.rename(columns={"group_id": "groupid"})
    products_df = products_df.drop_duplicates(keep="first").reset_index(drop=True)
    return groups_df, products_df

def build_full_file(base_dir: Path, out_file: Path):
    # collect all partitioned daily files
    daily_files = list((base_dir / "daily").rglob("*.parquet"))
    all_dates = {f.parent.name.split("=")[-1] for f in daily_files}

    # check if full file already exists
    if out_file.exists():
        try:
            old_df = pd.read_parquet(out_file, columns=["date"])
            existing_dates = set(old_df["date"].unique())
        except Exception:
            print("[WARN] Could not read existing full file, rebuilding it.")
            existing_dates = set()

        if existing_dates == all_dates:
            print("[INFO] Full file already up to date. Skipping rewrite.")
            return

    # build new full dataset with progress bar and row counts
    print(f"[INFO] Building full dataset from {len(daily_files)} partitions...")
    dfs, total_rows = [], 0
    for f in tqdm(daily_files, desc="Reading daily partitions"):
        df = pd.read_parquet(f)
        dfs.append(df)
        total_rows += len(df)
        tqdm.write(f"  Loaded {len(df):,} rows from {f.parent.name} "
                   f"(cumulative {total_rows:,})")
    full_df = pd.concat(dfs, ignore_index=True)
    full_df.to_parquet(out_file, compression="zstd", index=False)
    print(f"[OK] Wrote combined dataset ({len(full_df):,} rows) to {out_file}")

def prune_old_daily_prices(base_dir: Path, keep_days: int = 7):
    daily_dir = base_dir / "daily_prices"
    if not daily_dir.exists():
        return

    # Extract date=YYYY-MM-DD folders
    partitions = [p for p in daily_dir.iterdir() if p.is_dir() and p.name.startswith("date=")]
    if not partitions:
        return

    # Sort by date (descending = newest first)
    partitions_sorted = sorted(partitions, key=lambda p: p.name.split("=")[-1], reverse=True)

    # Keep the most recent N
    keep = set(partitions_sorted[:keep_days])
    delete = [p for p in partitions_sorted if p not in keep]

    for p in delete:
        shutil.rmtree(p, ignore_errors=True)
        print(f"[PRUNE] Removed old raw daily_prices partition {p.name}")

    if delete:
        print(f"[PRUNE] Kept {len(keep)} most recent daily_prices partitions, removed {len(delete)} older ones.")

def append_to_full_file(base_dir: Path, out_file: Path, new_days: list[str]):
    if not out_file.exists():
        # No full file yet, just build from scratch
        build_full_file(base_dir, out_file)
        return

    # Read existing full file
    old_df = pd.read_parquet(out_file)

    # Load only the new day partitions
    new_dfs = []
    for day in new_days:
        f = base_dir / "daily" / f"date={day}" / "part.parquet"
        if f.exists():
            new_dfs.append(pd.read_parquet(f))
    if not new_dfs:
        return  # nothing to append

    new_df = pd.concat(new_dfs, ignore_index=True)

    # Append and drop duplicates just in case
    full_df = pd.concat([old_df, new_df], ignore_index=True)
    full_df = full_df.drop_duplicates(
        subset=["product_id", "date", "sub_type_name"], keep="first"
    )

    # Rewrite once
    full_df.to_parquet(out_file, compression="zstd", index=False)
    print(f"[OK] Appended {len(new_df):,} rows to {out_file}")

# ----------------------------
# Main
# ----------------------------

def parse_args(argv=None):
    ap = argparse.ArgumentParser()
    ap.add_argument("--start-date", required=True, help="Start date (YYYY-MM-DD) for downloading new daily price archives")
    ap.add_argument("--end-date", required=True, help="End date (YYYY-MM-DD) for downloading new daily price archives")
    ap.add_argument("--interval", type=int, default=1, help="Interval between downloads in days (default=1). Example: 7 for weekly.")
    ap.add_argument("--base-dir", default="./pokemon-tcg-pricing/data", help="Base directory for all outputs (default=./pokemon-tcg-pricing/data)")
    ap.add_argument("--keep-extracted", action="store_true", help="If set, extracted raw archives are not deleted after processing")
    ap.add_argument("--full-file", action="store_true", help="If set, also generate one big merged dataset parquet file")
    return ap.parse_args(argv)

def main(argv=None):
    args = parse_args(argv)
    start_date = datetime.strptime(args.start_date, "%Y-%m-%d").date()
    end_date = datetime.strptime(args.end_date, "%Y-%m-%d").date()
    base_dir = Path(args.base_dir)
    sess = make_session()

    # Stage 1: Fetch new prices
    prices_df, new_days = build_prices(
        sess, base_dir, start_date, end_date,
        args.interval, args.keep_extracted
    )

    # If no new days -> skip heavy merge
    if not new_days:
        print("[INFO] No new days to add. Dataset already up to date.")
        return 0

    # Stage 2: Products + Groups
    groups_df, products_df = build_products_and_groups(sess, base_dir)

    # Normalize keys
    products_df["groupid"] = products_df["groupid"].astype(str)
    groups_df["groupid"] = groups_df["groupid"].astype(str)

    # Drop unused cols
    groups_df = groups_df.drop(columns=[c for c in groups_df.columns if c in PRUNE_COLS], errors="ignore")
    products_df = products_df.drop(columns=[c for c in products_df.columns if c in PRUNE_COLS], errors="ignore")

    # Stage 3: Merge only new days and write to daily/
    total_rows_written = 0
    for day in new_days:
        day_prices = pd.read_parquet(base_dir / "daily_prices" / f"date={day}" / "part.parquet")

        merged_df = day_prices.merge(products_df, on=["groupid", "product_id"], how="left")
        merged_df = merged_df.merge(groups_df, on="groupid", how="left")
        merged_df = merged_df.drop_duplicates(subset=["product_id", "date", "sub_type_name"], keep="first")

        out_path = base_dir / "daily" / f"date={day}" / "part.parquet"
        ensure_dir(out_path.parent)
        merged_df.to_parquet(out_path, compression="zstd", index=False)

        total_rows_written += len(merged_df)
        print(f"[OK] Updated daily partition for {day}: {len(merged_df):,} rows")

    # Stage 4: Optional full dataset
    if args.full_file:
        out_file = base_dir / "pokemon_prices_with_full_features.parquet"
        build_full_file(base_dir, out_file)

    # Stage 5: Cleanup temp + prune old raw prices
    shutil.rmtree(base_dir / "archives", ignore_errors=True)
    shutil.rmtree(base_dir / "extracted", ignore_errors=True)
    print("[CLEANUP] Removed archives/ and extracted/")

    prune_old_daily_prices(base_dir, keep_days=7)

    # Summary
    print("==== SUMMARY ====")
    print(f"New days processed: {len(new_days)}")
    print(f"Total rows written (new days only): {total_rows_written:,}")
    print("Partitioned dataset written to:", base_dir / "daily")
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
