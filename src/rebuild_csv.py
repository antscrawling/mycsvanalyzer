import argparse
import json
import sys
from pathlib import Path

def main():
    p = argparse.ArgumentParser(description="Rebuild chunks (Parquet) back into a single CSV or Parquet.")
    p.add_argument("--manifest", required=True, help="Path to src/manifest.json produced by pack_csv.py")
    p.add_argument("--output", required=True, help="Output file (.csv or .parquet)")
    p.add_argument("--delimiter", default=",", help="CSV delimiter for output; default ','")
    p.add_argument("--encoding", default="utf-8", help="CSV encoding for output; default 'utf-8'")
    args = p.parse_args()

    manifest_path = Path(args.manifest)
    with open(manifest_path, "r", encoding="utf-8") as f:
        manifest = json.load(f)

    out_suffix = Path(args.output).suffix.lower()
    if out_suffix not in (".csv", ".parquet"):
        print("Output must end with .csv or .parquet", file=sys.stderr)
        sys.exit(2)

    import pyarrow.parquet as pq
    import pyarrow.csv as pacsv

    # Concatenate all parquet parts as a dataset then write once.
    parts = [manifest_path.parent / c["file"] for c in manifest["chunks"]]
    dataset = pq.ParquetDataset([str(p) for p in parts])
    table = dataset.read()

    if out_suffix == ".parquet":
        pq.write_table(table, args.output, compression=manifest.get("compression", "zstd"))
        print(f"Wrote {args.output}")
        return

    # CSV output
    # Use Arrow CSV writer (fast, streaming) for Excel-friendly CSV.
    write_opts = pacsv.WriteOptions(include_header=True, delimiter=args.delimiter, quoting_style="needed")
    pacsv.write_csv(table, args.output, write_options=write_opts)
    print(f"Wrote {args.output}")

if __name__ == "__main__":
    main()
