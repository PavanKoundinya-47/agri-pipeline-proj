import argparse, os
from pipeline.ingestion import ingest_data
from pipeline.transform import transform_data
from pipeline.validation import validate_data
from pipeline.loading import store_data

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--raw_dir", default="data/raw")
    p.add_argument("--processed_dir", default="data/processed")
    p.add_argument("--report_path", default="data/data_quality_report.csv")
    return p.parse_args()

def main():
    args = parse_args()
    raw = ingest_data(args.raw_dir)
    transformed = transform_data(raw)
    os.makedirs(os.path.dirname(args.report_path), exist_ok=True)
    validate_data(transformed, args.report_path)
    store_data(transformed, args.processed_dir)
    print("Pipeline run complete.")
    print(f"Processed rows: {len(transformed)}")
    print(f"Report: {args.report_path}")

if __name__ == "__main__":
    main()
