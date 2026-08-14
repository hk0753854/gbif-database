import argparse

from gbif_data_pipeline.pipeline import run_pipeline


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Run GBIF data pipeline.",
    )

    parser.add_argument(
        "scientific_name",
        help="Scientific name to search on GBIF.",
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=100,
        help="Number of GBIF records to retrieve.",
    )

    parser.add_argument(
        "--output",
        default="data/occurrences.parquet",
        help="Output Parquet file path.",
    )

    args = parser.parse_args()

    output_path = run_pipeline(
        scientific_name=args.scientific_name,
        limit=args.limit,
        output_path=args.output,
    )

    print(f"Pipeline completed: {output_path}")


if __name__ == "__main__":
    main()