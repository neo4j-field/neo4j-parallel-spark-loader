import argparse
import os
import warnings

from benchmarking.utils.benchmark import generate_benchmarks

# Suppress FutureWarnings
warnings.simplefilter(action="ignore", category=FutureWarning)


def main():
    # Create argument parser
    parser = argparse.ArgumentParser(description="Benchmark generation script")

    # Add required arguments
    parser.add_argument(
        "-e",
        "--environment",
        required=True,
        choices=["local", "databricks"],
        help="The environment benchmarking is run in",
    )

    parser.add_argument(
        "-d",
        "--data_source",
        required=True,
        choices=["generated", "real"],
        help="Type of data source: 'generated' or 'real'",
    )

    # Parse arguments
    args = parser.parse_args()

    # Convert namespace to dictionary
    args_dict = vars(args)

    if args_dict["environment"] == "local":
        os.chdir("./benchmarking")

    try:
        # Call generate_benchmarks with unpacked arguments
        generate_benchmarks(**args_dict)
    except Exception as e:
        print(f"Error during benchmark generation: {str(e)}")
        raise


if __name__ == "__main__":
    main()
