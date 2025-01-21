import argparse
import warnings
benchmarking.utils.benchmark import generate_benchmarks

warnings.simplefilter(action="ignore", category=FutureWarning)

if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--environment", help="The environment benchmarking is run in.")
    ap.add_argument("-d", "--data_source", help="Generated or real")

    args = ap.parse_args()

    generate_benchmarks(**args)