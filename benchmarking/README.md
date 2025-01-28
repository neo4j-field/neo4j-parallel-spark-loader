# Benchmarking

This directory contains code to perform comparisons between serial and parallel relationship loading using Spark. Parallel loading utilizes the neo4j-parallel-spark-loader Python package.

## Datasets
You can choose to run the benchmarking code with synthetic data or with example graph datasets downloaded from public websites by setting a data_source parameter. If you choose data_source="real", you will read from these three data sources.

* Predefined components: [Reddit threads](https://snap.stanford.edu/data/reddit_threads.html)
* Bipartite graph: [Amazon ratings](https://networkrepository.com/rec-amazon-ratings.php)
* Monopartite graph: [Twitch gamers](https://snap.stanford.edu/data/twitch_gamers.html)

## Running in Databricks

To run the benchmarking code in Databricks, run the databricks_benchmarking.ipynb notebook in this directory. Update the notebook widgets with connection information for your Neo4j database. Run the notebook. It will generate a new output file in the `./output/{version}` directory with statistics for all of the benchmarking runs.

## Running locally

To run the benchmarking code locally, set environment variables for NEO4J_USERNAME, NEO4J_PASSWORD, NEO4J_URI, and NEO4J_DATABASE or include them in a .env file. Run main.py. Pass the -d argument with a value of "generated" or "real" and pass the -e argument with "local".

## Analyzing results
The analyze_results.ipynb notebook will generate charts to show the load times for each graph loading scenario in parallel and at two different group sizes. The results are read from the `./output` directory for the most recent run at the package version that you specify.

The visualize_groups_and_batches.ipynb will create heat maps to show the distribution of rows across groups and batches for the public graph datasets at the group number that you select.