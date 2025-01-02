# neo4j-parallel-spark-loader
Neo4j Parallel Spark Loader is a Python library for grouping and batching dataframes in a way that supports parallel relationship loading into Neo4j. As an ACID-compliant database, Neo4j uses locks when writing relationships to the database. When multiple processes attempt to write to the same node at the same time, deadlocks can occur. This is why the [Neo4j Spark Connector documentation](https://neo4j.com/docs/spark/current/write/relationship/) recommends reformatting Spark DataFrames to a single partition before writing relationships to Neo4j.

Neo4j Parallel Spark Loader allows parallel relationship writes to Neo4j without deadlocking by breaking a Spark dataframe into one or more batches of rows. Within each batch, rows are further subdivided into groups in such a way that each node ID value appears in only one group per batch. All groups within a batch can be written to Neo4j in parallel without deadlocking because the same node is never touched by relationships in concurrent write transactions. Batches are loaded one-after-the-other to ingest the whole dataframe to Neo4j.

## Key Features
Supports multiple relationship batching and grouping scenarios:
* Predefined Components
* Bipartite Data
* Monopartite Data

## Additional Dependencies

This package requires 
* [Neo4j Spark Connector](https://neo4j.com/docs/spark/current/installation/) JAR file installed on the Spark cluster

## A quick example
Imagine that you have a Spark DataFrame of order records. It includes columns `order_id`, `product_id`, and `quantity`. You would like to load a `INCLUDES_PRODUCT` relationship. 

```
import parallel_spark_loader

...show setting up spark session with config and loading data

# Create batches and groups
batched_purchase_df = parallel_spark_loader.bipartite.group_and_batch_spark_dataframe(order_df, 'order_id', 'store_id', 8)

# Load to Neo4j
includes_product_query = """
    MATCH (o:Order {id: event['order_id']}),
    (p:Product {id: event['product_id']})
    MERGE (o)-[r:INCLUDES_PRODUCT]->(p)
    ON CREATE SET r.quantity = event['quantity']"""

#Load groups in parallel for each batch
parallel_spark_loader.utils.ingest_spark_dataframe(purchase_df, "Overwrite", {"query": includes_product_query})

```

## Grouping and batching scenarios

Grouping and batching scenarios of various levels of complexity can be appropriate depending on the structure of the relationship data being loaded to Neo4j. The Neo4j Parallel Spark Loader library supports three scenarios: predefined components, bipartite data, and monopartite data.

### Predefined components scenario

In some relationship data, the relationships can be broken into distinct components based on a field in the relationship data. For example, you might have a DataFrame of HR data with columns for `employeeId`, `managerId`, and `department`. If we are wanting to create a `MANAGES` relationship between employees and managers, and we know in advance that all managers are in the same department as the employees they manage, we can separate the rows of the dataframe into components based on the `department` key.

Often the number of predefined components is greater than the number of workers in the Spark cluster, and the number of rows within each component is unequal. When running `parallel_spark_loader.predefined_components.group_and_batch_spark_dataframe()`, you specify the number of groups that you want to collect the partitioned data into. This should value should be less than or equal to the number of workers in your Spark cluster. Neo4j Parallel Spark Loader uses a greedy algorithm to assign partitions into groups in a way that attempts to balance the number of relationships within each group. When loading this ensures that each Spark worker stays equally instead of some workers waiting while other workers finish loading larger groups.

![Diagram showing nodes and relationships assigned to groups](./docs/assets/images/predefined-components.png)

We can visualize the nodes within the same group as a single aggregated node and the relationships that connect nodes within the same group as a single aggregated relationship. In this image, we can see that no aggregated nodes are connected to the same aggregated relationships. Therefore, transactions within the different aggregated relationships can run in parallel without deadlocking.

![Aggregated diagrame showing that predefined components groups will not conflict when running in parallel.](./docs/assets/images/predefined-components-aggregated-diagram.png)

### Bipartite data scenario

In many relationship datasets, there is not a paritioning key in the Spark DataFrame that can be used to divide the relationships into predefined components. However, we know that no nodes in the dataset will be *both a source and a target* for this relationship type. Often this is because the source nodes and the target nodes have different node labels and they represent different classes of things in the real world. For example, you might have a DataFrame of order data with columns for `orderId`, `productId`, and `quantity`, and you want to create `INCLUDES_PRODUCT` relationships between `Order` and `Product` nodes. You know that all source nodes of `INCLUDES_PRODUCT` relationships will be `Order` nodes, and all target nodes will be `Product` nodes. No nodes will be *both source and target* of that relationship.

When running `parallel_spark_loader.bipartite.group_and_batch_spark_dataframe()`, you specify the number of groups that you want to collect the source and target nodes into. Neo4j Parallel Spark Loader uses a greedy alogirthm to assign source node values to source-node groups so that each group represents roughly the same number of rows in the relationship DataFrame. Similarly, the library groups the target node values into target-node groups with roughly balanced size.

We can visualize the nodes within the same group as a single aggregated node and the relationships that connect nodes within the same group as a single aggregated relationship. 

![Diagram showing aggregated bipartite relationships colored by group](./docs/assets/images/bipartite-coloring-diagram.png)

In the aggregated biparite diagram, there are multiple relationships (each representing a group of individual relationships) connected to each node (representing a group of nodes). Using a straightforward alternating algorithm, we can color the relationships so that no relationships of the same color point to the same node. The relationship colors represent the batches applied to the data. In the picture above, the relationship groups represented by red arrows could be processed in parallel. Then the relationship groups represented by green arrows could be processed in parallel. 