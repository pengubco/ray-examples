# Two Cycle

Given a undirected graph G. G is either a cycle or a graph of two disjoint cycles. Implement an algorithm to tell
whether G is a cycle.

We first implement the algorithm that runs in one process, i.e., run locally without using Ray.
The algorithm is at [two_cycle_algorithm.py](two_cycle_algorithm.py). It can run on two types of graphs:

1. graphs stored in memory. See [cycle_graph_in_memory.py](cycle_graph_in_memory.py)
2. graphs stored in Postgres. See [cycle_graph_in_postgres.py](cycle_graph_in_postgres.py)

Both types of graph implement the graph interface defined at  [cycle_graph.py](cycle_graph.py).

You can verify the algorithm works like this. 
```shell
# install the postgres client driver
pip install psycopg2
# start a postgres server and run this program
docker run -d --name=pg -p 15432:5432 -e POSTGRES_PASSWORD=postgres postgres

python verify_two_cycle_algorithm_in_one_process.py
```