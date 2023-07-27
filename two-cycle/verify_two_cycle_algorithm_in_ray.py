import time

import ray

from cycle_graph_in_postgres import create_pg_cycle_graph, delete_pg_cycle_graph
from two_cycle_algorithm import two_cycle_ray


# start a postgres server and run this program
# docker run -d --name=pg -p 15432:5432 -e POSTGRES_PASSWORD=postgres postgres
def verify_two_cycle_algorithm_on_graph_in_postgres(n=10, threshold=10, epsilon=0.5, parallelism=3):
    pg_config = {
        "host": "192.168.1.16",
        "port": 15432,
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
    }
    threshold, epsilon, parallelism = 10, 1 / 2, 40

    create_pg_cycle_graph(pg_config, "g1", True, n)
    print(f'a graph of two cycles. {n} vertices')
    start_time = time.time()
    result = two_cycle_ray.remote(pg_config, "g1", threshold, epsilon, parallelism)
    assert ray.get(result)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')
    delete_pg_cycle_graph(pg_config, "g1")

    create_pg_cycle_graph(pg_config, "g2", True, n)
    print(f'a cycle. {n} vertices')
    start_time = time.time()
    result = two_cycle_ray.remote(pg_config, "g2", threshold, epsilon, parallelism)
    assert ray.get(result)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')
    delete_pg_cycle_graph(pg_config, "g2")


if __name__ == '__main__':
    s = {"n": 10000, "threshold": 10, "epsilon": 0.5, "parallelism": 40}
    ray.init(runtime_env={
        "working_dir": "./",
    })
    verify_two_cycle_algorithm_on_graph_in_postgres(**s)
