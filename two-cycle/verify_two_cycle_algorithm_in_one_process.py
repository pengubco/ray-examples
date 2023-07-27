import time

from cycle_graph_in_memory import InMemoryCycleGraph, create_in_memory_cycle_graph
from cycle_graph_in_postgres import create_pg_cycle_graph, create_empty_pg_cycle_graph, delete_pg_cycle_graph
from two_cycle_algorithm import two_cycle


def verify_two_cycle_algorithm_on_graph_in_memory(n=10, threshold=10, epsilon=0.5, parallelism=3):
    print('verify algorithm on graphs stored in memory')

    def new_graph_func(name):
        return InMemoryCycleGraph(name)

    def delete_graph_func(name):
        pass

    print(f'a graph of two cycles. {n} vertices')
    g = create_in_memory_cycle_graph("g1", True, n)
    start_time = time.time()
    assert two_cycle(g, threshold, epsilon, parallelism, new_graph_func, delete_graph_func)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')

    print(f'a cycle. {n} vertices')
    g2 = create_in_memory_cycle_graph("g2", False, n)
    start_time = time.time()
    assert not two_cycle(g2, threshold, epsilon, parallelism, new_graph_func, delete_graph_func)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')


# start a postgres server and run this program
# docker run -d --name=pg -p 15432:5432 -e POSTGRES_PASSWORD=postgres postgres
def verify_two_cycle_algorithm_on_graph_in_postgres(n=10, threshold=10, epsilon=0.5, parallelism=3):
    print('verify algorithm on graphs stored in postgres')
    pg_config = {
        "host": "localhost",
        "port": 15432,
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
    }

    def new_graph_func(name):
        return create_empty_pg_cycle_graph(pg_config, name)

    def delete_graph_func(name):
        delete_pg_cycle_graph(pg_config, name)

    g = create_pg_cycle_graph(pg_config, "g1", True, n)
    print(f'a graph of two cycles. {n} vertices')
    start_time = time.time()
    assert two_cycle(g, threshold, epsilon, parallelism, new_graph_func, delete_graph_func)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')
    delete_pg_cycle_graph(pg_config, "g1")

    g2 = create_pg_cycle_graph(pg_config, "g2", False, n)
    print(f'a cycle. {n} vertices')
    start_time = time.time()
    assert not two_cycle(g2, threshold, epsilon, parallelism, new_graph_func, delete_graph_func)
    print('success')
    print(f'Runtime: {time.time() - start_time:.2f} seconds')
    delete_pg_cycle_graph(pg_config, "g2")


if __name__ == '__main__':
    s = {"n": 10000, "threshold": 10, "epsilon": 0.5, "parallelism": 40}
    verify_two_cycle_algorithm_on_graph_in_memory(**s)
    print('==========')
    verify_two_cycle_algorithm_on_graph_in_postgres(**s)
