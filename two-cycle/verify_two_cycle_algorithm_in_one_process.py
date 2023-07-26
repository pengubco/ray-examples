from cycle_graph_in_memory import InMemoryCycleGraph, create_in_memory_cycle_graph
from cycle_graph_in_postgres import create_pg_cycle_graph, create_empty_pg_cycle_graph, delete_pg_cycle_graph
from two_cycle_algorithm import two_cycle


def verify_two_cycle_algorithm_on_graph_in_memory():
    print('verify algorithm on graphs stored in memory')
    threshold, epsilon = 100, 1 / 2
    new_graph_func = lambda name: InMemoryCycleGraph(name)
    delete_graph_func = lambda name: None
    print('a graph of two cycles of 10 vertices')
    g = create_in_memory_cycle_graph("g1", True, 10)
    assert two_cycle(g, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')

    print('a cycle of 10 vertices')
    g2 = create_in_memory_cycle_graph("g2", False, 10)
    assert not two_cycle(g2, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')

    print('a graph of two cycles of 1000 vertices')
    g3 = create_in_memory_cycle_graph("g3", True, 1000)
    assert two_cycle(g3, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')

    print('a cycle of 1000 vertices')
    g4 = create_in_memory_cycle_graph("g4", False, 1000)
    assert not two_cycle(g4, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')


# start a postgres server and run this program
# docker run -d --name=pg -p 15432:5432 -e POSTGRES_PASSWORD=postgres postgres
def verify_two_cycle_algorithm_on_graph_in_postgres():
    print('verify algorithm on graphs stored in postgres')
    pg_config = {
        "host": "localhost",
        "port": 15432,
        "database": "postgres",
        "user": "postgres",
        "password": "postgres",
    }

    threshold, epsilon = 100, 1 / 2

    new_graph_func = lambda name: create_empty_pg_cycle_graph(pg_config, name)
    delete_graph_func = lambda name: delete_pg_cycle_graph(pg_config, name)
    g = create_pg_cycle_graph(pg_config, "g1", True, 10)
    print('a graph of two cycles of 10 vertices')
    assert two_cycle(g, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')
    delete_pg_cycle_graph(pg_config, "g1")

    g2 = create_pg_cycle_graph(pg_config, "g2", False, 10)
    print('a cycle of 10 vertices')
    assert not two_cycle(g2, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')
    delete_pg_cycle_graph(pg_config, "g2")

    print('a graph of two cycles of 1000 vertices')
    g3 = create_pg_cycle_graph(pg_config, "g3", True, 1000)
    assert two_cycle(g3, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')
    delete_pg_cycle_graph(pg_config, "g3")

    g4 = create_pg_cycle_graph(pg_config, "g4", False, 1000)
    print('a cycle of 1000 vertices')
    assert not two_cycle(g4, threshold, epsilon, new_graph_func, delete_graph_func)
    print('success')
    delete_pg_cycle_graph(pg_config, "g4")


if __name__ == '__main__':
    verify_two_cycle_algorithm_on_graph_in_memory()
    print('==========')
    verify_two_cycle_algorithm_on_graph_in_postgres()
