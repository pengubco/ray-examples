import math
import ray

from cycle_graph import CycleGraph
from cycle_graph_in_postgres import PostgresGraph, create_empty_pg_cycle_graph, delete_pg_cycle_graph
from typing import Callable, List, Set


def shrink(vertices: List[int], sampled_vertices: Set[int], g: CycleGraph, g_shrunk: CycleGraph):
    """
    Shrinks the graph g by replacing the path between two vertices to an edge.

    :param vertices: The vertices to start the shrink process.
    :param sampled_vertices: All vertices the graph to be shrunk to.
    :param g: The original graph.
    :param g_shrunk: The graph after paths are shrunk to edges.
    """
    for v in vertices:
        # find sampled vertex on the left and on the right.
        l_v, r_v = -1, -1
        l, r = v, v
        while True:
            l = g.left(l)
            if l in sampled_vertices:
                l_v = l
                break

        while True:
            r = g.right(r)
            if r in sampled_vertices:
                r_v = r
                break

        g_shrunk.set_neighbors(v, l_v, r_v)


@ray.remote
def shrink_ray(vertices, sampled_vertices, pg_config, graph_name, shrunk_graph_name):
    g = PostgresGraph(pg_config, graph_name)
    g_shrunk = PostgresGraph(pg_config, shrunk_graph_name)
    shrink(vertices, sampled_vertices, g, g_shrunk)


def two_cycle_base(g: CycleGraph) -> bool:
    """
    two_cycle_base return True when a graph has two cycles.
    """
    n = g.vertex_cnt()
    visited_vertex_cnt = 1
    v = g.sample_vertices(1)[0]
    next_vertex = g.left(v)
    while next_vertex != v:
        visited_vertex_cnt += 1
        next_vertex = g.left(next_vertex)
    return visited_vertex_cnt < n


@ray.remote
def two_cycle_base_ray(pg_config, graph_name) -> bool:
    g = PostgresGraph(pg_config, graph_name)
    return two_cycle_base(g)


def two_cycle(g: CycleGraph, threshold: int, epsilon: float, parallelism: int,
              new_graph_func: Callable[[str], CycleGraph],
              delete_graph_func: Callable[[str], None]) -> bool:
    """
    two_cycle returns True when a graph has two cycles.

    :param g: the original graph to test two-cycle.
    :param threshold: Apply the two_cycle_base if the graph size is below threshold.
    :param epsilon: A number between 0 and 1. Each vertex is sampled with probability n^(-epsilon/2)
    :param parallelism: how many machines to run shrinking algorithm at the same time.
    :param new_graph_func: the method to create a graph for next round of shrink.
    :param delete_graph_func: the method to delete the graph used in previous round of shrink.
    """
    if g.vertex_cnt() <= threshold:
        return two_cycle_base(g)

    original_graph_name = g.name()
    shrink_round = 0
    while g.vertex_cnt() > threshold:
        number_of_vertices_to_sample = math.floor(g.vertex_cnt() ** (1 - epsilon / 2))
        sampled_vertices = g.sample_vertices(number_of_vertices_to_sample)
        sampled_vertices_set = set(sampled_vertices)
        m = number_of_vertices_to_sample // parallelism
        shrink_round += 1
        shrink_graph_name = "tmp_{}_{}".format(original_graph_name, shrink_round)
        g_shrunk = new_graph_func(shrink_graph_name)
        g_shrunk.set_vertices(sampled_vertices)
        for i in range(parallelism):
            end = number_of_vertices_to_sample if i == parallelism - 1 else (i + 1) * m
            shrink(sampled_vertices[i * m:end], sampled_vertices_set, g, g_shrunk)
        if g.name() != original_graph_name:
            delete_graph_func(g.name())
        g = g_shrunk
    result = two_cycle_base(g)
    if g.name() != original_graph_name:
        delete_graph_func(g.name())
    return result


@ray.remote
def two_cycle_ray(pg_config, graph_name, threshold: int, epsilon: float, parallelism: int):
    """
    two_cycle returns True when a graph has two cycles.

    :param pg_config: the connection configuration for the postgres server storing the graph.
    :param graph_name: name of the graph to test two-cycle.
    :param threshold: Apply the two_cycle_base if the graph size is below threshold.
    :param epsilon: A number between 0 and 1. Each vertex is sampled with probability n^(-epsilon/2)
    :param parallelism: how many machines to run shrinking algorithm at the same time.
    """
    g = PostgresGraph(pg_config, graph_name)
    if g.vertex_cnt() <= threshold:
        return ray.get(two_cycle_base_ray.remote(pg_config, g.name()))

    original_graph_name = g.name()
    shrink_round = 0
    while g.vertex_cnt() > threshold:
        number_of_vertices_to_sample = math.floor(g.vertex_cnt() ** (1 - epsilon / 2))
        sampled_vertices = g.sample_vertices(number_of_vertices_to_sample)
        sampled_vertices_set = set(sampled_vertices)
        shrink_round += 1
        svs_ref = ray.put(sampled_vertices_set)
        shrink_graph_name = "tmp_{}_{}".format(graph_name, shrink_round)
        shrunk_graph = create_empty_pg_cycle_graph(pg_config, shrink_graph_name)
        shrunk_graph.set_vertices(sampled_vertices_set)
        m = number_of_vertices_to_sample // parallelism
        sv_refs = []
        for i in range(parallelism):
            end = number_of_vertices_to_sample if i == parallelism - 1 else (i + 1) * m
            sv_refs.append(ray.put(sampled_vertices[i * m:end]))
        ray.get([shrink_ray.remote(sv, svs_ref, pg_config, graph_name, shrink_graph_name) for sv in
                 sv_refs])
        if g.name() != original_graph_name:
            delete_pg_cycle_graph(pg_config, g.name())
        g = PostgresGraph(pg_config, shrink_graph_name)

    result = ray.get(two_cycle_base_ray.remote(pg_config, g.name()))
    if g.name() != original_graph_name:
        delete_pg_cycle_graph(pg_config, g.name())
    return result
