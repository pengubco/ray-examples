import math
import string

from cycle_graph import CycleGraph
from typing import Callable, List, Set


def shrink(vertices: List[int], sampled_vertices: Set[int], g: CycleGraph, g_shrunk: CycleGraph):
    """
    Shrinks the graph g by replacing the path between two vertices to an edge.

    :param vertices: The vertices to start the shrink process.
    :param sampled_vertices: All vertices the graph to be shrunk to.
    :param g: The original graph.
    :param g_shrunk: The shrunk graph.
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


def two_cycle(g: CycleGraph, threshold: int, epsilon: float,
              new_graph_func: Callable[[str], CycleGraph],
              delete_graph_func: Callable[[str], None]) -> bool:
    """
    two_cycle returns True when a graph has two cycles.

    :param g:
    :param threshold: Apply the two_cycle_base if the graph size is below threshold.
    :param epsilon: A number between 0 and 1. Each vertex is sampled with probability n^(-epsilon/2)
    :param new_graph_func:
    """
    original_graph_name = g.name()
    shrink_graph_name = ""
    shrink_round = 0
    while g.vertex_cnt() >= threshold:
        number_of_vertices_to_sample = math.floor(g.vertex_cnt() ** (1 - epsilon / 2))
        sampled_vertices = g.sample_vertices(number_of_vertices_to_sample)
        sampled_vertices_set = set(sampled_vertices)
        # shrink_partition_size
        m = number_of_vertices_to_sample // 3
        shrink_round += 1
        shrink_graph_name = "{}_{}".format(original_graph_name, shrink_round)
        g_shrunk = new_graph_func(shrink_graph_name)
        g_shrunk.set_vertices(sampled_vertices)
        shrink(sampled_vertices[:m], sampled_vertices_set, g, g_shrunk)
        shrink(sampled_vertices[m:2 * m], sampled_vertices_set, g, g_shrunk)
        shrink(sampled_vertices[2 * m:], sampled_vertices_set, g, g_shrunk)
        # delete the graph used in previous shrink round
        if g.name() != original_graph_name:
            delete_graph_func(g.name())
        g = g_shrunk
    result = two_cycle_base(g)
    if g.name() != original_graph_name:
        delete_graph_func(g.name())
    return  result
