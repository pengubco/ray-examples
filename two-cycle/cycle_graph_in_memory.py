from typing import List
import random
from cycle_graph import CycleGraph


class InMemoryCycleGraph(CycleGraph):
    """
    An implementation o CycleGraph that stores the graph in memory.
    """
    def __init__(self, name):
        self._name = name
        self.vertices = []
        self.left_neighbors = {}
        self.right_neighbors = {}

    def set_neighbors(self, v, left_neighbor, right_neighbor) -> None:
        self.left_neighbors[v] = left_neighbor
        self.right_neighbors[v] = right_neighbor

    def left(self, v) -> int:
        return self.left_neighbors[v]

    def right(self, v) -> int:
        return self.right_neighbors[v]

    def vertex_cnt(self):
        return len(self.vertices)

    def sample_vertices(self, n) -> List[int]:
        return random.sample(self.vertices, n)

    def set_vertices(self, vertices) -> None:
        self.vertices = vertices

    def name(self) -> str:
        return self._name


def create_in_memory_cycle_graph(name, has_two_cycles, n) -> InMemoryCycleGraph:
    """
    Create and return a graph of one cycle or two cycles.

    :param name: name of the graph.
    :param has_two_cycles: Whether the graph has two independent cycles. If not, the graph is a cycle.
    :param n: Total number of vertices.
    :return:
    """
    g = InMemoryCycleGraph(name)
    g.vertices = [i for i in range(1, n + 1)]
    random.shuffle(g.vertices)
    if has_two_cycles:
        mid = random.randint(n // 3, 2 * n // 3)
        add_cycle_to_in_memory_graph(g, g.vertices[:mid])
        add_cycle_to_in_memory_graph(g, g.vertices[mid:])
    else:
        add_cycle_to_in_memory_graph(g, g.vertices)
    return g


def add_cycle_to_in_memory_graph(g, vertices) -> None:
    """
    Add edges between consecutive vertices to a graph.

    :param g: the graph.
    :param vertices:
    """
    n = len(vertices)
    for i in range(n):
        l = n - 1 if i == 0 else i - 1
        r = 0 if i == n - 1 else i + 1
        g.set_neighbors(vertices[i], vertices[l], vertices[r])
