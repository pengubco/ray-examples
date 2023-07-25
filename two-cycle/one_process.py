import math
import random


# Apply the 2-cycle graph algorithm to an in-memory graph, using one Python process.

class InMemoryGraph:
    def __init__(self):
        self.vertices = []
        self.left_neighbors = {}
        self.right_neighbors = {}

    def left(self, v):
        return self.left_neighbors[v]

    def right(self, v):
        return self.right_neighbors[v]

    def set_left(self, v, u):
        self.left_neighbors[v] = u

    def set_right(self, v, u):
        self.right_neighbors[v] = u

    def vertex_cnt(self):
        return len(self.vertices)


def shrink(vertices, sampled_vertices, g, g_shrunk):
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

        g_shrunk.set_left(v, l_v)
        g_shrunk.set_right(v, r_v)


def two_cycle_base(g):
    """
    two_cycle_base return True when a graph has two cycles.
    """
    n = g.vertex_cnt()
    visited_vertex_cnt = 1
    v = g.vertices[0]
    next_vertex = g.left(v)
    while next_vertex != v:
        visited_vertex_cnt += 1
        next_vertex = g.left(next_vertex)
    return visited_vertex_cnt < n


def two_cycle(g, threshold, epsilon):
    """
    two_cycle returns True when a graph has two cycles.

    :param g:
    :param threshold: Apply the two_cycle_base if the graph size is below threshold.
    :param epsilon: A number between 0 and 1. Each vertex is sampled with probability n^(-epsilon/2)
    """
    while g.vertex_cnt() >= threshold:
        number_of_vertices_to_sample = math.floor(g.vertex_cnt() ** (1 - epsilon / 2))
        sampled_vertices = random.sample(g.vertices, number_of_vertices_to_sample)
        # shrink_partition_size
        m = number_of_vertices_to_sample // 3
        g_shrunk = InMemoryGraph()
        g_shrunk.vertices = sampled_vertices
        shrink(sampled_vertices[:m], sampled_vertices, g, g_shrunk)
        shrink(sampled_vertices[m:2 * m], sampled_vertices, g, g_shrunk)
        shrink(sampled_vertices[2 * m:], sampled_vertices, g, g_shrunk)
        g = g_shrunk
    return two_cycle_base(g)


def add_cycle(g, vertices):
    """
    Add edges between consecutive vertices to a graph.

    :param g: the graph.
    :param vertices:
    """
    n = len(vertices)
    for i in range(n):
        r = 0 if i == n - 1 else i + 1
        g.set_right(vertices[i], vertices[r])
        l = n - 1 if i == 0 else i - 1
        g.set_left(vertices[i], vertices[l])


def create_graph(has_two_cycles, num_vertices):
    """
    Return a graph of one cycle or two cycles.

    :param has_two_cycles: Whether the graph has two independent cycles. If not, the graph is a cycle.
    :param num_vertices: Total number of vertices.
    :return:
    """
    g = InMemoryGraph()
    g.vertices = [i for i in range(1, num_vertices + 1)]
    g.n = len(g.vertices)
    random.shuffle(g.vertices)
    if has_two_cycles:
        mid = random.randint(num_vertices // 3, 2 * num_vertices // 3)
        add_cycle(g, g.vertices[:mid])
        add_cycle(g, g.vertices[mid:])
    else:
        add_cycle(g, g.vertices)
    return g


if __name__ == '__main__':
    threshold, epsilon = 100, 1 / 2
    g = create_graph(True, 10)
    assert two_cycle(g, threshold, epsilon)

    g2 = create_graph(False, 10)
    assert not two_cycle(g2, threshold, epsilon)

    g3 = create_graph(True, 10000)
    assert two_cycle(g3, threshold, epsilon)

    g4 = create_graph(False, 10000)
    assert not two_cycle(g4, threshold, epsilon)
