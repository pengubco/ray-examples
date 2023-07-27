import random
from typing import List

import psycopg2
from cycle_graph import CycleGraph


class PostgresGraph(CycleGraph):
    """
    An implementation o CycleGraph that stores the graph in postgres.
    1. Each graph is a postgres table. The table name is the graph name.
    2. The table has 3 columns: v, l, r. vertex, neighbor on the left, neighbor on the right.
    """

    def __init__(self, pg_config, name):
        self._name = name
        self.conn = None
        self.pg_config = pg_config

    def open_pg(self):
        if self.conn is not None:
            return
        self.conn = psycopg2.connect(**self.pg_config)
        self.conn.autocommit = True

    def close_pg(self):
        if self.conn is not None:
            self.conn.close()
        self.conn = None

    def set_neighbors(self, v, left_neighbor, right_neighbor) -> None:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = """
        INSERT INTO {table} (v, l, r)
        VALUES ({v}, {l}, {r})
        ON CONFLICT (v) DO UPDATE
        SET l = EXCLUDED.l, r = EXCLUDED.r;
        """.format(table=self._name, v=v, l=left_neighbor,
                   r=right_neighbor)
        cur.execute(sql)
        cur.close()

    def left(self, v) -> int:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = "select l from {table} where v = {v}".format(table=self._name, v=v)
        cur.execute(sql)
        l = cur.fetchone()[0]
        cur.close()
        return l

    def right(self, v) -> int:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = "select r from {table} where v = {v}".format(table=self._name, v=v)
        cur.execute(sql)
        r = cur.fetchone()[0]
        cur.close()
        return r

    def vertex_cnt(self) -> int:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = "select count(*) from {table}".format(table=self._name)
        cur.execute(sql)
        r = cur.fetchone()[0]
        cur.close()
        return r

    def sample_vertices(self, n) -> List[int]:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = "select v from {table} order by random() limit {n}".format(table=self._name, n=n)
        cur.execute(sql)
        rows = cur.fetchall()
        vertices = [row[0] for row in rows]
        cur.close()
        return vertices

    def set_vertices(self, vertices) -> None:
        if self.conn is None:
            self.open_pg()
        cur = self.conn.cursor()
        sql = "truncate table {table}".format(table=self._name)
        cur.execute(sql)
        cur.close()
        cur = self.conn.cursor()
        sql = "insert into {table}(v) values (%s)".format(table=self._name)
        cur.executemany(sql, [(v,) for v in vertices])
        cur.close()

    def name(self) -> str:
        return self._name


def create_empty_pg_cycle_graph(pg_config, graph_name):
    g = PostgresGraph(pg_config, graph_name)
    g.open_pg()
    cur = g.conn.cursor()

    sql = """
    create table if not exists {table} (
        v int primary key,
        l int,
        r int
    );""".format(table=graph_name)
    cur.execute(sql)
    cur.close()
    return g


def delete_pg_cycle_graph(pg_config, graph_name):
    conn = psycopg2.connect(**pg_config)
    conn.autocommit = True
    cur = conn.cursor()
    cur.execute("drop table {}".format(graph_name))
    cur.close()
    conn.close()


def create_pg_cycle_graph(pg_config, graph_name, has_two_cycles: bool, n) -> PostgresGraph:
    """
    Return a graph of one cycle or two cycles.

    :param has_two_cycles: Whether the graph has two independent cycles. If not, the graph is a cycle.
    :param n: Total number of vertices.
    :return:
    """
    g = create_empty_pg_cycle_graph(pg_config, graph_name)
    vertices = [i for i in range(1, n + 1)]
    g.set_vertices(vertices)
    random.shuffle(vertices)
    if has_two_cycles:
        mid = random.randint(n // 3, 2 * n // 3)
        add_cycle_to_pg_cycle_graph(g, vertices[:mid])
        add_cycle_to_pg_cycle_graph(g, vertices[mid:])
    else:
        add_cycle_to_pg_cycle_graph(g, vertices)
    return g


def add_cycle_to_pg_cycle_graph(g: PostgresGraph, vertices: List[int]) -> None:
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
