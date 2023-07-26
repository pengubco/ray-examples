from abc import abstractmethod
from typing import Protocol
from typing import List


class CycleGraph(Protocol):
    """
    Interface of a graph of either one cycle or two cycles.
    """

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def set_neighbors(self, v, left_neighbor, right_neighbor) -> None:
        pass

    @abstractmethod
    def left(self, v) -> int:
        pass

    @abstractmethod
    def right(self, v) -> int:
        pass

    @abstractmethod
    def vertex_cnt(self) -> int:
        pass

    @abstractmethod
    def sample_vertices(self, n) -> List[int]:
        pass

    @abstractmethod
    def set_vertices(self, vertices) -> None:
        pass
