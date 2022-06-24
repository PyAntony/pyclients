import abc


class PyClient(abc.ABC):
    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def test_connection(self) -> bool:
        pass

    @abc.abstractmethod
    def close_connection(self) -> None:
        pass
