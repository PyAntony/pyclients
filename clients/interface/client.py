import abc


class Client(abc.ABC):
    @abc.abstractmethod
    def connect(self) -> None:
        pass

    @abc.abstractmethod
    def test_connection(self) -> bool:
        pass

    @abc.abstractmethod
    def close(self) -> None:
        pass
