from typing import Union, Optional, Callable, Any
from invoke import Result as InvokeResult

Number = Union[int, float]
MaybeNumber = Optional[Number]
# TODO: param `Any` should be a parent class for all Records
RecordPredicate = Callable[[Any], bool]
PayloadMapper = Callable[[Any], Any]

CMD = str
CMDRunner = Callable[..., InvokeResult]
