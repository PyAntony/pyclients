from typing import Union, Optional, Callable, Any
from invoke import Result as InvokeResult

Number = Union[int, float]
MaybeNumber = Optional[Number]
Runnable = Callable[[], None]
Supplier = Callable[[], Any]
StringPredicate = Callable[[str], bool]
RecordPredicate = Callable[[Any], bool]
PayloadMapper = Callable[[Any], Any]

CMD = str
CMDRunner = Callable[..., InvokeResult]
