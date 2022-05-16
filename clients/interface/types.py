from typing import Union, ClassVar, Optional, Callable, Any

Number = Union[int, float]
MaybeNumber = Optional[Number]
RecordPredicate = Callable[[Any], bool]
PayloadMapper = Callable[[Any], Any]
