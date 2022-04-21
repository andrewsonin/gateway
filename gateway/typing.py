from io import RawIOBase, BufferedIOBase, TextIOBase, TextIOWrapper
from mmap import mmap
from os import PathLike
from typing import Union, IO

PD_READ_PICKLE_ANNOTATION = Union[
    PathLike,
    str,
    IO[str],
    RawIOBase,
    BufferedIOBase,
    TextIOBase,
    TextIOWrapper,
    mmap
]
