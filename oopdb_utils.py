from oopdb import *

from typing import List, Tuple

def print_rows(rows : List[Tuple], column_headers : List[str] = []) -> None:
    print(column_headers)
    for row in rows:
        print(row)