from typing import Any, List, Tuple
from prettytable import PrettyTable

def print_table(rows : List[Tuple], column_headers : List[str]) -> None:
    table = PrettyTable(column_headers)
    table.add_rows(rows)
    print(table)

def wrap_value(value : Any) -> str:
    res = str(value)
    if isinstance(value, str) or isinstance(value, bool):
        res = f"'{res}'"
    return res