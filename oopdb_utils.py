from typing import List, Tuple
from prettytable import PrettyTable

def print_table(rows : List[Tuple], column_headers : List[str]) -> None:
    table = PrettyTable(column_headers)
    table.add_rows(rows)
    print(table)