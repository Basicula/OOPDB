import sqlite3
import enum
from typing import Any, List
from .ColumnConfig import *

def format_array(array : List[Any], element_wrapper : str = ''):
    return ', '.join(f'{element_wrapper}{str(elem)}{element_wrapper}' for elem in array)

class OrderingTypes(enum.Enum):
    '''
    Supported ordering types that maps on SQL ordering types
        ASCENDING
            Default value when anything set explicitly
    '''
    ASCENDING = "ASC"
    DESCENDING = "DESC"

class RowsStyle(enum.Enum):
    '''
    Supported row styles
        DICTIONARY
            Each row will be represented as dictionary with column names as keys
        TUPLE
            Each row will be represented as tuple of values
    '''
    DICTIONARY = "DICT"
    TUPLE = "TUPLE"

class OOPDB:
    '''
    OOP abstraction for data base communication based on sqlite3
    '''

    def __init__(self) -> None:
        self.query = ""

    def open(self, db_path : str) -> 'OOPDB':
        '''
        db_path : str, required
            The path to the data base
        If the file doesn't exist creates new empty data base using set path
        '''
        if db_path:
            try:
                self.connection = sqlite3.connect(db_path, detect_types=sqlite3.PARSE_DECLTYPES)
                self.connection.row_factory = sqlite3.Row
                def bool_processor(v):
                    if v == b"True":
                        return True
                    elif v == b"False":
                        return False
                    print(f"Wrong value {v} for BOOL type")
                    raise ValueError
                sqlite3.register_converter("BOOL", bool_processor)
            except sqlite3.Error as e:
                print(f"The error '{e}' occurred")
        return self

    def close(self) -> None:
        self.connection.close()

    def execute(self) -> bool:
        '''
        Executes all queued commands
        
        Commonly used after pushing, updating and other commands without any output
        '''
        query = self.query
        if query[-1] != ';':
            query += ';'
        self.query = ""
        try:
            cursor = self.connection.cursor()
            cursor.executescript(query)
            self.connection.commit()
        except sqlite3.Error as e:
            print(f"The error '{e}' occurred for query '{query}'")
            return False

        return True

    def fetch(self, rows_style : RowsStyle = RowsStyle.TUPLE) -> List[Any]:
        '''
        Executes all queued commands

        Commonly used after select or other commands with any output

        rows_style - RowsStyle, optional
            Defines how fetched rows will be look like

        Returns list of rows
        '''
        query = self.query
        if query[-1] != ';':
            query += ';'
        self.query = ""
        try:
            cursor = self.connection.cursor()
            cursor.execute(query)
            if rows_style == RowsStyle.DICTIONARY:
                result = [dict(row) for row in cursor.fetchall()]
            else:
                result = [tuple(row) for row in cursor.fetchall()]
            return result
        except sqlite3.Error as e:
            print(f"The error '{e}' occurred for query '{query}'")
            return []

    def table_names(self) -> 'OOPDB':
        '''
        Adds to the queue table names getting command

        The result will be rows with table names
        '''
        self.query += "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'slite_%';"
        return self

    def column_names(self, table_name : str) -> 'OOPDB':
        '''
        Adds to the queue table's columns name getting command
        '''
        self.query += f"SELECT name FROM PRAGMA_TABLE_INFO('{table_name}');"
        return self

    def create_table(self, table_name : str, columns : List[ColumnConfig]) -> 'OOPDB':
        '''
        Adds to the queue table creation command

        table_name : str, required
            The name for the new table
        columns : List[ColumnConfig], required
            List of column configs for new table
        '''
        self.query += f"CREATE TABLE {table_name} ({format_array(columns)});"

        return self

    def insert_into(self, table_name : str, columns : List[str], values : List[Any]) -> 'OOPDB':
        '''
        Adds to the queue data row insertion command

        table_name : str, required
            The name for the target table
        columns : List[str], required
            List of column names that will be defined by new values
        values : List[Any], required
            List of values for selected columns
        '''
        self.query += f"INSERT INTO {table_name} ({format_array(columns)}) "
        self.query += "VALUES (" + format_array(values, '"') + ");"

        return self

    def select(self, table_name : str, columns : List[str] = []) -> 'OOPDB':
        '''
        Adds to the queue select data rows command

        table_name : str, required
            The name for the target table
        columns : List[str], optional
            List of column names, if empty all columns will be taken for result
        '''
        if len(columns) > 0:
            # format_array returns string with parentheses thats why substring[1:-1] is taken from returned string
            self.query += f"SELECT {format_array(columns)} "
        else:
            self.query += f"SELECT * "
        self.query += f"FROM {table_name} "

        return self

    def select_count(self, table_name : str) -> 'OOPDB':
        '''
        Adds to the queue select data rows count command

        table_name : str, required
            The name for the target table
        '''
        self.query += f"SELECT COUNT(*) FROM {table_name} "
        return self

    def inner_join(self, table : str, table_column : str, target_table_column : str) -> 'OOPDB':
        '''
        Adds to the queue inner join command
        
        table : str, required
            The name for the target table
        table_column : str, required
            The name for the column in the selecting table on which joining will be applied
        target_table_column : str, required
            The name for the target table column on which joining will be applied
        '''
        self.query += f"INNER JOIN {table} ON {table_column} = {table}.{target_table_column} "
        return self

    def order_by(self, columns : List[str], orders : List[OrderingTypes]) -> 'OOPDB':
        '''
        Adds to the queue inner ordering command

        columns : List[str], required
            List of column names that will be sorted
        orders : List[OrderingTypes], required
            List of sort orders for each column
        '''
        if len(columns) != len(orders):
            print(f"Order by command queueing failed due to mismatching sizes of '{columns}' and '{orders}' lists")
            return self

        column_orders = []
        for column, order in zip(columns, orders):
            column_orders.append(f"{column} {order.value}")

        self.query += f"ORDER BY {format_array(column_orders)} "
        return self

    def update(self, table_name : str, columns : List[str], values : List[Any]) -> 'OOPDB':
        '''
        Adds to the queue update command

        This command will set value from 'values' to the corresponding column with name from 'columns'
        so sizes of the columns and values lists must be equal

        table_name - str, required
            The name of the table that will be updated
        columns - List[str], required
            The list of column names that will be updated by new values from 'values'
        values - List[Any], required
            New values to be set in the table for given columns
        '''
        if len(columns) != len(values):
            print(f"Update command queueing failed due to mismatching sizes of '{columns}' and '{values}' lists")
            return self

        column_values = []
        for column, value in zip(columns, values):
            column_values.append(f"{column} = '{value}'")

        self.query += f"UPDATE {table_name} SET {format_array(column_values)} "
        return self