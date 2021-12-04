from typing import Any
import enum
import copy

class Operation(enum.Enum):
    EQUAL = "="
    GREATER_THAN = ">"
    LESS_THAN = "<"
    GREATER_THAN_OR_EQUAL = ">="
    LESS_THAN_OR_EQUAL = "<="
    NOT_EQUAL = "<>"
    BETWEEN = "BETWEEN"
    LIKE = "LIKE"
    IN = "IN"

class Expression:
    '''
    '''

    def __init__(self, column_name : str, operation : Operation, value : Any) -> None:
        '''
        '''
        Expression.__check_value_type_for_operation(operation, value)
        self.expression = f"{column_name} {operation.value} "
        if operation == Operation.IN:
            self.expression += f"({', '.join([str(elem) for elem in value])})"
        elif operation == Operation.BETWEEN:
            self.expression += f"{value[0]} AND {value[1]}"
        elif operation == Operation.LIKE:
            self.expression += f"'{value}'"
        else:
            self.expression += f"{value}"
        self.is_simple = True

    def OR(self, expression : 'Expression') -> 'Expression':
        '''
        '''
        return Expression.__binary_operation(self, expression, "OR")

    def AND(self, expression : 'Expression') -> 'Expression':
        '''
        '''
        return Expression.__binary_operation(self, expression, "AND")

    @staticmethod
    def __binary_operation(left_expression : 'Expression', right_expression : 'Expression', operation : str) -> 'Expression':
        '''
        '''
        right = Expression.__get_wrapped_expression_str(right_expression)
        left = Expression.__get_wrapped_expression_str(left_expression)
        res = copy.copy(left_expression)
        res.expression = f"{left} {operation} {right}"
        res.is_simple = False
        return res

    @staticmethod
    def __get_wrapped_expression_str(expression : 'Expression') -> str:
        res = expression.expression
        if not expression.is_simple:
            res = f"({res})"
        return res

    @staticmethod
    def NOT(expression : 'Expression') -> 'Expression':
        '''
        '''
        res = copy.copy(expression)
        res.expression = "NOT "
        if res.is_simple:
            res.expression += f"{expression.expression}"
        else:
            res.expression += f"({expression.expression})"
        return res

    @staticmethod
    def __check_value_type_for_operation(operation : Operation, value : Any) -> None:
        '''
        '''
        error_message = f"Value type for operation {operation.name} with given value type {type(value)} for {value}"
        ok = True
        if operation == Operation.IN and not isinstance(value, list):
            error_message += f" doesn't match with the desired type {list}"
            ok = False
        if operation == Operation.LIKE and not isinstance(value, str):
            error_message += f" doesn't match with the desired type {str}"
            ok = False
        if operation == Operation.BETWEEN and (not isinstance(value, tuple) or len(value) != 2):
            error_message += f" doesn't match with the desired type {tuple} that must have only two elements to represent pair"
            ok = False
        if not ok:
            raise Exception(error_message)
