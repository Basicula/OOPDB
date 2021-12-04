from typing import Any
import enum
import copy

class Operation(enum.Enum):
    '''
    Supported operations

    Each operation defines value type
        EQUAL
        GREATER_THAN
        LESS_THAN
        GREATER_THAN_OR_EQUAL
        LESS_THAN_OR_EQUAL
        NOT_EQUAL
            Supported any value type (str, int etc) that have to match column type
        BETWEEN
            Value type - tuple with 2 components, each component must have same type and match column type
        LIKE
            Value type - string(str)
        IN
            Value type - list
    '''

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
    Abstraction for bool expression that can be used in database queries
    '''

    def __init__(self, column_name : str, operation : Operation, value : Any) -> None:
        '''
        Creates expression after check whether the value type matches given operation,
        if not Exception will be raised

        column_name : str, required
            The column name that we want to filter out with given value and operation
        operation : Operation, required
            The operation to be applied. Operation defines value type see details in Operation enum
        value : Any, required
            The value for filtering. Its type depends on the given operation
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
        Creates new expression that combines two other expressions with the 'or'
        '''
        return Expression.__binary_operation(self, expression, "OR")

    def AND(self, expression : 'Expression') -> 'Expression':
        '''
        Creates new expression that combines two other expressions with the 'and'
        '''
        return Expression.__binary_operation(self, expression, "AND")

    @staticmethod
    def __binary_operation(left_expression : 'Expression', right_expression : 'Expression', operation : str) -> 'Expression':
        '''
        Common function that proccess composition of two other expressions by the given operation

        left_expression : Expression, required
            Left sided part of the future composite expression
        right_expression : Expression, required
            Right sided part of the future composite expression
        operation : str, required
            String representation of the concatenate operation
        '''
        right = Expression.__get_wrapped_expression_str(right_expression)
        left = Expression.__get_wrapped_expression_str(left_expression)
        res = copy.copy(left_expression)
        res.expression = f"{left} {operation} {right}"
        res.is_simple = False
        return res

    @staticmethod
    def __get_wrapped_expression_str(expression : 'Expression') -> str:
        '''
        Wraps expression string representation if it's composite expression

        expression : Expression, required
            Expression to be wrapped
        '''
        res = expression.expression
        if not expression.is_simple:
            res = f"({res})"
        return res

    @staticmethod
    def NOT(expression : 'Expression') -> 'Expression':
        '''
        Negation for expression

        expression : Expression, required
            Expression to be negated
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
        Checks if the value type matches supported types by the given operation

        operation : Operation, required
        value : Any, required
            Value to check type
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
