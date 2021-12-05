from oopdb.Expression import Expression, Operation
import unittest

class TestSimpleExpression(unittest.TestCase):
    def test_equality(self):
        exp = Expression("Column", Operation.EQUAL, "123")
        expected_expression = "Column = '123'"
        self.assertEqual(exp.expression, expected_expression)

    def test_less(self):
        exp = Expression("Column", Operation.LESS_THAN, 123)
        expected_expression = "Column < 123"
        self.assertEqual(exp.expression, expected_expression)

    def test_greater(self):
        exp = Expression("Column", Operation.GREATER_THAN, 123)
        expected_expression = "Column > 123"
        self.assertEqual(exp.expression, expected_expression)

    def test_greater_or_equal(self):
        exp = Expression("Column", Operation.GREATER_THAN_OR_EQUAL, 123)
        expected_expression = "Column >= 123"
        self.assertEqual(exp.expression, expected_expression)

    def test_less_or_equal(self):
        exp = Expression("Column", Operation.LESS_THAN_OR_EQUAL, "123")
        expected_expression = "Column <= '123'"
        self.assertEqual(exp.expression, expected_expression)

    def test_not_equal(self):
        exp = Expression("Column", Operation.NOT_EQUAL, "123")
        expected_expression = "Column <> '123'"
        self.assertEqual(exp.expression, expected_expression)

    def test_in(self):
        exp_in_good = Expression("ColumnIn", Operation.IN, [1,2,3])
        expected_expression = "ColumnIn IN (1, 2, 3)"
        self.assertEqual(exp_in_good.expression, expected_expression)

        with self.assertRaises(Exception):
            exp_in_bad_type = Expression("ColumnIn", Operation.IN, "1 2 3")

    def test_between(self):
        exp_between_good = Expression("ColumnBetween", Operation.BETWEEN, (1, 10))
        expected_expression = "ColumnBetween BETWEEN 1 AND 10"
        self.assertEqual(exp_between_good.expression, expected_expression)

        with self.assertRaises(Exception):
            exp_between_bad_size = Expression("ColumnBetween", Operation.BETWEEN, (1, 10, 12))

        with self.assertRaises(Exception):
            exp_between_bad_type = Expression("ColumnBetween", Operation.BETWEEN, "1 10")

    def test_like(self):
        exp_like_good = Expression("ColumnLike", Operation.LIKE, "prefix_%")
        expected_expression = "ColumnLike LIKE 'prefix_%'"
        self.assertEqual(exp_like_good.expression, expected_expression)
        
        with self.assertRaises(Exception):
            exp_like_bad_type = Expression("ColumnLike", Operation.LIKE, 123)

    def test_not(self):
        exp_not = Expression.NOT(Expression("ColumnNot", Operation.EQUAL, "123"))
        expected_expression = "NOT ColumnNot = '123'"
        self.assertEqual(exp_not.expression, expected_expression)

class TestCompositeExpression(unittest.TestCase):
    def test_or(self):
        exp1 = Expression("Column1", Operation.EQUAL, "123")
        exp2 = Expression("Column2", Operation.IN, [1,2,3,5])
        exp3 = Expression("Column3", Operation.LESS_THAN, 50)
        exp4 = exp1.OR(exp2)
        expected_expression = "Column1 = '123' OR Column2 IN (1, 2, 3, 5)"
        self.assertEqual(exp4.expression, expected_expression)
        exp5 = exp4.OR(exp3)
        expected_expression = f"({expected_expression}) OR Column3 < 50"
        self.assertEqual(exp5.expression, expected_expression)

    def test_and(self):
        exp1 = Expression("Column1", Operation.GREATER_THAN, "123")
        exp2 = Expression("Column2", Operation.BETWEEN, (1,5))
        exp3 = Expression("Column3", Operation.LIKE, "a%")
        exp4 = exp1.AND(exp2)
        expected_expression = "Column1 > '123' AND Column2 BETWEEN 1 AND 5"
        self.assertEqual(exp4.expression, expected_expression)
        exp5 = exp4.AND(exp3)
        expected_expression = f"({expected_expression}) AND Column3 LIKE 'a%'"
        self.assertEqual(exp5.expression, expected_expression)

    def test_not(self):
        exp1 = Expression("Column1", Operation.GREATER_THAN_OR_EQUAL, "123")
        exp2 = Expression("Column2", Operation.LESS_THAN_OR_EQUAL, "321")
        exp3 = Expression("Column3", Operation.NOT_EQUAL, "test")
        exp = Expression.NOT(exp3.AND(exp1.OR(exp2)))
        expected_expression = "NOT (Column3 <> 'test' AND (Column1 >= '123' OR Column2 <= '321'))"
        self.assertEqual(exp.expression, expected_expression)

    def test_all_in_one(self):
        exp_eq = Expression("ColumnEqual", Operation.EQUAL, 123)
        exp_gt = Expression("ColumnGT", Operation.GREATER_THAN, 12)
        exp_lt = Expression("ColumnLT", Operation.LESS_THAN, 21)
        exp_gteq = Expression("ColumnGTEQ", Operation.GREATER_THAN_OR_EQUAL, 23)
        exp_lteq = Expression("ColumnLTEQ", Operation.LESS_THAN_OR_EQUAL, 32)
        exp_neq = Expression("ColumnNEQ", Operation.NOT_EQUAL, 0)
        exp_between = Expression("ColumnBetween", Operation.BETWEEN, (123, 321))
        exp_in = Expression("ColumnIn", Operation.IN, [1,2,3,5,8,13])
        exp_like = Expression("ColumnLike", Operation.LIKE, "like%")
        all_in_one_exp = Expression.NOT(((exp_gt.OR(exp_lt)).AND(exp_gteq.OR(exp_lteq))).OR(exp_eq)).OR(exp_neq).AND(exp_between.OR(exp_in).OR(exp_like))
        expected_expression = ("("
                                    "("
                                    "NOT ("
                                            "("
                                                "("
                                                "ColumnGT > 12 OR "
                                                "ColumnLT < 21"
                                                ") AND "
                                                "("
                                                "ColumnGTEQ >= 23 OR "
                                                "ColumnLTEQ <= 32"
                                                ")"
                                            ") OR "
                                            "ColumnEqual = 123"
                                        ")"
                                    ") OR "
                                    "ColumnNEQ <> 0"
                                ") AND "
                                "("
                                "("
                                "ColumnBetween BETWEEN 123 AND 321 OR "
                                "ColumnIn IN (1, 2, 3, 5, 8, 13)"
                                ") OR "
                                "ColumnLike LIKE 'like%'"
                                ")")
        self.assertEqual(all_in_one_exp.expression, expected_expression)

if __name__ == "__main__":
    unittest.main()