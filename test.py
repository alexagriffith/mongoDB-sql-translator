import unittest
from translate import QueryTranslator, separate_query_params

test_cases = [
    {
        "mongo_db": "db.user.find();",
        "sql": "SELECT * FROM user;"
    },
    {
        "mongo_db": "db.user.find({name :'julio'});",
        "sql": "SELECT * FROM user WHERE name = 'julio';"
    },
    {
        "mongo_db": "db.user.find({_id :23113} ,{name :1 ,age :1});",
        "sql": "SELECT name, age FROM user WHERE _id = 23113;",
    },
    {
        "mongo_db": "db.user.find({age :{$gte :21}} ,{name :1 ,_id :1});",
        "sql": "SELECT name, _id FROM user WHERE age >= 21;"
    },
    {
        "mongo_db": "db.user.find({name:{ first: 'Alexa', last: 'Griffith'}});",
        "sql": "SELECT * FROM user WHERE name.first = 'Alexa' AND name.last = 'Griffith';"
    },
    # below are some extra examples found from googling mongoDB queries
    {
        "mongo_db": "db.raffle.find({ticket_no :{$in :[725, 542, 390]}})",
        "sql": "SELECT * FROM raffle WHERE ticket_no IN [725, 542, 390];"
    },
    {
        "mongo_db": "db.raffle.find({$or :[{ticket_no : 725}, {winner: true}]})",
        "sql": "SELECT * FROM raffle WHERE ticket_no = 725 OR winner = TRUE;"
    },
    {
        "mongo_db": "db.raffle.find({$or :[{ticket_no :{$in :[725, 542, 390]}}, {winner: true}]})",
        "sql": "SELECT * FROM raffle WHERE ticket_no IN [725, 542, 390] OR winner = TRUE;"
    },
    {
        "mongo_db": "db.users.find({age : {$lt : 30, $gt : 20}})",
        "sql": "SELECT * FROM users WHERE age < 30 AND age > 20;"
    },
    {
        "mongo_db": "db.users.find({$and : [{x : {$lt : 1}}, {x : 4}]})",
        "sql": "SELECT * FROM users WHERE x < 1 AND x = 4;"
    }
]


class TestQueryTranslator(unittest.TestCase):
    def test_translator(self):
        for test_case in test_cases:
            result = QueryTranslator(test_case["mongo_db"]).run()
            self.assertEqual(test_case["sql"], result)

    def test_separate_query_params(self):
        query_string_1 = "{_id: 23113}, {name: 1, age: 1}"
        self.assertEqual(["{_id: 23113}", "{name: 1, age: 1}"], separate_query_params(query_string_1))
        query_string_2 = ""
        self.assertEqual([], separate_query_params(query_string_2))

    def test_find_table_name_raises_error(self):
        misformed_query = "db.faketable.somethingelse()"
        with self.assertRaises(ValueError):
            QueryTranslator(misformed_query).get_table_name()
