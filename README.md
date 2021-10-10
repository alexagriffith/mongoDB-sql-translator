# mongoDB-sql-translator


# Getting Started
To run the translator use the command line like

```
python3 translate.py --mongodb_query="db.user.find();"
```
The only argument accepted is --mongodb_query which takes a string

# Assumptions
* If there are two sets of values in .find(), the first will be the where clause and the second will be the select clause
* All select clause values will be 1 or 0 (or nested like {a: {b: 1}}, and when flattened ({a.b: 1}), the value will then be 1 or 0)

# Todo/Improvements
* write more tests with more edgecases (the tests run all of the methods, but the methods are nicely separated, and unit testing on each of those is easy to do if there was more time)