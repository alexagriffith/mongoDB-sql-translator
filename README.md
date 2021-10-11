# mongoDB-sql-translator


# Getting Started
To run the translator use the command line like

```
python3 translate.py --mongodb_query="db.user.find();"
```
The only argument accepted is `--mongodb_query` which takes a string

----------------------------------------------

The translator only supports the following operations: 
```
$or
$and
$lt
$lte
$gt
$gte
$ne
$in
```
and only works for queries like `db.{table_name}.find()`

# Running Tests
To run the tests, use the command
```
python3 -m unittest test.py 
```

# Assumptions
* If there are two sets of values in .find(), the first will be the where clause and the second will be the select clause
* All select clause values will be 1 or 0 (or nested like {a: {b: 1}}, and when flattened ({a.b: 1}), the value will then be 1 or 0)

# Todo/Improvements
* write more tests with more edgecases (the tests run all of the methods, but the methods are nicely separated, and unit testing on each of those is easy to do if there was more time)
* dockerize the script
* setup CI/CD