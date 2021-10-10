import re
import logging
import argparse


def separate_query_params(query_string):
    """
    Given a query with the format db.user.find(...),
    separate_query_params is given everything inside of the ()
    and returns 0-2 sets of params that will make up the where/select
    clauses in the SQL query.

    I attempted to use regex for this at first but after a while,
    it became clear that being fancy with regex wasn't worth it
    because we just need the outside parenthesis of the group(s) of
    query params, and thats pretty simple to implement in code.

    given a string like {_id :23113} ,{name :1 ,age :1}
    this method returns the two sets of query params like [{_id :23113}, {name :1 ,age :1}]

    :param query_string: everything inside the db.table.find(..) parenthesis
    :return: 0-2 sets of query params
    """
    opens = ["[", "{"]
    closes = ["]", "}"]
    cnt = 0
    idx = 0
    result = []

    # make sure no trailing spaces are around the query parameters
    query_string = query_string.strip()

    # since we removed trailing spaces, we should always begin with a {
    # so, having the if cond of cnt <= 0 shouldn't cause a problem for finding the query groups
    while idx <= len(query_string) - 1:
        elem = query_string[idx]
        if elem in opens:
            cnt += 1
        if elem in closes:
            cnt -= 1
        if cnt <= 0:
            break
        idx += 1

    if idx == 0:
        return result

    result.append(query_string[0:idx + 1])
    # check if the second group exists
    if idx != len(query_string) - 1:
        # if there is another group, we know it will have a , and could include a space
        # so, lstrip'ing the space first ensures that we remove any leading spaces
        result.append((query_string[idx + 1:]).lstrip(" ").lstrip(",").lstrip(" "))
    return result


def is_select(parsed_query_params):
    """
    On deciding if the query param belongs to the select/where clause:
    If only one set of params is in the .find(...), to decide if it belongs to the
    select or where clause, the code assumes that the parsed query params of the
    select will be in the form {key: 1} or {key: 0}
    (https://docs.mongodb.com/manual/reference/method/db.collection.find/)

    The docs say that this signifies inclusion/exclusion in the query,
    and the inclusion values are truthy (1 or true, but the docs also state
    "Specifies the inclusion of a field. Non-zero integers are also treated as true.")
    For the purposes of this exercies, and following the examples given, this code assumes
    that the parsed values for a select query will be something like
    {key: 1, key:0, key.field: 1}. If it matches this pattern, we will assume select.
    If it doesn't, we will assume it is part of the while clause.

    :param parsed_params: query parameters that have been recursively filtered/formatted
        from the mongoDB query string
    :return:
    """
    for value in parsed_query_params:
        if value[1] != 1 or value[1] != 0:
            return False
    return True


class QueryTranslator(object):
    """
    QueryTranslator translates MongoDB queries to SQL queries.
    The QueryTranslator supports the following operators:
        $or, $and, $lt, $lte, $gt, $gte, $ne, $in
    """

    def __init__(self, mongo_query=""):
        self.mongo_query = mongo_query
        self.operation_conversions = {"$or": "OR", "$and": "AND", "$lt": "<", "$lte": "<=", "$gt": ">",
                                      "$gte": ">=", "$ne": "!=", "$in": "IN"}

    def get_table_name(self):
        """
        We expect the query to be in the format of db.{table_name}.find(..)

        :return: name of table in the query
        """
        re_table_name = "^db\.(\w+)\.find.*$"

        try:
            table_name = re.findall(re_table_name, self.mongo_query)
            return table_name[0]
        except:
            raise ValueError(
                "Unable to parse the table name from the query. \
                Check that the query has the form db.{table_name}.find(..)")

    def get_query_params(self):
        """
        The query can have either one or two sets of params.

        For example, db.user.find({_id :23113} ,{name :1 ,age :1});
        has two sets of params, one for the select clause and one for the where clause.
        A query can have either both, one, or none of these.

        :return: the groupings of query params that will make up the select/where clauses
        """
        separated_query_params = []
        # the regex matches everything inside the outer brackets
        # there could be multiple brackets at the beginning/end, so this accounts for that.
        # For example, {age :{$gte : 1}} - we want to be consistent and get all the brackets so
        # we don't need any special cases when we recursively manipulate the string.
        re_query_params = "(\{.*\})"

        # in this case, its okay to have an empty match when no
        # query params are in the mongoDB query
        all_query_params = re.findall(re_query_params, self.mongo_query)
        if all_query_params:
            separated_query_params = separate_query_params(all_query_params[0])
        else:
            logging.info("No query parametes were found for the mongoDB query.")
        return separated_query_params

    def translate(self):
        """
        translate extracts the table_name, select clause, and where clause
        from the mongoDB query using string manipulation and regex.

        Regex is used to find the table name and the query parameter values inside
        the .find(...) of the mongoDB query. To separate the different groups of query params,
        the string is split on the closing brackets. Each group is evaluated recursively to map the
        conditions in the clauses correctly. The query params take a bit of work to filter
        because of all the different ways they can be formed and nested.

        :return: the table name, and the select and/or where clause conditions
        """
        table_name = self.get_table_name()
        query_params = self.get_query_params()
        select_params, where_params = ['*'], []

        # validate there are 0-2 elements in the array
        if len(query_params) > 2:
            raise ValueError(f"0-2 matches were expected from the query, got {len(query_params)} instead")

        # check if lenght of query params is 1 or 2, and if it is 0, return default values
        if len(query_params) == 1:
            parsed_params = self.parse_params(query_params[0])

            # infer if the group corresponds to params for the select clause
            # (see is_select method description for explanation)
            if is_select(parsed_params):
                select_params = parsed_params
            else:
                # if no select group exists, then we should select *
                where_params = parsed_params
        elif len(query_params) == 2:
            # For this exercise, we assume if two groupings are present,
            # the first is the where clause and the second is the select.
            where_params = self.parse_params(query_params[0])
            select_params = self.parse_params(query_params[1])

        return table_name, select_params, where_params

    def remove_ends(self, first_char, last_char, query_str):
        """
        remove_ends is a helper function to remove one char
        from the beginning and end of a string if it exists

        this is sometimes needed in the case that we don't want to
        remove multiple char from the beginning/end of a string using
        strip()

        for example, using rstrip("}") on "{a: {b:c}}" will return
        "{a: {b:c", removing all } on the left side instead of one.

        :param char: the character in the string to remove
        :param query_str: the query string to evaluate
        :return: a new string with the two end chars removed
        """
        if query_str[0] == first_char and query_str[-1] == last_char:
            query_str = query_str[1:len(query_str) - 1]
        return query_str

    def merge_nested_values(self, key, value):
        """
        If a value is nested like [a: [>=, 5]]
        flattened to [a, >=, 5].

        :param key: the key value in the nested value, usually the one being compared
        :param value: the nested value in the params, usually containing the comparison
        and value to compare it to
        :return: the flattened, merged value representing the condition
        """
        if len(value) == 1:
            # happens when there is an operation like [a, [<= ,2]]
            # results in the format [a, <=, 2]
            merged_val = [[key] + [i for i in value[0]]]
        else:
            merged_vals = []
            if key == "AND" or key == "OR":
                merged_vals = [[key] + [i for i in value]]
            else:
                for val in value:
                    if val[1][0] == "'":
                        merged_vals.append([key + "." + val[0], val[1]])
                    else:
                        merged_vals.append([key, val[0], val[1]])
            merged_val = merged_vals
        return merged_val

    def parse_params(self, str_params):
        """
        parse_params recursively parses the string of query
        parameters from inside the mongodb query .find(...)

        using parse params for select may seem like a bit of an overkill,
        and I suggested separating it out, but it ends up with a lot of repeated code as well.

        :param str_params: string of params to be parsed and formatted for the sql query
        :return: a list of parameters that can be iteratively formatted into a sql query clause
        """
        parsed_values = []
        # remove outer {} or [], then split on comma
        str_params = self.remove_ends("{", "}", str_params)
        str_params = self.remove_ends("[", "]", str_params)

        # in this case, it is useful to use regex because we want to split string
        # on comma separated values that aren't in {} (another layer that we will parse
        # recursively, or in [] where the $in filter is used.
        re_comma = ",\s*(?![^{}]*\}|[^\[\]]*\])"
        param_values = re.split(re_comma, str_params)

        for param in param_values:
            # format the string into key, value
            param = self.remove_ends("{", "}", param)
            split_param = param.split(":", 1)

            key = split_param[0].strip()
            value = split_param[1].strip()

            # check if the value contains nested query params
            if value[0] == '{':
                value = self.parse_params(value)

            # check if the mongodb operation should be translated
            if key[0] == "$":
                key = self.operation_conversions.get(key)
                # handle the and / or operations with an array of values
                if key == 'OR' or key == 'AND':
                    value = self.parse_params(value)

            # handle nested values that have been filtered -
            # the goal is to flatten the array as much as possible, condensing the nested queries
            # and making the values easy to iterate through when formatting the sql query
            if isinstance(value[0], list):
                parsed_values = self.merge_nested_values(key, value)
            else:
                parsed_values.append([key, value])

        return parsed_values

    def build_select_clause(self, select_params):
        """
        build_select_clause iterates through the
        select parameters to return the list of
        selected columns for the sql query

        :param select_params: list of select parameters
        :return: sql select clause string
        """
        select_clause = ""

        while len(select_params) > 1:
            select_clause += select_params.pop(0)[0] + ", "

        select_clause += select_params.pop(0)[0] + " "

        return select_clause

    def build_where_clause(self, where_params, operator="AND"):
        """
        build_where_clause uses the where parameters to iteratively
        build the sql where clause using the operations and/or

        :param where_params: a list of parameters parsed from the mongoDB
        query
        :param operator: either and/or, used to join each where condition
        :return: the where clause for the sql query
        """
        where_clause = ""
        last_where_idx = len(where_params) - 1

        for idx, param in enumerate(where_params):
            # the and/or case is still nested, so
            # recursively call each and/or condition
            if param[0] == "AND" or param[0] == "OR":
                op = param.pop(0)
                where_clause += self.build_where_clause(param, op)

            else:
                if len(param) == 2:
                    # check for true/false and format to all caps --
                    # the only time true/false will show up is in the where clause,
                    # so we can just check for it here
                    if param[1].lower() == "true" or param[1].lower() == "false":
                        param[1] = param[1].upper()

                    param = [param[0], "=", param[1]]

                where_clause += " ".join(p for p in param)
                if idx != last_where_idx:
                    where_clause += " " + operator + " "

        return where_clause

    def build_sql(self, table_name, selects=None, where_params=None):
        """
        build_sql usese the parsed query params to build each clause
        of the sql query and return the formatted string

        :param table_name: name of the table in the FROM clause
        :param selects: the list of selects in the SELECT clause
        :param where_params: the list of conditions in the WHERE clause
        :return: a string representing the sql query
        """
        select_clause = "SELECT " + self.build_select_clause(selects)
        where = self.build_where_clause(where_params)
        where_clause = " WHERE " + where if where else ""
        return select_clause + "FROM " + table_name + where_clause + ';'

    def run(self):
        table_name, selects, where_clauses = self.translate()
        return self.build_sql(table_name, selects, where_clauses)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--mongodb_query", default=None, type=str, help="The mongoDB query to translate to SQL")
    args = parser.parse_args()
    print(QueryTranslator(mongo_query=args.mongodb_query).run())
