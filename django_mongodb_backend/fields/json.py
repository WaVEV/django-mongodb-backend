from functools import partialmethod
from itertools import chain

from django.db import NotSupportedError
from django.db.models.fields.json import (
    ContainedBy,
    DataContains,
    HasAnyKeys,
    HasKey,
    HasKeyLookup,
    HasKeys,
    JSONExact,
    KeyTransform,
    KeyTransformExact,
    KeyTransformIn,
    KeyTransformIsNull,
    KeyTransformNumericLookupMixin,
)

from django_mongodb_backend.lookups import builtin_lookup_expr, builtin_lookup_path
from django_mongodb_backend.query_utils import process_lhs, process_rhs, valid_path_key_name


def build_json_mql_path(lhs, key_transforms, as_path=False):
    # Build the MQL path using the collected key transforms.
    if as_path:
        return ".".join(chain([lhs], key_transforms))
    result = lhs
    for key in key_transforms:
        get_field = {"$getField": {"input": result, "field": key}}
        # Handle array indexing if the key is a digit. If key is something
        # like '001', it's not an array index despite isdigit() returning True.
        if key.isdigit() and str(int(key)) == key:
            result = {
                "$cond": {
                    "if": {"$isArray": result},
                    "then": {"$arrayElemAt": [result, int(key)]},
                    "else": get_field,
                }
            }
        else:
            result = get_field
    return result


def contained_by(self, compiler, connection, as_path=False):  # noqa: ARG001
    raise NotSupportedError("contained_by lookup is not supported on this database backend.")


def data_contains(self, compiler, connection, as_path=False):  # noqa: ARG001
    raise NotSupportedError("contains lookup is not supported on this database backend.")


def _has_key_predicate(path, root_column=None, negated=False, as_path=False):
    """Return MQL to check for the existence of `path`."""
    if as_path:
        return {path: {"$exists": not negated}}
    result = {
        "$and": [
            # The path must exist (i.e. not be "missing").
            {"$ne": [{"$type": path}, "missing"]},
            # If the JSONField value is None, an additional check for not null
            # is needed since $type returns null instead of "missing".
            {"$ne": [root_column, None]},
        ]
    }
    if negated:
        result = {"$not": result}
    return result


@property
def has_key_check_simple_expression(self):
    rhs = [self.rhs] if not isinstance(self.rhs, (list, tuple)) else self.rhs
    return self.is_simple_column and all(valid_path_key_name(key) for key in rhs)


def has_key_lookup(self, compiler, connection, as_path=False):
    """Return MQL to check for the existence of a key."""
    rhs = self.rhs
    lhs = process_lhs(self, compiler, connection, as_path=as_path)
    if not isinstance(rhs, (list, tuple)):
        rhs = [rhs]
    paths = []
    # Transform any "raw" keys into KeyTransforms to allow consistent handling
    # in the code that follows.
    for key in rhs:
        rhs_json_path = key if isinstance(key, KeyTransform) else KeyTransform(key, self.lhs)
        paths.append(rhs_json_path.as_mql(compiler, connection, as_path=as_path))
    keys = []
    for path in paths:
        keys.append(_has_key_predicate(path, lhs, as_path=as_path))
    if self.mongo_operator is None:
        return keys[0]
    return {self.mongo_operator: keys}


_process_rhs = JSONExact.process_rhs


def json_exact_process_rhs(self, compiler, connection):
    """Skip JSONExact.process_rhs()'s conversion of None to "null"."""
    return (
        super(JSONExact, self).process_rhs(compiler, connection)
        if connection.vendor == "mongodb"
        else _process_rhs(self, compiler, connection)
    )


def key_transform(self, compiler, connection, as_path=False):
    """
    Return MQL for this KeyTransform (JSON path).

    JSON paths cannot always be represented simply as $var.key1.key2.key3 due
    to possible array types. Therefore, indexing arrays requires the use of
    `arrayElemAt`. Additionally, $cond is necessary to verify the type before
    performing the operation.
    """
    key_transforms = [self.key_name]
    previous = self.lhs
    # Collect all key transforms in order.
    while isinstance(previous, KeyTransform):
        key_transforms.insert(0, previous.key_name)
        previous = previous.lhs
    lhs_mql = previous.as_mql(compiler, connection, as_path=as_path)
    return build_json_mql_path(lhs_mql, key_transforms, as_path=as_path)


def key_transform_exact_path(self, compiler, connection):
    lhs_mql = process_lhs(self, compiler, connection, as_path=True)
    return {
        "$and": [
            builtin_lookup_path(self, compiler, connection),
            _has_key_predicate(lhs_mql, None, as_path=True),
        ]
    }


def key_transform_in_expr(self, compiler, connection):
    """
    Return MQL to check if a JSON path exists and that its values are in the
    set of specified values (rhs).
    """
    lhs_mql = process_lhs(self, compiler, connection)
    # Traverse to the root column.
    previous = self.lhs
    while isinstance(previous, KeyTransform):
        previous = previous.lhs
    root_column = previous.as_mql(compiler, connection)
    value = process_rhs(self, compiler, connection)
    # Construct the expression to check if lhs_mql values are in rhs values.
    expr = connection.mongo_expr_operators[self.lookup_name](lhs_mql, value)
    return {"$and": [_has_key_predicate(lhs_mql, root_column), expr]}


def key_transform_is_null_expr(self, compiler, connection):
    """
    Return MQL to check the nullability of a key.

    If `isnull=True`, the query matches objects where the key is missing or the
    root column is null. If `isnull=False`, the query negates the result to
    match objects where the key exists.

    Reference: https://code.djangoproject.com/ticket/32252
    """
    lhs_mql = process_lhs(self, compiler, connection)
    rhs_mql = process_rhs(self, compiler, connection)
    # Get the root column.
    previous = self.lhs
    while isinstance(previous, KeyTransform):
        previous = previous.lhs
    root_column = previous.as_mql(compiler, connection)
    return _has_key_predicate(lhs_mql, root_column, negated=rhs_mql)


def key_transform_is_null_path(self, compiler, connection):
    """
    Return MQL to check the nullability of a key.

    If `isnull=True`, the query matches objects where the key is missing or the
    root column is null. If `isnull=False`, the query negates the result to
    match objects where the key exists.

    Reference: https://code.djangoproject.com/ticket/32252
    """
    lhs_mql = process_lhs(self, compiler, connection, as_path=True)
    rhs_mql = process_rhs(self, compiler, connection, as_path=True)
    return _has_key_predicate(lhs_mql, None, negated=rhs_mql, as_path=True)


def key_transform_numeric_lookup_mixin_expr(self, compiler, connection):
    """
    Return MQL to check if the field exists (i.e., is not "missing" or "null")
    and that the field matches the given numeric lookup expression.
    """
    expr = builtin_lookup_expr(self, compiler, connection)
    lhs = process_lhs(self, compiler, connection)
    # Check if the type of lhs is not "missing" or "null".
    not_missing_or_null = {"$not": {"$in": [{"$type": lhs}, ["missing", "null"]]}}
    return {"$and": [expr, not_missing_or_null]}


@property
def keytransform_is_simple_column(self):
    previous = self
    while isinstance(previous, KeyTransform):
        if not valid_path_key_name(previous.key_name):
            return False
        previous = previous.lhs
    return previous.is_simple_column


def register_json_field():
    ContainedBy.as_mql = contained_by
    DataContains.as_mql = data_contains
    HasAnyKeys.mongo_operator = "$or"
    HasKey.mongo_operator = None
    HasKeyLookup.as_mql_expr = partialmethod(has_key_lookup, as_path=False)
    HasKeyLookup.as_mql_path = partialmethod(has_key_lookup, as_path=True)
    HasKeyLookup.can_use_path = has_key_check_simple_expression
    HasKeys.mongo_operator = "$and"
    JSONExact.process_rhs = json_exact_process_rhs
    KeyTransform.as_mql_expr = partialmethod(key_transform, as_path=False)
    KeyTransform.as_mql_path = partialmethod(key_transform, as_path=True)
    KeyTransform.can_use_path = keytransform_is_simple_column
    KeyTransform.is_simple_column = keytransform_is_simple_column
    KeyTransformExact.as_mql_path = key_transform_exact_path
    KeyTransformIn.as_mql_expr = key_transform_in_expr
    KeyTransformIsNull.as_mql_expr = key_transform_is_null_expr
    KeyTransformIsNull.as_mql_path = key_transform_is_null_path
    KeyTransformNumericLookupMixin.as_mql_expr = key_transform_numeric_lookup_mixin_expr
