from django.core.exceptions import FieldDoesNotExist
from django.db import connection
from django.test import TestCase

from django_mongodb_backend.constraints import EmbeddedModelUniqueConstraint

from .models import EmbeddedSchemaTestModel, Movie


class EmbeddedModelUniqueConstraintTests(TestCase):
    def test_embedded_constraint_is_created(self):
        constraint = EmbeddedModelUniqueConstraint(
            name="embedded_created_constraint",
            fields=["embedded_model.integer"],
        )
        with connection.schema_editor() as editor:
            editor.add_constraint(constraint=constraint, model=EmbeddedSchemaTestModel)
        try:
            constraint_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=EmbeddedSchemaTestModel._meta.db_table,
            )
            self.assertIn(constraint.name, constraint_info)
            self.assertCountEqual(
                constraint_info[constraint.name]["columns"],
                constraint.fields,
            )
            self.assertEqual(
                constraint_info[constraint.name]["type"],
                "idx",
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_constraint(constraint=constraint, model=EmbeddedSchemaTestModel)

    def test_multiple_fields_embedded(self):
        constraint = EmbeddedModelUniqueConstraint(
            name="embedded_multi_constraint",
            fields=["integer", "embedded_model.integer"],
        )
        with connection.schema_editor() as editor:
            editor.add_constraint(constraint=constraint, model=EmbeddedSchemaTestModel)
        try:
            constraint_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=EmbeddedSchemaTestModel._meta.db_table,
            )
            self.assertIn(constraint.name, constraint_info)
            self.assertCountEqual(
                constraint_info[constraint.name]["columns"],
                constraint.fields,
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_constraint(constraint=constraint, model=EmbeddedSchemaTestModel)

    def test_multiple_fields_embedded_array(self):
        constraint = EmbeddedModelUniqueConstraint(
            name="embedded_multi_idx",
            fields=["reviews.rating"],
        )
        with connection.schema_editor() as editor:
            editor.add_constraint(constraint=constraint, model=Movie)
        try:
            constraint_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=Movie._meta.db_table,
            )
            self.assertIn(constraint.name, constraint_info)
            self.assertCountEqual(
                constraint_info[constraint.name]["columns"],
                constraint.fields,
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_constraint(constraint=constraint, model=Movie)

    def test_invalid_path(self):
        constraint = EmbeddedModelUniqueConstraint(
            name="embedded_multi_idx",
            fields=["title.title"],
        )
        msg = "Movie has no field named 'title.title"
        with connection.schema_editor() as editor, self.assertRaisesMessage(FieldDoesNotExist, msg):
            editor.add_constraint(constraint=constraint, model=Movie)
