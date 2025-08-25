from django.core.exceptions import FieldDoesNotExist
from django.db import connection
from django.test import TestCase

from django_mongodb_backend.indexes import EmbeddedModelIndex

from .models import EmbeddedSchemaTestModel, Movie
from .test_base import SchemaAssertionMixin


class EmbeddedModelIndexNameTests(TestCase):
    class LongDBTableModel:
        class _meta:
            db_table = "a_really_very_very_long_model_db_table_name"

    def test_name_is_generated(self):
        index = EmbeddedModelIndex(fields=["some_really_long_field_name"])
        index.set_name_with_model(self.LongDBTableModel)
        self.assertTrue(index.name.endswith("idx"))
        self.assertLessEqual(len(index.name), 30)
        self.assertEqual(index.name[:11], "a_really_ve")
        self.assertEqual(index.name[12 : 12 + 7], "some_re")

    def test_name_is_deterministic(self):
        index1 = EmbeddedModelIndex(fields=["field"])
        index2 = EmbeddedModelIndex(fields=["field"])
        index1.set_name_with_model(self.LongDBTableModel)
        index2.set_name_with_model(self.LongDBTableModel)
        self.assertEqual(index1.name, index2.name)

    def test_name_does_not_start_with_digit_or_underscore(self):
        class BadModel:
            class _meta:
                db_table = "_1bad_table"

        index = EmbeddedModelIndex(fields=["field"])
        index.set_name_with_model(BadModel)
        self.assertTrue(index.name[0].isalpha())
        self.assertEqual(index.name[0], "D")


class EmbeddedModelIndexSchemaTests(SchemaAssertionMixin, TestCase):
    def test_embedded_index_is_created(self):
        index = EmbeddedModelIndex(
            name="embedded_created_idx",
            fields=["embedded_model.integer"],
        )
        with connection.schema_editor() as editor:
            editor.add_index(index=index, model=EmbeddedSchemaTestModel)
        try:
            index_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=EmbeddedSchemaTestModel._meta.db_table,
            )
            self.assertIn(index.name, index_info)
            self.assertCountEqual(
                index_info[index.name]["columns"],
                index.fields,
            )
            self.assertEqual(
                index_info[index.name]["type"],
                "idx",
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_index(index=index, model=EmbeddedSchemaTestModel)

    def test_multiple_fields_embedded(self):
        index = EmbeddedModelIndex(
            name="embedded_multi_idx",
            fields=["integer", "embedded_model.integer", "embedded_model.string"],
        )
        with connection.schema_editor() as editor:
            editor.add_index(index=index, model=EmbeddedSchemaTestModel)
        try:
            index_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=EmbeddedSchemaTestModel._meta.db_table,
            )
            self.assertIn(index.name, index_info)
            self.assertCountEqual(
                index_info[index.name]["columns"],
                [*index.fields[:-1], "embedded_model.string_"],
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_index(index=index, model=EmbeddedSchemaTestModel)

    def test_multiple_fields_embedded_array(self):
        index = EmbeddedModelIndex(
            name="embedded_multi_idx",
            fields=["reviews.rating"],
        )
        with connection.schema_editor() as editor:
            editor.add_index(index=index, model=Movie)
        try:
            index_info = connection.introspection.get_constraints(
                cursor=None,
                table_name=Movie._meta.db_table,
            )
            self.assertIn(index.name, index_info)
            self.assertCountEqual(
                index_info[index.name]["columns"],
                index.fields,
            )
        finally:
            with connection.schema_editor() as editor:
                editor.remove_index(index=index, model=Movie)

    def test_invalid_path(self):
        index = EmbeddedModelIndex(
            name="embedded_multi_idx",
            fields=["title.title"],
        )
        msg = "Movie has no field named 'title.title"
        with connection.schema_editor() as editor, self.assertRaisesMessage(FieldDoesNotExist, msg):
            editor.add_index(index=index, model=Movie)
