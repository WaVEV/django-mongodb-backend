from django.db.models import Index, UniqueConstraint

from .indexes import EmbeddedModelIndex, EmbeddedModelndexMixin


@staticmethod
def _get_index_for_add_constraint(*args, **kwargs):
    return Index(*args, **kwargs)


class EmbeddedModelUniqueConstraint(EmbeddedModelndexMixin, UniqueConstraint):
    option = "constraints"

    @staticmethod
    def _get_index_for_add_constraint(*args, **kwargs):
        return EmbeddedModelIndex(*args, **kwargs)


def register_constraints():
    UniqueConstraint._get_index_for_add_constraint = _get_index_for_add_constraint
