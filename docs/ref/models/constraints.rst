==========================
Model constraint reference
==========================

.. module:: django_mongodb_backend.constraints
   :synopsis: Database constraints for MongoDB.

Some MongoDB-specific :doc:`constraints <django:ref/models/constraints>`, for
use on a model's :attr:`Meta.constraints<django.db.models.Options.constraints>`
option, are available in ``django_mongodb_backend.constraints``.

Embedded model constraints
==========================

``EmbeddedModelConstraint``
---------------------------

.. class:: EmbeddedModelUniqueConstraint(**kwargs)

    .. versionadded:: 6.0.2

    Subclass of :class:`~django.db.models.UniqueConstraint` for use on a model
    with an embedded model in order to add a unique constraint on embedded
    fields.
