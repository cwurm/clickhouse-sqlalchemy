from alembic.operations import Operations, MigrateOperation
from alembic.operations.ops import ExecuteSQLOp


@Operations.register_operation('create_mat_view')
class CreateMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def create_mat_view(cls, operations, name, selectable, engine, *columns,
                        **kwargs):
        """Issue a "CREATE MATERIALIZED VIEW" instruction."""

        op = CreateMatViewOp(name, selectable, engine, *columns, **kwargs)
        return operations.invoke(op)

    def reverse(self):
        return DropMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('drop_mat_view')
class DropMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def drop_mat_view(cls, operations, name, **kwargs):
        """Issue a "DROP TABLE" instruction."""

        sql = 'DROP TABLE '
        if kwargs.get('if_exists'):
            sql += 'IF EXISTS '

        sql += name

        if kwargs.get('on_cluster'):
            sql += ' ON CLUSTER ' + kwargs['on_cluster']

        op = ExecuteSQLOp(sql)
        return operations.invoke(op)

    def reverse(self):
        return CreateMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('attach_mat_view')
class AttachMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def attach_mat_view(cls, operations, name, selectable, engine, *columns,
                        **kwargs):
        """Issue a "ATTACH MATERIALIZED VIEW" instruction."""

        op = AttachMatViewOp(name, selectable, engine, *columns, **kwargs)
        return operations.invoke(op)

    def reverse(self):
        return DetachMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )


@Operations.register_operation('detach_mat_view')
class DetachMatViewOp(MigrateOperation):
    def __init__(self, name, selectable, engine, *columns, **kwargs):
        self.name = name
        self.selectable = selectable
        self.engine = engine
        self.columns = columns
        self.kwargs = kwargs

    @classmethod
    def detach_mat_view(cls, operations, name, **kwargs):
        """Issue a "DETACH VIEW" instruction."""

        sql = 'DETACH VIEW '

        if kwargs.get('if_exists'):
            sql += 'IF EXISTS '

        sql += name

        if kwargs.get('on_cluster'):
            sql += ' ON CLUSTER ' + kwargs['on_cluster']

        if kwargs.get('permanently'):
            sql += ' PERMANENTLY'

        op = ExecuteSQLOp(sql)
        return operations.invoke(op)

    def reverse(self):
        return AttachMatViewOp(
            self.name, self.selectable, self.engine, *self.columns,
            **self.kwargs
        )
