import logging

from alembic.autogenerate import comparators
from alembic.autogenerate.compare import _compare_columns
from alembic.operations.ops import ModifyTableOps
from alembic.util.sqla_compat import _reflect_table
from sqlalchemy import schema as sa_schema
from sqlalchemy import text

from clickhouse_sqlalchemy.sql.schema import Table
from . import operations

logger = logging.getLogger(__name__)


@comparators.dispatch_for('schema')
def compare_mat_view(autogen_context, upgrade_ops, schemas):
    connection = autogen_context.connection
    dialect = autogen_context.dialect
    metadata = autogen_context.metadata

    all_mat_views = set(dialect.get_view_names(connection))

    metadata_mat_views = metadata.info.setdefault('mat_views', set())

    statement_compiler = dialect.statement_compiler(dialect, None)
    ddl_compiler = dialect.ddl_compiler(dialect, None)

    for name in metadata_mat_views.difference(all_mat_views):
        view = metadata.mat_views[name]
        inner_table = view.inner_table

        selectable = statement_compiler.process(
            view.mv_selectable, literal_binds=True
        )
        engine = ddl_compiler.process(inner_table.engine)

        logger.info('Detected added materialized view %s', name)
        upgrade_ops.ops.append(
            operations.CreateMatViewOp(
                view.name, selectable, engine, *inner_table.columns
            )
        )

    existing_metadata = sa_schema.MetaData()
    inspector = autogen_context.inspector

    removed_mat_views = all_mat_views.difference(metadata_mat_views)
    mat_view_params_by_name = {}
    if removed_mat_views:
        rv = dialect._execute(
            connection,
            text(
                'SELECT name, as_select, engine_full '
                'FROM system.tables '
                'WHERE database = currentDatabase() AND name IN :names'
            ), names=list(removed_mat_views)
        )
        mat_view_params_by_name = {
            name: (selectable, engine) for name, selectable, engine in rv
        }

    for name in removed_mat_views:
        logger.info('Detected removed materialized view %s', name)
        selectable, engine = mat_view_params_by_name[name]
        table = Table(name, existing_metadata)
        _reflect_table(inspector, table, None)
        upgrade_ops.ops.append(
            operations.DropMatViewOp(name, selectable, engine, *table.columns)
        )

    existing_mat_views = all_mat_views.intersection(metadata_mat_views)

    for name in existing_mat_views:
        mv_uuid = dialect._execute(
            connection,
            text(
                'SELECT uuid '
                'FROM system.tables '
                'WHERE database = currentDatabase() AND name = :name'
            ), name=name, scalar=True
        )
        mv_uuid = str(mv_uuid)
        if mv_uuid == '00000000-0000-0000-0000-000000000000':
            conn_name = '.inner.' + name
        else:
            conn_name = '.inner_id.' + mv_uuid

        conn_table = Table(conn_name, existing_metadata)
        _reflect_table(inspector, conn_table, None)

        metadata_table = metadata.mat_views[name].inner_table
        modify_table_ops = ModifyTableOps(name, [])
        schema = None
        with _compare_columns(
                schema,
                conn_name,
                conn_table,
                metadata_table,
                modify_table_ops,
                autogen_context,
                inspector,
        ):
            comparators.dispatch('table')(
                autogen_context,
                modify_table_ops,
                schema,
                conn_name,
                conn_table,
                metadata_table,
            )

            if not modify_table_ops.is_empty():
                view = metadata.mat_views[name]
                inner_table = view.inner_table

                selectable = statement_compiler.process(
                    view.mv_selectable, literal_binds=True
                )
                engine = ddl_compiler.process(inner_table.engine)

                upgrade_ops.ops.extend([
                    operations.DetachMatViewOp(
                        name, selectable, engine, *inner_table.columns
                    ),
                    modify_table_ops,
                    operations.AttachMatViewOp(
                        name, selectable, engine, *inner_table.columns
                    )
                ])
