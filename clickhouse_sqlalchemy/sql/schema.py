from sqlalchemy import Table as TableBase
from sqlalchemy.sql.base import (
    _bind_or_error, DialectKWArgs, Immutable
)
from sqlalchemy.sql.schema import SchemaItem
from sqlalchemy.sql.selectable import FromClause
from clickhouse_sqlalchemy.sql.selectable import (
    Join, Select
)
from . import ddl


class Table(TableBase):
    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper,
                          self,
                          checkfirst=checkfirst, if_exists=if_exists)

    def join(self, right, onclause=None, isouter=False, full=False,
             type=None, strictness=None, distribution=None):
        return Join(self, right,
                    onclause=onclause, type=type,
                    isouter=isouter, full=full,
                    strictness=strictness, distribution=distribution)

    def select(self, whereclause=None, **params):
        return Select([self], whereclause, **params)

    @classmethod
    def _make_from_standard(cls, std_table, _extend_on=None):
        ch_table = cls(std_table.name, std_table.metadata)
        ch_table.schema = std_table.schema
        ch_table.fullname = std_table.fullname
        ch_table.implicit_returning = std_table.implicit_returning
        ch_table.comment = std_table.comment
        ch_table.info = std_table.info
        ch_table._prefixes = std_table._prefixes
        ch_table.dialect_options = std_table.dialect_options

        if _extend_on is None:
            ch_table._columns = std_table._columns

        return ch_table


class MaterializedView(DialectKWArgs, SchemaItem, Immutable, FromClause):
    __visit_name__ = 'materialized_view'

    def __init__(self, inner_model, selectable):
        self.mv_selectable = selectable
        self.inner_table = inner_model.__table__

        super(MaterializedView, self).__init__()

        table = inner_model.__table__
        metadata = self.inner_table.metadata

        metadata.info.setdefault('mat_views', set()).add(table.name)
        if not hasattr(metadata, 'mat_views'):
            metadata.mat_views = {}
        metadata.mat_views[table.name] = self

    @property
    def bind(self):
        return self.inner_table.metadata.bind

    @property
    def name(self):
        return self.inner_table.name

    # TODO: args
    def __new__(cls, inner_model, selectable):
        table = inner_model.__table__

        rv = object.__new__(cls)
        rv.__init__(inner_model, selectable)

        # TODO: use include_object
        inner_model.metadata._remove_table(table.name, table.schema)
        return rv

    def create(self, bind=None, checkfirst=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaGenerator, self, checkfirst=checkfirst)

    def drop(self, bind=None, checkfirst=False, if_exists=False):
        if bind is None:
            bind = _bind_or_error(self)
        bind._run_visitor(ddl.SchemaDropper, self,
                          checkfirst=checkfirst, if_exists=if_exists)
