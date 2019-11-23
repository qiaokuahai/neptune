# coding=utf-8
"""
提供mysql封装
"""

from __future__ import absolute_import
import logging
import collections
import contextlib
import copy
from sqlalchemy import text, and_, or_
import sqlalchemy.exc
from neptune.db import pool
from neptune.core import utils
from neptune.core import exceptions
from neptune.db import filter_wrapper
from neptune.core.i18n import _
LOG = logging.getLogger(__name__)


class ResourceBase(object):
    """
    资源基础操作子类
    继承本类需注意：
    1、覆盖orm_meta、_primary_keys、_default_filter、_default_order、_validate属性
    2、delete默认设置removed(datetime类型)列，若不存在则直接删除，若行为不符合请覆盖delete方法
    """
    # 使用的ORM Model
    orm_meta = None
    # 表对应的主键列，单个主键时，使用字符串，多个联合主键时为字符串列表
    _primary_keys = 'id'
    # 默认过滤查询，应用于每次查询本类资源，此处应当是静态数据，不可被更改
    _default_filter = {}
    # 默认排序，获取默认查询资源是被应用，('name', '+id', '-status'), +表示递增，-表示递减，默认递增
    _default_order = []
    # 数据验证ColumnValidator数组
    # field 字符串，字段名称
    # rule 不同验证不同类型，validator验证参数
    # rule_type 字符串，validator验证类型，支持[regex,email,phone,url,length,integer,float,in,notin, callback]
    #           默认regex（不指定rule也不会生效）,callback回调函数为func(value)
    # validate_on，数组，元素为字符串，['create:M', 'update:M', 'create_or_update:O']，第一个为场景，第二个为是否必须
    # error_msg，字符串，错误提示消息
    # converter，对象实例，converter中的类型，可以自定义
    _validate = []

    def __init__(self, session=None, transaction=None, dbpool=None):
        self._pool = dbpool or pool.POOL
        self._session = session
        self._transaction = transaction

    def _filter_hander_mapping(self):
        handlers = {
            'inet': filter_wrapper.FilterNetwork(),
            'cidr': filter_wrapper.FilterNetwork(),
            'small_integer': filter_wrapper.FilterNumber(),
            'integer': filter_wrapper.FilterNumber(),
            'big_integer': filter_wrapper.FilterNumber(),
            'numeric': filter_wrapper.FilterNumber(),
            'float': filter_wrapper.FilterNumber(),
            'date': filter_wrapper.FilterDateTime(),
            'datetime': filter_wrapper.FilterDateTime(),
            'boolean': filter_wrapper.FilterBool(),
            'jsonb': filter_wrapper.FilterJSON(),
        }
        return handlers

    def _filter_key_mapping(self):
        keys = {
            '$or': or_,
            '$and': and_,
        }
        return keys

    def _get_filter_handler(self, name):
        handlers = self._filter_hander_mapping()
        return handlers.get(name.lower(), filter_wrapper.Filter())

    def _apply_filters(self, query, orm_meta, filters=None, orders=None):

        def _extract_column_visit_name(column):
            '''
            获取列名称
            :param column: 列对象
            :type column: `ColumnAttribute`
            '''
            col_type = getattr(column, 'type', None)
            if col_type:
                return getattr(col_type, '__visit_name__', None)
            return None

        def _handle_filter(expr_wrapper, handler, op, column, value):
            '''
            将具体列+操作+值转化为SQL表达式（SQLAlchmey表达式）
            :param expr_wrapper: 表达式的外包装器，比如一对一的外键列expr_wrapper为relationship.column.has
            :type expr_wrapper: callable
            :param handler: filter wrapper对应的Filter对象
            :type handler: `talos.db.filter_wrapper.Filter`
            :param op: 过滤条件，如None, eq, ne, gt, gte, lt, lte 等
            :type op: str
            :param column: 列对象
            :type column: `ColumnAttribute`
            :param value: 过滤值
            :type value: any
            '''
            expr = None
            func = None
            if op:
                func = getattr(handler, 'op_%s' % op, None)
            else:
                func = getattr(handler, 'op', None)
            if func:
                expr = func(column, value)
                if expr is not None:
                    if expr_wrapper:
                        expr = expr_wrapper(expr)
            return expr

        def _get_expression(filters):
            '''
            将所有filters转换为表达式
            :param filters: 过滤条件字典
            :type filters: dict
            '''
            expressions = []
            unsupported = []
            for name, value in filters.items():
                if name in reserved_keys:
                    _unsupported, expr = _get_key_expression(name, value)
                else:
                    _unsupported, expr = _get_column_expression(name, value)
                unsupported.extend(_unsupported)
                if expr is not None:
                    expressions.append(expr)
            return unsupported, expressions

        def _get_key_expression(name, value):
            '''
            将$and, $or类的组合过滤转换为表达式
            :param name:
            :type name:
            :param value:
            :type value:
            '''
            key_wrapper = reserved_keys[name]
            unsupported = []
            expressions = []
            for key_filters in value:
                _unsupported, expr = _get_expression(key_filters)
                unsupported.extend(_unsupported)
                if expr:
                    expr = and_(*expr)
                    expressions.append(expr)
            if len(expressions) == 0:
                return unsupported, None
            if len(expressions) == 1:
                return unsupported, expressions[0]
            else:
                return unsupported, key_wrapper(*expressions)

        def _get_column_expression(name, value):
            """
            将列+值过滤转换为表达式
            :param name:
            :param value:
            :return:
            """
            expr_wrapper, column = filter_wrapper.column_from_expression(orm_meta, name)
            unsupported = []
            expressions = []
            if column is not None:
                handler = self._get_filter_handler(_extract_column_visit_name(column))
                if isinstance(value, collections.Mapping):
                    for operator, value in value.items():
                        expr = _handle_filter(expr_wrapper, handler, operator, column, value)
                        if expr is not None:
                            expressions.append(expr)
                        else:
                            unsupported.append((name, operator, value))
                else:
                    # op is None
                    expr = _handle_filter(expr_wrapper, handler, None, column, value)
                    if expr is not None:
                        expressions.append(expr)
                    else:
                        unsupported.append((name, None, value))
            if column is None:
                unsupported.insert(0, (name, None, value))
            if len(expressions) == 0:
                return unsupported, None
            if len(expressions) == 1:
                return unsupported, expressions[0]
            else:
                return unsupported, and_(*expressions)

        reserved_keys = self._filter_key_mapping()
        filters = filters or {}
        if filters:
            unsupported, expressions = _get_expression(filters)
            for expr in expressions:
                query = query.filter(expr)
        orders = orders or []
        if orders:
            for field in orders:
                order = '+'
                if field.startswith('+'):
                    order = '+'
                    field = field[1:]
                elif field.startswith('-'):
                    order = '-'
                    field = field[1:]
                expr_wrapper, column = filter_wrapper.column_from_expression(orm_meta, field)
                # 不支持relationship排序
                if column is not None and expr_wrapper is None:
                    if order == '+':
                        query = query.order_by(column)
                    else:
                        query = query.order_by(column.desc())
        return query

    def _get_query(self, session, orm_meta=None, filters=None, orders=None, joins=None, ignore_default=False):
        """获取一个query对象，这个对象已经应用了filter，可以确保查询的数据只包含我们感兴趣的数据，常用于过滤已被删除的数据

        :param session: session对象
        :type session: session
        :param orm_meta: ORM Model, 如果None, 则默认使用self.orm_meta
        :type orm_meta: ORM Model
        :param filters: 简单的等于过滤条件, eg.{'column1': value, 'column2':
        value}，如果None，则默认使用default filter
        :type filters: dict
        :param orders: 排序['+field', '-field', 'field']，+表示递增，-表示递减，不设置默认递增
        :type orders: list
        :param joins: 指定动态join,eg.[{'table': model, 'conditions': [model_a.col_1 == model_b.col_1]}]
        :type joins: list
        :returns: query对象
        :rtype: query
        :raises: ValueError
        """
        orm_meta = orm_meta or self.orm_meta
        filters = filters or {}
        # orders优先使用用户传递排序
        if not ignore_default:
            orders = self.default_order if orders is None else orders
        else:
            orders = orders or []
        orders = copy.copy(orders)
        joins = joins or []
        ex_tables = [item['table'] for item in joins]
        tables = list(ex_tables)
        tables.insert(0, orm_meta)
        if orm_meta is None:
            raise exceptions.CriticalError(msg=utils.format_kwstring(
                _('%(name)s.orm_meta can not be None'), name=self.__class__.__name__))
        query = session.query(*tables)
        if len(ex_tables) > 0:
            for item in joins:
                spec_args = [item['table']]
                if len(item['conditions']) > 1:
                    spec_args.append(and_(*item['conditions']))
                else:
                    spec_args.extend(item['conditions'])
                query = query.join(*spec_args,
                                   isouter=item.get('isouter', True))
        query = self._apply_filters(query, orm_meta, filters, orders)
        # 如果不是忽略default模式，default_filter必须进行过滤
        if not ignore_default:
            query = self._apply_filters(query, orm_meta, self.default_filter)
        return query

    @property
    def default_filter(self):
        """
        获取默认过滤条件，只读

        :returns: 默认过滤条件
        :rtype: dict
        """
        return copy.deepcopy(self._default_filter)

    @property
    def default_order(self):
        """
        获取默认排序规则，只读

        :returns: 默认排序规则
        :rtype: list
        """
        return copy.copy(self._default_order)

    @property
    def primary_keys(self):
        """
        获取默认主键列，只读

        :returns: 默认主键列
        :rtype: list
        """
        return copy.copy(self._primary_keys)

    @contextlib.contextmanager
    def transaction(self):
        """
        事务管理上下文, 如果资源初始化时指定使用外部事务，则返回的也是外部事务对象，

        保证事务统一性

        eg.

        with self.transaction() as session:

            self.create()

            self.update()

            self.delete()

            OtherResource(transaction=session).create()
        """
        session = None
        if self._transaction is None:
            try:
                old_transaction = self._transaction
                session = self._pool.transaction()
                self._transaction = session
                yield session
                session.commit()
            except Exception as e:
                LOG.exception(e)
                if session:
                    session.rollback()
                raise e
            finally:
                self._transaction = old_transaction
                if session:
                    session.remove()
        else:
            yield self._transaction

    @classmethod
    def extract_validate_fileds(cls, data):
        if data is None:
            return None
        new_data = {}
        for validator in cls._validate:
            if validator.field in data:
                new_data[validator.field] = data[validator.field]
        return new_data

    @contextlib.contextmanager
    def get_session(self):
        """
        会话管理上下文, 如果资源初始化时指定使用外部会话，则返回的也是外部会话对象
        """
        if self._session is None and self._transaction is None:
            try:
                old_session = self._session
                session = self._pool.get_session()
                self._session = session
                yield session
            finally:
                self._session = old_session
                if session:
                    session.remove()
        elif self._session:
            yield self._session
        else:
            yield self._transaction

    def _addtional_count(self, query, filters):
        return query

    def count(self, filters=None, offset=None, limit=None, hooks=None):
        """
        获取符合条件的记录数量

        :param filters: 过滤条件
        :type filters: dict
        :param offset: 起始偏移量
        :type offset: int
        :param limit: 数量限制
        :type limit: int
        :param hooks: 钩子函数列表，函数形式为func(query, filters)
        :type hooks: list
        :returns: 数量
        :rtype: int
        """
        offset = offset or 0
        with self.get_session() as session:
            query = self._get_query(session, filters=filters, orders=[])
            if hooks:
                for h in hooks:
                    query = h(query, filters)
            query = self._addtional_count(query, filters=filters)
            if offset:
                query = query.offset(offset)
            if limit is not None:
                query = query.limit(limit)
            return query.count()

    def _addtional_list(self, query, filters):
        return query

    def list(self, filters=None, orders=None, offset=None, limit=None, hooks=None):
        """
        获取符合条件的记录

        :param filters: 过滤条件
        :type filters: dict
        :param orders: 排序
        :type orders: list
        :param offset: 起始偏移量
        :type offset: int
        :param limit: 数量限制
        :type limit: int
        :param hooks: 钩子函数列表，函数形式为func(query, filters)
        :type hooks: list
        :returns: 记录列表
        :rtype: list
        """
        offset = offset or 0
        with self.get_session() as session:
            query = self._get_query(session, filters=filters, orders=orders)
            if hooks:
                for h in hooks:
                    query = h(query, filters)
            query = self._addtional_list(query, filters)
            if offset:
                query = query.offset(offset)
            if limit is not None:
                query = query.limit(limit)
            results = [rec.to_dict() for rec in query]
            return results
