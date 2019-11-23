from __future__ import absolute_import
from sqlalchemy.sql.expression import BinaryExpression
from sqlalchemy.sql.sqltypes import _type_map
from neptune.core import utils


def column_from_expression(table, expression):
    expr_wrapper = None
    column = getattr(table, expression, None)
    return expr_wrapper, column


def cast(column, value):
    """
    将python类型值转换为SQLAlchemy类型值

    :param column:
    :type column:
    :param value:
    :type value:
    """
    cast_to = _type_map.get(type(value), None)
    if cast_to is None:
        column = column.astext
    else:
        column = column.astext.cast(cast_to)
    return column


class NullFilter(object):

    def make_empty_query(self, column):
        return column.is_(None) & column.isnot(None)

    def op(self, column, value):
        pass

    def op_in(self, column, value):
        pass

    def op_nin(self, column, value):
        pass

    def op_eq(self, column, value):
        pass

    def op_ne(self, column, value):
        pass

    def op_lt(self, column, value):
        pass

    def op_lte(self, column, value):
        pass

    def op_gt(self, column, value):
        pass

    def op_gte(self, column, value):
        pass

    def op_like(self, column, value):
        pass

    def op_starts(self, column, value):
        pass

    def op_ends(self, column, value):
        pass
    
    def op_nlike(self, column, value):
        pass

    def op_ilike(self, column, value):
        pass

    def op_istarts(self, column, value):
        pass

    def op_iends(self, column, value):
        pass
    
    def op_nilike(self, column, value):
        pass
    
    def op_nnull(self, column, value):
        pass
    
    def op_null(self, column, value):
        pass


class Filter(NullFilter):

    def make_empty_query(self, column):
        return column == None & column != None

    def op(self, column, value):
        if utils.is_list_type(value):
            if isinstance(column, BinaryExpression):
                column = cast(column, value[0])
            expr = column.in_(tuple(value))
        else:
            if isinstance(column, BinaryExpression):
                column = cast(column, value)
            expr = column == value
        return expr

    def op_in(self, column, value):
        if utils.is_list_type(value):
            if isinstance(column, BinaryExpression):
                column = cast(column, value[0])
            expr = column.in_(tuple(value))
        else:
            if isinstance(column, BinaryExpression):
                column = cast(column, value)
            expr = column == value
        return expr

    def op_nin(self, column, value):
        if utils.is_list_type(value):
            if isinstance(column, BinaryExpression):
                column = cast(column, value[0])
            expr = column.notin_(tuple(value))
        else:
            if isinstance(column, BinaryExpression):
                column = cast(column, value)
            expr = column != value
        return expr

    def op_eq(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column == value
        return expr

    def op_ne(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column != value
        return expr

    def op_lt(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column < value
        return expr

    def op_lte(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column <= value
        return expr

    def op_gt(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column > value
        return expr

    def op_gte(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column >= value
        return expr

    def op_like(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.like('%%%s%%' % value)
        return expr

    def op_starts(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.like('%s%%' % value)
        return expr

    def op_ends(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.like('%%%s' % value)
        return expr
    
    def op_nlike(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.notlike('%%%s%%' % value)
        return expr

    def op_ilike(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.ilike('%%%s%%' % value)
        return expr

    def op_istarts(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.ilike('%s%%' % value)
        return expr

    def op_iends(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.ilike('%%%s' % value)
        return expr
    
    def op_nilike(self, column, value):
        if isinstance(column, BinaryExpression):
            column = cast(column, value)
        expr = column.notilike('%%%s%%' % value)
        return expr
    
    def op_nnull(self, column, value):
        expr = column.isnot(None)
        return expr
    
    def op_null(self, column, value):
        expr = column.is_(None)
        return expr


class FilterNumber(Filter):
    """数字类型过滤"""

    def op_like(self, column, value):
        pass
    
    def op_nlike(self, column, value):
        pass

    def op_starts(self, column, value):
        pass

    def op_ends(self, column, value):
        pass

    def op_ilike(self, column, value):
        pass
    
    def op_nilike(self, column, value):
        pass

    def op_istarts(self, column, value):
        pass

    def op_iends(self, column, value):
        pass

