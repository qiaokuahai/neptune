# coding: utf-8
from sqlalchemy import Column, ForeignKey, String, INTEGER
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class Address(Base):
    __tablename__ = 'address'

    id = Column(String(36), primary_key=True)
    location = Column(String(63), nullable=False)
    user_id = Column(ForeignKey(u'user.id', ondelete=u'CASCADE', onupdate=u'CASCADE'), nullable=False)

    user = relationship(u'User')


class Department(Base):
    __tablename__ = 'department'

    id = Column(String(36), primary_key=True)
    name = Column(String(63), nullable=False)


class User(Base):
    __tablename__ = 'user'
    attributes = ['id', 'name', 'department_id', 'age', 'department', 'addresses']

    id = Column(String(36), primary_key=True)
    name = Column(String(63), nullable=False)
    department_id = Column(ForeignKey(u'department.id', ondelete=u'RESTRICT', onupdate=u'RESTRICT'), nullable=False)
    age = Column(INTEGER, nullable=True)

    department = relationship(u'Department', lazy=False)
    addresses = relationship(u'Address', lazy=False, back_populates=u'user', uselist=True, viewonly=True)
