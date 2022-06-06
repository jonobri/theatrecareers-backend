from sqlalchemy import create_engine
from sqlalchemy import Table, Column, ForeignKey, ColumnDefault, Integer, Float, String, BIGINT, Boolean, TEXT, Date
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session

# import from python files
# base
from backend_base import Base


class Gender(Base):
    __tablename__ = 'gender'

    id = Column(Integer, primary_key=True)
    gender = Column(String(40))


class State(Base):
    __tablename__ = 'state'

    id = Column(Integer, primary_key=True)
    state = Column(String(40))


class FunctionList(Base):
    __tablename__ = 'function_list'

    primaryid = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(Integer)
    function_name = Column(String(40))
    # autoid = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    # tempid = Column(Integer)


class Role(Base):
    __tablename__ = 'role'

    id = Column(Integer, primary_key=True)
    role = Column(String(40))


class Event(Base):
    __tablename__ = 'event'

    id = Column(Integer, primary_key=True)
    event_name = Column(String(255))
    stateid = Column(Integer, ForeignKey('state.id'))
    year = Column(Integer)
    contributor_count = Column(Integer)


class Contributor(Base):
    __tablename__ = 'contributor'

    id = Column(Integer, primary_key=True)
    first_name = Column(String(255))
    last_name = Column(String(255))
    display_name = Column(String(255))
    genderid = Column(Integer, ForeignKey('gender.id'))
    stateid = Column(Integer, ForeignKey('state.id'))
    first_year = Column(Integer)
    last_year = Column(Integer)


class LinkTableOld(Base):
    # TODO: rename and refactor this throughout as 'linktable' since it's singular link in the db
    __tablename__ = 'link_table_old'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    eventid = Column(Integer, ForeignKey('event.id'))
    contributorid = Column(Integer, ForeignKey('contributor.id'))
    functionid = Column(Integer)
    roleid = Column(Integer, ForeignKey('role.id'), server_default='15', nullable=False)
    workid = Column(Integer, default=0)
    organisationid = Column(Integer, default=0)


class LinkTable(Base):
    # TODO: rename and refactor this throughout as 'linktable' since it's singular link in the db
    __tablename__ = 'link_table'

    id = Column(Integer, primary_key=True, autoincrement=True, nullable=False)
    eventid = Column(Integer, ForeignKey('event.id'))
    contributorid = Column(Integer, ForeignKey('contributor.id'))
    functionid = Column(Integer)
    roleid = Column(Integer, ForeignKey('role.id'), server_default='15', nullable=False)
    workid = Column(Integer, default=0)
    organisationid = Column(Integer, default=0)


class Career(Base):
    __tablename__ = 'career'

    id = Column(Integer, primary_key=True)
    contributorid = Column(Integer, ForeignKey('contributor.id'))
    roleid = Column(Integer, ForeignKey('role.id'))
    first_year = Column(Integer)
    length = Column(Integer)
    count = Column(Integer)
    
