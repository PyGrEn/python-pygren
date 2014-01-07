#!/usr/bin/env python
# encoding: UTF-8
"""
pygrendb.py

Created by Brett Beaudoin, MITRE Corporation
Copyright (c) The MITRE Corporation - 2013. All rights reserved.

Dependencies:
        'sqlalchemy' (http://www.sqlalchemy.org/)
"""
import sqlalchemy as sa

import pygrenutils as utils

from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.expression import Insert

@compiles(Insert)
def replace_string(insert, compiler, **kw):
    s = compiler.visit_insert(insert, **kw)
    s = s.replace("INSERT INTO", "REPLACE INTO")
    return s

class DB(object):
    """Database functionality"""
    def __init__(self, dbEngineURL):
        self.dbEngineURL = dbEngineURL
        self.dialect = dbEngineURL[:dbEngineURL.find(":")]
        self.engine = sa.create_engine(dbEngineURL, encoding='utf-8',
                                       echo=False)
        self.conn = self.engine.connect()
        self.metadata = sa.MetaData(bind=self.engine)

        self.engine = sa.create_engine(dbEngineURL, encoding='utf-8',
                                       echo=False)
        self.conn = self.engine.connect()
        self.metadata = sa.MetaData(bind=self.engine)

        self.predicates = sa.Table('predicates', self.metadata,
                sa.Column('id', sa.BigInteger, primary_key=True),
                sa.Column('pred', sa.String(1024), nullable=False, index=True))

        self.vertices = sa.Table('vertices', self.metadata,
                sa.Column('id', sa.BigInteger, primary_key=True),
                sa.Column('vert', sa.String(1024), nullable=False, index=True),
                sa.Column('flag', sa.Integer, nullable=False, default=-1),
                sa.Column('step', sa.Integer, nullable=False, default=-1),
                sa.Column('boundto', sa.Integer, nullable=False, default=-1))

        self.triples = sa.Table('triples', self.metadata,
                sa.Column('id', sa.BigInteger, primary_key=True),
                sa.Column('s', sa.BigInteger, nullable=False, index=True),
                sa.Column('p', sa.BigInteger, nullable=False, index=True),
                sa.Column('o', sa.BigInteger, nullable=False, index=True),
                sa.Column('flag', sa.Integer, nullable=False, default=-1),
                sa.Column('step', sa.Integer, nullable=False, default=-1))

        sa.Index('idx_triples_s_p', self.triples.c.s, self.triples.c.p)
        sa.Index('idx_triples_s_o', self.triples.c.s, self.triples.c.o)
        sa.Index('idx_triples_s_p_o', self.triples.c.s, self.triples.c.p,
                                                            self.triples.c.o)

        self.tables = {'predicates': self.predicates,
                       'vertices': self.vertices, 'triples': self.triples}

        self.metadata.create_all(self.engine)


    def execute(self, sql, data=[]):
        print "SQL = %s" % str(sql)
        sqlTemp = str(sql).strip().upper()
        if (sqlTemp.startswith("SELECT ") or sqlTemp.startswith("PRAGMA ")
            or sqlTemp.startswith("SHOW ") or sqlTemp.startswith("LIST ")):
            if len(data) == 0:
                result = self.conn.execute(sql, use_labels=True)
            else:
                result = self.conn.execute(sql, data, use_labels=True)
            ret = [row for row in list(result)]
        else:
            try:
                trans = self.conn.begin()
                try:
                    if len(data) == 0:
                        result = self.conn.execute(sql)
                    else:
                        result = self.conn.execute(sql, data)

                    trans.commit()
                    #print "Transaction committed"
                    ret = result.rowcount
                    result.close()
                except sa.exc.IntegrityError, err:
                    # Ignore duplicate entries
                    print "Transaction NOT committed: duplicate entry"
                    ret = 0
            except Exception, err:
                trans.rollback()
                print "Transaction rolled back: %s" % err
                stringError = "ERROR: %s" % err
                #print stringError
                ret = stringError
            finally:
                trans.close()
                #print "Transaction closed"
        return ret
