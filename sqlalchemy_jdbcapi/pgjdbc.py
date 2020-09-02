from __future__ import absolute_import
from __future__ import unicode_literals

import os
from sqlalchemy.dialects.postgresql.base import PGDialect
from sqlalchemy.sql import sqltypes
from sqlalchemy import util
from .base import BaseDialect, MixedBinary


colspecs = util.update_copy(
    PGDialect.colspecs, {sqltypes.LargeBinary: MixedBinary,},
)


class PGJDBCDialect(BaseDialect, PGDialect):
    jdbc_db_name = "postgresql"
    jdbc_driver_name = "org.postgresql.Driver"
    colspecs = colspecs

    def __init__(self, *args, **kwargs):
        super(PGJDBCDialect, self).__init__(*args, **kwargs)
        self.jdbc_driver_path = os.environ.get("PG_JDBC_DRIVER_PATH")
        #self.isolation_level=isolation_level

        if self.jdbc_driver_path is None:
            raise Exception(
                "To connect to DATABASE via JDBC, you must set the "
                "PG_JDBC_DRIVER_PATH path to the location of the JDBC driver"
            )

    def initialize(self, connection):
        super(PGJDBCDialect, self).initialize(connection)

    def on_connect(self):

        #print('InsideOnConnect,isolationLevelCore-{}'.format(self.isolation_level))

        fns=[]
        #if self.isolation_level is not None:
        def on_connect(conn):
                self.set_isolation_level(conn,self.isolation_level)
        fns.append(on_connect)
        
        if fns:

            def on_connect(conn):
                for fn in fns:
                    fn(conn)

            return on_connect
        else:
            return None

    def create_connect_args(self, url):
        if url is not None:
            params = super(PGJDBCDialect, self).create_connect_args(url)[1]
            driver = self.jdbc_driver_path

            cargs = (
                self.jdbc_driver_name,
                self._create_jdbc_url(url),
                [params["username"], params["password"]],
                driver,
            )
            #print("cargs-{}".format(cargs))
            return (cargs, {})
    
    def set_isolation_level(self, connection, level):
        if level is not None:
            level = level.replace("_", " ")

        # adjust for ConnectionFairy possibly being present
        if hasattr(connection, "connection"):
            connection = connection.connection

        if level == "AUTOCOMMIT":
            connection.jconn.setAutoCommit(True)
        else:
            connection.jconn.setAutoCommit(False)

    def _create_jdbc_url(self, url):
        """Create a JDBC url from a :class:`~sqlalchemy.engine.url.URL`"""
        return "jdbc:%s://%s%s/%s" % (
            self.jdbc_db_name,
            url.host,
            url.port is not None and ":%s" % url.port or "",
            url.database,
        )


dialect = PGJDBCDialect
