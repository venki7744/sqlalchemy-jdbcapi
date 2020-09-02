from sqlalchemy import String, TypeDecorator


class MixedBinary(TypeDecorator):
    impl = String

    def process_result_value(self, value, dialect):
        if isinstance(value, str):
            value = bytes(value, "utf-8")
        elif value is not None:
            value = bytes(value)
        return value


class BaseDialect(object):
    jdbc_db_name = None
    jdbc_driver_name = None
    supports_native_decimal = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    supports_unicode_binds = True
    description_encoding = None

    # def __init__(
    #     self,
    #     **kwargs
    # ):
    #     #default.DefaultDialect.__init__(self, **kwargs)
        #self.isolation_level = isolation_level
    
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

    def on_connect(self):

        #print('InsideOnConnect,isolationLevel-{}'.format(self.isolation_level))

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

    @classmethod
    def dbapi(cls):
        import jaydebeapi

        return jaydebeapi

    def is_disconnect(self, e, connection, cursor):
        if not isinstance(e, self.dbapi.ProgrammingError):
            return False
        e = str(e)
        return "connection is closed" in e or "cursor is closed" in e

    def do_rollback(self, dbapi_connection):
        pass
