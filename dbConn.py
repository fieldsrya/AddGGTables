#!/usr/local/bin/python3.6

import cx_Oracle

class orcl():

    def __init__(self,username,password,databaseName):
        self.username = username
        self.password = password
        self.databaseName = databaseName
        self.dsn = (self.username,self.password,self.databaseName)
        self.db = cx_Oracle.connect (*self.dsn)
        self.cursor = self.db.cursor()

    def __enter__(self):
        return orcl(self.username,self.password,self.databaseName)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.db:
            self.db.close()

    def printException (exception):
        error = exception.args
        print ("Error code = %s\n",error.code);
        print ("Error message = %s\n",error.message);

    def dbExecuteFetchAll(self,sql):
        try:
            self.cursor.execute (sql)
            data = self.cursor.fetchall()
            return data
        except cx_Oracle.DatabaseError as exception:
            print('Failed to execute query on %s\n',self.databaseName)
            printException (exception)
            exit (1)

    def dbExecuteFetchOne(self,sql):
        try:
            self.cursor.execute (sql)
            data = self.cursor.fetchone()
            return data
        except cx_Oracle.DatabaseError as exception:
            print('Failed to execute query on %s\n',self.databaseName)
            printException (exception)
            exit (1)

    def dbExecuteCommand(self,sql):
        try:
            self.cursor.execute (sql)
            #return data
        except cx_Oracle.DatabaseError as exception:
            print('Failed to execute query on %s\n',self.databaseName)
            printException (exception)
            exit (1)
    def dbCommit(self):
        try:
            self.db.commit()
        except cx_Oracle.DatabaseError as exception:
            print('Failed to execute query on %s\n',self.databaseName)
            printException (exception)
            exit (1)
