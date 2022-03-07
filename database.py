
import os
import sys
import psycopg2
import utils


__supported__ = [
	"PostgreSQL 14.2"
]


class Database(object):

	"""
		Class to abstract Python-database* connection.

		* Supported databases:

			PostgreSQL 14.2
	"""

	# initialize default class attributes
	supported = __supported__


	def __init__(self, database, user, password, host, port, sql_syntax):

		try:

			# initialize default instance attributes
			# None

			# store given parameters as instance attributes
			self.database = database
			self.user = user
			self.password = password
			self.host = host
			self.port = port
			self.__sql_syntax = self.__check_syntax(sql_syntax)

			# ignite database connection and initialize default parameters
			self.conn, self.cursor = self.connect()

		except Exception as exception:

			# print the exception and abort the execution
			utils.system_exception(exception, os.path.basename(__file__))
			sys.exit()


	def __check_syntax(self, sql_syntax):
		if sql_syntax not in self.supported:
			raise NotImplementedError("%s support is not implemented yet" % sql_syntax)
		return sql_syntax


	def connect(self):

		# establishing the connection
		conn = psycopg2.connect(
		   database=self.database,
		   user=self.user,
		   password=self.password,
		   host=self.host,
		   port=self.port
		)

		# creating a cursor object using the psycopg2.cursor method
		cursor = conn.cursor()

		return conn, cursor


	def disconnect(self):
		return self.conn.close()


	def __execute(self, sql_query, fetchone=False):
		self.cursor.execute(sql_query)
		return self.cursor.fetchone() if fetchone else self.cursor.fetchall()


	def select(self, table, columns=['*'], where=[], fetchone=False):

		# make the select statement according to the specified syntax
		if self.__sql_syntax == "PostgreSQL 14.2":

			sql_query = "SELECT " + ", ".join(columns) + " FROM " + table

			if where:
				sql_query += " WHERE " + " and ".join(where)

		# elif self.__sql_syntax == "Oracle Database 21c Express Edition"
		# elif self.__sql_syntax == ...

		else:

			raise NotImplementedError("%s support is not implemented yet" % self.sql_syntax)

		# execute select statement and return it's result
		return self.execute(sql_query, fetchone)
