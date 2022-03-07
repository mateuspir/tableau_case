
import os
import sys
import time
from datetime import datetime
from psycopg2 import sql
from psycopg2.errors import UniqueViolation
import utils
from database import Database
from data_generation import Rule


if __name__ == "__main__":

	# check if required arguments were given
	if len(sys.argv) < 2:
		warning = "\nrun: python %s <number_of_records> <name_of_table(s)>\n"
		warning += "\nexample 1: to insert 10 records in all tables\n\tpython %s 10"
		warning += "\n\nexample 2: to insert 10 records in table_1\n\tpython %s 10 table_1"
		warning += "\n\nexample 3: to insert 10 records in table_1 and table_2\n\tpython %s 10 table_1 table_2"
		print(warning % ((os.path.basename(__file__),) * 4))
		sys.exit()

	# make global variables lookups for selecting purposes
	global_lookups = {
		"existing_requests": [],
		"existing_analysts": [],
	}

	# make table schemas with the generation rules
	tables = {
		"analysts": {
			"lan_id": Rule(r"[a-z]\d{5}[a-z]", is_regex=True),
			"name": Rule(is_name=True),
			"role": Rule([
				"Service Desk analyst I",
				"Service Desk analyst II",
				"Service Desk analyst III"
			]),
			"hiring_date": Rule(
				is_datetime=True,
				start_datetime=datetime(year=2018, month=1, day=1),
				end_datetime=datetime(year=2021, month=6, day=1)
			)
		},
		"requests": {
			"number": Rule(r"REQ\d{6}", is_regex=True),
			"creation_date": Rule(
				is_datetime=True,
				start_datetime=datetime(year=2020, month=1, day=1),
				end_datetime=datetime(year=2022, month=3, day=1)
			),
			"requestor": Rule(is_name=True),
			"description": Rule([
				"The application homepage is not loading",
				"The application desktop application is crashing all the time",
				"The application homepage is in infinity loading"
		]),
			"affected_system": Rule([
				"Salesforce",
				"Oracle EBS"
			])
		},
		"tasks": {
			"task_id": Rule(r"\w{30}", is_regex=True),
			"request_number": Rule(r"REQ\d{6}", is_regex=True),
			"analyst_lan_id": Rule(r"[a-z]\d{5}[a-z]", is_regex=True),
			"datetime": Rule(
				is_datetime=True,
				start_datetime=datetime(year=2020, month=1, day=1),
				end_datetime=datetime(year=2022, month=3, day=1)
			),
			"short_description": Rule([
				"Configuration of the user's environment",
				"Reboot system",
				"Cleaning cache"
			])
		}
	}

	# identify if any table was specified
	if len(sys.argv) > 2:

		specified_tables = []
		for argv in sys.argv:
			if argv in tables.keys():
				specified_tables += [argv]

		# remove unspecified
		ignored_tables = 0
		if specified_tables:
			for table_name in tables.copy().keys():
				if table_name not in specified_tables:
					del tables[table_name]
					ignored_tables += 1

		# identify unknown table(s) passed as argument(s)
		if len(specified_tables) < len(sys.argv[2:]):
			unknown_tables = [i for i in sys.argv[2:] if i not in specified_tables]
			unknown_tables = "'" + "', '".join(unknown_tables) + "'"
			raise Exception("Unknown table(s): %s" % unknown_tables)

	# get the required argument
	n_records = int(sys.argv[1])

	# instantiate the database class
	db = Database(
		database="postgresDB",
		user='postgres',
		password='123',
		host='127.0.0.1',
		port='5432',
		sql_syntax="PostgreSQL 14.2"
	)

	# loop through all tables and insert artificial data
	for table_name, schema in tables.items():

		print("\nInserting %s artificial record(s) into %s table..." % (n_records, table_name))

		db.cursor.execute("SELECT COUNT(*) from %s" % table_name)
		previous_records_counter = db.cursor.fetchall()[0][0]
		print("Previous records counter: %s" % previous_records_counter)

		for _ in range(n_records):

			lookup = []
			for column in schema.keys():
				lookup = global_lookups.get(column, [])
				if lookup:
					break

			# execute all rules to generate artificial data
			data = {column: rule.generate(lookup) for column, rule in schema.items()}

			# build insert statement
			make_placeholders = lambda length, placeholder: ", ".join([placeholder] * length)
			query = "INSERT INTO {} (%s) VALUES (%s)" % (
				make_placeholders(length=len(data), placeholder="{}"),
				make_placeholders(length=len(data), placeholder="%s")
			)
			query = sql.SQL(query).format(
				sql.Identifier(table_name),
				*list(map(sql.Identifier, data.keys()))
			)

			# execute insert statement
			try:

				db.cursor.execute(query, (*data.values(),))

			except UniqueViolation as exception:

				# print the exception
				print("Duplicated primary key violation found... Skiping record.")

				# disconnect from the database
				db.disconnect()
				del db
				time.sleep(1)

				# instantiate the database class again
				db = Database(
					database="postgresDB",
					user='postgres',
					password='123',
					host='127.0.0.1',
					port='5432',
					sql_syntax="PostgreSQL 14.2"
				)

			except Exception as exception:
				
				# print the exception and abort the execution
				utils.system_exception(exception, os.path.basename(__file__))
				sys.exit()

			# update global variables lookups
			for lookup_field in global_lookups.copy().keys():
				if lookup_field in data:
					global_lookups[lookup_field] += [data[lookup_field]]

		# commit changes
		db.conn.commit()

		db.cursor.execute("SELECT COUNT(*) from %s" % table_name)
		new_records_counter = db.cursor.fetchall()[0][0]
		new = new_records_counter - previous_records_counter
		print("New records counter: %s \t [+%s]" % (new_records_counter, new), end="\n")

	# close database connection
	db.disconnect()
