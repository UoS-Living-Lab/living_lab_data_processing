"""
Helper file containing common functions for 
interfacing with the AZURE database
"""


__author__ = "Ethan Bellmer"
__version__ = "0.2"


import pyodbc
from flask import g


# Function for establishing a connection to the database
def db_connect(SQL_CONN_STR):
	try:
		print('Connecting to database...')
		# Create a new connection to the SQL Server using the prepared connection string
		cnxn = pyodbc.connect(SQL_CONN_STR)
	except pyodbc.Error as e:
		# Print error is one should occur
		sqlstate = e.args[1]
		print("An error occurred connecting to the database: " + sqlstate)
	else:
		print("Connected to database. Proceeding")
		return cnxn


# Executes a Stored Procedure in the database to get or create data
def execute_procedure(conn, sql, params, return_val = False):
	# Create new cursor from existing connection 
	cursor = conn.cursor()

	if return_val:
		# Attempt to execute the stored procedure
		try:
			# Execute the SQL statement with the parameters prepared
			cursor.execute(sql, params)
			# Fetch all results for the executed statement
			rows = cursor.fetchall()
			while rows:
				print(rows)
				return str(rows)
				#if cursor.nextset(): # Disabled during testing, unsure if required if result will always return one result
				#	rows = cursor.fetchall()
				#else:
				#	rows = None
			# Close open database cursor
			cursor.close()

		except pyodbc.Error as e:
			# Extract the error argument
			sqlstate = e.args[1]

			# Close cursor
			cursor.close()

			# Print error is one should occur and raise an exception
			print("An error occurred executing stored procedure: " + sqlstate)
	else:
		try:
			# Execute the SQL statement with the parameters prepared
			cursor.execute(sql, params)
			# Close open database cursor
			cursor.close()

		except pyodbc.Error as e:
			# Extract the error argument
			sqlstate = e.args[1]

			# Close cursor
			cursor.close()

			# Print error is one should occur and raise an exception
			print("An error occurred executing stored procedure: " + sqlstate)


# Executes a Stored Procedure in the database to create data without returning any values
# Legacy code, merged into execture_procedure function
def execute_procedure_no_return(conn, sql, params):
	# Create new cursor from existing connection 
	cursor = conn.cursor()

	try:
		# Execute the SQL statement with the parameters prepared
		cursor.execute(sql, params)
		# Close open database cursor
		cursor.close()

	except pyodbc.Error as e:
		# Extract the error argument
		sqlstate = e.args[1]

		# Close cursor
		cursor.close()

		# Print error is one should occur and raise an exception
		print("An error occurred executing stored procedure (noReturn): " + sqlstate)
		print(e) # Testing


# Helper function to get DB and return it to the 
def get_db(SQL_CONN_STR):
	if 'db' not in g:
		g.db = db_connect(SQL_CONN_STR)
	return g.db


def commit_db(e=None):
	if 'db' not in g:
		print('DB Connection doesn\'t exist')
	else:
		g.db.commit() # Commit the staged data to the DB


def close_db(e=None):
	db = g.pop('db', None)

	if db is not None:
		db.close() # Close the DB connection