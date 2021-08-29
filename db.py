import pyodbc


# Function for establishing a connection to the database
def dbConnect(SQL_CONN_STR):
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
def execProcedure(conn, sql, params):
	# Create new cursor from existing connection 
	cursor = conn.cursor()

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


# Executes a Stored Procedure in the database to create data without returning any values 
def execProcedureNoReturn(conn, sql, params):
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