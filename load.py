# This program loads data using basic, slow INSERTs, run it with -h to see the command line options
import time
import psycopg2
import argparse
import json
import pandas as pd
import assertion

DBname, DBuser, DBpwd, DBtable, DBfile = "postgres", "postgres", "ps", 'breadcrumb', 'UnspecifiedFile'

# Step 1: DB/table Creation - Handled via pipeline.sql
def DBconnect():
	'''Connect to database'''
	connection = psycopg2.connect(host="localhost", database=DBname, user=DBuser, password=DBpwd,)
	connection.autocommit = False
	if connection: print(f'Database connection successful! {connection}')
	return connection

# Step 2: Data Loading
def parseCLI():
	'''Parse command line arguments: get the data file name'''
	parser = argparse.ArgumentParser()
	parser.add_argument("-d", "--datafile", required=True)
	parser.add_argument("-t", "--table", required=True)
	global DBtable, DBfile
	DBtable = parser.parse_args().table
	DBfile = parser.parse_args().datafile
	return parser.parse_args().datafile, parser.parse_args().table

def readData(fname):
	'''Read the input data file into a list of breadcrumbs (dictionaries)'''
	print(f"readdata: reading from File: {fname}")
	bcrumbs = []
	with open(fname, mode="r") as f: 
		for crumb in json.load(f): bcrumbs.append(crumb)  # read the entire file into a list of breadcrumbs
	return bcrumbs

# Step 3: Data Validation
def assertCrumbs(bcrumbs):
	'''Check that the records have the required keys'''
	row_number = 1
	cleaned = []
	for crumb in bcrumbs:
		valid = True
		for key in ['EVENT_NO_TRIP', 'EVENT_NO_STOP', 'OPD_DATE', 'VEHICLE_ID', 'METERS', 'ACT_TIME', 'GPS_LONGITUDE', 'GPS_LATITUDE', 'GPS_SATELLITES', 'GPS_HDOP']:
			if key not in crumb: 
				print(f"Missing key: {key} in record {12*row_number}: {crumb}")
				valid = False
		if valid == True: valid = assertion.assertions(crumb) # If crumb is initially valid, check if valid post-assertions
		if valid == True: cleaned.append(crumb)
		row_number += 1
	return cleaned

# Step 4: Data Transformation
def _crumb2row(crumb, DBtable):
	'''Convert a list of records into a list of SQL values'''
	# Requires 2 calculated columns: Timestamp, speed (meters/sec), 
	if DBtable == 'breadcrumb':
		return (crumb[TODO:TIMESTAMP], crumb['GPS_LATITUDE'], crumb['GPS_LONGITUDE'], crumb[TODO:SPEED], crumb['EVENT_NO_TRIP'])
	elif DBtable == 'trip':
		return (crumb['EVENT_NO_TRIP'], crumb[TODO:ROUTEID], crumb['VEHICLE_ID'], crumb[TODO:SERVICEKEY], crumb[TODO:DIRECTION])
	else: print(f"Unknown table: {DBtable} in _crumb2row")

def crumbs2SQL(crumbs, DBtable): 
	return [f"INSERT INTO {DBtable} VALUES ({_crumb2row(c)});" for c in crumbs]

# Step 5: Data Storage
def loadDB(conn, sqlCMDS):
	with conn.cursor() as cursor:
		print(f"Loading {len(sqlCMDS)} rows")
		start = time.perf_counter()
		for cmd in sqlCMDS: cursor.execute(cmd)
		print(f'Finished Loading. Elapsed Time: {time.perf_counter() - start:0.4} seconds')

def main():
	global DBname, DBuser, DBpwd, DBtable, DBfile
	conn = DBconnect()
	DBfile, DBtable = parseCLI()
	data = readData(DBfile)
	crumbs = assertCrumbs(data)
	sqlCMDS = crumbs2SQL(crumbs)
	loadDB(conn, sqlCMDS)

if __name__ == "__main__":
	main()