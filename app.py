import pymongo
from pymongo import MongoClient
import logging
import time
import csv
import itertools
import os
import datetime



logger = logging.getLogger(__name__)
logging.basicConfig(filename="Lab4.log", level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
url = "mongodb://localhost:27017"
cluster = MongoClient(url)
db = cluster.cp_database
collection = db.test_collection




def create_table():
	with open("Odata2019File.csv", "r", encoding="cp1251") as csv_file:
		header = csv_file.readline().split(';')
		header = [word.strip('"') for word in header]
		header[-1] = header[-1].rstrip('"\n')
	
	return header

header = create_table()



def insert_from_csv(f, year):
 
	start_time = datetime.datetime.now() 
	logger.info(str(start_time) + " opening file " + f + '\n')
	
	with open(f, "r", encoding="cp1251") as csv_file:
		print("Reading " + f  )
		csv_reader = csv.DictReader(csv_file, delimiter=';')
		batches_inserted = 0
		batch_size = 50
		inserted_all = False


		while not inserted_all:
			try:
				insert_query = list()
				count = 0
				for row in csv_reader:
					temp = list()
					count += 1
					for key in row:

						if row[key] == 'null':
							pass
						elif key.lower() != 'birth' and 'ball' not in key.lower():
							row[key] = row[key].replace("'", "''")
						elif 'ball100' in key.lower():
							row[key] = row[key].replace(',', '.')
							row[key] = float(row[key])
						temp.append(row[key])
					insert_query.append(dict(zip(['year']+ header, [int(year)] + temp)))

					if count == batch_size:
						if not batches_inserted % 50:
							print(batches_inserted)
						count = 0
						collection.insert_many(insert_query)
						insert_query = list()

						batches_inserted += 1
						

				if count != 0:
					
					collection.insert_many(insert_query)
					insert_query = list()
				inserted_all = True
			except Exception as e:
				print(e)
				return None
				

					
					# csv_file.seek(0,0)
					# csv_reader = itertools.islice(csv.DictReader(csv_file, delimiter=';'), 
					# 	batches_inserted * batch_size, None)
			

	end_time = datetime.datetime.now()
	logger.info(str(end_time) + " -- finished ops\n")
	logger.info('time -- ' + str(end_time - start_time) + '\n\n')

	return None

	 
#Vlad = insert_from_csv("Odata2019File.csv", 2019)
#Vlad1 = insert_from_csv("Odata2020File.csv", 2020)    


def select_table():
	try:
		result = collection.aggregate([
			{
				'$match': {
					'engTestStatus': 'Зараховано'
				}
			},
			{
				'$group': {
					'_id': {'region': '$REGNAME', 'year': '$year'},
					'avg': { '$avg': '$engBall100' }
				}
			}
		])

		with open('result.csv', 'w', encoding="utf-8") as result_csvfile:
			csv_writer = csv.writer(result_csvfile)
			csv_writer.writerow(['region', 'year', 'avg_result'])
			for k in result:
				row = [k['_id']['region'],str(k['_id']['year']),str(k['avg'])]
				csv_writer.writerow(row)
			
	except Exception as e:
		print(e)
		pass

select_table()


