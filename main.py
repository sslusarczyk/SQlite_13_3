# main.py
import csv
from sqlalchemy import Table, Column, Float, Integer, String, MetaData, create_engine
from sqlalchemy.exc import IntegrityError

csv_filename1 = 'clean_stations.csv'
csv_filename2 = 'clean_measure.csv'

engine = create_engine('sqlite:///database.db', echo=True)
connection = engine.connect()

meta = MetaData()

clean_stations = Table(
    'clean_stations', meta,
    Column('station', String, primary_key=True),
    Column('latitude', Float),
    Column('longitude', Float),
    Column('elevation', Float),
    Column('country', String),
    Column('state', String),
)

clean_measure = Table(
    'clean_measure', meta,
    Column('station', String),
    Column('date', String),
    Column('precip', Float),
    Column('tobs', Integer),
)

meta.create_all(engine)

with open(csv_filename1, 'r') as file:
    csv_reader = csv.DictReader(file)
    data_to_insert1 = [{key: value.strip() for key, value in row.items()} for row in csv_reader]

with open(csv_filename2, 'r') as file:
    csv_reader = csv.DictReader(file)
    data_to_insert2 = [{key: value.strip() for key, value in row.items()} for row in csv_reader]

# Insert data into the tables
for row in data_to_insert1:
    try:
        connection.execute(clean_stations.insert(), row)
    except IntegrityError:
        pass  # Ignore if the record already exists

for row in data_to_insert2:
    try:
        connection.execute(clean_measure.insert(), row)
    except IntegrityError:
        pass  # Ignore if the record already exists

result1 = connection.execute("SELECT * FROM clean_stations LIMIT 5").fetchall()
result2 = connection.execute("SELECT * FROM clean_measure LIMIT 5").fetchall()

print(result1)
print(result2)

# Close the connection
connection.close()

print("Data inserted successfully.")