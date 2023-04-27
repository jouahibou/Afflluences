import csv
import datetime
import mysql.connector


db = mysql.connector.connect(
  host="localhost",
  user="root",
  password="",
  database="ecommerce_senegal"
)


analysis_datetime_utc = datetime.datetime(2023, 4, 3, 14, 0, 0)

# Chargement des données depuis les fichiers CSV
# Pour chaque site : date et heure d’ouverture du site , date et heure de fermeture du site
site_timetables = {}
with open('C:/Users/Lenovo/Downloads/Test technique data engineer/site_timetables.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        site_timetables[row['site_id']] = {
            
            'opening_datetime_utc': row['opening_datetime_utc'],
            'closing_datetime_utc': row['closing_datetime_utc']
        }

sensors_site = {}
with open('C:/Users/Lenovo/Downloads/Test technique data engineer/sensors_site.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sensors_site[row['sensor_id']] = row['site_id']

records = {}
with open('C:/Users/Lenovo/Downloads/Test technique data engineer/records.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile)
    for row in reader:
        sensor_id = row['sensor_id']
        record_datetime = datetime.datetime.strptime(row['record_datetime'], '%Y-%m-%d %H:%M:%S')
        if sensor_id not in records or record_datetime > records[sensor_id]['record_datetime']:
            records[sensor_id] = {
                'sensor_name': row['sensor_name'],
                'record_datetime': record_datetime,
                'entries': int(row['entries']),
                'exits': int(row['exits'])
            }
          

# Détermination des alertes à déclencher
alerts = []
for sensor_id, record in records.items():
    try:
        site_id = sensors_site[sensor_id]
    except KeyError:
        print(f"La clé '{sensor_id}' n'est pas présente dans le dictionnaire 'sensors_site' !")
        continue
    
    site_timetable = site_timetables.get(site_id)
    if site_timetable:
        opening_datetime_utc = site_timetable['opening_datetime_utc']
        closing_datetime_utc = site_timetable['closing_datetime_utc']
        
print(records)

db.commit()

# Fermeture de la connexion à la base de données
db.close()