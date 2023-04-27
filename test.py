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
        if opening_datetime_utc is None or closing_datetime_utc is None or \
                (opening_datetime_utc <= analysis_datetime_utc.strftime('%Y-%m-%d %H:%M:%S') <= closing_datetime_utc):
            if analysis_datetime_utc - record['record_datetime'] > datetime.timedelta(hours=2):
                if analysis_datetime_utc - record['record_datetime'] > datetime.timedelta(hours=24):
                    alert_level = 2
                else:
                    alert_level = 1
                alerts.append({
                    'sensor_id': sensor_id,
                    'sensor_name': record['sensor_name'],
                    'site_id': site_id,
                    'alert_datetime_utc': analysis_datetime_utc,
                    'record_datetime_utc': record['record_datetime'],
                    'alert_level': alert_level,
                    'last_record': record['entries'] + record['exits'],
                    'last_recordtime': record['record_datetime']
                })

# Insertion des alertes dans la table "sensors_alerts"
if len(alerts) > 0:
    cursor = db.cursor()
    for alert in alerts:
        values = (
            None,
            alert['sensor_name'],
            alert['sensor_id'],
            alert['alert_level'],
            alert['alert_datetime_utc'],
            alert['site_id'],
            alert['last_record'],
            alert['last_recordtime']
        )
        sql = "INSERT INTO sensors_alerts (alert_id, sensor_name, sensor_id, alert_level, alert_datetime, site_id, last_record, last_recordtime) VALUES (NULL, %s, %s, %s, %s, %s, %s, %s)"
        cursor.execute(sql, values)
    db.commit()
    # Fermeture de la connexion à la base de données
db.close()