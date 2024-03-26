import requests
import mysql.connector
from config import DB_CONFIG, JSON_URL_estacionesterrestres, url_municipios , url_comunidades, url_provincias 


print("Inicio del proceso")


print("*** Descargando JSON ") 
# Obtener datos del JSON
response = requests.get(JSON_URL_estacionesterrestres)
data = response.json()

response_municipios = requests.get(url_municipios)
data_municipios = response_municipios.json()

response_comunidades = requests.get(url_comunidades)
data_comunidades = response_comunidades.json()

response_provincias = requests.get(url_provincias)
data_provincias = response_provincias.json()

print("Conectado a los datos de gasolineras")
# Conexión a la base de datos MySQL
db_connection = mysql.connector.connect(**DB_CONFIG)
cursor = db_connection.cursor()

cursor2 = db_connection.cursor()
print("conectada a la Base de datos")

cursor.execute("""
    CREATE TABLE IF NOT EXISTS gas_municipios (
        id varchar(255) KEY,
        nombre VARCHAR(255),
        id_provincia VARCHAR(255)
    )
""")

# Crear tabla para comunidades autónomas
cursor.execute("""
    CREATE TABLE IF NOT EXISTS gas_comunidades_autonomas (
        id varchar(255)  PRIMARY KEY,
        nombre VARCHAR(255)
    )
""")

# Crear tabla para provincias
cursor.execute("""
    CREATE TABLE IF NOT EXISTS gas_provincias (
        id varchar(255) PRIMARY KEY,
        nombre VARCHAR(255),
        id_ccaa VARCHAR(255)
    )
""")

create_fuels_table_query = """
CREATE TABLE IF NOT EXISTS gas_fuels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(240)
)
"""

create_stations_table_query = """
CREATE TABLE IF NOT EXISTS gas_stations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    flag VARCHAR(100),
    address VARCHAR(255),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    city2 VARCHAR(100),
    province VARCHAR(100),
    open_time VARCHAR(100),
    latitude varchar(200),
    longitude  varchar(200)
)
"""

create_prices_table_query = """
CREATE TABLE IF NOT EXISTS gas_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id INT,
    fuel_id INT,
    price FLOAT,
    fecha date DEFAULT CURRENT_TIMESTAMP ,
    FOREIGN KEY (station_id) REFERENCES gas_stations(id),
    FOREIGN KEY (fuel_id) REFERENCES gas_fuels(id)
)
"""

cursor.execute(create_stations_table_query)
cursor.execute(create_fuels_table_query)
cursor.execute(create_prices_table_query)

db_connection.commit()
print("*** Inicio de proceso ***")

print("* Cargando Municipios")
i=0

for municipio in data_municipios:
    i=i+1
    cursor.execute("INSERT IGNORE INTO gas_municipios (id, nombre, id_provincia) VALUES (%s, %s, %s)", (municipio["IDMunicipio"], municipio["Municipio"], municipio["IDProvincia"]))

print (str(i) + " Municipios" )
print("* Cargando Provincias")
i=0


for provincia in data_provincias:
    cursor.execute("INSERT ignore INTO gas_provincias (id, nombre, id_ccaa) VALUES (%s, %s, %s)", (provincia["IDPovincia"], provincia["Provincia"],provincia["CCAA"]))
    i=i+1

print (str(i) + " Provincias" )
print("* Cargando Comunidades")
i=0

for comunidad in data_comunidades:
    cursor.execute("INSERT ignore INTO gas_comunidades_autonomas (ID, nombre) VALUES (%s, %s)", (comunidad["CCAA"], comunidad["IDCCAA"]))
    i=i+1

print (str(i) + " Comunidades" )

db_connection.commit()
print("* Cargando gasolineras")
i=0
j=0


for station in data['ListaEESSPrecio']:
    i=i+1
    sql="SELECT count(*) as cuantos FROM gas_stations WHERE name='" + station['IDEESS'] + "';"
    cursor2.execute(sql)
    a = cursor2.fetchone()
    if a[0]==0:
        j=j+1
        # Insertar estaciones
        station_data = (
            station['IDEESS'],
            station['Rótulo'],
            station['C.P.'],
            station['Dirección'],
            station['Localidad'],
            station['Municipio'],
            station['Provincia'],
            station['Horario'],
            station['Latitud'],
            station['Longitud (WGS84)']
        )
        insert_station_query = """
        INSERT INTO gas_stations  (name, flag, postal_code, address, city, city2, province, open_time, latitude, longitude)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_station_query, station_data)
        db_connection.commit()
        # station_id = cursor.lastrowid
    
print ("cargadas " + str(j) + " de " + str(i))
print("**** ")
print("* Cargando productos y precios")
i=0
j=0
k=0
for station in data['ListaEESSPrecio']:
    i=i+1
    campos = set(station.keys())
    for campo in campos:
        if campo.startswith("Precio"):
            sql="SELECT id as cuantos FROM gas_fuels WHERE name='" + campo[7:] + "';"
            cursor2.execute(sql)
            a = cursor2.fetchone()
            if a[0]==0:
                insert_fuel_query = " INSERT IGNORE INTO gas_fuels (name) VALUES ('" + campo[7:] + "') "
                cursor.execute(insert_fuel_query)
                db_connection.commit()
                k=k+1
                sql="SELECT id as cuantos FROM gas_fuels WHERE name='" + campo[7:] + "';"
                cursor2.execute(sql)
                a = cursor2.fetchone()
            # ************* cargo los precios
            b = str(a[0])
            sql="SELECT id FROM gas_stations WHERE name='" + str(station['IDEESS']) + "';"
            cursor2.execute(sql)
            a = cursor2.fetchone()
            p = str(station[str(campo)]).replace(',','.')
            if p!="":
                j=j+1
                insert_precio_query = " INSERT IGNORE INTO gas_prices (station_id, fuel_id, price) VALUES ('" + str(a[0]) + "', '" + b + "', " + p + ") "
                cursor.execute(insert_precio_query)
                db_connection.commit()

print ("creados " + str(k) + " nuevos productos")
print ("cargados " + str(j) + " nuevos precios")
cursor.close()
cursor2.close()
db_connection.close()
