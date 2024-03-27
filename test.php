<?php




// Configuración de la base de datos
$servername = "localhost";
$username = "bi";
$password = "bi";
$dbname = "bi";

echo "****************************\n";
echo "***  Inicio del proceso  *** \n";
echo "****************************\n";

// URL del JSON
$json_url = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/EstacionesTerrestres/";
$json_url_municipios = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/Listados/Municipios/";
$json_url_comunidades = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/Listados/ComunidadesAutonomas/";
$json_url_provincias = "https://sedeaplicaciones.minetur.gob.es/ServiciosRESTCarburantes/PreciosCarburantes/Listados/Provincias/";

$response_estacionesterrestres = file_get_contents($json_url);
$data_estacionesterrestres = json_decode($response_estacionesterrestres, true);

$response_municipios = file_get_contents($json_url_municipios);
$data_municipios = json_decode($response_municipios, true);

$response_comunidades = file_get_contents($json_url_comunidades);
$data_comunidades = json_decode($response_comunidades, true);

$response_provincias = file_get_contents($json_url_provincias);
$data_provincias = json_decode($response_provincias, true);


// Conexión a la base de datos MySQL
$conn = new mysqli($servername, $username, $password, $dbname);

// Verificar la conexión
if ($conn->connect_error) {
    die("Error de conexión: " . $conn->connect_error);
}

echo "Conectado a la Base de datos\n";

// Crear tabla para municipios
$sql = "CREATE TABLE IF NOT EXISTS gas_municipios (
    id VARCHAR(255) PRIMARY KEY,
    nombre VARCHAR(255),
    id_provincia VARCHAR(255)
)";
$conn->query($sql);

// Crear tabla para comunidades autónomas
$sql = "CREATE TABLE IF NOT EXISTS gas_comunidades_autonomas (
    id VARCHAR(255) PRIMARY KEY,
    nombre VARCHAR(255)
)";
$conn->query($sql);

// Crear tabla para provincias
$sql = "CREATE TABLE IF NOT EXISTS gas_provincias (
    id VARCHAR(255) PRIMARY KEY,
    nombre VARCHAR(255),
    id_ccaa VARCHAR(255)
)";
$conn->query($sql);

// Crear tabla para tipos de combustibles
$sql = "CREATE TABLE IF NOT EXISTS gas_fuels (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(240)
)";
$conn->query($sql);

// Crear tabla para estaciones de gasolina
$sql = "CREATE TABLE IF NOT EXISTS gas_stations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(255),
    flag VARCHAR(100),
    address VARCHAR(255),
    postal_code VARCHAR(10),
    city VARCHAR(100),
    city2 VARCHAR(100),
    province VARCHAR(100),
    open_time VARCHAR(100),
    latitude VARCHAR(200),
    longitude VARCHAR(200)
)";
$conn->query($sql);

// Crear tabla para precios de combustibles
$sql = "CREATE TABLE IF NOT EXISTS gas_prices (
    id INT AUTO_INCREMENT PRIMARY KEY,
    station_id INT,
    fuel_id INT,
    price FLOAT,
    fecha DATE DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (station_id) REFERENCES gas_stations(id),
    FOREIGN KEY (fuel_id) REFERENCES gas_fuels(id)
)";
$conn->query($sql);

echo "*** Inicio de proceso ***\n";

echo "* Cargando Municipios\n";

$i = 0;


foreach ($data_municipios as $municipio) {
    $i++;
    $stmt = $conn->prepare("INSERT IGNORE INTO gas_municipios (id, nombre, id_provincia) VALUES (?, ?, ?)");
    $stmt->bind_param("sss", $municipio["IDMunicipio"], $municipio["Municipio"], $municipio["IDProvincia"]);
    $stmt->execute();
}

echo $i . " Municipios\n";

echo "* Cargando Provincias\n";
$i = 0;


foreach ($data_provincias as $provincia) {
    $stmt = $conn->prepare("INSERT IGNORE INTO gas_provincias (id, nombre, id_ccaa) VALUES (?, ?, ?)");
    $stmt->bind_param("sss", $provincia["IDPovincia"], $provincia["Provincia"], $provincia["CCAA"]);
    $stmt->execute();
    $i++;
}

echo $i . " Provincias\n";

echo "* Cargando Comunidades\n";
$i = 0;

foreach ($data_comunidades as $comunidad) {
    $stmt = $conn->prepare("INSERT IGNORE INTO gas_comunidades_autonomas (ID, nombre) VALUES (?, ?)");
    $stmt->bind_param("ss", $comunidad["CCAA"], $comunidad["IDCCAA"]);
    $stmt->execute();
    $i++;
}

echo $i . " Comunidades\n";

echo "* Cargando gasolineras\n";
$i = 0;
$j = 0;

foreach ($data_estacionesterrestres['ListaEESSPrecio'] as $station) {
    $i++;
    $sql = "SELECT COUNT(*) AS cuantos FROM gas_stations WHERE name='" . $station['IDEESS'] . "'";
    $result = $conn->query($sql);
    $row = $result->fetch_assoc();
    if ($row['cuantos'] == 0) {
        $j++;
        // Insertar estaciones
        $stmt = $conn->prepare("INSERT INTO gas_stations (name, flag, postal_code, address, city, city2, province, open_time, latitude, longitude) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        $stmt->bind_param("ssssssssss", $station['IDEESS'], $station['Rótulo'], $station['C.P.'], $station['Dirección'], $station['Localidad'], $station['Municipio'], $station['Provincia'], $station['Horario'], $station['Latitud'], $station['Longitud (WGS84)']);
        $stmt->execute();
        $stmt->close();
    }
}

echo "Cargadas " . $j . " de " . $i . " gasolineras\n";

echo "****\n";
echo "* Cargando productos y precios\n";


$i = 0;
$j = 0;
$k = 0;


foreach ($data_estacionesterrestres['ListaEESSPrecio'] as $station) {
    $i++;
    $campos = array_keys($station);
    foreach ($campos as $campo) {
        if (strpos($campo, "Precio") === 0) {
            $sql = "SELECT id AS cuantos FROM gas_fuels WHERE name='" . substr($campo, 7) . "'";
            $result = $conn->query($sql);
			$row = $result->fetch_assoc();
			if ($row['cuantos'] == 0) {
				$stmt = $conn->prepare("INSERT INTO gas_fuels (name) VALUES (?)");
				$stmt->bind_param("s", substr($campo, 7));
				$stmt->execute();
				$stmt->close();
				$k++;
				$sql = "SELECT id AS cuantos FROM gas_fuels WHERE name='" . substr($campo, 7) . "'";
				$result = $conn->query($sql);
				$row = $result->fetch_assoc();
			}
			$idproducto=$row['cuantos'];
			$sql = "SELECT COUNT(*) AS cuantos FROM gas_stations WHERE name='" . $station['IDEESS'] . "'";
			$result = $conn->query($sql);
			$row = $result->fetch_assoc();
			$idstation = $row['cuantos'];
			$pvp = (string)$station[(string)$campo];
			$pvp = str_replace(',','.',$pvp);
            if ($pvp !=""){
				$j++;
				$sql = "INSERT IGNORE INTO gas_prices (station_id, fuel_id, price) VALUES ('" .$idstation."', '" .$idproducto."', " .$pvp.")";
				$result = $conn->query($sql);
			}
		}
	}
}

echo "creados " . $k . "  nuevos productos \n";
echo "cargados  " . $j . "  nuevos precios\n";

echo "****\n";

// Cerrar la declaración
$stmt->close();

// Cerrar la conexión
$conn->close();

echo "Proceso completado.\n";
?>