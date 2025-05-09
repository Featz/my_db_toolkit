# src/my_db_toolkit_pkg/oracle_connector.py
import oracledb
import os

class OracleThickConnector:
    """
    Una clase para gestionar conexiones a Oracle en modo grueso.
    Requiere que Oracle Instant Client esté instalado y su ruta sea proporcionada.
    """
    _oracle_client_initialized = False # Variable de clase para rastrear la inicialización

    def __init__(self, user, password, dsn, instant_client_dir):
        """
        Inicializa el conector con las credenciales y la ruta del Instant Client.

        Args:
            user (str): Nombre de usuario de Oracle.
            password (str): Contraseña de Oracle.
            dsn (str): DSN de Oracle (ej: 'localhost:1521/ORCLPDB1').
            instant_client_dir (str): Ruta al directorio de Oracle Instant Client.
        """
        self.db_type = "oracle" # Atributo para identificar el tipo de BD
        self.user = user
        self.password = password
        self.dsn = dsn
        self.instant_client_dir = instant_client_dir
        self.connection = None
        self.cursor = None

        if not OracleThickConnector._oracle_client_initialized:
            self._initialize_oracle_client()

    def _initialize_oracle_client(self):
        """
        Inicializa python-oracledb para usar el modo grueso con el Instant Client especificado.
        """
        if not self.instant_client_dir or not os.path.isdir(self.instant_client_dir):
            raise ValueError(
                f"La ruta del Instant Client para Oracle '{self.instant_client_dir}' no es válida o no existe."
            )
        
        try:
            print(f"Oracle Connector: Intentando inicializar Oracle Client desde: {self.instant_client_dir}")
            oracledb.init_oracle_client(lib_dir=self.instant_client_dir)
            OracleThickConnector._oracle_client_initialized = True
            print("Oracle Connector: Cliente Oracle inicializado en modo grueso exitosamente.")
        except oracledb.Error as e:
            if "DPY-INIT-002" in str(e): # Cliente ya inicializado
                OracleThickConnector._oracle_client_initialized = True
                print("Oracle Connector: Advertencia - El cliente Oracle ya estaba inicializado.")
            else:
                print(f"Oracle Connector: Error al inicializar Oracle Client: {e}")
                raise RuntimeError(f"Fallo al inicializar Oracle Client desde {self.instant_client_dir}: {e}") from e

    def connect(self):
        """
        Establece la conexión con la base de datos Oracle.
        """
        if not OracleThickConnector._oracle_client_initialized:
            print("Oracle Connector: Advertencia - El cliente Oracle no estaba inicializado antes de conectar. Intentando ahora.")
            self._initialize_oracle_client()

        if self.connection:
            print("Oracle Connector: Ya existe una conexión activa.")
            return

        try:
            print(f"Oracle Connector: Conectando a {self.dsn} como {self.user}...")
            self.connection = oracledb.connect(
                user=self.user,
                password=self.password,
                dsn=self.dsn
            )
            if not self.connection.thick:
                print("Oracle Connector: ADVERTENCIA - La conexión NO está en modo grueso a pesar de la inicialización.")
            else:
                print("Oracle Connector: Conexión en modo grueso confirmada.")
            
            self.cursor = self.connection.cursor()
            print("Oracle Connector: Conexión a Oracle establecida exitosamente.")
        except oracledb.Error as e:
            print(f"Oracle Connector: Error al conectar a Oracle: {e}")
            self.connection = None
            self.cursor = None
            raise

    def disconnect(self):
        """
        Cierra el cursor y la conexión a la base de datos.
        """
        if self.cursor:
            try:
                self.cursor.close()
            except oracledb.Error as e:
                print(f"Oracle Connector: Error al cerrar el cursor: {e}")
            finally:
                self.cursor = None
        if self.connection:
            try:
                self.connection.close()
                print("Oracle Connector: Conexión a Oracle cerrada.")
            except oracledb.Error as e:
                print(f"Oracle Connector: Error al cerrar la conexión: {e}")
            finally:
                self.connection = None
        else:
            print("Oracle Connector: No hay conexión activa para cerrar.")

    def execute_query(self, sql_query, params=None):
        """
        Ejecuta una consulta SELECT y devuelve los encabezados y las filas.
        """
        if not self.connection or not self.cursor:
            raise ConnectionError("Oracle Connector: No hay conexión activa a la base de datos.")

        try:
            print(f"Oracle Connector: Ejecutando consulta: {sql_query[:100]}{'...' if len(sql_query) > 100 else ''}")
            if params:
                self.cursor.execute(sql_query, params)
            else:
                self.cursor.execute(sql_query)

            if self.cursor.description:
                headers = [desc[0] for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                return headers, rows
            else:
                print("Oracle Connector: Advertencia - La consulta no devolvió descripción de columnas (¿no es un SELECT?).")
                return None, None
        except oracledb.Error as e:
            print(f"Oracle Connector: Error al ejecutar la consulta: {e}")
            raise
