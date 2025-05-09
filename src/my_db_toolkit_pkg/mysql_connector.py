# src/my_db_toolkit_pkg/mysql_connector.py
import mysql.connector # Esta es la dependencia de pyproject.toml
from mysql.connector import errorcode

class MySqlConnector:
    """
    Una clase para gestionar conexiones a MySQL.
    """
    def __init__(self, user, password, host, database, port=3306):
        """
        Inicializa el conector con las credenciales de MySQL.

        Args:
            user (str): Nombre de usuario de MySQL.
            password (str): Contraseña de MySQL.
            host (str): Host del servidor MySQL.
            database (str): Nombre de la base de datos a la que conectar.
            port (int, optional): Puerto del servidor MySQL. Defaults to 3306.
        """
        self.db_type = "mysql" # Atributo para identificar el tipo de BD
        self.user = user
        self.password = password
        self.host = host
        self.database = database
        self.port = port
        self.connection = None
        self.cursor = None

    def connect(self):
        """
        Establece la conexión con la base de datos MySQL.
        """
        if self.connection:
            print("MySQL Connector: Ya existe una conexión activa.")
            return

        try:
            print(f"MySQL Connector: Conectando a {self.host}:{self.port}/{self.database} como {self.user}...")
            self.connection = mysql.connector.connect(
                user=self.user,
                password=self.password,
                host=self.host,
                database=self.database,
                port=self.port,
                auth_plugin='mysql_native_password' # Común para versiones recientes de MySQL
            )
            self.cursor = self.connection.cursor()
            print("MySQL Connector: Conexión a MySQL establecida exitosamente.")
        except mysql.connector.Error as err:
            print(f"MySQL Connector: Error al conectar a MySQL: {err}")
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("MySQL Connector: Error de acceso - verifica usuario/contraseña.")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print(f"MySQL Connector: La base de datos '{self.database}' no existe.")
            else:
                print(err)
            self.connection = None # Asegurar que la conexión es None si falla
            self.cursor = None
            raise # Relanzar la excepción para que el llamador la maneje

    def disconnect(self):
        """
        Cierra el cursor y la conexión a la base de datos.
        """
        if self.cursor:
            try:
                self.cursor.close()
            except mysql.connector.Error as e:
                print(f"MySQL Connector: Error al cerrar el cursor: {e}")
            finally:
                self.cursor = None
        if self.connection and self.connection.is_connected():
            try:
                self.connection.close()
                print("MySQL Connector: Conexión a MySQL cerrada.")
            except mysql.connector.Error as e:
                print(f"MySQL Connector: Error al cerrar la conexión: {e}")
            finally:
                self.connection = None
        else:
            print("MySQL Connector: No hay conexión activa para cerrar o ya está cerrada.")

    def execute_query(self, sql_query, params=None):
        """
        Ejecuta una consulta SELECT y devuelve los encabezados y las filas.

        Args:
            sql_query (str): La consulta SQL a ejecutar.
            params (tuple or dict, optional): Parámetros para la consulta. 
                                             mysql-connector-python usa %s como placeholders.
                                             Si params es un diccionario, usa %(key)s.

        Returns:
            tuple: (list_of_headers, list_of_rows)
                   Retorna (None, None) si no hay conexión o si la consulta no es SELECT.
        
        Raises:
            mysql.connector.Error: Si ocurre un error durante la ejecución de la consulta.
        """
        if not self.connection or not self.cursor:
            raise ConnectionError("MySQL Connector: No hay conexión activa a la base de datos.")

        try:
            print(f"MySQL Connector: Ejecutando consulta: {sql_query[:100]}{'...' if len(sql_query) > 100 else ''}")
            
            # mysql-connector usa %s para placeholders, o %(name)s para diccionarios
            # El cursor.execute maneja la tupla de params directamente.
            self.cursor.execute(sql_query, params)

            if self.cursor.description:
                headers = [desc[0] for desc in self.cursor.description]
                rows = self.cursor.fetchall()
                # Las filas de mysql-connector pueden necesitar conversión si son tipos de datos complejos
                # pero para tipos estándar (números, strings, fechas) suelen estar bien.
                return headers, rows
            else:
                # Para INSERT, UPDATE, DELETE, description es None pero rowcount puede ser útil.
                # Para DDL, description también es None.
                print("MySQL Connector: Advertencia - La consulta no devolvió descripción de columnas (¿no es un SELECT?).")
                # Podrías devolver self.cursor.rowcount si es relevante para DML
                return None, None 
        except mysql.connector.Error as e:
            print(f"MySQL Connector: Error al ejecutar la consulta: {e}")
            raise # Relanzar para que el llamador la maneje
