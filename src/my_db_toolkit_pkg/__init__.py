# src/my_db_toolkit_pkg/__init__.py

# Importar las clases principales de los conectores
from .oracle_connector import OracleThickConnector
from .mysql_connector import MySqlConnector
# Podrías añadir aquí el PostgreSqlConnector si lo implementas y quieres exponerlo directamente.

# Definir la versión del paquete
__version__ = "0.1.0" # Sincronizar con pyproject.toml

# Opcional: __all__ controla lo que se importa con 'from my_db_toolkit_pkg import *'
# Es buena práctica definirlo explícitamente.
__all__ = [
    'OracleThickConnector',
    'MySqlConnector',
    # 'PostgreSqlConnector', # Descomentar si se implementa
]

# No es necesario importar 'utils' aquí a menos que quieras exponer
# 'export_query_to_csv' directamente desde 'my_db_toolkit_pkg.export_query_to_csv'.
# Es más común importarlo como 'from my_db_toolkit_pkg.utils import export_query_to_csv'.

print(f"Paquete my_db_toolkit_pkg versión {__version__} cargado.")
