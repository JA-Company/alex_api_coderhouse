import requests
import psycopg2

# Configuración de la API
api_url = 'URL_DE_LA_API'
api_params = {
    'parametro_1': 'valor_1',
    'parametro_2': 'valor_2',
    # Añade aquí los parámetros necesarios para la API
}

# Configuración de Redshift
redshift_config = {
    'dbname': 'NOMBRE_DE_LA_BASE_DE_DATOS',
    'user': 'USUARIO_DE_REDHSIFT',
    'password': 'CONTRASEÑA_DE_REDHSIFT',
    'host': 'HOST_DE_REDHSIFT',
    'port': 'PUERTO_DE_REDHSIFT',
}

# Extraer datos de la API
response = requests.get(api_url, params=api_params)
data = response.json()

# Crear tabla en Redshift (reemplaza con los nombres y tipos de columna adecuados)
create_table_query = '''
CREATE TABLE tabla_ejemplo (
    columna_1 tipo_de_dato_1,
    columna_2 tipo_de_dato_2,
    # Añade aquí las columnas necesarias
);
'''

# Cargar datos en Redshift
insert_query = '''
INSERT INTO tabla_ejemplo (columna_1, columna_2, ...)
VALUES (%s, %s, ...);
'''

try:
    # Conectarse a Redshift
    conn = psycopg2.connect(**redshift_config)
    cursor = conn.cursor()

    # Crear la tabla
    cursor.execute(create_table_query)

    # Cargar los datos en la tabla
    for item in data:
        # Extraer los valores necesarios del diccionario 'item' y pasarlos como parámetros
        cursor.execute(insert_query, (item['valor_columna_1'], item['valor_columna_2'], ...))

    # Confirmar los cambios
    conn.commit()

except (Exception, psycopg2.DatabaseError) as error:
    print('Error:', error)

finally:
    if conn is not None:
        conn.close()
