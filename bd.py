import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd



# ==================== CONEXIÓN A BASE DE DATOS ====================

def conexion_bd():
    """Conexión a la base de datos (ajusta según tu configuración)"""
    try:

        conn = psycopg2.connect(
            host='localhost',
            database='postgres',
            user='postgres',
            password='postgres',
            port=5432,
            cursor_factory=RealDictCursor
        )
        return conn
    except Exception as e:
        print(f"Error de conexión: {e}")
        return None

# ==================== FUNCIONES DE QUERY ====================

def normalizar_texto(texto) -> str:
    texto = texto.upper() if texto else None

    # retirar tilde a la ciudad
    texto = texto.replace('Á', 'A').replace('É', 'E').replace('Í', 'I').replace('Ó', 'O').replace('Ú', 'U')
    #la primera debe ser mayuscula
    #texto = texto.title() if texto else None
    return texto

def obtener_datos_celda(pais, departamento, ciudad: str = None, limit: int = 5000) -> pd.DataFrame:
    """Obtiene los datos de las celdas"""
    conn = conexion_bd()
    if not conn:
        return pd.DataFrame()
    
    # mayusculas para evitar problemas de case sensitive
    pais = normalizar_texto(pais).upper()
    departamento = departamento.upper()
    ciudad = ciudad.upper()    
    
    cursor = conn.cursor()
    
    query = """
        SELECT 
            id_celda,
            pais_region,
            departamento_region,
            ciudad_region,
            ST_AsGeoJSON(geometria) as geometry,
            ST_Y(centroide) as lat,
            ST_X(centroide) as lon,
            area_m2,
            fecha,
            round(temp_promedio::numeric, 2) as temp_promedio,
            round(precipitacion_promedio::numeric, 2) as precipitacion_promedio,
            round(humedad_promedio::numeric, 2) as humedad_promedio,
            round(elevacion_promedio::numeric, 2) as elevacion_promedio,
            round(viento_promedio::numeric, 2) as viento_promedio,
            round(puntuacion_calidad_datos::numeric, 2) as puntuacion_calidad_datos
        FROM celdas_terreno
    """
    if ciudad:
        query += f" WHERE Upper (ciudad_region) = '{ciudad}' and Upper (pais_region) = '{pais}' AND Upper (departamento_region) = '{departamento}'"
    
    query += f" LIMIT {limit}"
    
    cursor.execute(query)
    rows = cursor.fetchall()
    cursor.close()
    
    df = pd.DataFrame(rows)
    
    columnas_numericas = [
        'area_m2', 'lat', 'lon', 'temp_promedio', 
        'precipitacion_promedio', 'humedad_promedio', 
        'elevacion_promedio', 'viento_promedio', 'puntuacion_calidad_datos'
    ]
    
    for col in columnas_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    return df

