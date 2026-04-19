import bd
import json
import os
from dotenv import load_dotenv
import ee
import osmnx as ox
from shapely.geometry import shape, LineString, Point, MultiLineString
from ingesta_ee import obtener_ubicacion_geografica
from funciones import log

# =============================================================================
# Configuración de OSMNX
# =============================================================================
ox.settings.use_cache = True
ox.settings.log_console = False
ox.settings.timeout = 180  # 3 minutos de timeout para evitar cortes

# Aumentar el límite de área permitida a 25,000 km2 (para la mayoría de ciudades)
# para no saturar las consultas.
# El valor está en metros cuadrados. 25,000,000,000 m2.
ox.settings.max_query_area_size = 25 * 1000 * 1000 * 1000 * 1000    

#################################
# Cargar variables de entorno
#################################

load_dotenv(dotenv_path="_mientorno.env") 
gee_project = os.getenv("GEE_PROJECT")
_ee_inicializado = False

#################################
# Sesion en Google Earth Engine
#################################

def gee_inicializar():
    """Autentica e inicializa la API de Google Earth Engine."""
    global _ee_inicializado
    
    if _ee_inicializado:
        log("✅ GEE ya está inicializado")
        return
    
    import ee
    log("... Autenticando GEE...")
    ee.Authenticate()
    try:
        ee.Initialize(project=gee_project)
        log(f"... GEE inicializado con proyecto: {gee_project}")
        _ee_inicializado = True
    except Exception as e:
        log(f"Error al inicializar GEE: {e}")
        _ee_inicializado = False

# Catálogo actualizado con fuentes reales
CATALOGO_REFERENCIAS = {
    "rios": {
        "fuente": "GEE: WWF/HydroSHEDS", 
        "descripcion": "Ríos reales obtenidos de Google Earth Engine"
    },
    "carreteras": {
        "fuente": "OpenStreetMap (OSMnx)", 
        "descripcion": "Red vial real transitable"
    },
    "puntos_interes": {
        "fuente": "OpenStreetMap (OSMnx)", 
        "descripcion": "Infraestructura real (Mercados, Escuelas, etc)"
    }
}

def verificar_existencia_referencia(conexion, pais, departamento, ciudad, tipo):
    """Retorna True si ya tenemos datos de ese tipo para esa ciudad"""
    try:
        cur = conexion.cursor()
        cur.execute("""
            SELECT 1 FROM referencias_geo 
            WHERE pais_region = %s AND departamento_region = %s AND ciudad_region = %s AND tipo = %s LIMIT 1
        """, (pais, departamento, ciudad, tipo))
        res = cur.fetchone()
        cur.close() 
        return bool(res)
    except:
        return False


def geojson_a_wkt(geojson_geom):
    """Convierte geometría GeoJSON a WKT para PostGIS"""
    try:
        g = shape(geojson_geom)
        return g.wkt
    except:
        return None

def ingestar_referencia_demanda(pais, departamento, ciudad, tipo):
    """
    INGESTA REAL:
    - Rios -> Google Earth Engine (WWF/HydroSHEDS)
    - Carreteras/POIs -> OpenStreetMap (OSMnx)
    """

    # Calcula el bounding box de la ciudad para limitar las consultas
    bbox, bb_real = obtener_ubicacion_geografica(pais, departamento, ciudad)
    if bbox is None:
        return f"Error: No hay celdas base para {ciudad}. Ejecuta ingesta de terreno primero."

    min_lon, min_lat, max_lon, max_lat = bbox
    datos_a_insertar = []
    log(f"Ingestando {tipo.upper()} Real para {ciudad}")
    log(f"Bounding Box: [{min_lon}, {min_lat}] a [{max_lon}, {max_lat}]")

    try:
        # ---------------------------------------------------------
        # CASO 1: RÍOS (Fuente: Google Earth Engine)
        # ---------------------------------------------------------
        if tipo == "rios":
            # Region
            roi = ee.Geometry.Rectangle([min_lon, min_lat, max_lon, max_lat])
            
            # Colección de ríos de WWF
            rios_fc = ee.FeatureCollection("WWF/HydroSHEDS/v1/FreeFlowingRivers").filterBounds(roi)
            
            # Descargar datos a la BD local (limitar a 50 para no saturar)
            features = rios_fc.limit(2000).getInfo()['features']
            
            if not features:
                return f"No se encontraron ríos en GEE para {ciudad}."

            for f in features:
                props = f['properties']
                nombre = props.get('RIV_ORD', 'Rio Sin Nombre') # O usar otro campo disponible
                geom_geojson = f['geometry']
                
                wkt_geom = geojson_a_wkt(geom_geojson)
                if wkt_geom:
                    # Guardamos el orden del río como nombre o una etiqueta genérica
                    etiqueta = f"Río (Orden {nombre})"
                    datos_a_insertar.append((pais, departamento, ciudad, 'rios', etiqueta, wkt_geom))

        # ---------------------------------------------------------
        # CASO 2: CARRETERAS (Fuente: OpenStreetMap via OSMnx)
        # ---------------------------------------------------------
        elif tipo == "carreteras":
            log("Consultando OpenStreetMap (puede tardar unos segundos)...")
            # extrar grafo de Ox no de bbox, sino de perimetro real (bb_real) para evitar errores de área grande
            
            try:
                #filtro = {'highway': ['motorway', 'trunk', 'primary', 'secondary', 'tertiary']}
                filtro = {'highway': ['motorway', 'trunk', 'primary']}
                bb_shapely = shape(bb_real) if not hasattr(bb_real, 'geom_type') else bb_real
                if bb_shapely.geom_type != 'Polygon':
                    bb_shapely = bb_shapely.convex_hull
                gdf_edges = ox.features_from_polygon(bb_shapely, tags=filtro)
                # Quedarse solo con líneas (descartar nodos puntuales sueltos)
                gdf_edges = gdf_edges[gdf_edges.geometry.geom_type.isin(['LineString', 'MultiLineString'])]
                log(f"Se encontraron {len(gdf_edges)} tramos. Simplificando...")

                #test (gdf_edges)

                # Normalizar el campo highway (a veces viene como lista)
                gdf_edges['highway_tipo'] = gdf_edges['highway'].apply(
                    lambda x: x[0] if isinstance(x, list) else str(x)
                )

                for _, row in gdf_edges.iterrows():
                    nombre = row.get('name', row.get('highway_tipo', 'Vía Desconocida'))
                    if isinstance(nombre, list): nombre = nombre[0]
                    wkt_geom = row['geometry'].wkt
                    datos_a_insertar.append((pais, departamento, ciudad, 'carreteras', str(nombre), wkt_geom))
                    datos_a_insertar.append((pais, departamento, ciudad, 'carreteras', str(nombre), wkt_geom))
            except Exception as e:
                return f"Error OSM Carreteras: {str(e)}"


        # ---------------------------------------------------------
        # INSERCIÓN EN BASE DE DATOS
        # ---------------------------------------------------------
        if not datos_a_insertar:
            return f"No se encontraron datos reales de {tipo} en la zona."

        conn = bd.conexion_bd()
        cur = conn.cursor()
        
        # Usamos ST_GeomFromText para convertir el WKT de Python a Geometría PostGIS
        cur.executemany("""
            INSERT INTO referencias_geo (pais_region, departamento_region, ciudad_region, tipo, nombre, geometria)
            VALUES (%s, %s, %s, %s, %s, ST_SetSRID(ST_GeomFromText(%s), 4326))
        """, datos_a_insertar)
        
        conn.commit()
        cur.close()
        conn.close()
        
        return f"INGESTA REAL: Se guardaron {len(datos_a_insertar)} elementos de {tipo}."

    except Exception as e:
        return f"Error crítico en ingesta real: {str(e)}"


def carga_referencias(pais, departamento, ciudad, criterios):
    """
    Ingesta las referencias geográficas solicitadas si no existen.
    """
    conn = bd.conexion_bd()
    
    # Asegurar ingesta
    logs_ingesta = []
    for crit in criterios:
        tipo = crit['referencia']
        log(tipo)
        if not verificar_existencia_referencia(conn, pais, departamento, ciudad, tipo):
            res = ingestar_referencia_demanda(pais, departamento, ciudad, tipo)
            logs_ingesta.append(res)
    return {
            "logs": logs_ingesta,
            "celdas_filtradas": [],
            "referencias_mapa": []
            }

def analisis_postgis(pais, departamento, ciudad, criterios):
    """
    Construye una QUERY DINÁMICA DE POSTGIS basada en N criterios.
    """
    logs_ingesta = []
    conn = bd.conexion_bd()
    
    # 2. Construir Query (Igual que antes)
    sql_base = """
        SELECT c.id_celda, c.lat, c.lon, round(puntuacion_calidad_datos::numeric, 2) as puntuacion_calidad, c.temp_promedio, c.humedad_promedio
        FROM celdas_terreno c
        WHERE UPPER (c.ciudad_region) = UPPER (%s)
        AND UPPER (c.departamento_region) = UPPER (%s)
        AND UPPER (c.pais_region) = UPPER (%s)
    """
    params = [ciudad, departamento, pais]
    

    for crit in criterios:
        tipo = crit['referencia']
        dist_metros = crit['distancia']
        
        subquery = """
        EXISTS (
            SELECT 1 FROM referencias_geo r 
            WHERE UPPER (r.ciudad_region) = UPPER (c.ciudad_region) 
            AND UPPER (r.departamento_region) = UPPER (c.departamento_region) 
            AND UPPER (r.pais_region) = UPPER (c.pais_region) 

            AND r.tipo = %s 
            AND ST_DWithin(c.geometria::geography, r.geometria::geography, %s)
        )
        """
        
        if crit['condicion'] == 'cerca':
            sql_base += f" AND {subquery}"
        else:
            sql_base += f" AND NOT {subquery}"
            
        params.extend([tipo, dist_metros])
        
    sql_base += " ORDER BY round(puntuacion_calidad_datos::numeric, 2) DESC;"
    log ('SQL: ' + sql_base + ' - ' + str(params))

    # 3. Ejecutar y devolver
    try:
        import pandas as pd
        cursor = conn.cursor()
        log ('Ejecutando consulta en PostGIS...')
        cursor.execute(sql_base, tuple(params))
        cols = [desc[0] for desc in cursor.description]
        filas = cursor.fetchall()
        log (f"Se encontraron {len(filas)} celdas que cumplen los criterios.")

        df = pd.DataFrame(filas, columns=cols)
        
        # Recuperar geometrías para mapa
        tipos_usados = list(set([c['referencia'] for c in criterios]))
        sql_ref = "SELECT tipo, ST_AsGeoJSON(geometria) as geojson FROM referencias_geo WHERE UPPER (ciudad_region) = UPPER (%s) AND UPPER (departamento_region) = UPPER (%s) AND UPPER (pais_region) = UPPER (%s)   AND tipo = ANY(%s)"
        log ('Recuperando referencias para mapa...')
        cursor.execute(sql_ref, (ciudad, departamento, pais, tipos_usados))
        filas_ref = cursor.fetchall()

        referencias_visuales = []
        if filas_ref:
            for row in filas_ref:
                # Detectar si es tupla o dict
                if isinstance(row, (tuple, list)):
                    tipo_val = row[0]
                    geo_val = row[1]
                else:
                    # Es un Diccionario
                    tipo_val = row['tipo']
                    geo_val = row['geojson']

                # Si geo_val es string, decodificar; si ya es dict, usar directo
                if isinstance(geo_val, str):
                    geojson_geom = json.loads(geo_val)
                else:
                    geojson_geom = geo_val

                # Empaquetar siempre como Feature GeoJSON estándar
                feature = {
                    "type": "Feature",
                    "geometry": geojson_geom,
                    "properties": {"tipo": tipo_val}
                }

                referencias_visuales.append({
                    "tipo": tipo_val,
                    "geojson": feature
                })
            
        cursor.close()
        conn.close()
        
        return {
            "logs": logs_ingesta,
            "celdas_filtradas": df.to_dict(orient='records'),
            "referencias_mapa": referencias_visuales
        }
    except Exception as e:
        return {"logs": logs_ingesta + [f"Error SQL: {e}"], "celdas_filtradas": [], "referencias_mapa": []}

    
if __name__ == "__main__":
    gee_inicializar()
