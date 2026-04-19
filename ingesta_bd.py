from psycopg2.extras import execute_batch
from shapely.geometry import box, shape
from shapely.prepared import prep
from shapely.wkt import loads as wkt_loads
from typing import List, Dict

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from funciones import log
import ee
import os
import bd


try:
    import ingesta_ee
    log("Módulo ingesta cargado correctamente")
except Exception as e:
    log(f"Advertencia: No se pudo cargar ingesta: {str(e)}")
    log("Algunas funcionalidades pueden no estar disponibles")

# 1. CONFIGURACIÓN DE GOOGLE EARTH ENGINE

gee_project = os.getenv("GEE_PROJECT", "")

# Sesion en Google Earth Engine
_ee_initialized = False

def gee_inicializar():
    """Autentica e inicializa la API de Google Earth Engine."""
    global _ee_initialized
    if _ee_initialized:
        log("GEE ya está inicializado")
        return
    
    log("Abriendo navegador para autenticación de GEE...")
    ee.Authenticate()
    ee.Initialize(project=gee_project)
    ee_initialized = True

gee_inicializar()


def generar_id_celda(lat: float, lon: float, pais: str, departamento: str, ciudad: str, 
                     tamaño_grilla: int = 500) -> str:
    """
    Genera un id_celda único y descriptivo
    Formato: {PAIS}_{DEPARTAMENTO}_{CIUDAD}_{TAMAÑO_GRILLA}m_{LAT}_{LON}
    Ejemplo: PE_CAJ_500m_-7.1234_-78.5678
    """
    # Redondear a la grilla
    lat_redondeado = round(lat, 4)
    lon_redondeado = round(lon, 4)
    
    id_celda = f"{pais}_{departamento}_{ciudad}_{tamaño_grilla}m_{lat_redondeado}_{lon_redondeado}"
    return id_celda




class IngestionCeldasTerreno:
    def __init__(self, conn):
        self.conexion = conn
        self.cursor = self.conexion.cursor()
    
    def crear_celdas_grilla(self, limites: tuple, tamaño_celda_m) -> List[Dict]:
        """
        Crea una grilla de celdas para una región
        limites: (lon_min, lat_min, lon_max, lat_max)
        tamaño_celda_m: tamaño de celda en metros
        """
        lon_min, lat_min, lon_max, lat_max = limites
        
        # Convertir metros a grados (aproximado)
        # 1 grado ≈ 111 km
        tamaño_celda_grados = tamaño_celda_m / (111000)

        celdas = []
        lat = lat_min
        
        while lat < lat_max:
            lon = lon_min
            while lon < lon_max:
                # Crear polígono de la celda
                poligono_celda = box(lon, lat, 
                                  lon + tamaño_celda_grados, 
                                  lat + tamaño_celda_grados)
                
                centroide = poligono_celda.centroid
                
                celda = {
                    'id_celda': generar_id_celda(
                        centroide.y, centroide.x, 
                        'PE', 'LIM', tamaño_celda_m
                    ),
                    'geometria': poligono_celda.wkt,
                    'centroide': centroide.wkt,
                    'centroide_lat': centroide.y,
                    'centroide_lon': centroide.x,
                    'area_m2': tamaño_celda_m * tamaño_celda_m
                }
                
                celdas.append(celda)
                lon += tamaño_celda_grados
            lat += tamaño_celda_grados
        
        return celdas
    
    def cargar_datos_estadisticos(self, celda: Dict) -> Dict:
        #
        #Carga la celda con datos climáticos mensuales desde Google Earth Engine
        #Retorna diccionario con datos por mes
        #
        fecha_inicio = '2025-01-01'
        fecha_fin = '2025-12-31'

        lat = celda['centroide_lat']
        lon = celda['centroide_lon']

        try:
            import ee
            
            
            # Convertir WKT a geometría de GEE
            geometria_shapely = wkt_loads(celda['geometria'])
            coords = list(geometria_shapely.exterior.coords)
            geometria = ee.Geometry.Polygon([coords])
            
            # Obtener datos mensuales de GEE
            datos_mensuales = ingesta_ee.cargar_caracteristicas_celda_gee(geometria, fecha_inicio, fecha_fin)
            
            # Convertir formato de datos mensuales a formato para inserción
            celda['datos_mensuales'] = datos_mensuales
            
            return celda
        except Exception as e:
            log(f"Error cargando GEE para ({lat},{lon}): {e}")
            celda['datos_mensuales'] = {}
            return celda
        
    def insertar_celdas(self, celdas: List[Dict], tamaño_lote: int = 100):
        """
        Convierte cada celda en 12 registros (uno por cada mes)
        """        
        # Expandir las celdas en registros mensuales
        registros_mensuales = []
        
        for celda in celdas:
            if 'datos_mensuales' not in celda or not celda['datos_mensuales']:
                log(f"Celda {celda['id_celda']} no tiene datos mensuales")
                continue
            
            datos = celda['datos_mensuales']
            
            # Iterar sobre los 12 meses
            for mes_str, datos_mes in datos.items():
                if mes_str == 'elevacion':
                    continue
                
                try:
                    partes = mes_str.split('-')
                    año = int(partes[0])
                    mes = int(partes[1])
                    objeto_fecha = datetime(año, mes, 1)
                    
                    registro = {
                        'id_celda': celda['id_celda'],
                        'pais_region': celda.get('pais_region', 'PERU'),
                        'departamento_region': celda.get('departamento_region', 'LIM'),
                        'ciudad_region': celda.get('ciudad_region', 'LIMA'),
                        'geometria': celda['geometria'],
                        'centroide': celda['centroide'],
                        'area_m2': celda['area_m2'],
                        'fecha': objeto_fecha,
                        'temp_promedio': datos_mes.get('temp_promedio', None),
                        'precipitacion_promedio': datos_mes.get('precip_promedio', None),
                        'humedad_promedio': datos_mes.get('humedad_promedio', None),
                        'elevacion_promedio': datos.get('elevacion', None),
                        'viento_promedio': datos_mes.get('viento_promedio', None),
                        'puntuacion_calidad_datos': celda.get('puntuacion_calidad_datos', 0.8),
                    }
                    registros_mensuales.append(registro)
                except Exception as e:
                    log(f"Error conversión mes {mes_str} para celda {celda['id_celda']}: {e}")
                    continue
        
        # Insertar registros mensuales
        consulta_insertar = """
        INSERT INTO celdas_terreno (
            id_celda, pais_region, departamento_region, ciudad_region,
            geometria, centroide, lat, lon,
            area_m2, fecha,
            temp_promedio, precipitacion_promedio, humedad_promedio, elevacion_promedio, viento_promedio,
            puntuacion_calidad_datos, ultima_actualizacion
        ) VALUES (
            %(id_celda)s, %(pais_region)s, %(departamento_region)s, %(ciudad_region)s,
            ST_GeomFromText(%(geometria)s, 4326),
            ST_GeomFromText(%(centroide)s, 4326),
            ST_Y(ST_GeomFromText(%(centroide)s, 4326)), ST_X(ST_GeomFromText(%(centroide)s, 4326)),
            %(area_m2)s, %(fecha)s,
            %(temp_promedio)s, %(precipitacion_promedio)s, %(humedad_promedio)s,  %(elevacion_promedio)s, %(viento_promedio)s,    
            %(puntuacion_calidad_datos)s, NOW()
        )
        ON CONFLICT (id_celda, fecha) DO UPDATE SET
            temp_promedio = EXCLUDED.temp_promedio,
            precipitacion_promedio = EXCLUDED.precipitacion_promedio,
            humedad_promedio = EXCLUDED.humedad_promedio,
            elevacion_promedio = EXCLUDED.elevacion_promedio,
            viento_promedio = EXCLUDED.viento_promedio,
            ultima_actualizacion = NOW();
        """
        
        total_registros = len(registros_mensuales)
        insertados = 0
        fallidos = 0
        
        for idx_lote in range(0, total_registros, tamaño_lote):
            lote = registros_mensuales[idx_lote:idx_lote + tamaño_lote]
            try:
                execute_batch(self.cursor, consulta_insertar, lote, page_size=tamaño_lote)
                self.conexion.commit()
                insertados += len(lote)
                log(f"Lote {idx_lote // tamaño_lote + 1}: {len(lote)} registros mensuales insertados")
            except Exception as e:
                self.conexion.rollback()
                fallidos += len(lote)
                log(f"❌Error de lote en índice {idx_lote}: {e}")
        
        log(f"Resumen inserción en BD: {insertados} registros insertados, {fallidos} fallidos de {total_registros} total")
    


    
    def cargar_celda(self, celda: Dict, idx_celda: int, total_celdas: int) -> Dict:
        """
        Carga una celda individual con datos mensuales desde GEE
        """
        try:
            # Obtener datos mensuales completos (12 meses)
            celda = self.cargar_datos_estadisticos(celda)
            
            # Calcular score de calidad (según disponibilidad de datos)
            celda['puntuacion_calidad_datos'] = self.calcular_puntuacion_calidad(celda)
            
            if (idx_celda + 1) % 10 == 0 or idx_celda == 0:
                log(f"   Progreso: {idx_celda + 1}/{total_celdas} celdas cargadas")
            
            return celda
        except Exception as e:
            log(f"Error al prerparar celda {idx_celda}: {e}")
            celda['datos_mensuales'] = {}
            return celda
    
    
    def ingestar_ubicacion_geografica(self, pais: str, departamento: str, ciudad: str,  
                     tamaño_celda_m):
        
        limites_bb, limites_real = ingesta_ee.obtener_ubicacion_geografica(pais, departamento, ciudad)
        #  ingesta de  una región con carga de características GEE
        
        log(f"Iniciando ingesta de terreno para: {ciudad}, {departamento}, {pais}")
        log(f"Límites: {limites_bb} | Tamaño celda: {tamaño_celda_m}m")
        
        if _ee_initialized:
            log(f"MODO: Google Earth Engine ")        
        try:
            # Inicializar GEE si está disponible
            if _ee_initialized:
                log("Inicializando Google Earth Engine...")
            
            # Crear grid
            log("Creando celdas de grilla...", limites_bb)
            celdas_rectangulares = self.crear_celdas_grilla(limites_bb, tamaño_celda_m)
            log(f"Se crearon {len(celdas_rectangulares)} celdas de grilla\n")
            
            # Filtrado por polígono real
            log("Recortando grilla usando la geometría real del distrito...")
            
            # Convertir el GeoJSON de GEE a una forma de Shapely
            poligono_region = shape(limites_real)

            poligono_region = poligono_region.simplify(0.001, preserve_topology=True)
            
            # 'prep' acelera la verificación de intersección
            poligono_preparado = prep(poligono_region) 
            
            celdas_filtradas = []
            
            for celda in celdas_rectangulares:
                # Reconstruir la geometría de la celda (bbox) a partir de sus coordenadas
                geom_celda = wkt_loads(celda['geometria'])               
                
                # Verificar si la celda toca el polígono real
                if poligono_preparado.intersects(geom_celda):
                    celdas_filtradas.append(celda)
            
            # Reemplazar la lista original con la filtrada
            celdas = celdas_filtradas
            if len(celdas) == 0:
                log("Alerta: El filtrado eliminó todas las celdas. Verificar si los límites de GEE son correctos.")
                return
                
            log(f"Celdas dentro de la región: {len(celdas)} (se descartaron {len(celdas_rectangulares) - len(celdas)})")
            # ----------------------------------------------------


            # Agregar datos básicos a todas las celdas
            for celda in celdas:
                celda['pais_region'] = pais
                celda['departamento_region'] = departamento
                celda['ciudad_region'] = ciudad
            
            # Cargar datos - 1 hilo para evitar saturar GEE
            tiempo_inicio = time.time()
            
            celdas_cargadas = []
            with ThreadPoolExecutor(max_workers=1) as ejecutor:
                # Enviar todas las celdas para carga paralelo
                futuros = {
                    ejecutor.submit(self.cargar_celda, celda, idx, len(celdas)): idx 
                    for idx, celda in enumerate(celdas)
                }
                
                # Recopilar resultados a medida que se completan
                for futuro in as_completed(futuros):
                    celdas_cargadas.append(futuro.result())
            
            tiempo_transcurrido = time.time() - tiempo_inicio
            log(f"Carga completada para {len(celdas_cargadas)} celdas en {tiempo_transcurrido:.1f} segundos\n")
            
            # Insertar en BD
            log("💾 Insertando celdas en base de datos PostgreSQL...")
            self.insertar_celdas(celdas_cargadas)
            tiempo_total = time.time() - tiempo_inicio
            log(f"Tiempo total: {tiempo_total:.1f} segundos ({tiempo_total/60:.1f} minutos)")
            
        except Exception as e:
            log(f"\nError durante la ingesta: {e}")
            raise
    

    def calcular_puntuacion_calidad(self, celda: Dict) -> float:
        """
        Calcula un score de calidad basado en completitud de datos mensuales
        """
        if 'datos_mensuales' not in celda or not celda['datos_mensuales']:
            return 0.0
        
        datos = celda['datos_mensuales']
        
        # Contar meses con datos completos
        meses_completos = 0
        campos_requeridos = ['temp_promedio', 'precipitacion_promedio', 'humedad_promedio', 'viento_promedio']
        
        for mes_str, datos_mes in datos.items():
            if mes_str == 'elevacion':  # Este no requiere información mensual
                continue
            
            # Verificar si el mes tiene todos los campos requeridos
            if all(datos_mes.get(campo) is not None for campo in campos_requeridos):
                meses_completos += 1
        
        # Score: proporción de meses completos / 12 meses
        return min(1.0, (meses_completos / 12.0) if meses_completos > 0 else 0.8)


    def existe_ubicacion_geografica (self, pais: str, departamento: str, ciudad: str) -> bool:
        """
        Consulta rápida para saber si ya existen celdas de esa ciudad.
        Retorna True si hay datos, False si no.
        """
        try:

            cur = self.conexion.cursor()
            # Consulta optimizada: SELECT 1 ... LIMIT 1 es muy rápido
            # Normalizamos a mayúsculas para evitar errores de "Lima" vs "LIMA"
            consulta = """
                SELECT 1 FROM celdas_terreno 
                WHERE UPPER(pais_region) = UPPER(%s)
                AND UPPER(departamento_region) = UPPER(%s)
                AND UPPER(ciudad_region) = UPPER(%s)
                LIMIT 1;
            """
            cur.execute(consulta, (pais, departamento, ciudad))
            
            existe = False
            if cur.fetchone():
                existe = True
            
            cur.close()

        except Exception as e:
            log(f"Error verificando existencia: {e}")
            return False
            
        return existe



# Funcion principal
def ingestar(pais, departamento, ciudad):
    try:
        conn = bd.conexion_bd()
        if not conn:
            log("No se pudo conectar a la base de datos.")
        else:   
            ingestion = IngestionCeldasTerreno(conn)
            ingestion.ingestar_ubicacion_geografica(pais, departamento, ciudad, tamaño_celda_m=5000)
            ingestion.conexion.close()


    except Exception as e:
        log(f"Error fatal: {e}")
        raise


# hacer una funcion para paises que devuelvan en JSON
def obtener_paises() -> str:
    """
    Consulta la base de datos para obtener la lista de países disponibles
    Retorna el JSON de la forma: [{"iso_a3": "PER", "nombre": "Peru"}, ...]
    """

    try:
        conexion = bd.conexion_bd()
        cur = conexion.cursor()
        consulta = """
            SELECT json_agg(t) FROM (
                SELECT DISTINCT pais 
                FROM maestro_lugares
                ORDER BY pais
            ) t;
        """
        cur.execute(consulta)
        resultados = cur.fetchall()
        # El resultado es de tipo JSON
        paises = resultados[0]['json_agg'] if resultados and resultados[0] else []    
        
        cur.close()
        return paises

    except Exception as e:
        log(f"Error obteniendo países: {e}")
        return []   

# Funcion para obtener en JSON todas los departamentos y ciudades de un país tomando como argumento el ISO_A3
def obtener_departamentos_ciudades(pais: str) -> str:
    """
    Consulta la base de datos para obtener los departamentos y ciudades de un país dado su Pais
    Retorna el JSON de la forma: {"Departamentos": {"LIM": ["LIMA", "HUARAL"], "CAJ": ["CAJAMARCA"] }} } 
    """

    try:
        conexion = bd.conexion_bd()
        cur = conexion.cursor()
        consulta = """
            SELECT json_agg(t) FROM (
                SELECT departamento, ciudad FROM maestro_lugares
                WHERE UPPER(pais) = UPPER(%s)
                ORDER BY departamento, ciudad
            ) t;
        """
        cur.execute(consulta, (pais,))
        resultados = cur.fetchall()
        # El resultado es de tipo JSON
        departamentos_ciudades = resultados[0]['json_agg'] if resultados and resultados[0] else {"Departamentos": {}}    
        
        cur.close()
        return departamentos_ciudades

    except Exception as e:
        log(f"Error obteniendo departamentos y ciudades: {e}")
        return {}



