from dotenv import load_dotenv
from modelosIA import llm


# Variables de entorno
load_dotenv(dotenv_path="_mientorno.env") 

####################################
#   FUNCIONES DE FORMATEO

def log (evento: str):
    from datetime import datetime
    ahora = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print (f"[{ahora}] {evento}")
    return


def diccionario_a_tabla_md(datos_dict):
    """
    Convierte {'clave': 'valor'} en una tabla Markdown vertical.
    """
    # Encabezado de la tabla
    tabla = "| Campo | Detalle |\n| :--- | :--- |\n"
    tabla = ""
    
    # Rellenar filas
    for clave, valor in datos_dict.items():
        # Truco visual: Capitalizar la primera letra (pais -> Pais)
        clave_bonita = clave #clave.capitalize() 
        tabla += f"| **{clave_bonita}** | {valor} |\n"
        
    return tabla



def generar_mapa_resultados(datos_mapa, capas_extra=None):
    """
    Genera un mapa HTML con Folium y lo devuelve encapsulado en un IFRAME.
    """
    import folium
    import json
    from branca.colormap import LinearColormap
    import math
    import html

    if not datos_mapa and not capas_extra:
        return "<div style='padding:20px;'>No hay datos para mostrar en el mapa.</div>"

    # Configurar Centro
    try:
        lat_cen = float(datos_mapa[0].get('lat', -12.0)) if datos_mapa else -12.0464
        lon_cen = float(datos_mapa[0].get('lon', -77.0)) if datos_mapa else -77.0428
    except:
        lat_cen, lon_cen = -12.0464, -77.0428

    m = folium.Map(location=[lat_cen, lon_cen], zoom_start=10, tiles='Esri.WorldImagery')

    # Agregar la capa de mapa político
    folium.TileLayer(
        tiles='https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
        attr='Esri',
        name='Etiquetas',
        overlay=True
    ).add_to(m)

    # Colores
    colormap = LinearColormap(
        colors=['red', 'orange', 'yellow', 'green'],
        index=[0, 40, 70, 100],
        vmin=0, vmax=100,
        caption='Idoneidad del Terreno (Puntaje)'
    )
    m.add_child(colormap)

    # (Pane personalizado eliminado: usaremos el orden de dibujo por defecto)

    # Dibujar Celdas
    for celda in datos_mapa:
        try:
           
            # Puntaje
            puntaje = float(celda.get('puntaje', 0))
            puntaje = 0.0 if math.isnan(puntaje) else puntaje
            
            # Función auxiliar para manejo de números flotantes
            def safe_float(val):
                try: return float(val)
                except: return 0.0

            # CORRECCIÓN de Id: Busca 'id' O 'id_celda' para evitar el "N/A"
            id_celda = str(celda.get('id', celda.get('id_celda', 'N/A')))
            
            temp = safe_float(celda.get('temp_promedio', 0))
            humedad = safe_float(celda.get('humedad_promedio', 0))
            precip = safe_float(celda.get('precipitacion_promedio', 0))
            altitud = safe_float(celda.get('elevacion_promedio', 0))
            viento = safe_float(celda.get('viento_promedio', 0))
            explicacion = str(celda.get('explicacion', 'Sin diagnóstico'))

            geom = celda.get('geometry')
            if isinstance(geom, str):
                geom = json.loads(geom)

            color_relleno = colormap(puntaje)

            # TOOLTIP (CSS)
            tooltip_html = f"""
            <div style="font-family: Arial, sans-serif; font-size: 11px; width: 240px;">
                <div style="background-color: #333; color: white; padding: 8px; border-radius: 4px; margin-bottom: 6px;">
                    <div style="font-size: 10px; color: #ddd; margin-bottom: 2px;">
                        ID: {id_celda}
                    </div>
                    <div style="font-size: 15px; font-weight: bold; color: {color_relleno};">
                        Puntaje: {puntaje:.0f} / 100
                    </div>
                </div>

                <table style="width: 100%; border-collapse: collapse; font-size: 11px; margin-bottom: 5px;">
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 3px;">🌡️ <b>Temp:</b></td><td style="text-align: right;">{temp:.1f} °C</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 3px;">💧 <b>Humedad:</b></td><td style="text-align: right;">{humedad:.1f} %</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 3px;">🌧️ <b>Precip:</b></td><td style="text-align: right;">{precip:.1f} mm/d</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 3px;">⛰️ <b>Altitud:</b></td><td style="text-align: right;">{altitud:.0f} msnm</td></tr>
                    <tr style="border-bottom: 1px solid #eee;"><td style="padding: 3px;">💨 <b>Viento:</b></td><td style="text-align: right;">{viento:.1f} m/s</td></tr>
                </table>

                <div style="
                    margin-top: 8px; 
                    padding-top: 5px; 
                    border-top: 2px solid {color_relleno}; 
                    max-height: 80px;         /* Un poco más de altura */
                    overflow-y: auto;         /* Scroll vertical si es necesario */
                    white-space: normal !important; /* IMPORTANTE: Fuerza el salto de línea */
                    word-wrap: break-word;    /* Rompe palabras largas */
                    overflow-wrap: break-word;
                    line-height: 1.3; 
                    color: #444;
                    font-style: italic;
                ">
                    {explicacion}
                </div>
            </div>
            """

            folium.GeoJson(
                geom,
                style_function=lambda x, color=color_relleno: {
                    'fillColor': color, 'color': 'black', 'weight': 0.5, 'fillOpacity': 0.6
                },
                tooltip=folium.Tooltip(tooltip_html, sticky=True)
            ).add_to(m)

        except Exception as e:
            continue

    # Capas de Referencias sobre las celdas
    if capas_extra:
        log(f"🗺️ Pintando {len(capas_extra)} elementos de referencia en el mapa...")
        for capa in capas_extra:
            tipo = capa.get('tipo', 'desconocido')
            feature = capa.get('geojson')
            if not feature:
                continue

            color_linea = 'gray'
            grosor = 2
            if tipo == 'rios':
                color_linea = '#0077BE'
                grosor = 3
            elif tipo == 'carreteras':
                color_linea = "#580558"  
                grosor = 3
            def style_function(feat=None, color=color_linea, w=grosor):
                return {'color': color, 'weight': w, 'opacity': 1.0}

            try:
                # Dibujamos la referencia como capa GeoJSON normal, añadida
                # DESPUÉS de las celdas, por lo que queda por encima.
                folium.GeoJson(
                    feature,
                    name=tipo,
                    style_function=style_function
                ).add_to(m)
                log(f"Capa '{tipo}' agregada al mapa (tipo geom: {feature.get('geometry', {}).get('type', 'N/A')}).")
            except Exception as e:
                log(f"Error pintando capa '{tipo}': {e}")

    #  4. EMPAQUETADO EN IFRAME ---
    map_html = m.get_root().render()
    
    iframe = f"""
    <iframe
        srcdoc="{html.escape(map_html)}"
        width="100%"
        height="100%"
        style="width:100%; height:100%; border:none; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1);"
    ></iframe>
    """
    
    return iframe

########################################
#   ANALISIS DE BASE DE DATOS
########################################

def calcular_puntaje_ponderado(valor, min_val, max_val):
    """
    Calcula un puntaje de 0 a 100.
    Reglas:
    - Dentro del rango [min_val, max_val] = 100 pts
    - Fuera del rango = disminuye proporcionalmente según la distancia al rango
    """
    
    try:
        val = float(valor)
        min_v = float(min_val)
        max_v = float(max_val)
        
        rango_total = max_v - min_v
        if rango_total == 0:
            return 100.0
        
        # 1. Dentro del rango -> 100
        if min_v <= val <= max_v:
            return 100.0
        
        # 2. Fuera del rango -> reducir proporcionalmente
        if val < min_v:
            distancia = min_v - val
        else:  # val > max_v
            distancia = val - max_v
        
        # Reducir el puntaje: cada unidad de distancia reduce proporcionalmente
        reduccion = (distancia / rango_total) * 100.0
        puntaje = max(0.0, 100.0 - reduccion * 3)  # casigo por 3 para penalizar más rápido
        
        return puntaje
        
    except (ValueError, TypeError):
        return 0.0

def evaluar_idoneidad_terreno(df_celdas, reglas):
    """
    Evalúa celdas en formato 'Long' (una fila por fecha).
    Evalúa cada mes individualmente (detectando 'fecha').
    Promedia los meses para obtener el puntaje anual.
    Agrupa por id_celda devolviendo geometrías únicas.
    """
    import pandas as pd
    import numpy as np

    log(f"Iniciando la evaluación cronológica (columna 'fecha')...")

    # Validar que exista la columna fecha
    if 'fecha' not in df_celdas.columns:
        log("Error: No se encontró la columna 'fecha' en los datos.")
        df_celdas['puntaje'] = 0
        df_celdas['explicacion'] = "Faltan datos temporales."
        return df_celdas

    # Asegurar formato fecha
    df_celdas['fecha_dt'] = pd.to_datetime(df_celdas['fecha'], errors='coerce')
    df_celdas['mes'] = df_celdas['fecha_dt'].dt.month

    # Mapeo de reglas a columnas de la BD
    mapa_cols = {
        'temp': 'temp_promedio', 'temperatura': 'temp_promedio',
        'humedad': 'humedad_promedio',
        'precipitacion': 'precipitacion_promedio',
        'viento': 'viento_promedio',
        'altitud': 'elevacion_promedio', 'elevacion': 'elevacion_promedio',
    }

    # Inicializar una columna de puntaje global para cada fila (mes)
    df_celdas['puntaje_mes'] = 0.0
    df_celdas['cont_factores'] = 0
    df_celdas['motivo_fallo'] = ""

    # --- 1. EVALUACIÓN FILA POR FILA (MES A MES) ---
    for parametro, rango in reglas.items():
        col_db = mapa_cols.get(parametro.lower())
        
        if not col_db or col_db not in df_celdas.columns:
            continue
            
        # Lógica para rangos
        if isinstance(rango, dict) and 'min' in rango and 'max' in rango:
            vmin = float(rango['min'])
            vmax = float(rango['max'])
            
            # Aplicar función ponderada a toda la columna
            puntaje_param = df_celdas[col_db].apply(
                lambda x: calcular_puntaje_ponderado(x, vmin, vmax)
            )
            
            # Acumular
            df_celdas['puntaje_mes'] += puntaje_param
            df_celdas['cont_factores'] += 1
            
            # Registrar fallos (si puntaje es 0) para explicar luego por qué falló
            mask_fallo = puntaje_param == 0
            df_celdas.loc[mask_fallo, 'motivo_fallo'] += (
                f"{parametro} fuera de rango (" + df_celdas.loc[mask_fallo, col_db].astype(str) + "); "
            )


    # Promediar los factores para obtener el puntaje del MES
    mask_ok = df_celdas['cont_factores'] > 0
    df_celdas.loc[mask_ok, 'puntaje_mes'] /= df_celdas.loc[mask_ok, 'cont_factores']
    
    # --- 2. AGRUPACIÓN POR CELDA (RESUMEN ANUAL) ---
    # Resumir los 12 meses a una fila por celda
    
    try:
        acumulacion = {
            'puntaje_mes': 'mean',         # El puntaje final es el promedio del año
            'lat': 'first',                # Coordenadas no cambian
            'lon': 'first',
            'geometry': 'first',           # Geometría no cambia
            'temp_promedio': 'mean',       
            'precipitacion_promedio': 'mean',
            'elevacion_promedio': 'mean',
            'humedad_promedio': 'mean',
            'viento_promedio': 'mean',
            'motivo_fallo': lambda x: " ".join(set([s for s in x if s])) # Concatenar fallos únicos
        }
        
        # Asegurar que existan las columnas antes de agrupar
        agg_final = {k: v for k, v in acumulacion.items() if k in df_celdas.columns}
        
        # Verificar el ID correcto
        col_id = 'id_celda' if 'id_celda' in df_celdas.columns else 'id'
        
        # Limpiar
        df_celdas['puntaje_mes'] = df_celdas['puntaje_mes'].fillna(0.0).round(1)
        df_celdas['explicacion'] = "Datos insuficientes." # Por defecto

        df_resultado = df_celdas.groupby(col_id, as_index=False).agg(agg_final)
        
        # Renombrar puntaje_mes -> puntaje
        df_resultado.rename(columns={'puntaje_mes': 'puntaje'}, inplace=True)
        df_resultado['puntaje'] = df_resultado['puntaje'].round(1)
        df_resultado['explicacion'] = "Datos insuficientes." # Por defecto

        # Criterios para la explicación textual
        mask_bajo = df_resultado['puntaje'] < 60
        mask_medio = (df_resultado['puntaje'] >= 60) & (df_resultado['puntaje'] < 80)
        mask_alto = df_resultado['puntaje'] >= 80
        mask_con_fallos = df_resultado['motivo_fallo'] != ""
        
        df_resultado.loc[mask_bajo & mask_con_fallos, 'explicacion'] = \
            "Problemas estacionales detectados: " + df_resultado['motivo_fallo'].str.slice(0, 100) + "..."
            
        df_resultado.loc[mask_bajo & ~mask_con_fallos, 'explicacion'] = \
            "Condiciones variables o inestables reducen el potencial."

        df_resultado.loc[mask_medio, 'explicacion'] = "Condiciones aceptables con variaciones."
        df_resultado.loc[mask_alto, 'explicacion'] = "Condiciones adecuadas y estables."
        
        df_resultado['puntaje'] = df_resultado['puntaje'].fillna(0.0)

    except Exception as e:
        log(f"Error durante la agrupación final: {e}")
        df_resultado = pd.DataFrame()
    return df_resultado



