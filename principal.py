import os
import sys
import builtins
import gradio as gr
import json
from typing import Annotated, TypedDict, List
import uuid

from langgraph.checkpoint.memory import MemorySaver 
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage, BaseMessage, ToolMessage
from langgraph.graph import StateGraph, END, START
from langgraph.graph.message import add_messages

from modelosIA import llm
import prompts
import funciones
import bd
from funciones import log

from ingesta_bd import ingestar, IngestionCeldasTerreno
from ingesta_bd import obtener_departamentos_ciudades, obtener_paises
import ingesta_ref  
import folium

from langchain_core.tools import tool
from langchain_core.runnables import RunnableConfig



# --- REVISAR SI QUEDARÁ ESTA CONFIGURACIÓN GLOBAL UTF-8 ---

os.environ["PYTHONUTF8"] = "1"
try:
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
except AttributeError:
    pass

if not hasattr(builtins, "_original_open_seguro"):
    builtins._original_open_seguro = builtins.open

def open_utf8_safe(*args, **kwargs):
    original = builtins._original_open_seguro
    if 'encoding' in kwargs: return original(*args, **kwargs)
    mode = kwargs.get('mode')
    if mode is None and len(args) > 1: mode = args[1]
    if mode is None or 'b' not in str(mode): kwargs['encoding'] = 'utf-8'
    return original(*args, **kwargs)
builtins.open = open_utf8_safe



conexion = bd.conexion_bd()

# --- 2. CONFIGURACIÓN DEL ESTADO ---

AgentState = TypedDict("AgentState", {
    "mensajes": Annotated[List[BaseMessage], add_messages],
    "siguiente_nodo": str,
    "ultimo_agente": str,
    "caracteristicas_geoespaciales": str,
    "pais": str,
    "departamento": str,
    "ciudad": str,
    "datos_celdas": str,
})

# Almacén en memoria para datos espaciales pesados.
# Viven fuera del estado de LangGraph: no se serializan, no viajan al LLM.
_valores_globales: dict = {}  # clave: thread_id  →  {"celdas": [...], "capas": [...]}

def formatear_reglas_html(reglas):
    """
    Genera una tabla HTML.
    - Si 'reglas' es una lista (mes a mes): Tabla matricial (Param x Meses) con scroll.
    - Si 'reglas' es un dict (resumen): Tabla vertical simple.
    """
    if not reglas: 
        return "<div style='padding:10px; color:#666;'>Esperando definición de cultivo...</div>"

    # ==========================================
    # CASO A: LISTA DE MESES (Visión Anual)
    # ==========================================
    if isinstance(reglas, list):
        # 1. Organizar datos por mes (1..12) y recolectar parámetros únicos
        datos_por_mes = {m: {} for m in range(1, 13)}
        all_params = set()
        
        datos_validos = False
        
        for item in reglas:
            if not isinstance(item, dict): continue
            
            # Intentar obtener el número de mes
            mes_raw = item.get('mes')
            if mes_raw is None: continue 
            
            try:
                m_idx = int(mes_raw)
                if 1 <= m_idx <= 12:
                    datos_por_mes[m_idx] = item
                    datos_validos = True
                    # Recolectar claves (excepto 'mes')
                    for k in item.keys():
                        if k.lower() != 'mes':
                            all_params.add(k)
            except: continue
        
        # Si no se pudo estructurar como matriz mensual, hacer fallback a merge
        if not datos_validos:
            merged = {}
            for item in reglas:
                if isinstance(item, dict): merged.update(item)
            reglas = merged # Pasar al bloque de dict
        else:
            # Construir tabla vertical: filas = meses, columnas = condiciones
            meses_nombres = {
                1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril",
                5: "Mayo", 6: "Junio", 7: "Julio", 8: "Agosto",
                9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre"
            }

            ordered_params = sorted(list(all_params))
            etiquetas_param = {}
            for param in ordered_params:
                icon = "🔹"
                p_lower = param.lower()
                if "temp" in p_lower:
                    icon = "🌡️"
                elif "humedad" in p_lower:
                    icon = "💧"
                elif "precip" in p_lower:
                    icon = "🌧️"
                elif "suelo" in p_lower:
                    icon = "🌱"
                elif "alt" in p_lower or "elev" in p_lower:
                    icon = "⛰️"
                elif "viento" in p_lower:
                    icon = "💨"

                descripcion = ""
                for m in range(1, 13):
                    val_obj = datos_por_mes[m].get(param)
                    if isinstance(val_obj, dict) and "Descripcion" in val_obj:
                        descripcion = str(val_obj.get("Descripcion", "")).strip()
                        break

                etiquetas_param[param] = f"{icon} {descripcion}" if descripcion else f"{icon} {param}"

            html = """
            <div style='height: 100%; max-height: none; overflow: auto; border: 1px solid #eee; border-radius: 8px; background: white;'>
                <table style='width:100%; border-collapse: collapse; table-layout: fixed; font-family: sans-serif; font-size: 10px;'>
                    <thead>
                        <tr style='background-color: #2b3137; color: white;'>
                            <th style='padding: 6px; min-width: 80px; width: 80px; position: sticky; top: 0; left: 0; z-index: 3; background-color: #2b3137; border-right: 1px solid #555; color: #ffffff;'>Mes</th>
            """

            for param in ordered_params:
                etiqueta = etiquetas_param[param]
                html += (
                    "<th style='padding: 0; width: 82px; min-width: 82px; max-width: 82px; height: 150px; text-align: center; "
                    "position: sticky; top: 0; z-index: 2; background-color: #2b3137; border-left: 1px solid #3f4751; color: #ffffff;'>"
                    f"<div style='display: inline-block; transform: rotate(-90deg); transform-origin: center; white-space: nowrap; font-weight: 700; font-size: 12px; color: #ffffff;'>{etiqueta}</div>"
                    "</th>"
                )

            html += "</tr></thead><tbody>"

            for m in range(1, 13):
                bg = "#f9f9f9" if m % 2 == 0 else "#ffffff"
                html += (
                    f"<tr style='background-color: {bg}; border-bottom: 1px solid #eee;'>"
                    f"<td style='padding: 6px 8px; font-weight: bold; position: sticky; left: 0; z-index: 1; border-right: 1px solid #ddd; background-color: {bg};'>{meses_nombres[m]}</td>"
                )

                for param in ordered_params:
                    val_obj = datos_por_mes[m].get(param)

                    # Altitud/elevacion: si falta en algun mes, usar el primer valor disponible
                    if (val_obj is None) and ("alt" in param.lower() or "elev" in param.lower()):
                        for mes_ref in range(1, 13):
                            candidato = datos_por_mes[mes_ref].get(param)
                            if candidato is not None:
                                val_obj = candidato
                                break

                    val_str = "-"
                    if isinstance(val_obj, dict):
                        if ("min" in val_obj) and ("max" in val_obj):
                            val_str = f"{val_obj['min']}-{val_obj['max']}"
                        else:
                            val_str = "..."
                    elif val_obj is not None:
                        val_str = str(val_obj)

                    html += f"<td style='padding: 4px; width: 82px; min-width: 82px; max-width: 82px; text-align: center; color: #444;'>{val_str}</td>"

                html += "</tr>"

            html += "</tbody></table></div>"
            return html


    return f"<div>{str(reglas)}</div>"

# TOOLS 


@tool
def tool_obtener_paises_permitidos():
    """
    Obtiene una lista de países permitidos, con dos datos: el codigo ISO de 3 dígitos y el nombre del país. 
    Esta información se obtiene de la base de datos y se devuelve en formato JSON. El LLM lo usará para validar que el país que elija el usuario esté dentro de esta lista.
    """
    log(f"EJECUTANDO TOOL OBTENER PAISES PERMITIDOS")
    try:
        resultado = obtener_paises()  # Devuelve una lista de dicts con 'iso_a3' y 'nombre'

        return resultado
    except Exception as e:
        return f"Error en análisis: {str(e)}"
    
@tool
def tool_obtener_departamentos_ciudades_permitidas(pais: str):
    """
    Obtiene una lista de departamentos y ciudades permitidas para un país.
    Esta información se obtiene de la base de datos y se devuelve en formato JSON. El LLM lo usará para validar que el departamento/ciudad que elija el usuario esté dentro de esta lista.
    Argumento: país (nombre del país) para el cual se quieren obtener los departamentos y ciudades permitidas. El país ya debería haber sido validado con 'tool_obtener_paises_permitidos' antes de llamar a esta herramienta.
    """
    log(f"EJECUTANDO TOOL OBTENER DEPARTAMENTOS Y CIUDADES PERMITIDAS")
    try:
        resultado = obtener_departamentos_ciudades(pais)  # Devuelve una lista de dicts con 'iso_a3', 'departamento', y 'ciudad'
        return resultado
    except Exception as e:
        return f"Error en análisis: {str(e)}"
    

@tool
def tool_carga_referencias(criterios: List[dict], pais, departamento, ciudad: str):
    """
    Valida si las referencias existen en la base de datos. De lo contrario, se ingestan.
    La referencia solo puede ser "rios"
    La condicion puede ser "cerca" o "lejos"
    La distancia la tiene que dar el usuario en metros. Si no la da, entonces no uses esta tool.
    
    Argumentos
    ----------
    Los Argumentos son cuatro:
        1. criterios: Lista de 3 dicts. Ejemplo: 
                   [{"referencia": "rios", "condicion": "cerca", "distancia": 500}]
        2. ciudad: Nombre de la ciudad (ej: 'Canta').
        3. pais: Nombre del país (ej: 'Perú').
        4. departamento: Nombre del departamento o estado (ej: 'Lima').
    
    Descripcion del argumento "criterios":
        Valores válidos para "referencia": 'rios', 'carreteras', 'puntos_interes'.
        Valores válidos para "condicion": 'cerca', 'lejos'.
        Distancia en metros.
    """
    log(f"EJECUTANDO TOOL CARGA DE REFERENCIAS: {criterios} en {ciudad}")
    try:
        #pais = bd.normalizar_texto(pais)

        resultado = ingesta_ref.carga_referencias(pais, departamento, ciudad, criterios)
        return resultado
    except Exception as e:
        return f"Error en análisis: {str(e)}"
    

@tool
def tool_analisis_referencias(criterios: List[dict], pais, departamento, ciudad: str):
    """
    Realiza un análisis de distancia respecto a referencias como rios o carreteras.
    La referencia solo puede ser "rios" o "carreteras"
    La condicion puede ser "cerca" o "lejos"
    La distancia la tiene que dar el usuario en metros. Si no la da, entonces no uses esta tool.

    Args:
        criterios: Lista de dicts. Ejemplo: 
                   [{"referencia": "rios", "condicion": "cerca", "distancia": 500}]
                   Valores válidos referencia: 'rios', 'carreteras', 'puntos_interes'.
                   Valores válidos condicion: 'cerca', 'lejos'.
                   Distancia en metros.
        ciudad: Nombre de la ciudad (ej: 'Canta').
        pais: Nombre del país (ej: 'Perú').
        departamento: Nombre del departamento o estado (ej: 'Lima').
    """
    log(f"EJECUTANDO TOOL ANÁLISIS: {criterios} en {pais}, {departamento}, {ciudad}")
    try:
        pais = bd.normalizar_texto(pais)

        resultado = ingesta_ref.analisis_postgis(pais, departamento, ciudad, criterios)
        return resultado
    except Exception as e:
        return f"Error en análisis: {str(e)}"


@tool
def tool_guardar_ubicacion(pais: str, departamento: str, ciudad: str) -> str:
    """
    Guarda la ubicación geográfica identificada y validada (país, departamento y ciudad)
    en la memoria del sistema para usarla en los siguientes pasos del análisis.
    Debes llamar a esta herramienta una vez que hayas validado el país con 
    tool_obtener_paises_permitidos y confirmado la ubicación con el usuario.

    Argumentos:
        pais: Nombre del país validado (ej: 'Peru').
        departamento: Nombre del departamento, estado o región ADM1 (ej: 'Lima').
        ciudad: Nombre de la ciudad o municipio ADM2 (ej: 'Canta').
    """
    log(f"GUARDANDO UBICACIÓN: {pais}, {departamento}, {ciudad}")
    return f"Ubicación registrada correctamente: {pais} / {departamento} / {ciudad}"


    

# --- NODOS (AGENTES) ---
from pydantic import BaseModel, Field

class RegionIdentificada (BaseModel):
    pais: str = Field (description="Nombre del país o ADM0")
    departamento: str = Field (description="Nombre del departamento o estado o ADM1.")
    ciudad: str = Field (description = "Nombre de la ciudad o ADM2.")

def nodo_herramientas(state: AgentState):
    log('Nodo de Herramientas Ejecutado ---')
    ultimo_mensaje = state["mensajes"][-1]
    ultimo_agente = state["ultimo_agente"]
    
    mensajes_tool = []
    siguiente = ultimo_agente  # Por defecto, volver al agente que invocó
    
    # Datos de ubicación si la tool los captura
    ubicacion_extra = {}

    # Mapeo de herramientas disponibles
    mapa_tools = {
        "tool_obtener_paises_permitidos": tool_obtener_paises_permitidos,
        "tool_obtener_departamentos_ciudades_permitidas": tool_obtener_departamentos_ciudades_permitidas,
        "tool_guardar_ubicacion": tool_guardar_ubicacion,
        "tool_carga_referencias": tool_carga_referencias,
        "tool_analisis_referencias": tool_analisis_referencias
    }
    
    # Procesar todas las herramientas que el LLM haya pedido
    for tool_invocado in ultimo_mensaje.tool_calls:
        nombre_tool = tool_invocado["name"]
        args = tool_invocado["args"]
        tool_id = tool_invocado["id"]
        
        if nombre_tool in mapa_tools:
            log(f"Ejecutando herramienta: {nombre_tool} con args: {args}")
            try:
                resultado = mapa_tools[nombre_tool].invoke(args)
            except Exception as e:
                resultado = f"Error al ejecutar {nombre_tool}: {str(e)}"
                log(f"{resultado}")
            
            # Si es tool_guardar_ubicacion, capturar los datos estructurados
            if nombre_tool == "tool_guardar_ubicacion" and not str(resultado).startswith("Error"):
                ubicacion_extra = {
                    "pais": args.get("pais", ""),
                    "departamento": args.get("departamento", ""),
                    "ciudad": args.get("ciudad", ""),
                }
                siguiente = "nodo_condiciones_geograficas"  # Avanzar al siguiente nodo
                log(f"Ubicación capturada: {ubicacion_extra}")

            mensaje_tool = ToolMessage(
                tool_call_id=tool_id,
                content=str(resultado),
                name=nombre_tool
            )
            mensajes_tool.append(mensaje_tool)
        else:
            # Herramienta desconocida: siempre responder con ToolMessage para no romper el historial
            log(f"Herramienta desconocida solicitada: {nombre_tool}")
            mensajes_tool.append(ToolMessage(
                tool_call_id=tool_id,
                content=f"Herramienta '{nombre_tool}' no disponible.",
                name=nombre_tool
            ))
            
    return {
        "mensajes": mensajes_tool,
        "siguiente_nodo": siguiente,
        "ultimo_agente": "nodo_ubicacion_geografica" if siguiente == "nodo_condiciones_geograficas" else ultimo_agente,
        **ubicacion_extra
    }


def agente_supervisor(state: AgentState):
    log('--- Nodo Supervisor Ejecutado ---')
    ultimo_activo = state.get("ultimo_agente", "")
    
    # volver directamente al agente activo sin invocar el LLM supervisor
    if ultimo_activo == "nodo_ubicacion_geografica": return {"siguiente_nodo": "nodo_ubicacion_geografica"}
    if ultimo_activo == "nodo_condiciones_geograficas": return {"siguiente_nodo": "nodo_condiciones_geograficas"}
    if ultimo_activo == "nodo_evaluador": return {"siguiente_nodo": "end"}  # Evaluación terminada, no reiniciar

    mensajes = [SystemMessage(content=prompts.PROMPT_SUPERVISOR )] + state['mensajes']
    respuesta = llm.invoke(mensajes)
    contenido = respuesta.content.strip()
    
    log (f"Supervisor revisa la respuesta del último agente ({ultimo_activo}): {contenido[:1000]}...")
    if "##PASAR_A_AGENTE_UBICACION_GEOGRAFICA##" in contenido:
        contenido_limpio = contenido.replace("##PASAR_A_AGENTE_UBICACION_GEOGRAFICA##", "").strip()
        return {"mensajes": [AIMessage(content=contenido_limpio)], "siguiente_nodo": "nodo_ubicacion_geografica", "ultimo_agente": ""}
    else:
        return {"mensajes": [respuesta], "siguiente_nodo": "end", "ultimo_agente": "nodo_supervisor"}


def agente_ubicacion_geografica(state: AgentState):
    # Ubicación geográfica #

    log('--- Nodo Ubicación Geográfica Ejecutado ---')
    mensajes = [SystemMessage(content=prompts.PROMPT_UBICACION_GEOGRAFICA)] + state['mensajes']

    llm_con_tools = llm.bind_tools([tool_obtener_paises_permitidos, tool_obtener_departamentos_ciudades_permitidas,  tool_guardar_ubicacion])
    respuesta = llm_con_tools.invoke(mensajes)
    contenido = respuesta.content if respuesta.content else ""

    # Si el LLM invocó alguna herramienta, delegar al nodo_herramientas
    if hasattr(respuesta, 'tool_calls') and len(respuesta.tool_calls) > 0:
        log(f"Agente Ubicación solicitó herramienta(s): {[tc['name'] for tc in respuesta.tool_calls]}")
        return {
            "mensajes": [respuesta],
            "siguiente_nodo": "nodo_herramientas",
            "ultimo_agente": "nodo_ubicacion_geografica"
        }

    # Respuesta de texto sin tool_calls: terminar turno y esperar al usuario
    return {"mensajes": [respuesta], 
            "siguiente_nodo": "end", 
            "ultimo_agente": "nodo_ubicacion_geografica"}
    


def agente_condiciones_geograficas(state: AgentState):
    log('--- Nodo Condiciones Geográficas Ejecutado ---')
    mensajes = [SystemMessage(content=prompts.PROMPT_CONDICIONES_GEOGRAFICAS)] + state['mensajes']
    llm_con_tools = llm.bind_tools([tool_carga_referencias])
    respuesta = llm_con_tools.invoke(mensajes)
    
    contenido = respuesta.content if respuesta.content else ""

    pais = state.get('pais', '')
    departamento = state.get('departamento', '')
    ciudad = state.get('ciudad', '')

    # Comprobar tool_calls PRIMERO, aunque el LLM también devuelva contenido de texto
    if hasattr(respuesta, 'tool_calls') and respuesta.tool_calls:
        log("El Agente de Condiciones solicitó una herramienta.")
        return {
            "mensajes": [respuesta],
            "siguiente_nodo": "nodo_herramientas",
            "ultimo_agente": "nodo_condiciones_geograficas"
        }

    if "##PASAR_A_AGENTE_EVALUADOR##" in contenido:
        # Esto sucede cuando ya encontraron las características de la actividad económica y está listo para el siguiente paso
        contenido_limpio = contenido.replace("##PASAR_A_AGENTE_EVALUADOR##", "").strip()
        try:
            json_str = contenido_limpio.replace("```json", "").replace("```", "").strip()
            if "{" in json_str or "[" in json_str:
                contenido_dict = json_str 
            else:
                contenido_dict = json_str
            # Carga información satelital actualizada si no existe la región en la base de datos                
            ingesta = IngestionCeldasTerreno(conexion)
            if not ingesta.existe_ubicacion_geografica(pais, departamento, ciudad):
                log("Región no existe en BD, iniciando ingesta...")
                ingestar(pais, departamento, ciudad)
        except:
            contenido_dict = contenido_limpio
        
        return {
            "mensajes": [AIMessage(content="Analizando requerimientos del terreno...")],
            "siguiente_nodo": "nodo_evaluador",
            "ultimo_agente": "",
            "caracteristicas_geoespaciales": contenido_dict
        }
    else:
        # El agente hizo una pregunta al usuario regresar al mismo agente.
        return {"mensajes": [respuesta], 
                "siguiente_nodo": "nodo_condiciones_geograficas", 
                "ultimo_agente": "nodo_condiciones_geograficas"}


def agente_evaluador(state: AgentState, config: RunnableConfig):
    import pandas as pd
    import json

    log('--- Nodo Evaluador Ejecutado ---')

    thread_id = config.get("configurable", {}).get("thread_id", "default")
    
    # 1. RECUPERAR REGLAS
    raw_rules = state.get("caracteristicas_geoespaciales", "[]")
    reglas_terreno = {}

    try:
        if isinstance(raw_rules, str):
            texto = raw_rules.replace("```json", "").replace("```", "").strip()
            idx_inicio = texto.find("[")
            idx_fin = texto.rfind("]")
            
            if idx_inicio != -1 and idx_fin != -1:
                json_str = texto[idx_inicio : idx_fin + 1]
                lista_datos = json.loads(json_str)
            else:
                lista_datos = []
        elif isinstance(raw_rules, list):
            lista_datos = raw_rules
        else:
            lista_datos = []

        for item in lista_datos:
            if isinstance(item, dict):
                reglas_terreno.update(item)

    except Exception as e:
        log(f"Error reglas: {e}")
        reglas_terreno = {}

    log(f"Reglas aplicadas: {reglas_terreno}")

    # 2. TRAER DATOS
    pais = state.get("pais", "")
    departamento = state.get("departamento", "")
    ciudad = state.get("ciudad", "")
    log(f"Buscando datos para: {ciudad}...")
    data = bd.obtener_datos_celda(pais, departamento, ciudad, limit=10000)

    if data.empty:
        return {
            "mensajes": [AIMessage(content=f"No hay datos para {ciudad}.")],
            "siguiente_nodo": "nodo_supervisor",
        }

    # 3. EVALUACIÓN
    try:
        df_evaluado = funciones.evaluar_idoneidad_terreno(data.copy(), reglas_terreno)
    except Exception as e:
        log(f"Error evaluación: {e}")
        return {
            "mensajes": [AIMessage(content="Error técnico en evaluación.")],
            "siguiente_nodo": "end",
        }
    

    prompt_evaluador = prompts.PROMPT_EVALUADOR.format(
        ciudad=ciudad
    )
    mensajes_para_llm = [SystemMessage(content=prompt_evaluador)] + state['mensajes']
    
    log (f"5. LLM: Iniciando evaluación con Tool Analisis Referencias")
    llm_con_tools_evaluador = llm.bind_tools([tool_analisis_referencias])
    respuesta_llm = llm_con_tools_evaluador.invoke(mensajes_para_llm)

    log (f"LLM Tool Calls: {respuesta_llm.tool_calls}")
    log (f"LLM Content: {respuesta_llm.content[:1000]}")
    # ==============================================================================
    # Invocar al Tool
    # ==============================================================================
    if respuesta_llm.tool_calls:
        
        # Capturar los argumentos que generó el LLM
        call = respuesta_llm.tool_calls[0]
        args = call['args']
        resultado_tool = tool_analisis_referencias.invoke(args)

        if isinstance(resultado_tool, dict):
            celdas_filtradas = resultado_tool.get('celdas_filtradas', [])
            nuevas_capas = resultado_tool.get('referencias_mapa', [])
            ids_permitidos = [c.get('id_celda') for c in celdas_filtradas if c.get('id_celda')]
            df_filtrado_final = df_evaluado[df_evaluado['id_celda'].isin(ids_permitidos)]
            datos_para_mapa = df_filtrado_final.to_dict(orient='records')
        else:
            datos_para_mapa = df_evaluado.to_dict(orient='records')
            nuevas_capas = []

        _valores_globales[thread_id] = {"celdas": datos_para_mapa, "capas": nuevas_capas}
        log(f"Datos espaciales guardados en _valores_globales[{thread_id}]: {len(datos_para_mapa)} celdas, {len(nuevas_capas)} capas")

        # ToolMessage con solo el resumen para el LLM
        resumen = f"Análisis espacial completado. {len(datos_para_mapa)} celdas cumplen los criterios."
        mensaje_tool = ToolMessage(
            tool_call_id=call['id'],
            content=resumen,
            name=call['name']
        )
        mensajes_finales = mensajes_para_llm + [respuesta_llm, mensaje_tool]
        respuesta_final_llm = llm_con_tools_evaluador.invoke(mensajes_finales)

        return {
            "mensajes": [respuesta_final_llm],
            "siguiente_nodo": "nodo_supervisor",
            "ultimo_agente": "nodo_evaluador",
        }
        

    datos_para_mapa = df_evaluado.to_dict(orient='records')
    _valores_globales[thread_id] = {"celdas": datos_para_mapa, "capas": []}
    return {
        "mensajes": [respuesta_llm],
        "siguiente_nodo": "nodo_supervisor",
        "ultimo_agente": "nodo_evaluador",
    }

########################
# GRAFO ---
########################

def create_workflow():
    workflow = StateGraph(AgentState)

    workflow.add_node("nodo_supervisor", agente_supervisor)
    workflow.add_node("nodo_ubicacion_geografica", agente_ubicacion_geografica)
    workflow.add_node("nodo_herramientas", nodo_herramientas)
    workflow.add_node("nodo_condiciones_geograficas", agente_condiciones_geograficas)
    workflow.add_node("nodo_evaluador", agente_evaluador)

    workflow.add_edge(START, "nodo_supervisor")

    def route_supervisor(state: AgentState):
        if state["siguiente_nodo"] in ("nodo_ubicacion_geografica", "nodo_condiciones_geograficas", "nodo_evaluador"):
            return state["siguiente_nodo"]
        return END    
    def route_ubicacion_geografica(state: AgentState):
        if state["siguiente_nodo"] in ("nodo_condiciones_geograficas", "nodo_ubicacion_geografica", "nodo_herramientas", "nodo_supervisor"):
            return state["siguiente_nodo"]
        return END    
    
    def route_condiciones_geograficas(state: AgentState): 
        if state["siguiente_nodo"] in ("nodo_condiciones_geograficas", "nodo_evaluador", "nodo_herramientas", "nodo_supervisor"):
            return state["siguiente_nodo"]
        return END

    def route_evaluador(state: AgentState):
        if state["siguiente_nodo"] in ("nodo_supervisor", "nodo_evaluador"):
            return state["siguiente_nodo"]
        return END    

    def route_herramientas(state: AgentState):
        return state["siguiente_nodo"]
    
    workflow.add_conditional_edges("nodo_herramientas", route_herramientas, {
        "nodo_ubicacion_geografica": "nodo_ubicacion_geografica",
        "nodo_condiciones_geograficas": "nodo_condiciones_geograficas",
        "nodo_evaluador": "nodo_evaluador"
    })

    workflow.add_conditional_edges("nodo_supervisor", route_supervisor, 
        {"nodo_ubicacion_geografica": "nodo_ubicacion_geografica", 
         "nodo_condiciones_geograficas": "nodo_condiciones_geograficas",
         "nodo_evaluador": "nodo_evaluador",
         END: END})
    workflow.add_conditional_edges("nodo_ubicacion_geografica", route_ubicacion_geografica,
         {
            "nodo_ubicacion_geografica": "nodo_ubicacion_geografica",
            "nodo_condiciones_geograficas": "nodo_condiciones_geograficas", 
            "nodo_herramientas": "nodo_herramientas", 
          "nodo_supervisor": "nodo_supervisor",
          END: END})

    workflow.add_conditional_edges("nodo_condiciones_geograficas", route_condiciones_geograficas,
         {
            #"nodo_ubicacion_geografica": "nodo_ubicacion_geografica",
            "nodo_condiciones_geograficas": "nodo_condiciones_geograficas", 
            "nodo_herramientas": "nodo_herramientas",
            "nodo_evaluador": "nodo_evaluador",
            #"nodo_supervisor": "nodo_supervisor",
            END: END})

    workflow.add_edge("nodo_evaluador", "nodo_supervisor")
    #workflow.add_edge("nodo_evaluador", END)

    memory = MemorySaver()
    return workflow.compile(checkpointer=memory)


app_graph = create_workflow()

# --- 6. LOGICA CHAT Y DATOS ---

def logica_chat(mensaje, session_id):
    config = {"configurable": {"thread_id": session_id} }
    inputs = {"mensajes": [HumanMessage(content=mensaje)]}
    
    texto_acumulado = ""
    datos_mapa_final = None
    html_esperada = None # Variable para el HTML
    info_actual = None
    
    capas_visuales = [] # Inicializar

    try:
        for chunk in app_graph.stream(inputs, config=config, stream_mode="updates"):
            
            for nombre_nodo, valores in chunk.items():
                
                # A) REGLAS -> CONVERTIR A HTML
                if "caracteristicas_geoespaciales" in valores:
                    raw = valores["caracteristicas_geoespaciales"]
                    info_dict = {}
                    if isinstance(raw, str):
                        try:
                            clean = raw.replace("```json", "").replace("```", "").strip()
                            if "[" in clean:
                                info_dict = json.loads(clean[clean.find("["):clean.rfind("]")+1])
                            elif "{" in clean:
                                info_dict = json.loads(clean[clean.find("{"):clean.rfind("}")+1])
                            else:
                                info_dict = {"Info": raw}
                        except:
                            info_dict = {"Info": raw}
                    else:
                        info_dict = raw
                    
                    # Convertimos a HTML aquí mismo
                    html_esperada = formatear_reglas_html(info_dict)

                # B) RESULTADOS ESPACIALES — leídos desde _valores_globales, no del estado
                datos_espaciales = _valores_globales.get(session_id)
                if datos_espaciales:
                    datos_mapa_final = datos_espaciales.get("celdas", [])
                    capas_visuales = datos_espaciales.get("capas", [])

                # C) CHAT
                if "mensajes" in valores:
                    mensajes_nuevos = valores["mensajes"]
                    for msg in mensajes_nuevos:
                        if isinstance(msg, AIMessage) and msg.content:
                            if nombre_nodo == "nodo_condiciones_geograficas": continue
                            bloque_nuevo = f"\n\n{msg.content}"
                            texto_acumulado += bloque_nuevo

                # Solo yield si hay contenido nuevo que mostrar
                if texto_acumulado or datos_mapa_final or html_esperada or capas_visuales:
                    yield texto_acumulado, datos_mapa_final, html_esperada, info_actual, capas_visuales

    except Exception as e:
        yield f"❌ Error: {str(e)}", None, None, None, []

def interaccion_usuario(mensaje, historia):
    if not mensaje: return "", historia
    if historia is None: historia = []
    historia.append({"role": "user", "content": mensaje})
    return "", historia

def interaccion_bot(historia, state_datos, session_id):
    if not historia: yield [], None, None, None; return

    try:
        ultimo_mensaje = historia[-1].get("content", "")
    except (IndexError, AttributeError):
        yield historia, gr.update(), gr.update(), gr.update(), state_datos
        return
    
    historia.append({"role": "assistant", "content": ""})
    
    try:
        # Recibimos html_esp en lugar de dict_esp
        for resp_txt, datos_mapa, html_esp, info_act, capas_extra in logica_chat(ultimo_mensaje, session_id):
            
            historia[-1]["content"] = resp_txt
            
            update_mapa = gr.update()
            if datos_mapa or capas_extra:
                html_mapa = funciones.generar_mapa_resultados(datos_mapa if datos_mapa else [], 
                    capas_extra=capas_extra)
                # Mantener el mismo tamano del mapa base cuando se renderizan resultados.
                update_mapa = gr.HTML(value=f'<div class="mapa-cuadrado">{html_mapa}</div>')
            
            # Actualizamos el HTML de esperadas
            update_esp = gr.HTML(value=html_esp, visible=True) if html_esp else gr.update()
                        
            yield historia, update_mapa, update_esp #, update_act
                
    except Exception as e:
        historia[-1]["content"] = f"Error: {str(e)}"
        yield historia, gr.update(), gr.update(), gr.update()


def generar_mapa_html():
    """
    Genera el mapa base inicial usando Folium y Esri Satellite 
    para que sea IDÉNTICO al mapa de resultados.
    """
    try:
        # 1. Crear mapa centrado en Perú (Mismo zoom y centro que funciones.py)
        m = folium.Map(
            location=[-12.0464, -77.0428], 
            zoom_start=6,
            tiles='Esri.WorldImagery' # <--- CLAVE: El mismo fondo satelital
        )
        
        # 2. Agregar etiquetas (Fronteras y Nombres)
        # Esto hace que el mapa satelital tenga nombres de ciudades encima
        folium.TileLayer(
            tiles='https://server.arcgisonline.com/ArcGIS/rest/services/Reference/World_Boundaries_and_Places/MapServer/tile/{z}/{y}/{x}',
            attr='Esri',
            name='Etiquetas',
            overlay=True
        ).add_to(m)

        # 3. Control de Capas
        folium.LayerControl().add_to(m)

        # 4. Retornar HTML envuelto para forzar formato cuadrado
        raw_html = m._repr_html_()
        return f'<div class="mapa-cuadrado">{raw_html}</div>'
        
    except Exception as e:
        return f"<div style='padding:20px; color:red;'>Error cargando mapa base: {str(e)}</div>"



def consolidar_reglas(lista_datos):
    """
    Consolida una lista de dicts parciales en un solo dict unificado.
    Útil para fusionar reglas mes a mes en un único diccionario de criterios.
    """
    reglas = {}
    if isinstance(lista_datos, list):
        for item in lista_datos:
            if isinstance(item, dict):
                reglas.update(item)
    return reglas

# --- 8. INTERFAZ GRÁFICA ---

with gr.Blocks(title="Asesor Geoespacial") as interfaz:
    
    mensaje_inicial = [{"role": "assistant", "content": "Hola, soy tu asesor Geoespacial. Dime ¿qué actividad estás interesado en emprender?"}]
    
    gr.Markdown("## 🌍 Sistema multiagente para análisis de idoneidad de terrenos")

    session_id = gr.State(lambda: str(uuid.uuid4()))
    estado_datos = gr.State([])
    
    with gr.Row(equal_height=True, elem_id="fila_principal"):
        # --- IZQUIERDA: MAPA CUADRADO ---
        with gr.Column(scale=7, min_width=700, elem_id="col_mapa"):
            gr.Markdown("### 🗺️ Visualización geográfica")
            mapa_view = gr.HTML(value=generar_mapa_html(), elem_id="mapa_geografico")

        # --- CENTRO: CARACTERISTICAS ---
        with gr.Column(scale=2, min_width=230, elem_id="col_caracteristicas"):
            gr.Markdown("### 📊 Características idóneas")
            gr.Markdown("**Condiciones:**")
            view_esperadas = gr.HTML(
                value="<div class='tabla-placeholder'>Esperando condiciones idóneas...</div>",
                label="Reglas del terreno",
                elem_id="panel_caracteristicas"
            )

        # --- DERECHA: CHAT ---
        with gr.Column(scale=3, min_width=300, elem_id="col_asistente"):
            gr.Markdown("### 💬 Asistente geoespacial")
            chatbot = gr.Chatbot(value=mensaje_inicial, height=560, label="Chat")
            msg = gr.Textbox(placeholder="Escribe tu consulta...", container=False)

            with gr.Row():
                submit_btn = gr.Button("Enviar", variant="primary")
                clear_btn = gr.Button("Limpiar")

    # --- EVENTOS ---
    lista_outputs = [chatbot, mapa_view, view_esperadas]  # Retirado view_actuales

    msg.submit(
        interaccion_usuario, [msg, chatbot], [msg, chatbot], queue=False
    ).then(
        interaccion_bot, [chatbot, estado_datos, session_id], lista_outputs
    )

    submit_btn.click(
        interaccion_usuario, [msg, chatbot], [msg, chatbot], queue=False
    ).then(
        interaccion_bot, [chatbot, estado_datos, session_id], lista_outputs
    )
    
    def limpiar_todo():
        nuevo_id = str(uuid.uuid4())
        return mensaje_inicial, generar_mapa_html(), None, nuevo_id
    
    
    clear_btn.click(limpiar_todo, None, lista_outputs + [session_id], queue=False)

    # Regenerar session_id en cada carga/recarga de página
    interfaz.load(lambda: str(uuid.uuid4()), None, session_id)


if __name__ == "__main__":
    custom_css = """
    .gradio-container {
        max-width: 100% !important;
        padding-left: 0 !important;
        padding-right: 0 !important;
    }

    :root {
        --tam-panel-principal: min(700px, calc(100vw - 860px));
    }

    #fila_principal {
        align-items: flex-start;
        justify-content: flex-start;
        flex-wrap: nowrap !important;
        column-gap: 6px;
    }

    /* Evitar hueco: el ancho de la columna del mapa coincide con el mapa cuadrado. */
    #col_mapa {
        flex: 0 0 var(--tam-panel-principal) !important;
        max-width: var(--tam-panel-principal) !important;
        min-width: 560px !important;
    }

    #col_caracteristicas {
        flex: 0 0 522px !important;
        max-width: 522px !important;
        min-width: 522px !important;
    }

    #col_asistente {
        flex: 0 0 300px !important;
        min-width: 300px !important;
        max-width: 300px !important;
    }

    #mapa_geografico {
        display: flex;
        justify-content: flex-start;
    }

    /* Mapa cuadrado, alineado a la izquierda y con alto equivalente al bloque principal. */
    .mapa-cuadrado {
        width: var(--tam-panel-principal) !important;
        height: var(--tam-panel-principal) !important;
        max-width: 100%;
        overflow: hidden;
        border-radius: 6px;
        border: 1px solid #e4e4e4;
        background: #fff;
    }

    /* Folium crea wrappers internos con proporciones fijas; los anulamos para mantener 1:1. */
    .mapa-cuadrado > div,
    .mapa-cuadrado > div > div {
        width: 100% !important;
        height: 100% !important;
        padding-bottom: 0 !important;
    }

    .mapa-cuadrado > div > div > span {
        display: none !important;
    }

    .mapa-cuadrado .folium-map {
        width: 100% !important;
        height: 100% !important;
    }

    .mapa-cuadrado iframe {
        width: 100% !important;
        height: 100% !important;
        max-height: none !important;
        border: 0 !important;
        display: block !important;
    }

    #panel_caracteristicas {
        height: var(--tam-panel-principal);
        overflow: auto;
        border: 1px solid #e4e4e4;
        border-radius: 8px;
        background: #fff;
        padding: 8px;
        box-sizing: border-box;
    }

    .tabla-placeholder {
        color: #666;
        font-size: 13px;
        padding: 8px;
    }

    /* En pantallas pequenas mantenemos proporcion cuadrada para evitar desbordes. */
    @media (max-width: 1024px) {
        :root {
            --tam-panel-principal: min(100vw - 48px, 420px);
        }

        #fila_principal {
            column-gap: 10px;
        }

        #col_mapa,
        #col_caracteristicas,
        #col_asistente {
            flex: 1 1 100% !important;
            min-width: 100% !important;
            max-width: 100% !important;
        }

        .mapa-cuadrado {
            width: var(--tam-panel-principal) !important;
            height: var(--tam-panel-principal) !important;
        }

        #panel_caracteristicas {
            height: auto;
            min-height: 280px;
        }
    }
    """
    interfaz.launch(css=custom_css)