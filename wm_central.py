import html                     # para escapar texto en la página web
import json                     # mensajes de Kafka en formato JSON
import os                       # variables de entorno (Railway)
import socket                   # sockets (comunicación con los Monitores)
import sqlite3                  # base de datos SQLite
import sys                      # argumentos de la línea de comandos
import threading                # hilos
import time                     # pausas y fechas
import uuid                     # identificador único de consumidor Kafka
from collections import deque   # lista con tamaño máximo (para los mensajes del panel)
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer   # servidor web del panel
from urllib.parse import parse_qs                                      # leer formularios web

from kafka import KafkaProducer, KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import NoBrokersAvailable, TopicAlreadyExistsError

# ---------------------------------------------------------------------------
# WM_Central  -  Sistema central de WaterManagement
#
#   * SERVIDOR de sockets para los Monitores (WM_WS_M): registro y estado.
#   * Cliente de KAFKA:
#        - lee peticiones de riego      (wm.requests)       <- Operarios / Engine manual
#        - lee telemetría de las WS     (wm.ws.telemetry)   <- Engines
#        - envía órdenes a las WS       (wm.ws.commands)    -> Engines
#        - envía mensajes a operarios   (wm.operators)      -> Operarios
#   * Base de datos SQLite (operarios, estaciones, histórico de riegos).
#   * Panel de monitorización WEB (HTTP): estado en tiempo real y órdenes.
#
# Uso:
#   python wm_central.py <PuertoSockets> <KafkaIP> <KafkaPuerto> [PuertoWeb]
# ---------------------------------------------------------------------------


########## CONSTANTES ##########

# --- Protocolo de sockets (idéntico al del Monitor) ---
HEADER = 64                     # 64 bytes con la longitud del mensaje (como en los ejemplos)
FIN = "FIN"                     # mensaje para despedirse
FORMAT = 'utf-8'
SEP = "#"
TIMEOUT_MONITOR = 5             # si un Monitor no envía nada en 5 s => lo damos por desconectado
TIMEOUT_AUTORIZACION = 5        # segundos que esperamos a que la WS autorice un riego

# --- Topics de Kafka ---
TOPIC_REQUESTS = "wm.requests"
TOPIC_COMMANDS = "wm.ws.commands"
TOPIC_TELEMETRY = "wm.ws.telemetry"
TOPIC_OPERATORS = "wm.operators"
TOPICS = [TOPIC_REQUESTS, TOPIC_COMMANDS, TOPIC_TELEMETRY, TOPIC_OPERATORS]

# --- Estados que se muestran en el panel ---
DISPONIBLE = "DISPONIBLE"
REGANDO = "REGANDO"
FUGA = "FUGA"
FUERA_SERVICIO = "FUERA DE SERVICIO"
DESCONECTADA = "DESCONECTADA"

DURACION_POR_DEFECTO = 20
DB_FILE = os.environ.get("WM_DB", "wm_central.db")     # en Railway: ruta dentro de un volumen
OPERARIOS_INICIALES = [("FO-01", "Operario 1"), ("FO-02", "Operario 2"), ("FO-03", "Operario 3"),
                       ("FO-04", "Operario 4"), ("FO-05", "Operario 5")]
OPERADORES_ESPECIALES = ("MANUAL", "CENTRAL")           # riego desde la propia WS o desde el panel

MOTIVOS = {"TIEMPO": "tiempo límite alcanzado",
           "MANUAL_FIN": "finalizado desde la propia estación",
           "FUGA": "fuga detectada",
           "BLOQUEO": "bloqueo ordenado por CENTRAL",
           "DESCONEXION": "estación desconectada"}


########## VARIABLES COMPARTIDAS ##########

estaciones = {}                 # id de WS -> diccionario con su información (ver nueva_estacion)
lock_est = threading.Lock()     # cerrojo que protege a "estaciones"
lock_db = threading.Lock()      # cerrojo para acceder a SQLite
mensajes = deque(maxlen=60)     # últimos mensajes (se muestran en el panel)
producer = None
kafka_listo = threading.Event()


########## LOG ##########

def log(texto):
    linea = f"{time.strftime('%H:%M:%S')} {texto}"
    print(linea, flush=True)
    mensajes.append(linea)


########## FUNCIONES DE MENSAJERIA (mismo protocolo que cliente/servidor concurrente) ##########
# Antes de cada mensaje se envían 64 bytes que indican su longitud, así el receptor sabe cuántos bytes leer.

def send(sock, msg):
    message = msg.encode(FORMAT)
    msg_length = len(message)
    send_length = str(msg_length).encode(FORMAT)
    send_length += b' ' * (HEADER - len(send_length))
    sock.sendall(send_length)
    sock.sendall(message)


def recibir_exacto(sock, n):
    # recv puede devolver menos bytes de los pedidos: leemos hasta tener n
    datos = b''
    while len(datos) < n:
        trozo = sock.recv(n - len(datos))
        if not trozo:
            raise ConnectionError("conexión cerrada por el otro extremo")
        datos += trozo
    return datos


def recibir(sock):
    msg_length = int(recibir_exacto(sock, HEADER).decode(FORMAT))
    return recibir_exacto(sock, msg_length).decode(FORMAT)


########## BASE DE DATOS (SQLite) ##########

def db_ejecutar(sql, params=(), leer=False):
    # Abre la BD, ejecuta una sentencia SQL y la cierra. El cerrojo evita accesos simultáneos.
    with lock_db:
        con = sqlite3.connect(DB_FILE)
        try:
            cursor = con.execute(sql, params)
            filas = cursor.fetchall() if leer else None
            con.commit()
            return filas
        finally:
            con.close()


def iniciar_db():
    db_ejecutar("CREATE TABLE IF NOT EXISTS operarios (id TEXT PRIMARY KEY, nombre TEXT)")
    db_ejecutar("CREATE TABLE IF NOT EXISTS estaciones "
                "(id TEXT PRIMARY KEY, ubicacion TEXT, estado TEXT)")
    db_ejecutar("CREATE TABLE IF NOT EXISTS riegos "
                "(id INTEGER PRIMARY KEY AUTOINCREMENT, ws TEXT, operario TEXT, "
                "fecha TEXT, duracion REAL, volumen REAL, motivo TEXT)")
    for id_op, nombre in OPERARIOS_INICIALES:
        db_ejecutar("INSERT OR IGNORE INTO operarios VALUES (?, ?)", (id_op, nombre))


def operario_existe(id_op):
    return len(db_ejecutar("SELECT id FROM operarios WHERE id = ?", (id_op,), leer=True)) > 0


########## ESTACIONES (estado en memoria) ##########

def nueva_estacion(ws_id, ubicacion):
    return {"id": ws_id, "ubicacion": ubicacion,
            "conectado": False,      # ¿hay un Monitor conectado por socket?
            "conn": None,            # socket de ese Monitor
            "monitor": "WAIT",       # último estado enviado por el Monitor: WAIT / OK / KO
            "engine": None,          # último estado enviado por el Engine (por Kafka)
            "caudal": 0.0, "volumen": 0.0, "operario": "-",
            "riego_op": None,        # operario del riego en curso (None = no hay riego)
            "riego_inicio": None,
            "pendiente": None}       # petición esperando autorización: (operario, hora)


def cargar_estaciones():
    # Al arrancar: cargamos de la BD las estaciones ya dadas de alta. Todas DESCONECTADAS
    # hasta que su Monitor conecte (no conocemos su estado real).
    for ws_id, ubicacion, estado in db_ejecutar("SELECT id, ubicacion, estado FROM estaciones", leer=True):
        estaciones[ws_id] = nueva_estacion(ws_id, ubicacion)
        log(f"[BD] Estación {ws_id} ({ubicacion}) cargada: DESCONECTADA hasta que conecte")


def estado_visible(ws):
    # Calcula el estado que se muestra en el panel. Llamar SIEMPRE con lock_est cogido.
    if not ws["conectado"]:
        return DESCONECTADA
    if ws["monitor"] == "KO" or ws["engine"] == "LEAK":
        return FUGA
    if ws["engine"] is None:
        return DESCONECTADA            # el Engine aún no ha dicho nada
    if ws["engine"] == "OUT_OF_SERVICE":
        return FUERA_SERVICIO
    if ws["engine"] == "WATERING":
        return REGANDO
    return DISPONIBLE


########## KAFKA ##########

def conectar_kafka():
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_ADDR,
                value_serializer=lambda dato: json.dumps(dato).encode(FORMAT))
            log(f"[KAFKA] Conectado al broker {KAFKA_ADDR}")
        except NoBrokersAvailable:
            log(f"[KAFKA] Broker {KAFKA_ADDR} no disponible. Reintento en 3s")
            time.sleep(3)


def crear_topics():
    try:
        admin = KafkaAdminClient(bootstrap_servers=KAFKA_ADDR)
        for topic in TOPICS:
            try:
                admin.create_topics([NewTopic(name=topic, num_partitions=1, replication_factor=1)])
            except TopicAlreadyExistsError:
                pass
        admin.close()
    except Exception as e:
        log(f"[KAFKA] No se han podido crear los topics ({e}). Se confía en la creación automática")


def publicar(topic, dato):
    producer.send(topic, dato)
    producer.flush()


def avisar(operador, tipo, ws_id, texto, mostrar=True):
    # Envía un mensaje a un operario (y lo muestra también en la pantalla de CENTRAL)
    if mostrar:
        log(f"[CENTRAL -> {operador}] {texto}")
    publicar(TOPIC_OPERATORS, {"operator": operador, "type": tipo, "ws": ws_id, "msg": texto})


########## LOGICA: PETICIONES DE RIEGO ##########

def denegar(operador, ws_id, motivo):
    avisar(operador, "DENIED", ws_id, f"Riego DENEGADO en {ws_id}: {motivo}")


def tratar_peticion(operador, ws_id, duracion):
    # Pasos de la mecánica de la solución: validar -> pedir autorización a la WS -> riego
    avisar(operador, "INFO", ws_id, f"Petición recibida: riego en {ws_id} durante {duracion} s")

    # 1) ¿Operario válido?
    if operador not in OPERADORES_ESPECIALES and not operario_existe(operador):
        denegar(operador, ws_id, "operario no registrado en CENTRAL")
        return

    # 2) ¿Existe la WS y está disponible?
    motivo = None                       # si queda None, la petición es válida
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is None:
            motivo = "la estación no existe"
        else:
            estado = estado_visible(ws)
            if estado != DISPONIBLE:
                motivo = f"la estación no está disponible (estado: {estado})"
            elif ws["pendiente"] is not None or ws["riego_op"] is not None:
                motivo = "la estación está atendiendo otra petición"
            else:
                ws["pendiente"] = (operador, time.time())      # reservamos la WS
    if motivo is not None:
        denegar(operador, ws_id, motivo)
        return

    # 3) Pedimos autorización a la WS (le enviamos la orden START por Kafka)
    avisar(operador, "INFO", ws_id, f"{ws_id} disponible. Solicitando autorización a la estación...")
    publicar(TOPIC_COMMANDS, {"ws": ws_id, "cmd": "START", "operator": operador, "duration": duracion})


def cerrar_riego(ws_id, motivo, volumen=None, duracion=None):
    # Da por terminado el riego en curso de una WS (guarda en BD y avisa al operario).
    # Es "idempotente": si ya se cerró, no hace nada (puede llamarse desde varios sitios).
    with lock_est:
        ws = estaciones.get(ws_id)
        if ws is None or ws["riego_op"] is None:
            return
        operador = ws["riego_op"]
        if volumen is None:
            volumen = ws["volumen"]
        if duracion is None:
            duracion = time.time() - ws["riego_inicio"]
        ws["riego_op"] = None
        ws["riego_inicio"] = None
        ws["operario"] = "-"
        ws["caudal"] = 0.0
        ws["volumen"] = 0.0
    db_ejecutar("INSERT INTO riegos (ws, operario, fecha, duracion, volumen, motivo) VALUES (?,?,?,?,?,?)",
                (ws_id, operador, time.strftime("%Y-%m-%d %H:%M:%S"), duracion, volumen, motivo))
    avisar(operador, "END", ws_id,
           f"RESUMEN del riego en {ws_id}: volumen {volumen:.1f} L, duración {duracion:.0f} s, "
           f"motivo: {MOTIVOS.get(motivo, motivo)}")


def tratar_telemetria(t):
    ws_id = t.get("ws")
    tipo = t.get("type")
    with lock_est:
        ws = estaciones.get(ws_id)
    if ws is None:
        return                                  # estación no dada de alta: la ignoramos

    if tipo == "TICK":
        with lock_est:
            ws["engine"] = t["state"]
            ws["caudal"] = t["flow"]
            ws["volumen"] = t["volume"]
            ws["operario"] = t["operator"] or "-"
            operador = ws["riego_op"]
        if t["state"] == "WATERING" and operador:
            # El operario solo recibe los mensajes de la WS que le está prestando servicio
            avisar(operador, "PROGRESS", ws_id,
                   f"{ws_id} regando: caudal {t['flow']} L/min, volumen acumulado {t['volume']} L",
                   mostrar=False)

    elif tipo == "ACCEPTED":
        with lock_est:
            ws["pendiente"] = None
            ws["riego_op"] = t["operator"]
            ws["riego_inicio"] = time.time()
        avisar(t["operator"], "AUTHORIZED", ws_id,
               f"{ws_id} ha AUTORIZADO el riego. Electroválvula abierta")

    elif tipo == "DENIED":
        with lock_est:
            ws["pendiente"] = None
        denegar(t["operator"], ws_id, t.get("reason", "denegado por la estación"))

    elif tipo == "FINISHED":
        cerrar_riego(ws_id, t["reason"], t["volume"], t["duration"])


########## HILOS ##########

def hilo_kafka():
    # Lee las peticiones de riego y la telemetría de las estaciones
    consumer = KafkaConsumer(TOPIC_REQUESTS, TOPIC_TELEMETRY,
                             bootstrap_servers=KAFKA_ADDR,
                             group_id=f"wm-central-{uuid.uuid4()}",
                             auto_offset_reset="latest",
                             value_deserializer=lambda b: json.loads(b.decode(FORMAT)))
    while not consumer.assignment():
        consumer.poll(timeout_ms=200)
    kafka_listo.set()
    log("[KAFKA] CENTRAL escuchando peticiones y telemetría")

    for mensaje in consumer:
        try:
            if mensaje.topic == TOPIC_REQUESTS:
                p = mensaje.value
                tratar_peticion(p["operator"], p["ws"], int(p.get("duration", DURACION_POR_DEFECTO)))
            elif mensaje.topic == TOPIC_TELEMETRY:
                tratar_telemetria(mensaje.value)
        except Exception as e:
            # Un mensaje mal formado no debe tumbar el hilo: lo registramos y seguimos
            log(f"[ERROR] Mensaje ignorado ({e}): {mensaje.value}")


def hilo_estado():
    # Cada segundo: publica la tabla de estaciones (la ven los operarios) y guarda cambios en la BD
    kafka_listo.wait()
    ultimo = {}
    while True:
        with lock_est:
            tabla = [{"id": ws["id"], "ubicacion": ws["ubicacion"], "estado": estado_visible(ws)}
                     for ws in estaciones.values()]
        publicar(TOPIC_OPERATORS, {"operator": "*", "type": "STATUS", "estaciones": tabla})
        for fila in tabla:
            if ultimo.get(fila["id"]) != fila["estado"]:
                if fila["id"] in ultimo:
                    log(f"[WS {fila['id']}] {ultimo[fila['id']]} -> {fila['estado']}")
                ultimo[fila["id"]] = fila["estado"]
                db_ejecutar("UPDATE estaciones SET estado = ? WHERE id = ?", (fila["estado"], fila["id"]))
        time.sleep(1)


def hilo_vigilar_pendientes():
    # Si una WS no responde a la solicitud de autorización a tiempo, se deniega el riego
    while True:
        caducadas = []
        with lock_est:
            for ws in estaciones.values():
                if ws["pendiente"] is not None and time.time() - ws["pendiente"][1] > TIMEOUT_AUTORIZACION:
                    caducadas.append((ws["pendiente"][0], ws["id"]))
                    ws["pendiente"] = None
        for operador, ws_id in caducadas:
            denegar(operador, ws_id, "la estación no ha respondido a la solicitud de autorización")
        time.sleep(1)


########## LADO SERVIDOR: MONITORES (sockets) ##########

def handle_monitor(conn, addr):
    print(f"[NUEVA CONEXION] Monitor {addr} connected.")
    conn.settimeout(TIMEOUT_MONITOR)
    ws_id = None
    try:
        # ---- 1) Registro / autenticación ----
        peticion = recibir(conn).split(SEP)
        if peticion[0] != "REGISTER" or len(peticion) < 3 or not peticion[1].strip():
            send(conn, f"KO{SEP}registro incorrecto")
            return
        nuevo_id, ubicacion = peticion[1].strip(), peticion[2]

        with lock_est:
            ws = estaciones.get(nuevo_id)
            alta = ws is None
            if alta:
                ws = nueva_estacion(nuevo_id, ubicacion)
                estaciones[nuevo_id] = ws
            elif ubicacion != "UNKNOWN":
                ws["ubicacion"] = ubicacion
            anterior = ws["conn"]               # si ya había un Monitor con este ID, lo sustituimos
            ws["conn"] = conn
            ws["conectado"] = True
            ws["monitor"] = "WAIT"
        ws_id = nuevo_id
        if anterior is not None:
            try:
                anterior.close()
            except OSError:
                pass
        db_ejecutar("INSERT INTO estaciones (id, ubicacion, estado) VALUES (?, ?, ?) "
                    "ON CONFLICT(id) DO UPDATE SET ubicacion = excluded.ubicacion",
                    (ws_id, ubicacion, DESCONECTADA))
        log(f"[WS {ws_id}] {'ALTA de nueva estación' if alta else 'Registro'} en {ubicacion}. Monitor autenticado")
        send(conn, "OK")

        # ---- 2) Estado periódico del Monitor ----
        while True:
            msg = recibir(conn).split(SEP)
            if msg[0] == FIN:
                print(f"[MONITOR] {ws_id} se despide (FIN)")
                break
            if msg[0] == "STATUS" and len(msg) >= 3 and msg[1] == ws_id:
                with lock_est:
                    anterior_estado = ws["monitor"]
                    ws["monitor"] = msg[2]
                if msg[2] == "KO" and anterior_estado != "KO":
                    log(f"[!!! AVERIA !!!] El Monitor de {ws_id} informa de una FUGA")
                    cerrar_riego(ws_id, "FUGA")      # si estaba regando, se corta
                elif msg[2] == "OK" and anterior_estado == "KO":
                    log(f"[AVERIA RESUELTA] El Monitor de {ws_id} informa de que la fuga está resuelta")
                send(conn, "OK")
            else:
                send(conn, f"KO{SEP}mensaje desconocido")

    except (socket.timeout, ConnectionError, OSError, ValueError) as e:
        print(f"[MONITOR] Conexión con {ws_id or addr} terminada: {e}")
    finally:
        conn.close()
        if ws_id is not None:
            with lock_est:
                ws = estaciones.get(ws_id)
                era_actual = ws is not None and ws["conn"] is conn   # ¿no nos ha sustituido otro Monitor?
                if era_actual:
                    ws["conectado"] = False
                    ws["conn"] = None
                    ws["monitor"] = "WAIT"
                    ws["engine"] = None
            if era_actual:
                log(f"[WS {ws_id}] DESCONECTADA")
                cerrar_riego(ws_id, "DESCONEXION")


def start_servidor_sockets():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", PUERTO_SOCKETS))
    server.listen()
    log(f"[LISTENING] CENTRAL a la escucha de Monitores en el puerto {PUERTO_SOCKETS}")
    while True:
        conn, addr = server.accept()
        threading.Thread(target=handle_monitor, args=(conn, addr), daemon=True).start()


########## PANEL WEB ##########

CSS = """
body{font-family:Segoe UI,Arial,sans-serif;background:#10202b;color:#e8eef2;margin:20px}
h1{font-size:22px;margin:0 0 4px} .sub{color:#8fa6b5;margin-bottom:14px}
table{border-collapse:collapse;width:100%;margin-bottom:14px}
th,td{padding:8px 10px;border-bottom:1px solid #2c4252;text-align:left}
th{color:#8fa6b5;font-weight:600}
.disp{background:#2e7d32} .riego{background:#2e7d32;animation:parpadeo 1s infinite}
.fuga{background:#c62828} .fuera{background:#ef6c00} .desc{background:#757575}
@keyframes parpadeo{50%{opacity:.3}}
.resumen span{display:inline-block;padding:4px 10px;border-radius:12px;margin-right:6px;font-size:13px}
button{padding:4px 9px;margin-right:3px;border:0;border-radius:4px;cursor:pointer}
input{width:50px}
pre{background:#0a141b;padding:10px;border-radius:6px;font-size:12px;line-height:1.5;overflow:auto}
"""

CLASES = {DISPONIBLE: "disp", REGANDO: "riego", FUGA: "fuga",
          FUERA_SERVICIO: "fuera", DESCONECTADA: "desc"}


def generar_pagina():
    with lock_est:
        filas = [(ws["id"], ws["ubicacion"], estado_visible(ws), ws["caudal"], ws["volumen"], ws["operario"])
                 for ws in sorted(estaciones.values(), key=lambda w: w["id"])]
    conteo = {estado: 0 for estado in CLASES}
    for fila in filas:
        conteo[fila[2]] += 1

    resumen = "".join(f"<span class='{CLASES[e]}'>{n} {e.capitalize()}</span>" for e, n in conteo.items())

    cuerpo = ""
    for ws_id, ubicacion, estado, caudal, volumen, operario in filas:
        regando = estado == REGANDO
        texto_estado = "Fuera de Servicio" if estado == FUERA_SERVICIO else estado
        cuerpo += (f"<tr><td>{html.escape(ws_id)}</td><td>{html.escape(ubicacion)}</td>"
                   f"<td class='{CLASES[estado]}'>{texto_estado}</td>"
                   f"<td>{caudal if regando else '-'}</td><td>{volumen if regando else '-'}</td>"
                   f"<td>{html.escape(operario) if regando else '-'}</td>"
                   f"<td><form method='post'><input type='hidden' name='ws' value='{html.escape(ws_id)}'>"
                   f"<input type='number' name='duracion' value='{DURACION_POR_DEFECTO}' min='1'> s "
                   f"<button name='accion' value='start'>Iniciar riego</button>"
                   f"<button name='accion' value='block'>Bloquear</button>"
                   f"<button name='accion' value='activate'>Activar</button></form></td></tr>")

    return ("<!DOCTYPE html><html><head><meta charset='utf-8'><meta http-equiv='refresh' content='2'>"
            "<title>WM_Central</title><style>" + CSS + "</style></head><body>"
            "<h1>WaterManagement · Panel de CENTRAL</h1>"
            f"<div class='sub'>{len(filas)} estaciones registradas · Kafka: {html.escape(KAFKA_ADDR)}</div>"
            f"<div class='resumen'>{resumen}</div><br>"
            "<table><tr><th>ID</th><th>Ubicación</th><th>Estado</th><th>Caudal (L/min)</th>"
            "<th>Volumen (L)</th><th>Operario</th><th>Acciones</th></tr>" + cuerpo + "</table>"
            "<b>Órdenes a TODAS las estaciones:</b> "
            "<form method='post' style='display:inline'><input type='hidden' name='ws' value='*'>"
            f"<input type='number' name='duracion' value='{DURACION_POR_DEFECTO}' min='1'> s "
            "<button name='accion' value='start'>Iniciar riego</button>"
            "<button name='accion' value='block'>Bloquear</button>"
            "<button name='accion' value='activate'>Activar</button></form>"
            "<h3>Mensajes de la aplicación</h3><pre>" + html.escape("\n".join(list(mensajes)[-18:])) +
            "</pre></body></html>")


def ejecutar_accion(accion, ws_id, duracion):
    # Órdenes manuales de CENTRAL sobre una o todas las WS (punto 11 de la mecánica)
    if accion == "start":
        with lock_est:
            objetivos = list(estaciones.keys()) if ws_id == "*" else [ws_id]
        for objetivo in objetivos:
            tratar_peticion("CENTRAL", objetivo, duracion)
    elif accion == "block":
        log(f"[CENTRAL] Orden BLOQUEAR a {'todas las WS' if ws_id == '*' else ws_id}")
        publicar(TOPIC_COMMANDS, {"ws": ws_id, "cmd": "BLOCK"})
    elif accion == "activate":
        log(f"[CENTRAL] Orden ACTIVAR a {'todas las WS' if ws_id == '*' else ws_id}")
        publicar(TOPIC_COMMANDS, {"ws": ws_id, "cmd": "ACTIVATE"})


class PanelHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        pagina = generar_pagina().encode(FORMAT)
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(pagina)))
        self.end_headers()
        self.wfile.write(pagina)

    def do_POST(self):
        longitud = int(self.headers.get("Content-Length", 0))
        datos = parse_qs(self.rfile.read(longitud).decode(FORMAT))
        accion = datos.get("accion", [""])[0]
        ws_id = datos.get("ws", ["*"])[0]
        try:
            duracion = int(datos.get("duracion", [DURACION_POR_DEFECTO])[0])
        except ValueError:
            duracion = DURACION_POR_DEFECTO
        ejecutar_accion(accion, ws_id, duracion)
        self.send_response(303)                  # redirigimos al panel
        self.send_header("Location", "/")
        self.end_headers()

    def log_message(self, *args):
        pass                                     # no llenamos la consola con cada petición HTTP


########## MAIN ##########

print("****** WELCOME TO WM_CENTRAL: SISTEMA CENTRAL DE WATERMANAGEMENT ****")

if len(sys.argv) in (4, 5):
    PUERTO_SOCKETS = int(sys.argv[1])
    KAFKA_ADDR = f"{sys.argv[2]}:{sys.argv[3]}"
    # Puerto web: 4º argumento, o la variable PORT (la que asigna Railway), o 8080
    PUERTO_WEB = int(os.environ.get("PORT", sys.argv[4] if len(sys.argv) == 5 else 8080))

    iniciar_db()
    cargar_estaciones()
    conectar_kafka()
    crear_topics()

    threading.Thread(target=hilo_kafka, daemon=True).start()
    kafka_listo.wait()          # no seguimos hasta que Kafka esté listo para recibir
    threading.Thread(target=hilo_estado, daemon=True).start()
    threading.Thread(target=hilo_vigilar_pendientes, daemon=True).start()
    threading.Thread(target=start_servidor_sockets, daemon=True).start()

    log(f"[WEB] Panel de monitorización en http://0.0.0.0:{PUERTO_WEB}")
    try:
        ThreadingHTTPServer(("0.0.0.0", PUERTO_WEB), PanelHandler).serve_forever()   # el hilo principal sirve el panel
    except KeyboardInterrupt:
        print("CENTRAL detenida")
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: "
          "<PuertoSockets> <KafkaIP> <KafkaPuerto> [PuertoWeb]")
