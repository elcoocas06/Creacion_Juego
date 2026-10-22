import json       # para convertir diccionarios <-> texto JSON (mensajes de Kafka)
import random     # para simular el caudal del caudalímetro
import socket     # sockets (comunicación con el Monitor)
import sys        # argumentos de la línea de comandos
import threading  # hilos
import time       # pausas y medir tiempos
import uuid       # para generar un identificador único de consumidor Kafka

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# WM_WS_E  -  Engine de la Watering Station
#
#   * CLIENTE de sockets frente al Monitor (WM_WS_M): contesta a la comprobación
#     de salud "HEALTH" que el Monitor le envía cada segundo.
#   * CLIENTE de Kafka frente a CENTRAL:
#        - recibe órdenes   (topic wm.ws.commands):  START / BLOCK / ACTIVATE
#        - envía telemetría (topic wm.ws.telemetry): estado, caudal, volumen...
#        - envía peticiones de riego manual (topic wm.requests)
#
# Simula: caudalímetro, detector de fuga y electroválvula.
#
# Uso:
#   python wm_ws_e.py <KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>
# ---------------------------------------------------------------------------


########## CONSTANTES ##########

# --- Protocolo de sockets (idéntico al del Monitor) ---
HEADER = 64                     # 64 bytes con la longitud del mensaje (como en los ejemplos)
FIN = "FIN"                     # mensaje para despedirse
FORMAT = 'utf-8'
SEP = "#"
OK = "OK"
KO = "KO"

# --- Topics de Kafka ---
TOPIC_REQUESTS = "wm.requests"          # FO / Engine (manual)  -> CENTRAL
TOPIC_COMMANDS = "wm.ws.commands"       # CENTRAL -> Engines
TOPIC_TELEMETRY = "wm.ws.telemetry"     # Engines -> CENTRAL

# --- Estados internos de la estación ---
DISPONIBLE = "AVAILABLE"
REGANDO = "WATERING"
FUERA_SERVICIO = "OUT_OF_SERVICE"
FUGA = "LEAK"

DURACION_POR_DEFECTO = 20               # segundos de riego si no se indica otra cosa


########## VARIABLES COMPARTIDAS ENTRE HILOS ##########

ws_id = None                 # lo recibimos del Monitor en el primer HEALTH
estado = DISPONIBLE          # estado actual de la estación
bloqueada = False            # True si CENTRAL ha ordenado BLOQUEAR
ko_forzado = False           # True si el usuario ha simulado una avería con el teclado
riego = None                 # datos del riego en curso (diccionario) o None si no hay
producer = None              # productor de Kafka

# RLock = cerrojo "reentrante": el mismo hilo puede cogerlo varias veces seguidas
# (hace falta porque unas funciones llaman a otras que también usan el cerrojo)
lock = threading.RLock()
kafka_listo = threading.Event()      # se activa cuando el consumidor de Kafka ya está preparado
hilos_lanzados = False


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


########## KAFKA ##########

def conectar_kafka():
    # Intentamos conectar con el broker hasta que lo consigamos
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_ADDR,
                value_serializer=lambda dato: json.dumps(dato).encode(FORMAT))
            print(f"[KAFKA] Conectado al broker {KAFKA_ADDR}")
        except NoBrokersAvailable:
            print(f"[KAFKA] Broker {KAFKA_ADDR} no disponible. Reintento en 3s")
            time.sleep(3)


def publicar(topic, dato):
    producer.send(topic, dato)
    producer.flush()


########## LOGICA DE LA ESTACION ##########
# Todas estas funciones se llaman con "with lock" para que los hilos
# (menú, Kafka, telemetría) no modifiquen el estado a la vez.

def iniciar_riego(operador, duracion):
    # CENTRAL nos pide (y nosotros autorizamos o denegamos) un riego
    global estado, riego
    with lock:
        if estado != DISPONIBLE:
            motivo = f"la estación no está disponible (estado actual: {estado})"
            print(f"[RIEGO] Petición de {operador} DENEGADA: {motivo}")
            publicar(TOPIC_TELEMETRY, {"type": "DENIED", "ws": ws_id,
                                       "operator": operador, "reason": motivo})
            return
        estado = REGANDO
        riego = {"operador": operador, "inicio": time.time(),
                 "limite": duracion, "volumen": 0.0, "caudal": 0.0}
        print(f"[VALVULA] ABIERTA. Riego iniciado por {operador} (límite {duracion} s)")
        publicar(TOPIC_TELEMETRY, {"type": "ACCEPTED", "ws": ws_id, "operator": operador})


def terminar_riego(motivo):
    # motivo: TIEMPO | MANUAL_FIN | FUGA | BLOQUEO
    global estado, riego
    with lock:
        if riego is None:
            return
        duracion = time.time() - riego["inicio"]
        publicar(TOPIC_TELEMETRY, {"type": "FINISHED", "ws": ws_id,
                                   "operator": riego["operador"],
                                   "volume": round(riego["volumen"], 1),
                                   "duration": round(duracion),
                                   "reason": motivo})
        print(f"[VALVULA] CERRADA. Riego terminado ({motivo}). "
              f"Volumen total: {riego['volumen']:.1f} L, duración: {duracion:.0f} s")
        riego = None
        if motivo == "FUGA":
            estado = FUGA
        elif motivo == "BLOQUEO":
            estado = FUERA_SERVICIO
        else:
            estado = DISPONIBLE


def alternar_averia():
    # Tecla 'k': simula una fuga (KO) o la repara
    global ko_forzado, estado
    with lock:
        ko_forzado = not ko_forzado
        if ko_forzado:
            print("[!!! AVERIA !!!] Fuga simulada. Se responderá KO al Monitor")
            terminar_riego("FUGA")        # si estaba regando, se corta inmediatamente
            estado = FUGA
        else:
            print("[AVERIA RESUELTA] Fuga reparada. Se responderá OK al Monitor")
            estado = FUERA_SERVICIO if bloqueada else DISPONIBLE


def bloquear():
    global bloqueada, estado
    with lock:
        bloqueada = True
        terminar_riego("BLOQUEO")         # si estaba regando, lo corta
        if estado == DISPONIBLE:
            estado = FUERA_SERVICIO
        print("[ESTADO] FUERA DE SERVICIO por orden de CENTRAL")


def activar():
    global bloqueada, estado
    with lock:
        bloqueada = False
        if estado == FUERA_SERVICIO:
            estado = DISPONIBLE
        print("[ESTADO] ESTACIÓN ACTIVADA por orden de CENTRAL")


########## HILOS ##########

def hilo_telemetria():
    # Cada segundo envía a CENTRAL el estado; si riega, también caudal y volumen
    global riego
    kafka_listo.wait()
    while True:
        with lock:
            if estado == REGANDO and riego is not None:
                riego["caudal"] = round(random.uniform(8.0, 12.0), 1)     # litros/minuto simulados
                riego["volumen"] += riego["caudal"] / 60                  # litros en 1 segundo
                transcurrido = time.time() - riego["inicio"]
                print(f"[RIEGO] {riego['operador']} | caudal {riego['caudal']} L/min | "
                      f"volumen {riego['volumen']:.1f} L | {transcurrido:.0f}/{riego['limite']} s")
                publicar(TOPIC_TELEMETRY, {"type": "TICK", "ws": ws_id, "state": estado,
                                           "flow": riego["caudal"],
                                           "volume": round(riego["volumen"], 1),
                                           "operator": riego["operador"]})
                if transcurrido >= riego["limite"]:
                    terminar_riego("TIEMPO")
            else:
                publicar(TOPIC_TELEMETRY, {"type": "TICK", "ws": ws_id, "state": estado,
                                           "flow": 0.0, "volume": 0.0, "operator": ""})
        time.sleep(1)


def hilo_comandos():
    # Escucha las órdenes de CENTRAL en Kafka
    # group_id único: así CADA Engine recibe TODOS los mensajes (y luego filtra los suyos)
    consumer = KafkaConsumer(TOPIC_COMMANDS,
                             bootstrap_servers=KAFKA_ADDR,
                             group_id=f"wm-engine-{ws_id}-{uuid.uuid4()}",
                             auto_offset_reset="latest",
                             value_deserializer=lambda b: json.loads(b.decode(FORMAT)))
    # Esperamos a que Kafka nos asigne la partición antes de decir que estamos listos
    while not consumer.assignment():
        consumer.poll(timeout_ms=200)
    kafka_listo.set()
    print(f"[KAFKA] Escuchando órdenes de CENTRAL para {ws_id}")

    for mensaje in consumer:
        orden = mensaje.value
        if orden.get("ws") not in (ws_id, "*"):      # "*" = orden para todas las WS
            continue                                  # no es para nosotros
        cmd = orden.get("cmd")
        print(f"[CENTRAL] Orden recibida: {cmd}")
        if cmd == "START":
            iniciar_riego(orden.get("operator", "?"), int(orden.get("duration", DURACION_POR_DEFECTO)))
        elif cmd == "BLOCK":
            bloquear()
        elif cmd == "ACTIVATE":
            activar()


def mostrar_menu():
    print(f"""
===== MENU ENGINE ({ws_id}) =====
 [k] Simular / reparar avería (KO)
 [s] Solicitar riego en esta estación (manual)
 [f] Finalizar el riego en curso
 [e] Ver estado
""")


def hilo_menu():
    # Lee el teclado. Si no hay teclado (Docker sin -it) el menú se desactiva.
    mostrar_menu()
    while True:
        try:
            opcion = input().strip().lower()
        except EOFError:
            print("(No hay teclado disponible: menú desactivado)")
            return
        if opcion == "k":
            alternar_averia()
        elif opcion == "s":
            try:
                texto = input(f"Duración del riego en segundos [{DURACION_POR_DEFECTO}]: ").strip()
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                print("Duración no válida")
                continue
            # El riego manual también pasa por CENTRAL (valida y nos devuelve la orden START)
            publicar(TOPIC_REQUESTS, {"operator": "MANUAL", "ws": ws_id, "duration": duracion})
            print("[RIEGO] Petición manual enviada a CENTRAL")
        elif opcion == "f":
            if riego is None:
                print("No hay ningún riego en curso")
            else:
                terminar_riego("MANUAL_FIN")
        elif opcion == "e":
            with lock:
                print(f"[ESTADO] {ws_id}: {estado} | avería simulada: {ko_forzado} | bloqueada: {bloqueada}")
        else:
            mostrar_menu()


def lanzar_hilos():
    threading.Thread(target=hilo_comandos, daemon=True).start()
    threading.Thread(target=hilo_telemetria, daemon=True).start()
    threading.Thread(target=hilo_menu, daemon=True).start()


########## LADO CLIENTE: MONITOR ##########

def hablar_con_monitor():
    global ws_id, hilos_lanzados
    while True:       # bucle exterior: si se pierde la conexión, volvemos a conectar
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            print(f"Conectando con el Monitor en [{ADDR_MONITOR}] ...")
            sock.connect(ADDR_MONITOR)
            print("Conectado al Monitor. Esperando comprobaciones de salud...")

            while True:
                # El Monitor nos pregunta "HEALTH#<id_ws>" cada segundo
                peticion = recibir(sock).split(SEP)
                if peticion[0] == "HEALTH" and len(peticion) > 1:
                    if not hilos_lanzados:
                        ws_id = peticion[1]        # aprendemos nuestro ID del Monitor
                        print(f"Esta estación es la {ws_id} (ID validado por CENTRAL)")
                        lanzar_hilos()
                        hilos_lanzados = True
                    respuesta = KO if ko_forzado else OK
                    send(sock, f"HEALTH_ANS{SEP}{respuesta}")
                else:
                    print("Mensaje desconocido del Monitor:", peticion)

        except (socket.timeout, ConnectionError, OSError, ValueError) as e:
            print(f"Conexión con el Monitor perdida ({e}). Reintento en 2s")
            sock.close()
            time.sleep(2)


########## MAIN ##########

print("****** WELCOME TO WM_WS_E: ENGINE DE LA WATERING STATION ****")

if len(sys.argv) == 5:
    KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
    ADDR_MONITOR = (sys.argv[3], int(sys.argv[4]))
    conectar_kafka()
    hablar_con_monitor()
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: "
          "<KafkaIP> <KafkaPuerto> <MonitorIP> <MonitorPuerto>")
