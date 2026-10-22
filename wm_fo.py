import json       # mensajes de Kafka en formato JSON
import sys        # argumentos de la línea de comandos
import threading  # hilos
import time       # pausas
import uuid       # identificador único de consumidor Kafka

from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import NoBrokersAvailable

# ---------------------------------------------------------------------------
# WM_FO  -  Aplicación del operario de campo (Field Operator)
#
# Solo habla con Kafka (nunca directamente con una WS ni con la BD):
#   - envía sus peticiones de riego a CENTRAL     (topic wm.requests)
#   - recibe los mensajes de CENTRAL              (topic wm.operators)
#        * mensajes dirigidos a ÉL: pasos de la petición, progreso del riego, resumen final
#        * la tabla con el estado de todas las WS (para ver cuáles están disponibles)
#
# Dos modos de uso:
#   - Interactivo (menú):      python wm_fo.py <KafkaIP> <KafkaPuerto> <ID_Operario>
#   - Automático (con fichero): python wm_fo.py <KafkaIP> <KafkaPuerto> <ID_Operario> servicios.txt
#
# Formato del fichero (una línea por servicio):   ID_WS,duracion_en_segundos
#   Ejemplo:   WS-04,15
#   Las líneas vacías y las que empiezan por # se ignoran.
# ---------------------------------------------------------------------------


########## CONSTANTES ##########

FORMAT = 'utf-8'
TOPIC_REQUESTS = "wm.requests"
TOPIC_OPERATORS = "wm.operators"
DURACION_POR_DEFECTO = 20
ESPERA_ENTRE_SERVICIOS = 4         # segundos de espera tras concluir un servicio (enunciado)
MARGEN_RESPUESTA = 30              # segundos extra que esperamos a CENTRAL antes de rendirnos


########## VARIABLES COMPARTIDAS ##########

producer = None
tabla = {}                                  # id de WS -> datos (ubicación, estado)
lock_tabla = threading.Lock()
tabla_recibida = threading.Event()          # CENTRAL ya nos ha enviado la tabla al menos una vez
concluido = threading.Event()               # el servicio actual ha terminado (con éxito o denegado)


########## KAFKA ##########

def conectar_kafka():
    global producer
    while producer is None:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_ADDR,
                value_serializer=lambda dato: json.dumps(dato).encode(FORMAT))
        except NoBrokersAvailable:
            print(f"[KAFKA] Broker {KAFKA_ADDR} no disponible. Reintento en 3s")
            time.sleep(3)


def solicitar_riego(ws_id, duracion):
    print(f"\n>>> Solicito a CENTRAL riego en {ws_id} durante {duracion} s")
    producer.send(TOPIC_REQUESTS, {"operator": OPERADOR, "ws": ws_id, "duration": duracion})
    producer.flush()


def hilo_mensajes():
    # Recibe los mensajes de CENTRAL. group_id único: cada operario recibe todos los mensajes
    # y se queda solo con los suyos.
    consumer = KafkaConsumer(TOPIC_OPERATORS,
                             bootstrap_servers=KAFKA_ADDR,
                             group_id=f"wm-fo-{OPERADOR}-{uuid.uuid4()}",
                             auto_offset_reset="latest",
                             value_deserializer=lambda b: json.loads(b.decode(FORMAT)))
    for mensaje in consumer:
        datos = mensaje.value
        if datos.get("operator") not in (OPERADOR, "*"):      # "*" = mensaje para todos
            continue
        tipo = datos.get("type")

        if tipo == "STATUS":
            with lock_tabla:
                tabla.clear()
                for estacion in datos["estaciones"]:
                    tabla[estacion["id"]] = estacion
            tabla_recibida.set()
        else:
            print(f"[CENTRAL] {datos['msg']}")
            if tipo in ("END", "DENIED"):         # el servicio ha concluido (éxito o fracaso)
                concluido.set()


########## MODO AUTOMATICO (fichero) ##########

def leer_servicios(ruta):
    servicios = []
    with open(ruta, encoding=FORMAT) as fichero:
        for numero, linea in enumerate(fichero, start=1):
            linea = linea.strip()
            if not linea or linea.startswith("#"):
                continue
            campos = linea.split(",")
            try:
                ws_id = campos[0].strip()
                duracion = int(campos[1]) if len(campos) > 1 else DURACION_POR_DEFECTO
                servicios.append((ws_id, duracion))
            except ValueError:
                print(f"Línea {numero} ignorada (formato incorrecto): {linea}")
    return servicios


def modo_fichero(ruta):
    servicios = leer_servicios(ruta)
    print(f"Se han leído {len(servicios)} servicios de {ruta}")
    for ws_id, duracion in servicios:
        concluido.clear()
        solicitar_riego(ws_id, duracion)
        # Esperamos a que concluya (éxito o fracaso). Si CENTRAL no contesta, nos rendimos.
        if not concluido.wait(timeout=duracion + MARGEN_RESPUESTA):
            print("[FO] CENTRAL no ha respondido a tiempo. Paso al siguiente servicio")
        print(f"Esperando {ESPERA_ENTRE_SERVICIOS} segundos antes del siguiente servicio...")
        time.sleep(ESPERA_ENTRE_SERVICIOS)
    print("Todos los servicios del fichero han sido procesados")


########## MODO INTERACTIVO (menú) ##########

def mostrar_disponibles():
    with lock_tabla:
        disponibles = [e for e in tabla.values() if e["estado"] == "DISPONIBLE"]
        total = len(tabla)
    print(f"\nEstaciones DISPONIBLES ({len(disponibles)} de {total}):")
    for e in sorted(disponibles, key=lambda x: x["id"]):
        print(f"   {e['id']}  -  {e['ubicacion']}")
    if not disponibles:
        print("   (ninguna)")


def modo_menu():
    while True:
        print(f"\n===== MENU OPERARIO {OPERADOR} =====\n"
              " [1] Ver estaciones disponibles\n"
              " [2] Solicitar riego\n"
              " [3] Salir")
        try:
            opcion = input("> ").strip()
        except EOFError:
            return
        if opcion == "1":
            mostrar_disponibles()
        elif opcion == "2":
            ws_id = input("ID de la estación (ej. WS-04): ").strip()
            texto = input(f"Duración en segundos [{DURACION_POR_DEFECTO}]: ").strip()
            try:
                duracion = int(texto) if texto else DURACION_POR_DEFECTO
            except ValueError:
                print("Duración no válida")
                continue
            solicitar_riego(ws_id, duracion)
        elif opcion == "3":
            return


########## MAIN ##########

print("****** WELCOME TO WM_FO: APLICACION DEL OPERARIO ****")

if len(sys.argv) in (4, 5):
    KAFKA_ADDR = f"{sys.argv[1]}:{sys.argv[2]}"
    OPERADOR = sys.argv[3]

    conectar_kafka()
    threading.Thread(target=hilo_mensajes, daemon=True).start()

    # Esperamos a recibir la primera tabla de CENTRAL: así sabemos que CENTRAL está viva
    # y que nuestro consumidor ya funciona antes de enviar nada.
    print(f"Operario {OPERADOR}: esperando a CENTRAL...")
    if tabla_recibida.wait(timeout=20):
        print("Conectado con CENTRAL")
    else:
        print("CENTRAL no responde todavía (¿está arrancada?). Continúo de todas formas")

    try:
        if len(sys.argv) == 5:
            modo_fichero(sys.argv[4])
        else:
            modo_menu()
    except KeyboardInterrupt:
        pass
    print("SE ACABO LO QUE SE DABA")
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: "
          "<KafkaIP> <KafkaPuerto> <ID_Operario> [fichero_servicios]")
