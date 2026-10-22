import socket
import sys
import threading
import time

# ---------------------------------------------------------------------------
# WM_WS_M  -  Monitor de la Watering Station
#
#   * CLIENTE de sockets frente a WM_Central (registro/autenticación + estado)
#   * SERVIDOR de sockets frente a WM_WS_E   (comprobación de salud cada segundo)
#
# Uso:
#   python wm_ws_m.py <PuertoEscucha> <CentralIP> <CentralPuerto> <ID_WS> [Ubicacion]
# ---------------------------------------------------------------------------

HEADER = 64                 # igual que en los ejemplos: 64 bytes con la longitud del mensaje
FORMAT = 'utf-8'
FIN = "FIN"
SEP = "#"
PERIODO = 1                 # segundos entre comprobaciones / estados
TIMEOUT_ENGINE = 2          # segundos sin respuesta del Engine => avería
TIMEOUT_CENTRAL = 5         # segundos de timeout en el socket hacia CENTRAL

# Estados del Engine vistos por el Monitor
WAIT = "WAIT"               # el Engine todavía no se ha conectado nunca
OK = "OK"
KO = "KO"                   # respuesta KO, timeout o desconexión => FUGA

# Estado compartido entre hilos
estado_engine = WAIT
lock_estado = threading.Lock()
engine_conectado = False
lock_engine = threading.Lock()
autenticado = threading.Event()     # CENTRAL ya ha validado el ID de esta WS


########## FUNCIONES DE MENSAJERIA (mismo protocolo que cliente/servidor concurrente) ##########

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


########## ESTADO DEL ENGINE ##########

def obtener_estado():
    with lock_estado:
        return estado_engine


def cambiar_estado(nuevo):
    global estado_engine
    with lock_estado:
        anterior = estado_engine
        estado_engine = nuevo
    if anterior != nuevo:
        if nuevo == KO:
            print(f"[!!! AVERIA !!!] Fuga detectada en {WS_ID} ({anterior} -> KO). Se notifica a CENTRAL")
        elif nuevo == OK and anterior == KO:
            print(f"[AVERIA RESUELTA] {WS_ID} vuelve a estar operativa (KO -> OK). Se notifica a CENTRAL")
        else:
            print(f"[ESTADO] Engine de {WS_ID}: {anterior} -> {nuevo}")


########## LADO SERVIDOR: ENGINE ##########

def handle_engine(conn, addr):
    global engine_conectado
    print(f"[NUEVA CONEXION] Engine {addr} connected.")
    conn.settimeout(TIMEOUT_ENGINE)

    try:
        # No empezamos a monitorizar hasta que CENTRAL haya validado el ID
        autenticado.wait()

        connected = True
        while connected:
            # El Engine aprende su ID a partir de este mensaje
            send(conn, f"HEALTH{SEP}{WS_ID}")
            respuesta = recibir(conn).split(SEP)
            if respuesta[0] == "HEALTH_ANS" and len(respuesta) > 1 and respuesta[1] == OK:
                cambiar_estado(OK)
            else:
                cambiar_estado(KO)
            time.sleep(PERIODO)

    except (socket.timeout, ConnectionError, OSError, ValueError) as e:
        print(f"[ENGINE] Sin respuesta o conexión interrumpida: {e}")
    finally:
        conn.close()
        with lock_engine:
            engine_conectado = False
        # Si el Engine ya había estado conectado y se cae => avería
        if obtener_estado() != WAIT:
            cambiar_estado(KO)
        print("[ENGINE] Conexión cerrada. Esperando a que el Engine vuelva a conectar")


def start_servidor_engine():
    global engine_conectado
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", PORT_ESCUCHA))     # 0.0.0.0: accesible también dentro de Docker
    server.listen()
    print(f"[LISTENING] Monitor a la escucha del Engine en el puerto {PORT_ESCUCHA}")

    while True:
        conn, addr = server.accept()
        with lock_engine:
            ocupado = engine_conectado
            if not ocupado:
                engine_conectado = True
        if not ocupado:
            thread = threading.Thread(target=handle_engine, args=(conn, addr), daemon=True)
            thread.start()
        else:
            print("OOppsss... Ya hay un Engine conectado a este Monitor. Conexión rechazada")
            conn.close()


########## LADO CLIENTE: CENTRAL ##########

def hablar_con_central():
    """Devuelve False si CENTRAL rechaza el registro; no vuelve si todo va bien."""
    espera = 1
    while True:
        client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        client.settimeout(TIMEOUT_CENTRAL)
        try:
            print(f"Conectando con CENTRAL en [{ADDR_CENTRAL}] ...")
            client.connect(ADDR_CENTRAL)
            print(f"Establecida conexión en [{ADDR_CENTRAL}]")

            # 1) Autenticación / registro
            send(client, f"REGISTER{SEP}{WS_ID}{SEP}{UBICACION}")
            respuesta = recibir(client).split(SEP)
            if respuesta[0] != "OK":
                print(f"CENTRAL rechaza el registro de {WS_ID}: {respuesta[1:]}")
                client.close()
                return False
            print(f"WS {WS_ID} autenticada y registrada en CENTRAL")
            autenticado.set()
            espera = 1

            # 2) Estado periódico (sirve también de heartbeat)
            while True:
                estado = obtener_estado()
                print("Envio a CENTRAL: ", f"STATUS{SEP}{WS_ID}{SEP}{estado}")
                send(client, f"STATUS{SEP}{WS_ID}{SEP}{estado}")
                print("Recibo de CENTRAL: ", recibir(client))
                time.sleep(PERIODO)

        except KeyboardInterrupt:
            print("SE ACABO LO QUE SE DABA")
            try:
                print("Envio a CENTRAL: ", FIN)
                send(client, FIN)
            except OSError:
                pass
            client.close()
            return True

        except (socket.timeout, ConnectionError, OSError, ValueError) as e:
            print(f"Conexión con CENTRAL perdida ({e}). Reintento en {espera}s")
            client.close()
            time.sleep(espera)
            espera = min(espera * 2, 10)


########## MAIN ##########

print("****** WELCOME TO WM_WS_M: MONITOR DE LA WATERING STATION ****")

if len(sys.argv) in (5, 6):
    PORT_ESCUCHA = int(sys.argv[1])
    ADDR_CENTRAL = (sys.argv[2], int(sys.argv[3]))
    WS_ID = sys.argv[4]
    UBICACION = sys.argv[5] if len(sys.argv) == 6 else "UNKNOWN"

    # Hilo servidor (Engine) en segundo plano; el hilo principal hace de cliente de CENTRAL
    threading.Thread(target=start_servidor_engine, daemon=True).start()

    if not hablar_con_central():
        sys.exit(2)
else:
    print("Oops!. Parece que algo falló. Necesito estos argumentos: "
          "<PuertoEscucha> <CentralIP> <CentralPuerto> <ID_WS> [Ubicacion]")
