# API(chat_web): API_chat.py
# v4.1.2 - "WHATSAPP NO LUGAR DO APP + QR 2 FASES + FRONT COMPAT + CODIGO_QR + FIX VPS"
# =====================================================================================
# FLUXO:
# 1) 1ª leitura do QR -> /qr/scan decide "cadastro"
# 2) cadastro do responsável / ativação da pulseira vincula codigo_qr -> login_vinculo
# 3) 2ª leitura do QR -> /qr/scan decide "onboarding"
# 4) onboarding do voluntário -> encontro / tipo / localização / foto
# 5) API envia resumo + localização + foto para o WhatsApp do responsável
# 6) Resposta do responsável via webhook entra no chat do voluntário
#
# OBS:
# - mantém fluxo legado por login_vinculo
# - adiciona compatibilidade com codigo_qr sem quebrar front existente
# - ajustes para VPS / Hostinger / produção
# =====================================================================================

from typing import List, Optional, Dict, Any
from fastapi import FastAPI, UploadFile, File, Form, HTTPException, Query, Body, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import PlainTextResponse
from pydantic import BaseModel, Field
import os
from dotenv import load_dotenv
load_dotenv()
import shutil
import json
import sys
import traceback
import random
import time
import requests
from datetime import datetime
from mysql.connector import pooling
from starlette.requests import Request as StarletteRequest
import threading
import math
import subprocess

app = FastAPI(title="API Chat Pulseira Inteligente")

DEBUG = os.getenv("DEBUG", "true").strip().lower() == "true"

# =========================
# WHATSAPP CLOUD API
# =========================
WHATSAPP_ENABLED = os.getenv("WHATSAPP_ENABLED", "true").strip().lower() == "true"
WHATSAPP_TOKEN = os.getenv("WHATSAPP_TOKEN", "").strip()
WHATSAPP_PHONE_NUMBER_ID = os.getenv("WHATSAPP_PHONE_NUMBER_ID", "").strip()
WHATSAPP_WABA_ID = os.getenv("WHATSAPP_WABA_ID", "").strip()
WHATSAPP_API_VERSION = os.getenv("WHATSAPP_API_VERSION", "v22.0").strip()
WHATSAPP_VERIFY_TOKEN = os.getenv("WHATSAPP_VERIFY_TOKEN", "ray_edvenced_webhook_2026").strip()
PUBLIC_BASE_URL = os.getenv("PUBLIC_BASE_URL", "").strip().rstrip("/")


# =========================
# =========================
# 🔥 FUNÇÃO INTERNA: ENVIO WHATSAPP (POST)
# =========================
def _wa_post(payload: dict):
    """
    Envia requisição para API do WhatsApp Cloud (Meta)
    """
    import requests

    # 🔒 Validação básica
    if not WHATSAPP_PHONE_NUMBER_ID:
        raise HTTPException(status_code=500, detail="WHATSAPP_PHONE_NUMBER_ID não configurado")

    if not WHATSAPP_TOKEN:
        raise HTTPException(status_code=500, detail="WHATSAPP_TOKEN não configurado")

    # 📡 URL da API da Meta
    url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{WHATSAPP_PHONE_NUMBER_ID}/messages"

    # 📦 Headers
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
        "Content-Type": "application/json",
    }

    # 🚀 Envio da requisição
    resp = requests.post(url, headers=headers, json=payload, timeout=30)

    # 🔍 Tenta converter resposta
    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    # 🧪 LOG DE DIAGNÓSTICO
    print("📤 PAYLOAD WHATSAPP:", json.dumps(payload, ensure_ascii=False), flush=True)
    print("📥 STATUS META:", resp.status_code, flush=True)
    print("📥 RESPOSTA META:", json.dumps(data, ensure_ascii=False), flush=True)

    # ❌ Tratamento de erro
    if not resp.ok:
        raise HTTPException(
            status_code=500,
            detail={
                "erro": "Falha ao enviar mensagem para o WhatsApp",
                "status_code": resp.status_code,
                "resposta_meta": data,
                "payload_enviado": payload,
            },
        )

    # ✅ Retorno sucesso
    return data

# =========================
# =========================================
# 🔐 CONFIGURAÇÃO GERAL
# =========================================


# =========================================
# 🗄️ BANCO DE DADOS (MYSQL)
# =========================================


# =========================================
# 📱 WHATSAPP CLOUD API
# =========================================


# 🔑 TOKEN PERMANENTE (Meta)



# 🔄 VERSÃO DA API


# 🔐 TOKEN DE VERIFICAÇÃO DO WEBHOOK

# 🌐 URL PÚBLICA (IMPORTANTE PRA FOTO/LOCALIZAÇÃO)

# =========================================
# LONG-POLL VOLUNTÁRIO
# =========================
_POLL_EVENTS: Dict[tuple, threading.Event] = {}
_POLL_LOCK = threading.Lock()


def _poll_key(destino: Optional[str], encontro_id: Optional[int], login_vinculo: Optional[str]) -> tuple:
    d = (destino or "").strip().lower()
    eid = int(encontro_id or 0)
    lv = (login_vinculo or "").strip()
    return (d, eid, lv)


def _get_event(destino: Optional[str], encontro_id: Optional[int], login_vinculo: Optional[str]) -> threading.Event:
    key = _poll_key(destino, encontro_id, login_vinculo)
    with _POLL_LOCK:
        ev = _POLL_EVENTS.get(key)
        if ev is None:
            ev = threading.Event()
            _POLL_EVENTS[key] = ev
        return ev


def _notify_poll(destino: Optional[str], encontro_id: Optional[int], login_vinculo: Optional[str]):
    d = (destino or "").strip().lower()
    if d not in ("voluntario", "web"):
        return

    d = "voluntario"
    eid = int(encontro_id or 0)
    lv = (login_vinculo or "").strip()

    keys = {
        _poll_key(d, eid, lv),
        _poll_key(d, 0, lv),
        _poll_key(d, eid, ""),
        _poll_key(d, 0, ""),
    }

    with _POLL_LOCK:
        for k in keys:
            ev = _POLL_EVENTS.get(k)
            if ev is None:
                ev = threading.Event()
                _POLL_EVENTS[k] = ev
            ev.set()


# =========================
# LOG HELPERS
# =========================
def _log(title: str, data=None):
    print(f"\n=== {title} ===", flush=True)
    if data is not None:
        try:
            print(json.dumps(data, ensure_ascii=False, indent=2), flush=True)
        except Exception:
            print(str(data), flush=True)


def _dbg(title: str, data=None):
    if not DEBUG:
        return
    _log(title, data)


def _log_exc(prefix: str, e: Exception):
    print(f"❌ {prefix}: {repr(e)}", flush=True)
    traceback.print_exc(file=sys.stdout)


# =========================
# CORS
# =========================
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://edvenced.com.br",
        "https://www.edvenced.com.br"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# =========================
# UPLOAD DIRS
# =========================
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
AUDIOS_DIR = os.path.join(BASE_DIR, "audios_upload")
FOTOS_DIR = os.path.join(BASE_DIR, "fotos_upload")

os.makedirs(AUDIOS_DIR, exist_ok=True)
os.makedirs(FOTOS_DIR, exist_ok=True)

app.mount("/media/fotos", StaticFiles(directory=FOTOS_DIR), name="media_fotos")
app.mount("/media/audios", StaticFiles(directory=AUDIOS_DIR), name="media_audios")

# =========================
# DB CONFIG
# =========================
DBCFG = {
    "host": os.getenv("DB_HOST", "127.0.0.1"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "api_user"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "ray_edvenced_zap"),
    "auth_plugin": os.getenv("DB_AUTH_PLUGIN", "mysql_native_password"),
    "connection_timeout": 10,
}
_pool = None


# =========================
# HELPERS GERAIS
# =========================
def _only_digits(s: str) -> str:
    return "".join([c for c in (s or "") if c.isdigit()])


def _is_tel_valido_br(tel: str) -> bool:
    t = _only_digits(tel or "")
    return len(t) in (10, 11)


def _to_wa_number(raw: str) -> str:
    d = _only_digits(raw or "")
    if len(d) in (10, 11):
        return f"55{d}"
    return d


def _safe_ext(filename: str, allowed: Optional[List[str]] = None, default: str = ".bin") -> str:
    allowed = allowed or [".png", ".jpg", ".jpeg", ".webp"]
    _, ext = os.path.splitext(filename or "")
    ext = (ext or "").lower().strip()
    return ext if ext in allowed else default


def _unique_photo_name(original: str) -> str:
    ext = _safe_ext(original, allowed=[".png", ".jpg", ".jpeg", ".webp"], default=".png")
    ts = int(datetime.utcnow().timestamp())
    rnd = random.randint(1000, 9999)
    return f"captura_{ts}_{rnd}{ext}"


def _unique_audio_name(original: str) -> str:
    ext = _safe_ext(original, allowed=[".m4a", ".aac", ".wav", ".mp3", ".ogg", ".webm"], default=".m4a")
    ts = int(datetime.utcnow().timestamp())
    rnd = random.randint(1000, 9999)
    return f"audio_{ts}_{rnd}{ext}"


def _now_str():
    return datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")


def _norm_destino(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    v = (s or "").strip().lower()
    if not v:
        return None
    if v in ("voluntario", "web"):
        return "voluntario"
    if v == "app":
        return "app"
    return None


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371000.0
    phi1 = math.radians(lat1)
    phi2 = math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# =========================
# DB HELPERS
# =========================
def _get_pool():
    global _pool
    if _pool is None:
        _log("DBCFG", {
            "host": DBCFG.get("host"),
            "port": DBCFG.get("port"),
            "user": DBCFG.get("user"),
            "database": DBCFG.get("database"),
        })
        print("🛠️ Criando pool MySQL...", flush=True)
        _pool = pooling.MySQLConnectionPool(
            pool_name="pool1",
            pool_size=8,
            pool_reset_session=True,
            **DBCFG
        )
        test = _pool.get_connection()
        cur = test.cursor()
        cur.execute("SELECT NOW(), DATABASE(), USER()")
        print("✅ Pool OK:", cur.fetchone(), flush=True)
        cur.close()
        test.close()
    return _pool


def _open_cursor():
    cnx = _get_pool().get_connection()
    try:
        cnx.ping(reconnect=True, attempts=1, delay=0)
    except Exception:
        pass
    cur = cnx.cursor()
    return cnx, cur


def _try_create_index(cur, sql: str):
    try:
        cur.execute(sql)
    except Exception as e:
        errno = getattr(e, "errno", None)
        if errno in (1061,):
            return
        _log_exc("Aviso: falha criando índice", e)


def _try_add_column(cur, table: str, column_def: str):
    try:
        cur.execute(f"ALTER TABLE {table} ADD COLUMN {column_def}")
    except Exception as e:
        errno = getattr(e, "errno", None)
        if errno in (1060,):
            return
        _log_exc(f"Aviso: falha ADD COLUMN em {table}", e)


def _column_exists(cur, table: str, column: str) -> bool:
    cur.execute("""
        SELECT 1
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA=%s AND TABLE_NAME=%s AND COLUMN_NAME=%s
        LIMIT 1
    """, (DBCFG["database"], table, column))
    return cur.fetchone() is not None


# =========================
# REQUEST LOG MIDDLEWARE
# =========================
@app.middleware("http")
async def log_requests(request: StarletteRequest, call_next):
    body_bytes = b""
    try:
        body_bytes = await request.body()
        ct = (request.headers.get("content-type") or "").lower()
        info = {
            "method": request.method,
            "path": request.url.path,
            "query": str(request.url.query),
            "content_type": ct
        }

        if ct.startswith("application/json") and body_bytes:
            try:
                info["body_json"] = json.loads(body_bytes.decode("utf-8"))
            except Exception:
                info["body_json"] = "<json inválido>"
        elif "multipart/form-data" in ct:
            info["multipart"] = "<skipped>"
        else:
            info["body"] = "<skipped>"

        _dbg("REQ", info)
    except Exception as e:
        _log_exc("Falha ao logar request", e)

    async def receive():
        return {"type": "http.request", "body": body_bytes, "more_body": False}

    request2 = StarletteRequest(request.scope, receive)
    response = await call_next(request2)
    _dbg("RESP", {"status_code": response.status_code, "path": request.url.path})
    return response


# =========================
# DDL / AJUSTES IDMP
# =========================
def _init_db():
    ddl = [
        """
        CREATE TABLE IF NOT EXISTS contatos (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(120) NULL,
            telefone VARCHAR(20) NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS responsaveis (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(120) NULL,
            telefone VARCHAR(20) NULL,
            whatsapp VARCHAR(20) NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS voluntarios (
            id INT AUTO_INCREMENT PRIMARY KEY,
            nome VARCHAR(120) NULL,
            telefone VARCHAR(20) NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS pulseiras_qr (
            id INT AUTO_INCREMENT PRIMARY KEY,
            login_vinculo VARCHAR(120) NULL,
            codigo_qr VARCHAR(255) NULL,
            responsavel_id INT NULL,
            nome_dependente VARCHAR(120) NULL,
            ativo TINYINT NOT NULL DEFAULT 1,
            ativada TINYINT NOT NULL DEFAULT 0,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS encontros (
            id INT AUTO_INCREMENT PRIMARY KEY,
            pulseira_id INT NULL,
            responsavel_id INT NULL,
            voluntario_id INT NULL,
            tipo_vulneravel VARCHAR(30) NULL,
            foto_arquivo VARCHAR(255) NULL,
            status VARCHAR(20) NULL DEFAULT 'pendente',
            voluntario_presente TINYINT NOT NULL DEFAULT 0,
            envio_de_localizacao TINYINT NOT NULL DEFAULT 0,
            onboarding_whatsapp_enviado TINYINT NOT NULL DEFAULT 0,
            onboarding_whatsapp_enviado_em DATETIME NULL,
            whatsapp_ultimo_erro TEXT NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS localizacoes (
            id INT AUTO_INCREMENT PRIMARY KEY,
            encontro_id INT NULL,
            voluntario_id INT NULL,
            voluntario_nome VARCHAR(120) NULL,
            voluntario_telefone VARCHAR(20) NULL,
            latitude DOUBLE NULL,
            longitude DOUBLE NULL,
            accuracy DOUBLE NULL,
            ts_client BIGINT NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS mensagens (
            id INT AUTO_INCREMENT PRIMARY KEY,
            encontro_id INT NULL,
            tipo VARCHAR(20) NULL,
            conteudo_texto TEXT NULL,
            arquivo_audio VARCHAR(255) NULL,
            arquivo_foto VARCHAR(255) NULL,
            telefone_origem VARCHAR(20) NULL,
            nome_origem VARCHAR(120) NULL,
            telefone_alvo VARCHAR(20) NULL,
            status VARCHAR(20) NULL,
            pendente_para VARCHAR(20) NULL,
            remetente_tipo VARCHAR(20) NULL,
            entregue_em DATETIME NULL,
            ack_por VARCHAR(20) NULL,
            created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
        """
        CREATE TABLE IF NOT EXISTS chat_status (
            id TINYINT PRIMARY KEY,
            status VARCHAR(20) NOT NULL
        ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
        """,
    ]

    cnx, cur = _open_cursor()
    try:
        for q in ddl:
            cur.execute(q)

        _try_add_column(cur, "pulseiras_qr", "login_vinculo VARCHAR(120) NULL")
        _try_add_column(cur, "pulseiras_qr", "codigo_qr VARCHAR(255) NULL")
        _try_add_column(cur, "pulseiras_qr", "responsavel_id INT NULL")
        _try_add_column(cur, "pulseiras_qr", "nome_dependente VARCHAR(120) NULL")
        _try_add_column(cur, "pulseiras_qr", "ativo TINYINT NOT NULL DEFAULT 1")
        _try_add_column(cur, "pulseiras_qr", "ativada TINYINT NOT NULL DEFAULT 0")
        _try_add_column(cur, "pulseiras_qr", "created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP")

        _try_add_column(cur, "responsaveis", "whatsapp VARCHAR(20) NULL")

        _try_add_column(cur, "encontros", "tipo_vulneravel VARCHAR(30) NULL")
        _try_add_column(cur, "encontros", "foto_arquivo VARCHAR(255) NULL")
        _try_add_column(cur, "encontros", "status VARCHAR(20) NULL DEFAULT 'pendente'")
        _try_add_column(cur, "encontros", "voluntario_presente TINYINT NOT NULL DEFAULT 0")
        _try_add_column(cur, "encontros", "envio_de_localizacao TINYINT NOT NULL DEFAULT 0")
        _try_add_column(cur, "encontros", "onboarding_whatsapp_enviado TINYINT NOT NULL DEFAULT 0")
        _try_add_column(cur, "encontros", "onboarding_whatsapp_enviado_em DATETIME NULL")
        _try_add_column(cur, "encontros", "whatsapp_ultimo_erro TEXT NULL")

        _try_add_column(cur, "localizacoes", "encontro_id INT NULL")
        _try_add_column(cur, "localizacoes", "voluntario_id INT NULL")
        _try_add_column(cur, "localizacoes", "voluntario_nome VARCHAR(120) NULL")
        _try_add_column(cur, "localizacoes", "voluntario_telefone VARCHAR(20) NULL")
        _try_add_column(cur, "localizacoes", "accuracy DOUBLE NULL")
        _try_add_column(cur, "localizacoes", "ts_client BIGINT NULL")
        _try_add_column(cur, "localizacoes", "created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP")

        _try_add_column(cur, "mensagens", "encontro_id INT NULL")
        _try_add_column(cur, "mensagens", "tipo VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "conteudo_texto TEXT NULL")
        _try_add_column(cur, "mensagens", "arquivo_audio VARCHAR(255) NULL")
        _try_add_column(cur, "mensagens", "arquivo_foto VARCHAR(255) NULL")
        _try_add_column(cur, "mensagens", "telefone_origem VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "nome_origem VARCHAR(120) NULL")
        _try_add_column(cur, "mensagens", "telefone_alvo VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "status VARCHAR(20) NULL DEFAULT 'pendente'")
        _try_add_column(cur, "mensagens", "pendente_para VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "remetente_tipo VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "entregue_em DATETIME NULL")
        _try_add_column(cur, "mensagens", "ack_por VARCHAR(20) NULL")
        _try_add_column(cur, "mensagens", "created_at DATETIME NULL DEFAULT CURRENT_TIMESTAMP")

        cur.execute("INSERT IGNORE INTO chat_status (id, status) VALUES (1, 'ativo')")

        _try_create_index(cur, "CREATE INDEX idx_pulseiras_login_vinculo ON pulseiras_qr(login_vinculo)")
        _try_create_index(cur, "CREATE INDEX idx_pulseiras_codigo_qr ON pulseiras_qr(codigo_qr)")
        _try_create_index(cur, "CREATE INDEX idx_pulseiras_resp_id ON pulseiras_qr(responsavel_id)")
        _try_create_index(cur, "CREATE INDEX idx_resp_tel ON responsaveis(telefone)")
        _try_create_index(cur, "CREATE INDEX idx_resp_whatsapp ON responsaveis(whatsapp)")
        _try_create_index(cur, "CREATE INDEX idx_vol_tel ON voluntarios(telefone)")
        _try_create_index(cur, "CREATE INDEX idx_encontros_pulseira ON encontros(pulseira_id)")
        _try_create_index(cur, "CREATE INDEX idx_encontros_resp ON encontros(responsavel_id)")
        _try_create_index(cur, "CREATE INDEX idx_encontros_vol ON encontros(voluntario_id)")
        _try_create_index(cur, "CREATE INDEX idx_encontros_status_created ON encontros(status, created_at)")
        _try_create_index(cur, "CREATE INDEX idx_msgs_encontro ON mensagens(encontro_id)")
        _try_create_index(cur, "CREATE INDEX idx_msgs_pendente ON mensagens(encontro_id, pendente_para, status, id)")
        _try_create_index(cur, "CREATE INDEX idx_locs_encontro ON localizacoes(encontro_id, id)")

        try:
            cur.execute("UPDATE mensagens SET status='pendente' WHERE status IS NULL OR status=''")
        except Exception:
            pass

        try:
            cur.execute("UPDATE encontros SET status='pendente' WHERE status IS NULL OR status=''")
        except Exception:
            pass

        cnx.commit()
        print("✅ Banco OK (v4.1.2).", flush=True)
    finally:
        cur.close()
        cnx.close()


@app.on_event("startup")
def _startup():
    try:
        _init_db()
    except Exception as e:
        _log_exc("Falha ao inicializar o banco", e)


# =========================
# MODELOS
# =========================
class ContatoIn(BaseModel):
    nome: str = Field(min_length=1, max_length=120)
    telefone: str = Field(min_length=8, max_length=20)


class ContatoOut(ContatoIn):
    id: int


class VoluntarioIn(BaseModel):
    nome: str = Field(min_length=1, max_length=120)
    telefone: str = Field(min_length=8, max_length=20)


class VoluntarioOut(VoluntarioIn):
    id: int


class CadastroUsuarioIn(BaseModel):
    nome_responsavel: str = Field(min_length=1, max_length=120)
    telefone_responsavel: str = Field(min_length=8, max_length=20)
    nome_vulneravel: str = Field(min_length=1, max_length=120)
    id_pulseira: str = Field(min_length=1, max_length=120)
    codigo_qr: Optional[str] = Field(default=None, max_length=255)


class QrScanOut(BaseModel):
    ok: bool
    codigo_qr: str
    proximo_passo: str
    login_vinculo: Optional[str] = None
    pulseira_id: Optional[int] = None
    responsavel_id: Optional[int] = None


class VincularWhatsAppIn(BaseModel):
    id_pulseira: str = Field(min_length=1, max_length=120)
    responsavel_whatsapp: str = Field(min_length=10, max_length=20)


class LocalizacaoIn(BaseModel):
    telefone_vulneravel: Optional[str] = Field(default=None, min_length=8, max_length=20)
    latitude: float
    longitude: float
    accuracy: Optional[float] = None
    timestamp: Optional[int] = None
    voluntario_id: Optional[int] = None
    voluntario_nome: Optional[str] = None
    voluntario_telefone: Optional[str] = None
    encontro_id: Optional[int] = None

    origem: Optional[str] = Field(default=None, max_length=20)
    telefone_origem: Optional[str] = Field(default=None, max_length=20)
    nome_origem: Optional[str] = Field(default=None, max_length=120)

    login_vinculo: Optional[str] = Field(default=None, max_length=120)
    id_pulseira: Optional[str] = Field(default=None, max_length=120)


class LocalizacaoOut(BaseModel):
    id: int
    telefone_vulneravel: Optional[str] = None
    latitude: float
    longitude: float
    accuracy: Optional[float] = None
    timestamp: Optional[int] = None
    voluntario_id: Optional[int] = None
    voluntario_nome: Optional[str] = None
    voluntario_telefone: Optional[str] = None
    created_at: str


class LiberarLocalizacaoIn(BaseModel):
    telefone_vulneravel: Optional[str] = Field(default=None, min_length=8, max_length=20)
    login_vinculo: Optional[str] = Field(default=None, max_length=120)
    id_pulseira: Optional[str] = Field(default=None, max_length=120)


class EncontroIn(BaseModel):
    telefone_vulneravel: Optional[str] = Field(default=None, min_length=8, max_length=20)
    nome_voluntario: Optional[str] = Field(default=None, max_length=120)
    voluntario_telefone: Optional[str] = Field(default=None, max_length=20)
    foto_arquivo: Optional[str] = Field(default=None, max_length=255)
    telefone_origem: Optional[str] = Field(default=None, max_length=20)

    login_vinculo: Optional[str] = Field(default=None, max_length=120)
    id_pulseira: Optional[str] = Field(default=None, max_length=120)


class MensagemTextoIn(BaseModel):
    telefone_alvo: Optional[str] = Field(default=None, max_length=20)
    telefone_origem: Optional[str] = Field(default=None, max_length=20)
    nome_origem: Optional[str] = Field(default=None, max_length=120)
    texto: str = Field(min_length=1)
    encontro_id: Optional[int] = None
    origem: Optional[str] = Field(default="voluntario", max_length=20)
    voluntario_telefone: Optional[str] = Field(default=None, max_length=20)

    login_vinculo: Optional[str] = Field(default=None, max_length=120)
    id_pulseira: Optional[str] = Field(default=None, max_length=120)


class MensagemAckIn(BaseModel):
    id: int
    ack_por: Optional[str] = Field(default=None, max_length=20)


class TipoVulneravelIn(BaseModel):
    telefone_alvo: Optional[str] = Field(default=None, max_length=20)
    tipo: str = Field(min_length=1, max_length=30)
    telefone_origem: Optional[str] = Field(default=None, max_length=20)
    nome_origem: Optional[str] = Field(default=None, max_length=120)
    encontro_id: Optional[int] = None

    login_vinculo: Optional[str] = Field(default=None, max_length=120)
    id_pulseira: Optional[str] = Field(default=None, max_length=120)


# =========================
# RESOLVERS
# =========================
def _resolve_login_vinculo_from_payload(*values: Optional[str]) -> Optional[str]:
    for v in values:
        s = (v or "").strip()
        if s:
            return s
    return None


def _resolve_pulseira_por_codigo_qr(cur, codigo_qr: Optional[str]) -> Optional[Dict[str, Any]]:
    cq = (codigo_qr or "").strip()
    if not cq:
        return None

    cur.execute("""
        SELECT id, codigo_qr, login_vinculo, responsavel_id, ativo
        FROM pulseiras_qr
        WHERE codigo_qr=%s
        ORDER BY id DESC
        LIMIT 1
    """, (cq,))
    row = cur.fetchone()

    if not row:
        return None

    return {
        "id": int(row[0]),
        "codigo_qr": row[1],
        "login_vinculo": row[2],
        "responsavel_id": int(row[3]) if row[3] is not None else None,
        "ativo": int(row[4]) if row[4] is not None else 0,
    }


def _ensure_pulseira_qr_slot(cur, codigo_qr: str) -> int:
    cq = (codigo_qr or "").strip()
    if not cq:
        raise HTTPException(400, "codigo_qr inválido.")

    info = _resolve_pulseira_por_codigo_qr(cur, cq)
    if info:
        return int(info["id"])

    cur.execute("""
        INSERT INTO pulseiras_qr (codigo_qr, ativo, ativada)
        VALUES (%s, 1, 0)
    """, (cq,))
    return int(cur.lastrowid)


def _ensure_pulseira(
    cur,
    login_vinculo: str,
    responsavel_id: Optional[int] = None,
    codigo_qr: Optional[str] = None
) -> int:
    lv = (login_vinculo or "").strip()
    cq = (codigo_qr or "").strip()

    if not lv:
        raise HTTPException(400, "login_vinculo/id_pulseira inválido.")

    cur.execute("""
        SELECT id
        FROM pulseiras_qr
        WHERE login_vinculo=%s
        ORDER BY id DESC
        LIMIT 1
    """, (lv,))
    row = cur.fetchone()

    if row and row[0]:
        pulseira_id = int(row[0])

        if responsavel_id is not None:
            cur.execute("""
                UPDATE pulseiras_qr
                SET responsavel_id=%s
                WHERE id=%s
            """, (int(responsavel_id), pulseira_id))

        if cq:
            cur.execute("""
                UPDATE pulseiras_qr
                SET codigo_qr=%s
                WHERE id=%s AND (codigo_qr IS NULL OR codigo_qr='')
            """, (cq, pulseira_id))

        cur.execute("""
            UPDATE pulseiras_qr
            SET ativada=1
            WHERE id=%s
        """, (pulseira_id,))

        return pulseira_id

    if cq:
        info_qr = _resolve_pulseira_por_codigo_qr(cur, cq)
        if info_qr:
            pulseira_id = int(info_qr["id"])
            login_existente = (info_qr.get("login_vinculo") or "").strip()

            if login_existente and login_existente != lv:
                raise HTTPException(409, f"codigo_qr já vinculado a outro login_vinculo: {login_existente}")

            cur.execute("""
                UPDATE pulseiras_qr
                SET login_vinculo=%s,
                    responsavel_id=COALESCE(%s, responsavel_id),
                    ativada=1
                WHERE id=%s
            """, (lv, responsavel_id, pulseira_id))
            return pulseira_id

    cols = ["login_vinculo", "ativo", "ativada"]
    vals = [lv, 1, 1]

    if cq:
        cols.append("codigo_qr")
        vals.append(cq)
    else:
        cols.append("codigo_qr")
        vals.append(lv)

    if responsavel_id is not None:
        cols.append("responsavel_id")
        vals.append(int(responsavel_id))

    sql = f"""
        INSERT INTO pulseiras_qr ({", ".join(cols)})
        VALUES ({", ".join(["%s"] * len(vals))})
    """
    cur.execute(sql, tuple(vals))
    return int(cur.lastrowid)


def _resolve_pulseira_id(cur, login_vinculo: Optional[str]) -> Optional[int]:
    lv = (login_vinculo or "").strip()
    if not lv:
        return None
    cur.execute("""
        SELECT id
        FROM pulseiras_qr
        WHERE login_vinculo=%s
        ORDER BY id DESC
        LIMIT 1
    """, (lv,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _resolve_encontro_por_pulseira_id(cur, pulseira_id: int) -> Optional[int]:
    cur.execute("""
        SELECT id
        FROM encontros
        WHERE pulseira_id=%s
        ORDER BY id DESC
        LIMIT 1
    """, (int(pulseira_id),))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _resolve_encontro_pendente_por_pulseira_id(cur, pulseira_id: int) -> Optional[int]:
    cur.execute("""
        SELECT id
        FROM encontros
        WHERE pulseira_id=%s AND status='pendente'
        ORDER BY id DESC
        LIMIT 1
    """, (int(pulseira_id),))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _resolve_encontro_por_login_vinculo(cur, login_vinculo: Optional[str]) -> Optional[int]:
    pid = _resolve_pulseira_id(cur, login_vinculo)
    if not pid:
        return None
    return _resolve_encontro_por_pulseira_id(cur, pid)


def _resolve_encontro_pendente_por_login_vinculo(cur, login_vinculo: Optional[str]) -> Optional[int]:
    pid = _resolve_pulseira_id(cur, login_vinculo)
    if not pid:
        return None
    return _resolve_encontro_pendente_por_pulseira_id(cur, pid)


def _resolve_responsavel_por_telefone(cur, tel: str) -> Optional[int]:
    d = _only_digits(tel or "")
    if not d:
        return None
    cur.execute("""
        SELECT id
        FROM responsaveis
        WHERE telefone=%s
        ORDER BY id DESC
        LIMIT 1
    """, (d,))
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else None


def _resolve_voluntario_por_telefone(cur, tel: Optional[str]) -> Optional[Dict[str, Any]]:
    d = _only_digits(tel or "")
    if not _is_tel_valido_br(d):
        return None

    cur.execute("""
        SELECT id, nome, telefone
        FROM voluntarios
        WHERE telefone=%s
        ORDER BY id DESC
        LIMIT 1
    """, (d,))
    row = cur.fetchone()
    if not row:
        return None
    return {"id": int(row[0]), "nome": row[1], "telefone": row[2]}


def _ensure_encontro(cur, pulseira_id: int, responsavel_id: Optional[int] = None, voluntario_id: Optional[int] = None) -> int:
    eid = _resolve_encontro_pendente_por_pulseira_id(cur, pulseira_id) or _resolve_encontro_por_pulseira_id(cur, pulseira_id)
    if eid:
        cur.execute("""
            UPDATE encontros
            SET responsavel_id = COALESCE(%s, responsavel_id),
                voluntario_id = COALESCE(%s, voluntario_id),
                status = COALESCE(status, 'pendente')
            WHERE id=%s
        """, (responsavel_id, voluntario_id, eid))
        return int(eid)

    cur.execute("""
        INSERT INTO encontros (pulseira_id, responsavel_id, voluntario_id, status, voluntario_presente, envio_de_localizacao)
        VALUES (%s, %s, %s, 'pendente', 0, 0)
    """, (pulseira_id, responsavel_id, voluntario_id))
    return int(cur.lastrowid)


def _aprender_voluntario_no_encontro(cur, encontro_id: int, voluntario_id: Optional[int], voluntario_nome: Optional[str], voluntario_telefone: Optional[str]):
    if not encontro_id:
        return

    vid = voluntario_id
    vnome = (voluntario_nome or "").strip() or None
    vtel = _only_digits(voluntario_telefone or "") or None

    if vtel and not _is_tel_valido_br(vtel):
        vtel = None

    if (vid is None or vid == 0) and vtel:
        info = _resolve_voluntario_por_telefone(cur, vtel)
        if info:
            vid = info["id"]
            if not vnome:
                vnome = info["nome"]

    if vtel and vid is None:
        cur.execute("""
            INSERT INTO voluntarios (nome, telefone)
            VALUES (%s, %s)
        """, (vnome or "Voluntário", vtel))
        vid = int(cur.lastrowid)

    cur.execute("""
        UPDATE encontros
        SET voluntario_id = COALESCE(%s, voluntario_id),
            voluntario_presente = 1
        WHERE id=%s
    """, (vid, int(encontro_id)))


def _get_encontro_core(cur, encontro_id: int):
    cur.execute("""
        SELECT
            e.id,
            e.pulseira_id,
            e.responsavel_id,
            e.voluntario_id,
            e.created_at,
            e.tipo_vulneravel,
            e.foto_arquivo,
            e.status,
            e.voluntario_presente,
            e.envio_de_localizacao,
            e.onboarding_whatsapp_enviado,
            e.onboarding_whatsapp_enviado_em,
            e.whatsapp_ultimo_erro,
            p.login_vinculo,
            p.nome_dependente,
            r.nome,
            r.telefone,
            COALESCE(r.whatsapp, r.telefone),
            v.nome,
            v.telefone
        FROM encontros e
        LEFT JOIN pulseiras_qr p ON p.id = e.pulseira_id
        LEFT JOIN responsaveis r ON r.id = e.responsavel_id
        LEFT JOIN voluntarios v ON v.id = e.voluntario_id
        WHERE e.id=%s
        LIMIT 1
    """, (int(encontro_id),))
    return cur.fetchone()


def _get_ultima_loc_voluntario(cur, encontro_id: int):
    cur.execute("""
        SELECT latitude, longitude, accuracy, created_at, voluntario_id, voluntario_nome, voluntario_telefone
        FROM localizacoes
        WHERE encontro_id=%s AND (voluntario_id IS NOT NULL OR voluntario_nome IS NOT NULL OR voluntario_telefone IS NOT NULL)
        ORDER BY id DESC
        LIMIT 1
    """, (int(encontro_id),))
    return cur.fetchone()


def _get_ultima_loc_usuario(cur, encontro_id: int):
    cur.execute("""
        SELECT latitude, longitude, accuracy, created_at
        FROM localizacoes
        WHERE encontro_id=%s AND voluntario_id IS NULL AND (voluntario_nome IS NULL OR voluntario_nome='')
        ORDER BY id DESC
        LIMIT 1
    """, (int(encontro_id),))
    return cur.fetchone()


def _get_ultima_localizacao_para_encontro(cur, encontro_id: int):
    cur.execute("""
        SELECT latitude, longitude, accuracy, created_at
        FROM localizacoes
        WHERE encontro_id=%s
        ORDER BY id DESC
        LIMIT 1
    """, (int(encontro_id),))
    return cur.fetchone()


# =================================================================================================================================================================================================================
# WHATSAPP HELPERS (ajustado)
# =================================================================================================================================================================================================================

# =========================
# CONFIG / NOMES DE TEMPLATE
# =========================
# ✅ JÁ EXISTIA
def _wa_template_name() -> str:
    return os.getenv("WHATSAPP_TEMPLATE_NAME", "alerta_de_localizacao").strip()


# ✅ JÁ EXISTIA
def _wa_template_lang() -> str:
    return os.getenv("WHATSAPP_TEMPLATE_LANG", "pt_BR").strip()


# ✅ JÁ EXISTIA
def _wa_is_configured() -> bool:
    return bool(
        WHATSAPP_ENABLED
        and WHATSAPP_TOKEN
        and WHATSAPP_PHONE_NUMBER_ID
        and PUBLIC_BASE_URL
    )


# =========================
# ENVIO SIMPLES PARA WHATSAPP
# =========================

# 🆕 ENTROU AGORA
# Envia texto simples para o responsável
def _wa_send_text(to_number: str, texto: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": _to_wa_number(to_number),
        "type": "text",
        "text": {
            "body": texto
        }
    }
    return _wa_post(payload)


# ✅ JÁ EXISTIA
# Envia template aprovado
def _wa_send_template(to_number: str, nome: str, tipo: str, voluntario: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": _to_wa_number(to_number),
        "type": "template",
        "template": {
            "name": _wa_template_name(),
            "language": {"code": _wa_template_lang()},
            "components": [
                {
                    "type": "body",
                    "parameters": [
                        {"type": "text", "text": (nome or "Responsável")},
                        {"type": "text", "text": (tipo or "não informado")},
                        {"type": "text", "text": (voluntario or "Voluntário")},
                    ],
                }
            ],
        },
    }
    return _wa_post(payload)


# ✅ JÁ EXISTIA
# Envia localização
def _wa_send_location(to_number: str, latitude: float, longitude: float, nome: str = "Localização do encontro"):
    payload = {
        "messaging_product": "whatsapp",
        "to": _to_wa_number(to_number),
        "type": "location",
        "location": {
            "latitude": float(latitude),
            "longitude": float(longitude),
            "name": nome,
        },
    }
    return _wa_post(payload)


# ✅ JÁ EXISTIA
# Envia imagem por link público
def _wa_send_image_by_link(to_number: str, image_url: str, caption: Optional[str] = None):
    image_obj = {"link": image_url}
    if caption:
        image_obj["caption"] = caption

    payload = {
        "messaging_product": "whatsapp",
        "to": _to_wa_number(to_number),
        "type": "image",
        "image": image_obj,
    }
    return _wa_post(payload)


# 🆕 ENTROU AGORA
# Envia áudio por link público
def _wa_send_audio_by_link(to_number: str, audio_url: str):
    payload = {
        "messaging_product": "whatsapp",
        "to": _to_wa_number(to_number),
        "type": "audio",
        "audio": {
            "link": audio_url
        }
    }
    return _wa_post(payload)


# =========================
# SUPORTE A ÁUDIO VINDO DA META
# =========================
# Essas funções serão usadas no webhook para:
# 1) pegar o ID do áudio enviado pelo responsável
# 2) consultar a URL temporária da Meta
# 3) baixar o binário
# 4) salvar no servidor
# 5) depois inserir no chat como mensagem pendente para o voluntário

# 🆕 ENTROU AGORA
def _wa_get_media_url(media_id: str) -> dict:
    if not media_id:
        raise HTTPException(400, "media_id ausente.")

    if not WHATSAPP_TOKEN:
        raise HTTPException(500, "WHATSAPP_TOKEN não configurado.")

    url = f"https://graph.facebook.com/{WHATSAPP_API_VERSION}/{media_id}"
    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
    }

    resp = requests.get(url, headers=headers, timeout=30)

    try:
        data = resp.json()
    except Exception:
        data = {"raw": resp.text}

    if not resp.ok:
        raise HTTPException(
            status_code=500,
            detail={
                "erro": "Falha ao consultar mídia na Meta",
                "status_code": resp.status_code,
                "resposta_meta": data,
            },
        )

    return data


# 🆕 ENTROU AGORA
def _wa_download_media_bytes(media_url: str) -> bytes:
    if not media_url:
        raise HTTPException(400, "media_url ausente.")

    if not WHATSAPP_TOKEN:
        raise HTTPException(500, "WHATSAPP_TOKEN não configurado.")

    headers = {
        "Authorization": f"Bearer {WHATSAPP_TOKEN}",
    }

    resp = requests.get(media_url, headers=headers, timeout=60)

    if not resp.ok:
        raise HTTPException(
            status_code=500,
            detail={
                "erro": "Falha ao baixar mídia da Meta",
                "status_code": resp.status_code,
                "resposta_meta": resp.text,
            },
        )

    return resp.content


# 🆕 ENTROU AGORA
# Baixa um áudio que veio do WhatsApp e salva em AUDIOS_DIR
def _wa_save_incoming_audio_from_meta(media_id: str, original_mime_type: Optional[str] = None) -> str:
    meta = _wa_get_media_url(media_id)
    media_url = meta.get("url")
    if not media_url:
        raise HTTPException(500, "Meta não retornou URL da mídia.")

    audio_bytes = _wa_download_media_bytes(media_url)

    # escolhe extensão razoável
    ext = ".ogg"
    mime = (original_mime_type or meta.get("mime_type") or "").lower()

    if "mpeg" in mime or "mp3" in mime:
        ext = ".mp3"
    elif "wav" in mime:
        ext = ".wav"
    elif "aac" in mime:
        ext = ".aac"
    elif "webm" in mime:
        ext = ".webm"
    elif "ogg" in mime:
        ext = ".ogg"
    elif "m4a" in mime or "mp4" in mime:
        ext = ".m4a"

    filename = _unique_audio_name(f"meta_audio{ext}")
    path = os.path.join(AUDIOS_DIR, filename)

    with open(path, "wb") as f:
        f.write(audio_bytes)

    return filename


# =========================
# STATUS / RESPOSTA META
# =========================

# ✅ JÁ EXISTIA
def _meta_response_ok(resp: Optional[dict]) -> bool:
    """
    A Meta costuma retornar 'messages' em caso de sucesso.
    """
    return bool(resp and isinstance(resp, dict) and resp.get("messages"))


# =========================
# ERRO DE WHATSAPP NO ENCONTRO
# =========================

# ✅ JÁ EXISTIA
def _set_whatsapp_error(cur, encontro_id: int, erro_obj):
    try:
        if isinstance(erro_obj, (dict, list)):
            erro_txt = json.dumps(erro_obj, ensure_ascii=False)
        else:
            erro_txt = str(erro_obj)

        cur.execute("""
            UPDATE encontros
            SET whatsapp_ultimo_erro=%s
            WHERE id=%s
        """, (erro_txt[:5000], int(encontro_id)))
    except Exception as e:
        _log_exc("Falha ao gravar whatsapp_ultimo_erro", e)


# ✅ JÁ EXISTIA
def _clear_whatsapp_error(cur, encontro_id: int):
    try:
        cur.execute("""
            UPDATE encontros
            SET whatsapp_ultimo_erro=NULL
            WHERE id=%s
        """, (int(encontro_id),))
    except Exception as e:
        _log_exc("Falha ao limpar whatsapp_ultimo_erro", e)


# =========================
# DISPARO DO PACOTE COMPLETO
# =========================
def _maybe_send_onboarding_to_whatsapp(cur, encontro_id: int):
    """
    Envia ao responsável:
    1) template aprovado
    2) localização
    3) foto

    Regra de segurança:
    - NÃO envia se ainda faltar tipo, foto ou localização.
    - NÃO dispara fallback por dado incompleto.
    - Fallback com telefone do voluntário só ocorre se houver erro real
      no envio da localização ou exception técnica.
    """
    responsavel_whatsapp = None
    nome_voluntario_final = None
    telefone_voluntario_final = None

    def _fallback_contato_voluntario(motivo: str):
        try:
            if not responsavel_whatsapp:
                return {"ok": False, "skipped": "sem_responsavel_whatsapp"}

            tel_vol = _only_digits(telefone_voluntario_final or "")
            nome_vol = (nome_voluntario_final or "").strip() or "Voluntário"

            if not tel_vol or len(tel_vol) < 10:
                return {"ok": False, "skipped": "telefone_voluntario_ausente"}

            texto = (
                "⚠️ Tivemos uma falha técnica no envio completo do alerta.\n\n"
                "Para não perder o contato com quem encontrou a pessoa/pet:\n\n"
                f"Voluntário: {nome_vol}\n"
                f"Telefone: {tel_vol}\n\n"
                "Tente falar diretamente com o voluntário."
            )

            return _wa_send_text(
                to_number=responsavel_whatsapp,
                texto=texto
            )

        except Exception as e:
            _log_exc("Falha no fallback emergencial do voluntário", e)
            return {"ok": False, "erro": "fallback_exception", "detail": repr(e), "motivo": motivo}

    try:
        eid = int(encontro_id)

        if not _wa_is_configured():
            erro = {"ok": False, "erro": "whatsapp_not_configured"}
            _set_whatsapp_error(cur, eid, erro)
            return erro

        cur.execute("""
            SELECT
                e.id,
                e.tipo_vulneravel,
                e.foto_arquivo,
                e.onboarding_whatsapp_enviado,
                r.nome,
                COALESCE(r.whatsapp, r.telefone) AS responsavel_whatsapp,
                v.nome,
                v.telefone,
                p.nome_dependente
            FROM encontros e
            LEFT JOIN responsaveis r ON r.id = e.responsavel_id
            LEFT JOIN voluntarios v ON v.id = e.voluntario_id
            LEFT JOIN pulseiras_qr p ON p.id = e.pulseira_id
            WHERE e.id=%s
            LIMIT 1
        """, (eid,))
        row = cur.fetchone()

        if not row:
            erro = {"ok": False, "erro": "encontro_not_found"}
            _set_whatsapp_error(cur, eid, erro)
            return erro

        (
            _id,
            tipo_vulneravel,
            foto_arquivo,
            onboarding_enviado,
            nome_responsavel,
            responsavel_whatsapp,
            nome_voluntario,
            telefone_voluntario,
            nome_vulneravel,
        ) = row

        if int(onboarding_enviado or 0) == 1:
            return {"ok": True, "skipped": "already_sent"}

        if not responsavel_whatsapp:
            erro = {"ok": False, "erro": "responsavel_whatsapp_ausente"}
            _set_whatsapp_error(cur, eid, erro)
            return erro

        # ✅ Aqui NÃO manda fallback. Só aguarda completar o onboarding.
        if not tipo_vulneravel:
            return {"ok": False, "skipped": "tipo_vulneravel_ausente"}

        if not foto_arquivo:
            return {"ok": False, "skipped": "foto_arquivo_ausente"}

        cur.execute("""
            SELECT latitude, longitude, voluntario_nome, voluntario_telefone
            FROM localizacoes
            WHERE encontro_id=%s
              AND latitude IS NOT NULL
              AND longitude IS NOT NULL
            ORDER BY id DESC
            LIMIT 1
        """, (eid,))
        loc = cur.fetchone()

        # ✅ Aqui também NÃO manda fallback. Pode ser só timing: localização ainda não chegou.
        if not loc:
            return {"ok": False, "skipped": "localizacao_ausente"}

        lat, lng, voluntario_nome_loc, voluntario_telefone_loc = loc

        nome_voluntario_final = (
            (nome_voluntario or "").strip()
            or (voluntario_nome_loc or "").strip()
            or "Voluntário"
        )

        telefone_voluntario_final = (
            _only_digits(telefone_voluntario or "")
            or _only_digits(voluntario_telefone_loc or "")
        )

        nome_vulneravel_final = (
            (nome_vulneravel or "").strip()
            or (nome_responsavel or "").strip()
            or "Pessoa"
        )

        foto_url = f"{PUBLIC_BASE_URL}/media/fotos/{foto_arquivo}"

        _dbg("WHATSAPP_ONBOARDING_PRONTO", {
            "encontro_id": eid,
            "responsavel_whatsapp": responsavel_whatsapp,
            "tipo_vulneravel": tipo_vulneravel,
            "nome_voluntario": nome_voluntario_final,
            "telefone_voluntario": telefone_voluntario_final,
            "latitude": float(lat),
            "longitude": float(lng),
            "foto_url": foto_url,
        })

        # 1) TEMPLATE
        r1 = _wa_send_template(
            to_number=responsavel_whatsapp,
            nome=nome_vulneravel_final,
            tipo=tipo_vulneravel,
            voluntario=nome_voluntario_final,
        )

        if not _meta_response_ok(r1):
            erro = {"ok": False, "erro": "template_failed", "detail": r1}
            _set_whatsapp_error(cur, eid, erro)
            return erro

        # 2) LOCALIZAÇÃO
        r2 = _wa_send_location(
            to_number=responsavel_whatsapp,
            latitude=float(lat),
            longitude=float(lng),
            nome="Localização do encontro",
        )

        if not _meta_response_ok(r2):
            fallback = _fallback_contato_voluntario("location_failed")

            erro = {
                "ok": False,
                "erro": "location_failed",
                "detail": r2,
                "fallback_voluntario": fallback,
            }

            _set_whatsapp_error(cur, eid, erro)

            cur.execute("""
                UPDATE encontros
                SET onboarding_whatsapp_enviado=0,
                    onboarding_whatsapp_enviado_em=NULL
                WHERE id=%s
            """, (eid,))

            return erro

        # 3) FOTO
        legenda = f"Tipo: {tipo_vulneravel} | Voluntário: {nome_voluntario_final}"

        r3 = _wa_send_image_by_link(
            to_number=responsavel_whatsapp,
            image_url=foto_url,
            caption=legenda,
        )

        if not _meta_response_ok(r3):
            erro = {
                "ok": False,
                "erro": "image_failed",
                "detail": r3,
                "foto_url": foto_url,
            }

            _set_whatsapp_error(cur, eid, erro)

            cur.execute("""
                UPDATE encontros
                SET onboarding_whatsapp_enviado=0,
                    onboarding_whatsapp_enviado_em=NULL
                WHERE id=%s
            """, (eid,))

            return erro

        cur.execute("""
            UPDATE encontros
            SET onboarding_whatsapp_enviado=1,
                onboarding_whatsapp_enviado_em=NOW(),
                whatsapp_ultimo_erro=NULL
            WHERE id=%s
        """, (eid,))

        _clear_whatsapp_error(cur, eid)

        return {
            "ok": True,
            "template": r1,
            "location": r2,
            "image": r3,
            "foto_url": foto_url,
            "latitude": float(lat),
            "longitude": float(lng),
        }

    except HTTPException as e:
        fallback = _fallback_contato_voluntario("http_exception")

        erro = {
            "ok": False,
            "erro": "http_exception",
            "detail": e.detail,
            "fallback_voluntario": fallback,
        }

        try:
            cur.execute("""
                UPDATE encontros
                SET onboarding_whatsapp_enviado=0,
                    onboarding_whatsapp_enviado_em=NULL
                WHERE id=%s
            """, (int(encontro_id),))

            _set_whatsapp_error(cur, int(encontro_id), erro)
        except Exception:
            pass

        return erro

    except Exception as e:
        _log_exc("Falha ao enviar onboarding para WhatsApp", e)

        fallback = _fallback_contato_voluntario("exception")

        erro = {
            "ok": False,
            "erro": "exception",
            "detail": repr(e),
            "fallback_voluntario": fallback,
        }

        try:
            cur.execute("""
                UPDATE encontros
                SET onboarding_whatsapp_enviado=0,
                    onboarding_whatsapp_enviado_em=NULL
                WHERE id=%s
            """, (int(encontro_id),))

            _set_whatsapp_error(cur, int(encontro_id), erro)
        except Exception:
            pass

        return erro
# =========================
# QR / SCAN
# =========================
@app.get("/qr/scan", response_model=QrScanOut, tags=["qr"])
def qr_scan(codigo_qr: str = Query(..., min_length=1, max_length=255)):
    cq = (codigo_qr or "").strip()
    if not cq:
        raise HTTPException(400, "codigo_qr inválido.")

    cnx, cur = _open_cursor()
    try:
        info = _resolve_pulseira_por_codigo_qr(cur, cq)

        # 1ª leitura: ainda não existe slot -> cria e manda para cadastro
        if not info:
            pulseira_id = _ensure_pulseira_qr_slot(cur, cq)
            cnx.commit()
            return QrScanOut(
                ok=True,
                codigo_qr=cq,
                proximo_passo="cadastro",
                login_vinculo=None,
                pulseira_id=pulseira_id,
                responsavel_id=None,
            )

        pulseira_id = int(info["id"])
        login_vinculo = (info.get("login_vinculo") or "").strip()
        responsavel_id = info.get("responsavel_id")

        # Se ainda não foi ativada/vinculada -> cadastro
        if not login_vinculo or not responsavel_id:
            return QrScanOut(
                ok=True,
                codigo_qr=cq,
                proximo_passo="cadastro",
                login_vinculo=login_vinculo or None,
                pulseira_id=pulseira_id,
                responsavel_id=responsavel_id,
            )

        # Se já foi ativada -> onboarding
        return QrScanOut(
            ok=True,
            codigo_qr=cq,
            proximo_passo="onboarding",
            login_vinculo=login_vinculo,
            pulseira_id=pulseira_id,
            responsavel_id=responsavel_id,
        )
    finally:
        cur.close()
        cnx.close()

# =========================
# CHAT APÓS ALERTA INICIAL
# =========================
# Este bloco libera o envio de texto/áudio do chat web
# para o WhatsApp do responsável somente depois que
# o alerta inicial (template + localização + foto) já foi enviado.


def _can_forward_chat_to_whatsapp(cur, encontro_id: int) -> bool:
    """
    Só permite encaminhar texto/áudio ao WhatsApp
    depois que o pacote inicial do onboarding já foi enviado.
    """
    cur.execute("""
        SELECT onboarding_whatsapp_enviado
        FROM encontros
        WHERE id=%s
        LIMIT 1
    """, (int(encontro_id),))
    row = cur.fetchone()
    return bool(row and int(row[0] or 0) == 1)


# =========================
# CHAT -> WHATSAPP (HELPERS)
# =========================
def _build_whatsapp_text_from_volunteer(nome_origem: Optional[str], texto: str) -> str:
    nome = (nome_origem or "").strip() or "Voluntário"
    body = (texto or "").strip()
    return f"💬 Mensagem de {nome}:\n\n{body}"


def _build_public_audio_url(filename: str) -> str:
    if not PUBLIC_BASE_URL:
        raise HTTPException(500, "PUBLIC_BASE_URL não configurado.")
    return f"{PUBLIC_BASE_URL}/media/audios/{filename}"


def _resolve_responsavel_whatsapp_by_encontro(cur, encontro_id: int) -> str:
    row = _get_encontro_core(cur, int(encontro_id))
    if not row:
        raise HTTPException(404, "Encontro não encontrado.")

    responsavel_whatsapp = row[17]  # COALESCE(r.whatsapp, r.telefone)
    if not responsavel_whatsapp:
        raise HTTPException(400, "WhatsApp do responsável não encontrado.")

    return responsavel_whatsapp


# =========================
# CHAT -> WHATSAPP (TEXTO)
# =========================
def _forward_volunteer_text_to_whatsapp(
    cur,
    encontro_id: int,
    texto: str,
    nome_origem: Optional[str] = None
) -> Dict[str, Any]:
    """
    Envia TEXTO do voluntário/chat web para o WhatsApp do responsável.
    """
    try:
        if not _wa_is_configured():
            return {"ok": False, "erro": "whatsapp_not_configured"}

        if not _can_forward_chat_to_whatsapp(cur, int(encontro_id)):
            return {"ok": False, "erro": "aguardando_alerta_inicial"}

        responsavel_whatsapp = _resolve_responsavel_whatsapp_by_encontro(cur, int(encontro_id))
        mensagem = _build_whatsapp_text_from_volunteer(nome_origem, texto)

        meta_resp = _wa_send_text(
            to_number=responsavel_whatsapp,
            texto=mensagem
        )

        return {
            "ok": True,
            "tipo": "texto",
            "to": _to_wa_number(responsavel_whatsapp),
            "meta": meta_resp
        }

    except HTTPException as e:
        return {
            "ok": False,
            "erro": "whatsapp_text_http_exception",
            "detail": e.detail
        }

    except Exception as e:
        _log_exc("Falha em _forward_volunteer_text_to_whatsapp", e)
        return {
            "ok": False,
            "erro": "whatsapp_text_exception",
            "detail": repr(e)
        }

# =========================
# CHAT -> WHATSAPP (ÁUDIO)
# =========================
def _convert_audio_to_whatsapp_ogg(src_filename: str) -> str:
    """
    Converte o áudio salvo pelo chat para .ogg/opus,
    preservando compatibilidade com Android e melhorando suporte a iPhone.
    """
    src_path = os.path.join(AUDIOS_DIR, src_filename)

    if not os.path.exists(src_path):
        raise HTTPException(404, f"Áudio não encontrado: {src_filename}")

    base, _ = os.path.splitext(src_filename)
    out_filename = f"{base}.ogg"
    out_path = os.path.join(AUDIOS_DIR, out_filename)

    if os.path.exists(out_path) and os.path.getsize(out_path) > 0:
        return out_filename

    cmd = [
        "ffmpeg",
        "-y",
        "-i", src_path,
        "-vn",
        "-map", "0:a:0",
        "-ac", "1",
        "-ar", "48000",
        "-c:a", "libopus",
        "-b:a", "64k",
        "-application", "voip",
        "-f", "ogg",
        out_path
    ]

    proc = subprocess.run(cmd, capture_output=True, text=True)

    if proc.returncode != 0 or not os.path.exists(out_path) or os.path.getsize(out_path) <= 0:
        raise HTTPException(
            status_code=500,
            detail={
                "erro": "Falha ao converter áudio para formato aceito pelo WhatsApp",
                "src_filename": src_filename,
                "cmd": " ".join(cmd),
                "stderr": (proc.stderr or "")[-3000:],
                "stdout": (proc.stdout or "")[-1000:],
            },
        )

    return out_filename


def _forward_volunteer_audio_to_whatsapp(
    cur,
    encontro_id: int,
    filename: str
) -> Dict[str, Any]:
    """
    Envia ÁUDIO do voluntário/chat web para o WhatsApp do responsável.
    Mantém o fluxo atual do Android e converte qualquer entrada para .ogg/opus.
    """
    try:
        if not _wa_is_configured():
            return {"ok": False, "erro": "whatsapp_not_configured"}

        if not _can_forward_chat_to_whatsapp(cur, int(encontro_id)):
            return {"ok": False, "erro": "aguardando_alerta_inicial"}

        responsavel_whatsapp = _resolve_responsavel_whatsapp_by_encontro(cur, int(encontro_id))

        filename_ogg = _convert_audio_to_whatsapp_ogg(filename)
        audio_url = _build_public_audio_url(filename_ogg)

        _dbg("WHATSAPP_AUDIO_PRONTO", {
            "encontro_id": int(encontro_id),
            "audio_original": filename,
            "audio_convertido": filename_ogg,
            "audio_url": audio_url,
            "to": _to_wa_number(responsavel_whatsapp),
        })

        meta_resp = _wa_send_audio_by_link(
            to_number=responsavel_whatsapp,
            audio_url=audio_url
        )

        if not _meta_response_ok(meta_resp):
            return {
                "ok": False,
                "erro": "whatsapp_audio_meta_failed",
                "audio_original": filename,
                "audio_convertido": filename_ogg,
                "audio_url": audio_url,
                "meta": meta_resp
            }

        return {
            "ok": True,
            "tipo": "audio",
            "to": _to_wa_number(responsavel_whatsapp),
            "audio_original": filename,
            "audio_convertido": filename_ogg,
            "audio_url": audio_url,
            "meta": meta_resp
        }

    except HTTPException as e:
        return {
            "ok": False,
            "erro": "whatsapp_audio_http_exception",
            "detail": e.detail
        }

    except Exception as e:
        _log_exc("Falha em _forward_volunteer_audio_to_whatsapp", e)
        return {
            "ok": False,
            "erro": "whatsapp_audio_exception",
            "detail": repr(e)
        }


# =========================
# CHAT -> WHATSAPP (ERROS)
# =========================
def _save_whatsapp_error_if_needed(cur, encontro_id: int, wa_result: Optional[dict]):
    """
    Salva erro do envio ao WhatsApp no encontro, sem derrubar o restante do fluxo.
    """
    if not wa_result:
        return

    if wa_result.get("ok"):
        _clear_whatsapp_error(cur, int(encontro_id))
        return

    _set_whatsapp_error(cur, int(encontro_id), wa_result)
# =========================
# CADASTRO / PRIMEIRA LEITURA
# =========================
@app.post("/cadastro/ativar_pulseira", tags=["cadastro_usuario"])
def cadastro_ativar_pulseira(payload: CadastroUsuarioIn):
    nome_responsavel = (payload.nome_responsavel or "").strip()
    tel_responsavel = _only_digits(payload.telefone_responsavel)
    nome_vulneravel = (payload.nome_vulneravel or "").strip()
    login_vinculo = (payload.id_pulseira or "").strip()
    codigo_qr = (payload.codigo_qr or "").strip() or None

    if not nome_responsavel:
        raise HTTPException(400, "nome_responsavel inválido.")
    if not nome_vulneravel:
        raise HTTPException(400, "nome_vulneravel inválido.")
    if not login_vinculo:
        raise HTTPException(400, "id_pulseira inválido.")
    if not _is_tel_valido_br(tel_responsavel):
        raise HTTPException(400, "telefone_responsavel inválido.")

    wa_number = _to_wa_number(tel_responsavel)

    cnx, cur = _open_cursor()
    try:
        resp_id = _resolve_responsavel_por_telefone(cur, tel_responsavel)
        if resp_id:
            cur.execute("""
                UPDATE responsaveis
                SET nome=%s,
                    telefone=%s,
                    whatsapp=%s
                WHERE id=%s
            """, (nome_responsavel, tel_responsavel, wa_number, int(resp_id)))
        else:
            cur.execute("""
                INSERT INTO responsaveis (nome, telefone, whatsapp)
                VALUES (%s, %s, %s)
            """, (nome_responsavel, tel_responsavel, wa_number))
            resp_id = int(cur.lastrowid)

        pulseira_id = _ensure_pulseira(
            cur,
            login_vinculo,
            responsavel_id=int(resp_id),
            codigo_qr=codigo_qr
        )

        cur.execute("""
            UPDATE pulseiras_qr
            SET nome_dependente=%s
            WHERE id=%s
        """, (nome_vulneravel, int(pulseira_id)))

        encontro_id = _ensure_encontro(
            cur,
            pulseira_id,
            responsavel_id=int(resp_id),
            voluntario_id=None
        )

        cnx.commit()

        return {
            "ok": True,
            "encontro_id": int(encontro_id),
            "pulseira_id": int(pulseira_id),
            "login_vinculo": login_vinculo,
            "id_pulseira": login_vinculo,
            "codigo_qr": codigo_qr,
            "responsavel_id": int(resp_id),
            "nome_responsavel": nome_responsavel,
            "telefone_responsavel": tel_responsavel,
            "responsavel_whatsapp": wa_number,
            "nome_vulneravel": nome_vulneravel,
        }
    except HTTPException:
        cnx.rollback()
        raise
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /cadastro/ativar_pulseira", e)
        raise HTTPException(500, "Falha ao ativar pulseira.")
    finally:
        cur.close()
        cnx.close()


@app.post("/pulseira/ativar", tags=["cadastro_usuario"])
def pulseira_ativar_alias(payload: CadastroUsuarioIn):
    return cadastro_ativar_pulseira(payload)


@app.post("/cadastro_usuario", tags=["cadastro_usuario"])
def cadastro_usuario_alias(payload: CadastroUsuarioIn):
    return cadastro_ativar_pulseira(payload)


@app.post("/responsavel/vincular_whatsapp", tags=["cadastro_usuario"])
def vincular_whatsapp_responsavel(payload: VincularWhatsAppIn):
    login_vinculo = (payload.id_pulseira or "").strip()
    wa = _to_wa_number(payload.responsavel_whatsapp)

    if not login_vinculo:
        raise HTTPException(400, "id_pulseira inválido.")
    if len(_only_digits(wa)) < 12:
        raise HTTPException(400, "responsavel_whatsapp inválido. Use com DDI, ex: 5521999998888")

    cnx, cur = _open_cursor()
    try:
        pulseira_id = _ensure_pulseira(cur, login_vinculo)
        encontro_id = _ensure_encontro(cur, pulseira_id)

        cur.execute("""
            SELECT responsavel_id
            FROM encontros
            WHERE id=%s
            LIMIT 1
        """, (int(encontro_id),))
        row = cur.fetchone()
        if not row or not row[0]:
            raise HTTPException(404, "Responsável ainda não vinculado ao encontro.")

        responsavel_id = int(row[0])
        cur.execute("""
            UPDATE responsaveis
            SET whatsapp=%s
            WHERE id=%s
        """, (wa, responsavel_id))
        cnx.commit()

        return {
            "ok": True,
            "encontro_id": int(encontro_id),
            "login_vinculo": login_vinculo,
            "responsavel_whatsapp": wa,
        }
    except HTTPException:
        cnx.rollback()
        raise
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /responsavel/vincular_whatsapp", e)
        raise HTTPException(500, "Falha ao vincular WhatsApp do responsável.")
    finally:
        cur.close()
        cnx.close()


# =========================
# CONTATOS / VOLUNTÁRIOS
# =========================
@app.post("/cadastrar", response_model=ContatoOut, status_code=201, tags=["contatos"])
def cadastrar(contato: ContatoIn):
    nome = (contato.nome or "").strip() or "Anônimo"
    tel = _only_digits(contato.telefone)

    if not _is_tel_valido_br(tel):
        raise HTTPException(400, "Telefone inválido.")

    cnx, cur = _open_cursor()
    try:
        cur.execute("""
            INSERT INTO contatos (nome, telefone)
            VALUES (%s, %s)
        """, (nome, tel))
        cid = int(cur.lastrowid)
        cnx.commit()
        return ContatoOut(id=cid, nome=nome, telefone=tel)
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /cadastrar", e)
        raise HTTPException(500, "Falha ao cadastrar contato.")
    finally:
        cur.close()
        cnx.close()


@app.post("/voluntario/cadastrar", response_model=VoluntarioOut, status_code=201, tags=["voluntarios"])
def cadastrar_voluntario(payload: VoluntarioIn):
    nome = (payload.nome or "").strip() or "Voluntário"
    tel = _only_digits(payload.telefone)

    if not _is_tel_valido_br(tel):
        raise HTTPException(400, "Telefone inválido.")

    cnx, cur = _open_cursor()
    try:
        info = _resolve_voluntario_por_telefone(cur, tel)
        if info:
            cur.execute("""
                UPDATE voluntarios
                SET nome=%s
                WHERE id=%s
            """, (nome, int(info["id"])))
            vid = int(info["id"])
        else:
            cur.execute("""
                INSERT INTO voluntarios (nome, telefone)
                VALUES (%s, %s)
            """, (nome, tel))
            vid = int(cur.lastrowid)

        cnx.commit()
        return VoluntarioOut(id=vid, nome=nome, telefone=tel)
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /voluntario/cadastrar", e)
        raise HTTPException(500, "Falha ao cadastrar voluntário.")
    finally:
        cur.close()
        cnx.close()


# =========================
# =========================
# ENCONTRO / ONBOARDING
# =========================
@app.post("/encontro", status_code=201, tags=["comunicacao_app"])
def registrar_encontro(payload: EncontroIn):
    login_vinculo = _resolve_login_vinculo_from_payload(payload.login_vinculo, payload.id_pulseira)
    nome_vol = (payload.nome_voluntario or "").strip() or "Voluntário"
    vol_tel = _only_digits(payload.voluntario_telefone or payload.telefone_origem or "") or None

    if login_vinculo:
        cnx, cur = _open_cursor()
        try:
            pulseira_id = _ensure_pulseira(cur, login_vinculo)
            voluntario_id = None
            if vol_tel and _is_tel_valido_br(vol_tel):
                info = _resolve_voluntario_por_telefone(cur, vol_tel)
                if info:
                    voluntario_id = info["id"]
                else:
                    cur.execute("INSERT INTO voluntarios (nome, telefone) VALUES (%s, %s)", (nome_vol, vol_tel))
                    voluntario_id = int(cur.lastrowid)

            encontro_id = _ensure_encontro(cur, pulseira_id, voluntario_id=voluntario_id)
            _aprender_voluntario_no_encontro(cur, encontro_id, voluntario_id, nome_vol, vol_tel)
            cnx.commit()

            return {
                "ok": True,
                "id": int(encontro_id),
                "encontro_id": int(encontro_id),
                "login_vinculo": login_vinculo,
                "nome_voluntario": nome_vol,
                "voluntario_telefone": vol_tel,
                "voluntario_id": int(voluntario_id) if voluntario_id else None,
                "voluntario_presente": 1,
                "status": "pendente",
            }
        except Exception as e:
            cnx.rollback()
            _log_exc("Erro em /encontro", e)
            raise HTTPException(500, "Falha ao registrar encontro.")
        finally:
            cur.close()
            cnx.close()

    tel_vul = _only_digits(payload.telefone_vulneravel or "")
    if not _is_tel_valido_br(tel_vul):
        raise HTTPException(400, "telefone_vulneravel inválido e login_vinculo ausente.")

    cnx, cur = _open_cursor()
    try:
        pulseira_id = _ensure_pulseira(cur, f"legacy_{tel_vul}")
        responsavel_id = _resolve_responsavel_por_telefone(cur, tel_vul)
        voluntario_id = None
        if vol_tel and _is_tel_valido_br(vol_tel):
            info = _resolve_voluntario_por_telefone(cur, vol_tel)
            if info:
                voluntario_id = info["id"]
            else:
                cur.execute("INSERT INTO voluntarios (nome, telefone) VALUES (%s, %s)", (nome_vol, vol_tel))
                voluntario_id = int(cur.lastrowid)

        encontro_id = _ensure_encontro(cur, pulseira_id, responsavel_id=responsavel_id, voluntario_id=voluntario_id)
        _aprender_voluntario_no_encontro(cur, encontro_id, voluntario_id, nome_vol, vol_tel)
        cnx.commit()

        return {
            "ok": True,
            "id": int(encontro_id),
            "encontro_id": int(encontro_id),
            "telefone_vulneravel": tel_vul,
            "nome_voluntario": nome_vol,
            "voluntario_telefone": vol_tel,
            "voluntario_id": int(voluntario_id) if voluntario_id else None,
            "voluntario_presente": 1,
            "status": "pendente",
        }
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /encontro", e)
        raise HTTPException(500, "Falha ao registrar encontro.")
    finally:
        cur.close()
        cnx.close()


@app.post("/encontro/tipo_vulneravel", tags=["comunicacao_app"])
def encontro_tipo_vulneravel(payload: TipoVulneravelIn):
    tipo = (payload.tipo or "").strip().lower()
    if not tipo:
        raise HTTPException(400, "tipo vazio.")

    login_vinculo = _resolve_login_vinculo_from_payload(payload.login_vinculo, payload.id_pulseira)

    cnx, cur = _open_cursor()
    try:
        encontro_id = payload.encontro_id

        if not encontro_id and login_vinculo:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, login_vinculo) or _resolve_encontro_por_login_vinculo(cur, login_vinculo)

        if not encontro_id:
            tel = _only_digits(payload.telefone_alvo or "")
            if not _is_tel_valido_br(tel):
                raise HTTPException(400, "telefone_alvo inválido e login_vinculo ausente.")
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if not encontro_id:
            raise HTTPException(404, "Encontro não encontrado.")

        cur.execute("""
            UPDATE encontros
            SET tipo_vulneravel=%s
            WHERE id=%s
        """, (tipo, int(encontro_id)))

        try:
            wa_result = _maybe_send_onboarding_to_whatsapp(cur, int(encontro_id))
            _dbg("WHATSAPP/TIPO_TRIGGER", wa_result)
        except Exception as e:
            _log_exc("Erro ao tentar disparar WhatsApp após /encontro/tipo_vulneravel", e)

        cnx.commit()

        return {
            "ok": True,
            "encontro_id": int(encontro_id),
            "login_vinculo": login_vinculo,
            "tipo_vulneravel": tipo
        }
    except HTTPException:
        cnx.rollback()
        raise
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /encontro/tipo_vulneravel", e)
        raise HTTPException(500, "Falha ao salvar tipo_vulneravel.")
    finally:
        cur.close()
        cnx.close()


@app.get("/encontro/pending", tags=["comunicacao_app"])
def buscar_encontro_pendente(
    telefone_vulneravel: Optional[str] = Query(default=None),
    telefone: Optional[str] = Query(default=None),
    login_vinculo: Optional[str] = Query(default=None),
    id_pulseira: Optional[str] = Query(default=None),
):
    lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

    cnx, cur = _open_cursor()
    try:
        encontro_id = None
        if lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) or _resolve_encontro_por_login_vinculo(cur, lv)
        else:
            tel = _only_digits((telefone_vulneravel or telefone or ""))
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if not encontro_id:
            return {"has_event": False}

        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            return {"has_event": False}

        loc = _get_ultima_localizacao_para_encontro(cur, int(encontro_id))

        (
            eid,
            pulseira_id,
            responsavel_id,
            voluntario_id,
            created_at,
            tipo_vulneravel,
            foto_arquivo,
            status,
            voluntario_presente,
            envio_de_localizacao,
            onboarding_whatsapp_enviado,
            onboarding_whatsapp_enviado_em,
            whatsapp_ultimo_erro,
            login_vinculo_db,
            nome_vulneravel,
            nome_responsavel,
            telefone_responsavel,
            responsavel_whatsapp,
            nome_voluntario,
            voluntario_telefone
        ) = row

        return {
            "has_event": True,
            "id": int(eid),
            "encontro_id": int(eid),
            "login_vinculo": login_vinculo_db,
            "id_pulseira": login_vinculo_db,
            "pulseira_id": int(pulseira_id) if pulseira_id else None,
            "responsavel_id": int(responsavel_id) if responsavel_id else None,
            "nome_responsavel": nome_responsavel,
            "telefone_responsavel": telefone_responsavel,
            "responsavel_whatsapp": responsavel_whatsapp,
            "nome_vulneravel": nome_vulneravel,
            "voluntario_id": int(voluntario_id) if voluntario_id else None,
            "nome_voluntario": nome_voluntario or "Voluntário",
            "voluntario_telefone": voluntario_telefone,
            "foto_url": f"/media/fotos/{foto_arquivo}" if foto_arquivo else None,
            "voluntario_presente": int(voluntario_presente or 0),
            "status": status or "pendente",
            "envio_de_localizacao": int(envio_de_localizacao or 0),
            "onboarding_whatsapp_enviado": int(onboarding_whatsapp_enviado or 0),
            "created_at": created_at.strftime("%Y-%m-%d %H:%M:%S") if created_at else None,
            "tipo_vulneravel": tipo_vulneravel or None,
            "latitude": float(loc[0]) if loc else None,
            "longitude": float(loc[1]) if loc else None,
            "accuracy": loc[2] if loc else None,
            "location_created_at": loc[3].strftime("%Y-%m-%d %H:%M:%S") if (loc and loc[3]) else None,
        }
    except Exception as e:
        _log_exc("Erro em /encontro/pending", e)
        return {"has_event": False, "error": "db_error", "detail": repr(e)}
    finally:
        cur.close()
        cnx.close()
        
# =========================
# LOCALIZAÇÃO (FLUXO CORRETO)
# =========================

@app.post("/localizacao", tags=["localizacao"])
def salvar_localizacao(payload: LocalizacaoIn):
    """
    Recebe localização do voluntário ou usuário
    e salva vinculando corretamente ao encontro.
    """

    # 🔎 Resolver login_vinculo (PRINCIPAL)
    lv = _resolve_login_vinculo_from_payload(
        payload.login_vinculo,
        payload.id_pulseira
    )

    cnx, cur = _open_cursor()

    try:
        encontro_id = payload.encontro_id

        # 🔥 PRIORIDADE 1 → login_vinculo
        if not encontro_id and lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) \
                or _resolve_encontro_por_login_vinculo(cur, lv)

        # 🔥 PRIORIDADE 2 → telefone (fallback)
        if not encontro_id and payload.telefone_vulneravel:
            tel = _only_digits(payload.telefone_vulneravel)
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") \
                    or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        # ❌ Sem encontro → erro
        if not encontro_id:
            raise HTTPException(404, "Encontro não encontrado.")

        # 🔎 valida encontro
        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            raise HTTPException(404, "Encontro não encontrado.")

        login_vinculo_db = row[13]

        # 👤 origem
        origem = (payload.origem or "").strip().lower()

        voluntario_nome = payload.voluntario_nome
        voluntario_telefone = _only_digits(payload.voluntario_telefone or "") or None

        # 💾 salva localização
        cur.execute("""
            INSERT INTO localizacoes (
                encontro_id,
                voluntario_id,
                voluntario_nome,
                voluntario_telefone,
                latitude,
                longitude,
                accuracy,
                ts_client
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(encontro_id),
            payload.voluntario_id,
            voluntario_nome,
            voluntario_telefone,
            float(payload.latitude),
            float(payload.longitude),
            payload.accuracy,
            payload.timestamp
        ))

        # 🔥 LIBERA FLAG DE LOCALIZAÇÃO
        cur.execute("""
            UPDATE encontros
            SET envio_de_localizacao=1
            WHERE id=%s
        """, (int(encontro_id),))

        # 🔥 aprende voluntário (IMPORTANTE PRO WHATSAPP)
        if origem in ("voluntario", "web"):
            _aprender_voluntario_no_encontro(
                cur,
                int(encontro_id),
                payload.voluntario_id,
                voluntario_nome,
                voluntario_telefone
            )

        cnx.commit()

        return {
            "ok": True,
            "encontro_id": int(encontro_id),
            "login_vinculo": login_vinculo_db,
            "latitude": payload.latitude,
            "longitude": payload.longitude
        }

    except HTTPException:
        cnx.rollback()
        raise
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /localizacao", e)
        raise HTTPException(500, "Falha ao salvar localização.")
    finally:
        cur.close()
        cnx.close()



# =========================
# FOTO
# =========================
@app.post("/foto", tags=["foto"])
async def receber_foto(
    foto: UploadFile = File(...),
    telefone_alvo: Optional[str] = Form(default=None),
    encontro_id: Optional[int] = Form(default=None),
    origem: Optional[str] = Form(default="voluntario"),
    telefone_origem: Optional[str] = Form(default=None),
    nome_origem: Optional[str] = Form(default=None),
    login_vinculo: Optional[str] = Form(default=None),
    id_pulseira: Optional[str] = Form(default=None),
):
    cnx, cur = _open_cursor()
    try:
        origem_lc = (origem or "voluntario").strip().lower()
        if origem_lc not in ("voluntario", "app"):
            raise HTTPException(400, "origem inválida. Use 'voluntario' ou 'app'.")

        lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)
        if not encontro_id and lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) or _resolve_encontro_por_login_vinculo(cur, lv)

        if not encontro_id and telefone_alvo:
            tel = _only_digits(telefone_alvo)
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if not encontro_id:
            raise HTTPException(404, "Encontro não encontrado.")

        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            raise HTTPException(404, "Encontro não encontrado.")

        login_vinculo_db = row[13]
        telefone_alvo_final = row[16] or None

        filename = _unique_photo_name(foto.filename or "captura.png")
        path = os.path.join(FOTOS_DIR, filename)
        with open(path, "wb") as f:
            shutil.copyfileobj(foto.file, f)

        tel_origem = _only_digits(telefone_origem or "") or None
        nome = (nome_origem or "").strip() or ("Voluntário" if origem_lc == "voluntario" else "Usuário")

        cur.execute("""
            INSERT INTO mensagens
              (encontro_id, tipo, arquivo_foto, telefone_alvo, status,
               pendente_para, telefone_origem, nome_origem, remetente_tipo)
            VALUES
              (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(encontro_id),
            "foto",
            filename,
            telefone_alvo_final,
            "entregue" if origem_lc == "voluntario" else "pendente",
            None if origem_lc == "voluntario" else "voluntario",
            tel_origem,
            nome,
            origem_lc
        ))

        msg_id = int(cur.lastrowid)

        cur.execute("""
            UPDATE encontros
            SET foto_arquivo=%s
            WHERE id=%s
        """, (filename, int(encontro_id)))

        if origem_lc == "voluntario":
            _aprender_voluntario_no_encontro(cur, int(encontro_id), None, nome, tel_origem)

        if origem_lc == "voluntario":
            try:
                wa_result = _maybe_send_onboarding_to_whatsapp(cur, int(encontro_id))
                _dbg("WHATSAPP/FOTO_TRIGGER", wa_result)
            except Exception as e:
                _log_exc("Erro ao tentar disparar WhatsApp após /foto", e)

        cnx.commit()

        return {
            "ok": True,
            "id": msg_id,
            "encontro_id": int(encontro_id),
            "arquivo": filename,
            "foto_url": f"/media/fotos/{filename}",
            "login_vinculo": login_vinculo_db,
        }

    except HTTPException:
        cnx.rollback()
        raise
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro REAL em /foto", e)
        raise HTTPException(500, "Falha ao processar foto.")
    finally:
        try:
            if foto and not foto.file.closed:
                await foto.close()
        except Exception:
            pass
        cur.close()
        cnx.close()



# =========================
# MENSAGENS
# =========================
# Fluxo:
# 1) chat web envia texto/áudio
# 2) API resolve encontro pelo login_vinculo / id_pulseira / encontro_id
# 3) salva mensagem no banco
# 4) se origem for voluntario/web -> tenta encaminhar para WhatsApp do responsável
# 5) se origem for app -> deixa pendente para o voluntário
# 6) long-poll entrega mensagens pendentes ao chat
# 7) ack marca como entregue


def _close_audio_safely(audio_obj):
    try:
        if audio_obj and getattr(audio_obj, "file", None) and not audio_obj.file.closed:
            audio_obj.file.close()
    except Exception:
        pass


def _safe_commit(cnx):
    cnx.commit()


def _safe_wa_text(cur, encontro_id: int, texto: str, nome_origem: Optional[str] = None) -> Dict[str, Any]:
    try:
        return _forward_volunteer_text_to_whatsapp(
            cur,
            int(encontro_id),
            texto,
            nome_origem
        )
    except Exception as e:
        return {
            "ok": False,
            "stage": "forward_text_to_whatsapp",
            "error": str(e)
        }


def _safe_wa_audio(cur, encontro_id: int, filename: str) -> Dict[str, Any]:
    try:
        return _forward_volunteer_audio_to_whatsapp(cur, int(encontro_id), filename)
    except Exception as e:
        return {
            "ok": False,
            "stage": "forward_audio_to_whatsapp",
            "error": str(e)
        }


@app.post("/mensagem/texto", tags=["mensagens"])
def enviar_texto(payload: MensagemTextoIn):
    texto = (payload.texto or "").strip()
    if not texto:
        raise HTTPException(400, "texto vazio.")

    origem_raw = (payload.origem or "web").strip().lower()
    if origem_raw in ("voluntario", "web"):
        origem = "voluntario"
    elif origem_raw == "app":
        origem = "app"
    else:
        raise HTTPException(400, "origem inválida. Use 'voluntario', 'web' ou 'app'.")

    tel_origem = _only_digits(payload.telefone_origem or payload.voluntario_telefone or "") or None
    nome_origem = (payload.nome_origem or "").strip() or (
        "Voluntário" if origem == "voluntario" else "Usuário"
    )

    lv = _resolve_login_vinculo_from_payload(payload.login_vinculo, payload.id_pulseira)
    encontro_id = payload.encontro_id

    cnx, cur = _open_cursor()

    try:
        # =========================
        # Resolver encontro
        # =========================
        if not encontro_id and lv:
            encontro_id = (
                _resolve_encontro_pendente_por_login_vinculo(cur, lv)
                or _resolve_encontro_por_login_vinculo(cur, lv)
            )

        if not encontro_id and payload.telefone_alvo:
            tel = _only_digits(payload.telefone_alvo)
            if _is_tel_valido_br(tel):
                encontro_id = (
                    _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}")
                    or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")
                )

        if not encontro_id:
            raise HTTPException(404, "Encontro não encontrado.")

        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            raise HTTPException(404, "Encontro não encontrado.")

        login_vinculo_db = row[13] if len(row) > 13 else lv
        telefone_alvo_final = row[17] if len(row) > 17 else None

        # =========================
        # Fluxo voluntário/web -> salva e envia para WhatsApp
        # =========================
        if origem == "voluntario":
            cur.execute("""
                INSERT INTO mensagens
                  (encontro_id, tipo, conteudo_texto, telefone_origem, nome_origem,
                   telefone_alvo, status, pendente_para, remetente_tipo)
                VALUES
                  (%s, 'texto', %s, %s, %s, %s, 'entregue', NULL, 'voluntario')
            """, (
                int(encontro_id),
                texto,
                tel_origem,
                nome_origem,
                telefone_alvo_final
            ))
            msg_id = int(cur.lastrowid)

            _aprender_voluntario_no_encontro(
                cur,
                int(encontro_id),
                None,
                nome_origem,
                tel_origem
            )

            wa_result = _safe_wa_text(
                cur,
                int(encontro_id),
                texto,
                nome_origem
            )
            _save_whatsapp_error_if_needed(cur, int(encontro_id), wa_result)

            _safe_commit(cnx)

            return {
                "ok": True,
                "id": msg_id,
                "encontro_id": int(encontro_id),
                "login_vinculo": login_vinculo_db,
                "whatsapp": wa_result,
            }

        # =========================
        # Fluxo app -> deixa pendente para o voluntário
        # =========================
        cur.execute("""
            INSERT INTO mensagens
              (encontro_id, tipo, conteudo_texto, telefone_origem, nome_origem,
               telefone_alvo, status, pendente_para, remetente_tipo)
            VALUES
              (%s, 'texto', %s, %s, %s, %s, 'pendente', 'voluntario', 'app')
        """, (
            int(encontro_id),
            texto,
            tel_origem,
            nome_origem,
            telefone_alvo_final
        ))
        msg_id = int(cur.lastrowid)

        _safe_commit(cnx)
        _notify_poll("voluntario", int(encontro_id), login_vinculo_db)

        return {
            "ok": True,
            "id": msg_id,
            "encontro_id": int(encontro_id),
            "login_vinculo": login_vinculo_db,
            "pendente_para": "voluntario",
        }

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass


@app.post("/mensagem/enviar", tags=["mensagens"])
def enviar_texto_compat(payload: Dict[str, Any] = Body(...)):
    model = MensagemTextoIn(
        telefone_alvo=payload.get("telefone_alvo") or payload.get("telefone_vulneravel") or payload.get("telefone"),
        telefone_origem=payload.get("telefone_origem") or payload.get("voluntario_telefone"),
        nome_origem=payload.get("nome_origem") or payload.get("nome"),
        texto=str(payload.get("texto") or payload.get("mensagem") or payload.get("msg") or "").strip(),
        encontro_id=payload.get("encontro_id") or payload.get("encontroId"),
        origem=(payload.get("origem") or "web"),
        voluntario_telefone=payload.get("voluntario_telefone"),
        login_vinculo=payload.get("login_vinculo") or payload.get("id_pulseira"),
        id_pulseira=payload.get("id_pulseira") or payload.get("login_vinculo"),
    )
    return enviar_texto(model)


@app.post("/mensagem/audio", tags=["mensagens"])
async def enviar_audio(
    telefone_alvo: Optional[str] = Form(default=None),
    telefone_origem: Optional[str] = Form(default=None),
    nome_origem: Optional[str] = Form(default=None),
    encontro_id: Optional[int] = Form(default=None),
    origem: Optional[str] = Form(default="web"),
    login_vinculo: Optional[str] = Form(default=None),
    id_pulseira: Optional[str] = Form(default=None),
    audio: UploadFile = File(...)
):
    origem_raw = (origem or "web").strip().lower()
    if origem_raw in ("voluntario", "web"):
        origem_lc = "voluntario"
    elif origem_raw == "app":
        origem_lc = "app"
    else:
        raise HTTPException(400, "origem inválida. Use 'voluntario', 'web' ou 'app'.")

    if not audio:
        raise HTTPException(400, "arquivo de áudio ausente.")

    cnx, cur = _open_cursor()

    try:
        # =========================
        # Resolver encontro
        # =========================
        lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

        if not encontro_id and lv:
            encontro_id = (
                _resolve_encontro_pendente_por_login_vinculo(cur, lv)
                or _resolve_encontro_por_login_vinculo(cur, lv)
            )

        if not encontro_id and telefone_alvo:
            tel = _only_digits(telefone_alvo)
            if _is_tel_valido_br(tel):
                encontro_id = (
                    _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}")
                    or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")
                )

        if not encontro_id:
            raise HTTPException(404, "Encontro não encontrado.")

        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            raise HTTPException(404, "Encontro não encontrado.")

        login_vinculo_db = row[13] if len(row) > 13 else lv
        telefone_alvo_final = row[17] if len(row) > 17 else None

        nome = (nome_origem or "").strip() or (
            "Voluntário" if origem_lc == "voluntario" else "Usuário"
        )
        tel_origem = _only_digits(telefone_origem or "") or None

        # =========================
        # Salvar arquivo de áudio
        # =========================
        os.makedirs(AUDIOS_DIR, exist_ok=True)

        filename = _unique_audio_name(audio.filename or "audio.webm")
        path = os.path.join(AUDIOS_DIR, filename)

        with open(path, "wb") as f:
            shutil.copyfileobj(audio.file, f)

        # =========================
        # Salvar mensagem no banco
        # =========================
        cur.execute("""
            INSERT INTO mensagens
              (encontro_id, tipo, arquivo_audio, telefone_origem, nome_origem,
               telefone_alvo, status, pendente_para, remetente_tipo)
            VALUES
              (%s, 'audio', %s, %s, %s, %s, %s, %s, %s)
        """, (
            int(encontro_id),
            filename,
            tel_origem,
            nome,
            telefone_alvo_final,
            "entregue" if origem_lc == "voluntario" else "pendente",
            None if origem_lc == "voluntario" else "voluntario",
            origem_lc
        ))
        msg_id = int(cur.lastrowid)

        wa_result = None

        # =========================
        # Fluxo voluntário/web -> envia para WhatsApp
        # =========================
        if origem_lc == "voluntario":
            _aprender_voluntario_no_encontro(
                cur,
                int(encontro_id),
                None,
                nome,
                tel_origem
            )

            wa_result = _safe_wa_audio(cur, int(encontro_id), filename)
            _save_whatsapp_error_if_needed(cur, int(encontro_id), wa_result)

        _safe_commit(cnx)

        # =========================
        # Fluxo app -> notifica long-poll do voluntário
        # =========================
        if origem_lc != "voluntario":
            _notify_poll("voluntario", int(encontro_id), login_vinculo_db)

        return {
            "ok": True,
            "id": msg_id,
            "encontro_id": int(encontro_id),
            "audio_url": f"/media/audios/{filename}",
            "login_vinculo": login_vinculo_db,
            "whatsapp": wa_result,
            "pendente_para": None if origem_lc == "voluntario" else "voluntario",
        }

    finally:
        _close_audio_safely(audio)
        try:
            cur.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass


@app.get("/mensagem/pending", tags=["mensagens"])
def buscar_mensagem_pendente(
    telefone: Optional[str] = Query(default=None, min_length=8, max_length=20),
    telefone_alvo: Optional[str] = Query(default=None, min_length=8, max_length=20),
    encontro_id: Optional[int] = Query(default=None),
    destino: Optional[str] = Query(default="voluntario", max_length=20),
    login_vinculo: Optional[str] = Query(default=None, max_length=120),
    id_pulseira: Optional[str] = Query(default=None, max_length=120),
    last_id: int = Query(0, ge=0),
    wait_seconds: int = Query(25, ge=0, le=60),
    sleep_ms: int = Query(30, ge=5, le=2000),
):
    destino_lc = _norm_destino(destino)
    if destino is not None and destino_lc is None:
        raise HTTPException(400, "destino inválido. Use 'app', 'voluntario' ou 'web'.")

    lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

    cnx, cur = _open_cursor()

    try:
        resolved_encontro_id = encontro_id

        if not resolved_encontro_id and lv:
            resolved_encontro_id = (
                _resolve_encontro_pendente_por_login_vinculo(cur, lv)
                or _resolve_encontro_por_login_vinculo(cur, lv)
            )

        if not resolved_encontro_id:
            tel_in = telefone_alvo or telefone
            tel = _only_digits(tel_in or "")
            if _is_tel_valido_br(tel):
                resolved_encontro_id = (
                    _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}")
                    or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")
                )

        if not resolved_encontro_id:
            return {"has_msg": False, "error": "encontro_not_found"}

        row = _get_encontro_core(cur, int(resolved_encontro_id))
        login_vinculo_db = row[13] if row and len(row) > 13 else lv

        def _fetch_one():
            cur.execute("""
                SELECT id, tipo, conteudo_texto, arquivo_audio, arquivo_foto,
                       telefone_origem, nome_origem, telefone_alvo, created_at,
                       encontro_id, pendente_para, status, remetente_tipo
                FROM mensagens
                WHERE encontro_id=%s
                  AND status='pendente'
                  AND pendente_para=%s
                  AND id > %s
                ORDER BY id ASC
                LIMIT 1
            """, (
                int(resolved_encontro_id),
                destino_lc or "voluntario",
                int(last_id or 0)
            ))
            return cur.fetchone()

        def _serialize(r, took_ms: Optional[int] = None):
            data = {
                "has_msg": True,
                "id": int(r[0]),
                "tipo": r[1],
                "texto": r[2],
                "audio_url": f"/media/audios/{r[3]}" if r[3] else None,
                "foto_url": f"/media/fotos/{r[4]}" if r[4] else None,
                "telefone_origem": r[5],
                "nome_origem": r[6] or "Responsável",
                "telefone_alvo": r[7],
                "created_at": r[8].strftime("%Y-%m-%d %H:%M:%S") if r[8] else None,
                "encontro_id": r[9],
                "pendente_para": r[10] or "voluntario",
                "status": r[11] or "pendente",
                "remetente_tipo": r[12] or None,
                "login_vinculo": login_vinculo_db,
            }
            if took_ms is not None:
                data["took_ms"] = took_ms
            return data

        if not wait_seconds or wait_seconds <= 0:
            r = _fetch_one()
            if not r:
                return {"has_msg": False}
            return _serialize(r)

        t0 = time.time()
        deadline = t0 + float(wait_seconds)
        ev = _get_event(destino_lc or "voluntario", int(resolved_encontro_id), login_vinculo_db)

        while True:
            r = _fetch_one()
            if r:
                return _serialize(r, int((time.time() - t0) * 1000))

            now = time.time()
            if now >= deadline:
                return {
                    "has_msg": False,
                    "timeout": True,
                    "login_vinculo": login_vinculo_db,
                    "took_ms": int((time.time() - t0) * 1000)
                }

            remaining = max(0.0, deadline - now)
            wait_chunk = min(remaining, max(0.02, float(sleep_ms) / 1000.0))
            ev.wait(timeout=wait_chunk)
            if ev.is_set():
                ev.clear()

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass


@app.post("/mensagem/ack", tags=["mensagens"])
def ack_mensagem(payload: MensagemAckIn):
    ack_por = _norm_destino(payload.ack_por) or "voluntario"

    cnx, cur = _open_cursor()

    try:
        cur.execute("""
            SELECT id, status, pendente_para
            FROM mensagens
            WHERE id=%s
            LIMIT 1
        """, (payload.id,))
        row = cur.fetchone()

        if not row:
            raise HTTPException(404, "Mensagem não encontrada.")

        status_atual = (row[1] or "").strip().lower()
        pendente_para = (row[2] or "").strip().lower()

        if status_atual != "pendente":
            return {
                "ok": True,
                "updated": 0,
                "info": "already_not_pendente",
                "status": status_atual
            }

        if pendente_para and ack_por != pendente_para:
            raise HTTPException(
                400,
                f"ack_por inválido. Esperado '{pendente_para}', recebeu '{ack_por}'."
            )

        cur.execute("""
            UPDATE mensagens
            SET status='entregue',
                entregue_em=NOW(),
                ack_por=%s
            WHERE id=%s AND status='pendente'
        """, (ack_por, payload.id))
        cnx.commit()

        return {"ok": True, "updated": cur.rowcount}

    finally:
        try:
            cur.close()
        except Exception:
            pass
        try:
            cnx.close()
        except Exception:
            pass
# =========================
# DISTÂNCIA
# =========================
@app.get("/encontro/distancia", tags=["comunicacao_app"])
def encontro_distancia(
    encontro_id: Optional[int] = Query(default=None),
    login_vinculo: Optional[str] = Query(default=None),
    id_pulseira: Optional[str] = Query(default=None),
    telefone_vulneravel: Optional[str] = Query(default=None),
    modo: str = Query(default="caminhada")
):
    lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

    cnx, cur = _open_cursor()
    try:
        if not encontro_id and lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) or _resolve_encontro_por_login_vinculo(cur, lv)

        if not encontro_id and telefone_vulneravel:
            tel = _only_digits(telefone_vulneravel)
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if not encontro_id:
            return {"ok": False, "msg": "Nenhum encontro encontrado."}

        u = _get_ultima_loc_usuario(cur, int(encontro_id))
        v = _get_ultima_loc_voluntario(cur, int(encontro_id))

        if not u or not v:
            return {
                "ok": False,
                "encontro_id": int(encontro_id),
                "tem_usuario": bool(u),
                "tem_voluntario": bool(v),
                "msg": "Localizações insuficientes."
            }

        u_lat, u_lon, u_acc, u_dt = u
        v_lat, v_lon, v_acc, v_dt, v_id, v_nome, v_tel = v

        dist_m = _haversine_m(float(u_lat), float(u_lon), float(v_lat), float(v_lon))
        modo_lc = (modo or "").strip().lower()
        vel_m_s = 8.3 if modo_lc == "carro" else 1.4
        eta_s = dist_m / max(vel_m_s, 0.1)
        eta_min = int(round(eta_s / 60))

        if dist_m <= 5:
            status_txt = "No local"
        elif dist_m <= 20:
            status_txt = "Muito perto"
        else:
            status_txt = "A caminho"

        return {
            "ok": True,
            "encontro_id": int(encontro_id),
            "distancia_m": int(round(dist_m)),
            "eta_min": max(0, eta_min),
            "status": status_txt,
            "usuario": {
                "lat": float(u_lat),
                "lon": float(u_lon),
                "acc": u_acc,
                "ts": u_dt.strftime("%Y-%m-%d %H:%M:%S") if u_dt else None,
            },
            "voluntario": {
                "id": int(v_id) if v_id else None,
                "nome": (v_nome or "Voluntário"),
                "telefone": v_tel,
                "lat": float(v_lat),
                "lon": float(v_lon),
                "acc": v_acc,
                "ts": v_dt.strftime("%Y-%m-%d %H:%M:%S") if v_dt else None,
            }
        }
    except Exception as e:
        _log_exc("Erro em /encontro/distancia", e)
        raise HTTPException(500, "Falha ao calcular distância.")
    finally:
        cur.close()
        cnx.close()


# =========================
# LEGADO / NO-OP COMPAT
# =========================
@app.post("/encontro/liberar_localizacao", tags=["comunicacao_app"])
def liberar_localizacao(payload: LiberarLocalizacaoIn):
    lv = _resolve_login_vinculo_from_payload(payload.login_vinculo, payload.id_pulseira)
    cnx, cur = _open_cursor()
    try:
        encontro_id = None
        if lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) or _resolve_encontro_por_login_vinculo(cur, lv)
        elif payload.telefone_vulneravel:
            tel = _only_digits(payload.telefone_vulneravel)
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if encontro_id:
            cur.execute("""
                UPDATE encontros
                SET envio_de_localizacao=1
                WHERE id=%s
            """, (int(encontro_id),))
            cnx.commit()

        return {"ok": True, "encontro_id": int(encontro_id or 0), "updated": int(1 if encontro_id else 0)}
    except Exception as e:
        cnx.rollback()
        _log_exc("Erro em /encontro/liberar_localizacao", e)
        raise HTTPException(500, "Falha ao liberar localização.")
    finally:
        cur.close()
        cnx.close()


@app.get("/app/poll", tags=["comunicacao_app"])
def app_poll(
    telefone_vulneravel: Optional[str] = Query(default=None),
    wait_seconds: int = Query(25, ge=1, le=60),
    sleep_ms: int = Query(30, ge=5, le=2000),
    encontro_id: Optional[int] = Query(default=None),
    last_msg_id: Optional[int] = Query(default=0, ge=0),
    last_envio_de_localizacao: Optional[int] = Query(default=None),
    login_vinculo: Optional[str] = Query(default=None),
    id_pulseira: Optional[str] = Query(default=None),
):
    lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

    cnx, cur = _open_cursor()
    try:
        if not encontro_id and lv:
            encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, lv) or _resolve_encontro_por_login_vinculo(cur, lv)

        if not encontro_id and telefone_vulneravel:
            tel = _only_digits(telefone_vulneravel)
            if _is_tel_valido_br(tel):
                encontro_id = _resolve_encontro_pendente_por_login_vinculo(cur, f"legacy_{tel}") or _resolve_encontro_por_login_vinculo(cur, f"legacy_{tel}")

        if not encontro_id:
            return {"ok": True, "has_msg": False, "has_event": False}

        row = _get_encontro_core(cur, int(encontro_id))
        if not row:
            return {"ok": True, "has_msg": False, "has_event": False}

        return {
            "ok": True,
            "has_event": True,
            "has_msg": False,
            "encontro_id": int(encontro_id),
            "envio_de_localizacao": int(row[9] or 0),
            "onboarding_whatsapp_enviado": int(row[10] or 0),
            "msg": None
        }
    except Exception as e:
        _log_exc("Erro em /app/poll", e)
        return {"ok": False, "has_msg": False, "has_event": False, "error": "poll_error"}
    finally:
        cur.close()
        cnx.close()


@app.get("/web/poll", tags=["comunicacao_app"])
def web_poll(
    telefone_vulneravel: Optional[str] = Query(default=None),
    wait_seconds: int = Query(25, ge=1, le=60),
    sleep_ms: int = Query(30, ge=5, le=2000),
    last_envio_de_localizacao: Optional[int] = Query(default=None),
    login_vinculo: Optional[str] = Query(default=None),
    id_pulseira: Optional[str] = Query(default=None),
):
    return app_poll(
        telefone_vulneravel=telefone_vulneravel,
        wait_seconds=wait_seconds,
        sleep_ms=sleep_ms,
        encontro_id=None,
        last_msg_id=0,
        last_envio_de_localizacao=last_envio_de_localizacao,
        login_vinculo=login_vinculo,
        id_pulseira=id_pulseira,
    )


# =========================
# HISTÓRICO
# =========================
@app.get("/historico/voluntarios", tags=["historico"])
def historico_voluntarios(
    telefone_vulneravel: Optional[str] = Query(default=None),
    telefone: Optional[str] = Query(default=None),
    telefone_alvo: Optional[str] = Query(default=None),
    login_vinculo: Optional[str] = Query(default=None),
    id_pulseira: Optional[str] = Query(default=None),
    limit: int = Query(50, ge=1, le=200),
):
    lv = _resolve_login_vinculo_from_payload(login_vinculo, id_pulseira)

    cnx, cur = _open_cursor()
    try:
        if lv:
            pulseira_id = _resolve_pulseira_id(cur, lv)
            if not pulseira_id:
                return {"ok": True, "itens": []}

            cur.execute("""
                SELECT
                    e.id,
                    e.created_at,
                    e.voluntario_id,
                    e.tipo_vulneravel,
                    e.status,
                    e.foto_arquivo,
                    v.nome,
                    v.telefone,
                    p.login_vinculo
                FROM encontros e
                LEFT JOIN voluntarios v ON v.id = e.voluntario_id
                LEFT JOIN pulseiras_qr p ON p.id = e.pulseira_id
                WHERE e.pulseira_id=%s
                ORDER BY e.id DESC
                LIMIT %s
            """, (int(pulseira_id), limit))
        else:
            tel = _only_digits(telefone_vulneravel or telefone or telefone_alvo or "")
            if not _is_tel_valido_br(tel):
                raise HTTPException(400, "telefone inválido.")
            pulseira_id = _resolve_pulseira_id(cur, f"legacy_{tel}")
            if not pulseira_id:
                return {"ok": True, "itens": []}

            cur.execute("""
                SELECT
                    e.id,
                    e.created_at,
                    e.voluntario_id,
                    e.tipo_vulneravel,
                    e.status,
                    e.foto_arquivo,
                    v.nome,
                    v.telefone,
                    p.login_vinculo
                FROM encontros e
                LEFT JOIN voluntarios v ON v.id = e.voluntario_id
                LEFT JOIN pulseiras_qr p ON p.id = e.pulseira_id
                WHERE e.pulseira_id=%s
                ORDER BY e.id DESC
                LIMIT %s
            """, (int(pulseira_id), limit))

        rows = cur.fetchall() or []
        itens = []

        for r in rows:
            encontro_id = int(r[0])
            created_at = r[1]
            voluntario_id = int(r[2]) if r[2] else None
            tipo_vulneravel = (r[3] or "").strip().lower() or None
            status_encontro = (r[4] or "").strip().lower() or None
            foto_arq = (r[5] or "").strip() or None
            nome_vol = (r[6] or "Voluntário").strip()
            tel_vol = (r[7] or "").strip() or None
            lv_db = r[8]

            cur.execute("""
                SELECT tipo, conteudo_texto, created_at, nome_origem, arquivo_foto, arquivo_audio
                FROM mensagens
                WHERE encontro_id=%s
                ORDER BY id DESC
                LIMIT 1
            """, (encontro_id,))
            m = cur.fetchone()

            ultima_msg = None
            ultima_msg_em = None
            foto_url = f"/media/fotos/{foto_arq}" if foto_arq else None

            if m:
                tipo_msg = (m[0] or "texto").strip().lower()
                conteudo = (m[1] or "").strip()
                ultima_msg_em = m[2]

                if tipo_msg == "texto" and conteudo:
                    ultima_msg = conteudo
                elif tipo_msg == "foto":
                    ultima_msg = "[foto]"
                    if m[4]:
                        foto_url = f"/media/fotos/{m[4]}"
                elif tipo_msg == "audio":
                    ultima_msg = "[áudio]"
                else:
                    ultima_msg = f"[{tipo_msg}]"
            else:
                ultima_msg = "Encontro registrado"
                ultima_msg_em = created_at

            itens.append({
                "encontroId": encontro_id,
                "voluntarioId": voluntario_id,
                "nome": nome_vol,
                "telefone": tel_vol,
                "fotoUrl": foto_url,
                "ultimaMsg": ultima_msg,
                "ultimaMsgEm": ultima_msg_em.strftime("%Y-%m-%d %H:%M:%S") if ultima_msg_em else None,
                "status": status_encontro,
                "tipo_vulneravel": tipo_vulneravel,
                "login_vinculo": lv_db,
            })

        return {"ok": True, "login_vinculo": lv, "itens": itens}
    except Exception as e:
        _log_exc("Erro em /historico/voluntarios", e)
        raise HTTPException(500, "Falha ao listar voluntários do histórico.")
    finally:
        cur.close()
        cnx.close()


@app.get("/historico/mensagens", tags=["historico"])
def historico_mensagens(
    encontro_id: int = Query(..., ge=1),
    limit: int = Query(500, ge=1, le=2000),
):
    cnx, cur = _open_cursor()
    try:
        cur.execute("""
            SELECT id, tipo, status, pendente_para, remetente_tipo,
                   conteudo_texto, arquivo_foto, arquivo_audio,
                   telefone_origem, nome_origem, created_at
            FROM mensagens
            WHERE encontro_id=%s
            ORDER BY id ASC
            LIMIT %s
        """, (int(encontro_id), limit))

        rows = cur.fetchall() or []
        items = []
        for r in rows:
            tipo = (r[1] or "texto").strip().lower()
            arquivo_url = None
            if tipo == "foto" and r[6]:
                arquivo_url = f"/media/fotos/{r[6]}"
            elif tipo == "audio" and r[7]:
                arquivo_url = f"/media/audios/{r[7]}"

            items.append({
                "id": int(r[0]),
                "tipo": tipo,
                "status": r[2] or None,
                "pendente_para": r[3] or None,
                "remetente_tipo": r[4] or None,
                "texto": r[5],
                "arquivo_url": arquivo_url,
                "telefone_origem": r[8],
                "nome_origem": r[9] or "Voluntário",
                "em": r[10].strftime("%Y-%m-%d %H:%M:%S") if r[10] else None,
            })

        return {"ok": True, "encontro_id": int(encontro_id), "items": items}
    except Exception as e:
        _log_exc("Erro em /historico/mensagens", e)
        raise HTTPException(500, "Falha ao listar mensagens do histórico.")
    finally:
        cur.close()
        cnx.close()


# =========================
# STATUS CHAT
# =========================
def _set_status(novo_status: str):
    cnx, cur = _open_cursor()
    try:
        cur.execute("UPDATE chat_status SET status=%s WHERE id=1", (novo_status,))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO chat_status (id, status) VALUES (1, %s)", (novo_status,))
        cnx.commit()
    finally:
        cur.close()
        cnx.close()


def _get_status() -> str:
    cnx, cur = _open_cursor()
    try:
        cur.execute("SELECT status FROM chat_status WHERE id=1")
        row = cur.fetchone()
        return row[0] if row else "ativo"
    finally:
        cur.close()
        cnx.close()


@app.get("/status_chat", tags=["default"])
def status_chat():
    return {"status": _get_status()}


@app.post("/status_chat", tags=["default"])
def alterar_status_chat(acao: str = Form(...)):
    acao_lc = acao.strip().lower()
    if acao_lc in {"encerrar", "encerrado"}:
        _set_status("encerrado")
    elif acao_lc in {"abrir", "ativo", "reabrir"}:
        _set_status("ativo")
    else:
        raise HTTPException(400, "Ação inválida. Use 'encerrar' ou 'abrir'.")
    return {"status": _get_status()}


# =========================
# WEBHOOK META / WHATSAPP
# =========================

def _resolve_encontro_ativo_do_responsavel(cur, wa_from: str):
    """
    Resolve o encontro correto para uma resposta vinda do WhatsApp.

    REGRA:
    - Usa o telefone apenas para localizar o responsável
    - Depois pega o encontro MAIS RECENTE e APTO A RESPOSTA:
        * status = 'pendente'
        * onboarding_whatsapp_enviado = 1
    - Retorna também pulseira/login_vinculo/codigo_qr para amarrar a conversa
    """

    wa_digits = _only_digits(wa_from or "")
    if not wa_digits:
        # LOG 1:
        _dbg("WHATSAPP/RESOLVE_ENCONTRO/WA_VAZIO", {
            "wa_from": wa_from
        })
        return None

    wa_com_ddi = _to_wa_number(wa_digits)

    candidatos = {wa_digits, wa_com_ddi}

    # se vier com 55, também tenta sem 55
    if wa_digits.startswith("55") and len(wa_digits) >= 12:
        candidatos.add(wa_digits[2:])

    if wa_com_ddi.startswith("55") and len(wa_com_ddi) >= 12:
        candidatos.add(wa_com_ddi[2:])

    candidatos = [c for c in candidatos if c]

    # LOG 2:
    _dbg("WHATSAPP/RESOLVE_ENCONTRO/CANDIDATOS", {
        "wa_from_original": wa_from,
        "wa_digits": wa_digits,
        "wa_com_ddi": wa_com_ddi,
        "candidatos": candidatos,
    })

    if not candidatos:
        return None

    # =========================
    # 1) LOCALIZAR RESPONSÁVEL
    # =========================
    placeholders = ", ".join(["%s"] * len(candidatos))

    sql_resp = f"""
        SELECT
            id,
            telefone,
            COALESCE(whatsapp, telefone) AS whatsapp_efetivo
        FROM responsaveis
        WHERE COALESCE(whatsapp, telefone) IN ({placeholders})
           OR telefone IN ({placeholders})
        ORDER BY id DESC
        LIMIT 1
    """
    cur.execute(sql_resp, tuple(candidatos + candidatos))
    resp = cur.fetchone()

    if not resp:
        # LOG 3:
        _dbg("WHATSAPP/RESOLVE_ENCONTRO/RESPONSAVEL_NAO_ENCONTRADO", {
            "candidatos": candidatos
        })
        return None

    responsavel_id = int(resp[0])
    telefone_legacy = resp[1]
    whatsapp_efetivo = resp[2]

    # LOG 4:
    _dbg("WHATSAPP/RESOLVE_ENCONTRO/RESPONSAVEL_OK", {
        "responsavel_id": responsavel_id,
        "telefone_legacy": telefone_legacy,
        "whatsapp_efetivo": whatsapp_efetivo,
    })

    # =========================
    # 2) PEGAR O ENCONTRO ATIVO MAIS RECENTE
    # =========================
    cur.execute("""
        SELECT
            e.id,
            p.login_vinculo,
            p.codigo_qr,
            p.id AS pulseira_id,
            e.status,
            e.onboarding_whatsapp_enviado
        FROM encontros e
        LEFT JOIN pulseiras_qr p ON p.id = e.pulseira_id
        WHERE e.responsavel_id = %s
          AND e.status = 'pendente'
          AND e.onboarding_whatsapp_enviado = 1
        ORDER BY e.id DESC
        LIMIT 1
    """, (responsavel_id,))
    row = cur.fetchone()

    if not row:
        # LOG 5:
        _dbg("WHATSAPP/RESOLVE_ENCONTRO/ENCONTRO_ATIVO_NAO_ENCONTRADO", {
            "responsavel_id": responsavel_id
        })
        return None

    payload = {
        "encontro_id": int(row[0]),
        "login_vinculo": row[1],
        "codigo_qr": row[2],
        "pulseira_id": int(row[3]) if row[3] is not None else None,
        "telefone_legacy": telefone_legacy,
        "responsavel_id": responsavel_id,
        "status": row[4],
        "onboarding_whatsapp_enviado": int(row[5] or 0),
    }

    # LOG 6:
    _dbg("WHATSAPP/RESOLVE_ENCONTRO/ENCONTRO_OK", payload)
    return payload


# =========================
# HELPERS DE MÍDIA VINDOS DA META
# =========================

def _wa_save_incoming_image_from_meta(media_id: str, original_mime_type: Optional[str] = None) -> str:
    """
    Baixa uma imagem recebida pelo WhatsApp e salva em FOTOS_DIR.
    Retorna apenas o filename salvo.
    """
    meta = _wa_get_media_url(media_id)
    media_url = meta.get("url")
    if not media_url:
        raise HTTPException(500, "Meta não retornou URL da imagem.")

    img_bytes = _wa_download_media_bytes(media_url)

    mime = (original_mime_type or meta.get("mime_type") or "").lower()
    ext = ".jpg"

    if "png" in mime:
        ext = ".png"
    elif "webp" in mime:
        ext = ".webp"
    elif "jpeg" in mime or "jpg" in mime:
        ext = ".jpg"

    filename = _unique_photo_name(f"meta_image{ext}")
    path = os.path.join(FOTOS_DIR, filename)

    with open(path, "wb") as f:
        f.write(img_bytes)

    return filename


@app.get("/webhook/meta_whatsapp", response_class=PlainTextResponse, tags=["whatsapp"])
def verificar_webhook_meta_whatsapp(
    hub_mode: Optional[str] = Query(None, alias="hub.mode"),
    hub_verify_token: Optional[str] = Query(None, alias="hub.verify_token"),
    hub_challenge: Optional[str] = Query(None, alias="hub.challenge"),
):
    # LOG 7:
    _dbg("WHATSAPP/WEBHOOK_VERIFY_IN", {
        "hub.mode": hub_mode,
        "hub.verify_token": hub_verify_token,
        "hub.challenge": hub_challenge,
        "verify_token_esperado": WHATSAPP_VERIFY_TOKEN,
    })

    if hub_mode == "subscribe" and hub_verify_token == WHATSAPP_VERIFY_TOKEN and hub_challenge:
        # LOG 8:
        _dbg("WHATSAPP/WEBHOOK_VERIFY_OK", {
            "hub.challenge": hub_challenge
        })
        return hub_challenge

    # LOG 9:
    _dbg("WHATSAPP/WEBHOOK_VERIFY_FAIL", {
        "hub.mode": hub_mode,
        "hub.verify_token": hub_verify_token,
        "verify_token_esperado": WHATSAPP_VERIFY_TOKEN,
    })
    raise HTTPException(status_code=403, detail="token inválido")


@app.post("/webhook/meta_whatsapp", tags=["whatsapp"])
async def receber_webhook_meta_whatsapp(request: Request):
    """
    Fluxo:
    - Recebe mensagens vindas do WhatsApp do responsável
    - Usa o telefone apenas para identificar o responsável
    - Resolve o encontro ativo mais recente com onboarding já enviado
    - Se for texto: grava como mensagem pendente para o voluntário
    - Se for áudio: baixa da Meta, salva no servidor e grava como pendente
    - Se for imagem: baixa da Meta, salva em FOTOS_DIR e grava como pendente
    - Se for localização: grava em localizacoes + cria mensagem texto com link do Maps
    - Dispara _notify_poll(...) para o chat receber
    """

    raw_body = ""
    body = {}

    try:
        raw_bytes = await request.body()
        raw_body = (raw_bytes or b"").decode("utf-8", errors="ignore")
    except Exception as e:
        _log_exc("Erro ao ler body bruto do webhook do WhatsApp", e)

    # LOG 10:
    _dbg("WHATSAPP/WEBHOOK_POST_RAW", {
        "content_type": request.headers.get("content-type"),
        "body_raw": raw_body[:5000] if raw_body else ""
    })

    try:
        body = json.loads(raw_body) if raw_body else {}
    except Exception as e:
        _log_exc("Erro ao fazer parse JSON do webhook do WhatsApp", e)
        body = {}

    # LOG 11:
    _dbg("WHATSAPP/WEBHOOK_POST_IN", body)

    try:
        entries = body.get("entry", []) or []

        # LOG 12:
        _dbg("WHATSAPP/WEBHOOK_ENTRIES", {
            "entries_count": len(entries)
        })

        for entry_idx, entry in enumerate(entries):
            changes = entry.get("changes", []) or []

            # LOG 13:
            _dbg("WHATSAPP/WEBHOOK_ENTRY", {
                "entry_idx": entry_idx,
                "changes_count": len(changes),
                "entry_keys": list((entry or {}).keys()),
            })

            for change_idx, change in enumerate(changes):
                field = (change.get("field") or "").strip()
                value = change.get("value", {}) or {}

                contacts = value.get("contacts", []) or []
                messages = value.get("messages", []) or []
                statuses = value.get("statuses", []) or []

                # LOG 14:
                _dbg("WHATSAPP/WEBHOOK_CHANGE", {
                    "entry_idx": entry_idx,
                    "change_idx": change_idx,
                    "field": field,
                    "contacts_count": len(contacts),
                    "messages_count": len(messages),
                    "statuses_count": len(statuses),
                    "value_keys": list((value or {}).keys()),
                })

                # ignora eventos sem mensagem (ex.: status de entrega/leitura)
                if not messages:
                    # LOG 15:
                    _dbg("WHATSAPP/WEBHOOK_SEM_MESSAGES", {
                        "entry_idx": entry_idx,
                        "change_idx": change_idx,
                        "field": field,
                        "statuses": statuses,
                        "value": value,
                    })
                    continue

                wa_from_name = None
                if contacts:
                    wa_from_name = (((contacts[0] or {}).get("profile") or {}).get("name"))

                for msg_idx, msg in enumerate(messages):
                    wa_from = _only_digits(msg.get("from") or "")
                    msg_type = (msg.get("type") or "").strip().lower()
                    msg_id = (msg.get("id") or "").strip()
                    msg_ts = (msg.get("timestamp") or "").strip()

                    texto = None
                    audio_id = None
                    audio_mime_type = None

                    image_id = None
                    image_mime_type = None
                    image_caption = None

                    loc_lat = None
                    loc_lng = None
                    loc_name = None
                    loc_address = None

                    # LOG 16:
                    _dbg("WHATSAPP/WEBHOOK_MSG_IN", {
                        "entry_idx": entry_idx,
                        "change_idx": change_idx,
                        "msg_idx": msg_idx,
                        "wa_from": wa_from,
                        "wa_from_name": wa_from_name,
                        "msg_type": msg_type,
                        "msg_id": msg_id,
                        "msg_ts": msg_ts,
                        "msg_keys": list((msg or {}).keys()),
                    })

                    # =========================
                    # TEXTO
                    # =========================
                    if msg_type == "text":
                        texto = (((msg.get("text") or {}).get("body")) or "").strip()

                        # LOG 17:
                        _dbg("WHATSAPP/WEBHOOK_TEXTO_RECEBIDO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                            "texto": texto,
                        })

                    # =========================
                    # ÁUDIO
                    # =========================
                    elif msg_type == "audio":
                        audio_obj = msg.get("audio") or {}
                        audio_id = (audio_obj.get("id") or "").strip()
                        audio_mime_type = (audio_obj.get("mime_type") or "").strip()

                        # LOG 18:
                        _dbg("WHATSAPP/WEBHOOK_AUDIO_RECEBIDO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                            "audio_id": audio_id,
                            "audio_mime_type": audio_mime_type,
                        })

                    # =========================
                    # IMAGEM / FOTO
                    # =========================
                    elif msg_type == "image":
                        image_obj = msg.get("image") or {}
                        image_id = (image_obj.get("id") or "").strip()
                        image_mime_type = (image_obj.get("mime_type") or "").strip()
                        image_caption = (image_obj.get("caption") or "").strip()

                        # LOG 19:
                        _dbg("WHATSAPP/WEBHOOK_IMAGE_RECEBIDA", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                            "image_id": image_id,
                            "image_mime_type": image_mime_type,
                            "image_caption": image_caption,
                        })

                    # =========================
                    # LOCALIZAÇÃO
                    # =========================
                    elif msg_type == "location":
                        loc_obj = msg.get("location") or {}
                        loc_lat = loc_obj.get("latitude")
                        loc_lng = loc_obj.get("longitude")
                        loc_name = (loc_obj.get("name") or "").strip()
                        loc_address = (loc_obj.get("address") or "").strip()

                        # LOG 20:
                        _dbg("WHATSAPP/WEBHOOK_LOCATION_RECEBIDA", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                            "latitude": loc_lat,
                            "longitude": loc_lng,
                            "name": loc_name,
                            "address": loc_address,
                        })

                    # ignora outros tipos
                    else:
                        # LOG 21:
                        _dbg("WHATSAPP/WEBHOOK_TIPO_IGNORADO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_type": msg_type,
                            "msg_id": msg_id,
                        })
                        continue

                    if msg_type == "text" and not texto:
                        # LOG 22:
                        _dbg("WHATSAPP/WEBHOOK_TEXTO_VAZIO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                        })
                        continue

                    if msg_type == "audio" and not audio_id:
                        # LOG 23:
                        _dbg("WHATSAPP/WEBHOOK_AUDIO_SEM_ID", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                        })
                        continue

                    if msg_type == "image" and not image_id:
                        # LOG 24:
                        _dbg("WHATSAPP/WEBHOOK_IMAGE_SEM_ID", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                        })
                        continue

                    if msg_type == "location" and (loc_lat is None or loc_lng is None):
                        # LOG 25:
                        _dbg("WHATSAPP/WEBHOOK_LOCATION_INVALIDA", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_id": msg_id,
                            "latitude": loc_lat,
                            "longitude": loc_lng,
                        })
                        continue

                    cnx, cur = _open_cursor()
                    try:
                        # LOG 26:
                        _dbg("WHATSAPP/WEBHOOK_DB_CURSOR_OK", {
                            "wa_from": wa_from,
                            "msg_type": msg_type,
                            "msg_id": msg_id,
                        })

                        # =========================
                        # RESOLVER ENCONTRO ATIVO DO RESPONSÁVEL
                        # =========================
                        encontro_info = _resolve_encontro_ativo_do_responsavel(cur, wa_from)

                        if not encontro_info:
                            # LOG 27:
                            _dbg("WHATSAPP/WEBHOOK_SEM_ENCONTRO", {
                                "wa_from": wa_from,
                                "wa_from_name": wa_from_name,
                                "msg_type": msg_type,
                                "msg_id": msg_id,
                            })
                            continue

                        encontro_id = int(encontro_info["encontro_id"])
                        login_vinculo = encontro_info["login_vinculo"]
                        codigo_qr = encontro_info["codigo_qr"]
                        pulseira_id = encontro_info["pulseira_id"]
                        telefone_legacy = encontro_info["telefone_legacy"]

                        # LOG 28:
                        _dbg("WHATSAPP/WEBHOOK_ENCONTRO_RESOLVIDO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_type": msg_type,
                            "msg_id": msg_id,
                            "encontro_id": encontro_id,
                            "login_vinculo": login_vinculo,
                            "codigo_qr": codigo_qr,
                            "pulseira_id": pulseira_id,
                            "telefone_legacy": telefone_legacy,
                        })

                        # =========================
                        # CASO 1: TEXTO
                        # =========================
                        if msg_type == "text":
                            cur.execute("""
                                INSERT INTO mensagens
                                  (encontro_id, tipo, conteudo_texto, telefone_origem, nome_origem,
                                   telefone_alvo, status, pendente_para, remetente_tipo)
                                VALUES
                                  (%s, 'texto', %s, %s, %s, %s, 'pendente', 'voluntario', 'whatsapp')
                            """, (
                                encontro_id,
                                texto,
                                _only_digits(wa_from),
                                wa_from_name or "Responsável",
                                telefone_legacy
                            ))
                            nova_msg_id = cur.lastrowid
                            cnx.commit()

                            # LOG 29:
                            _dbg("WHATSAPP/WEBHOOK_TEXTO_SALVO", {
                                "mensagem_id": nova_msg_id,
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "codigo_qr": codigo_qr,
                                "texto": texto,
                            })

                            _notify_poll("voluntario", encontro_id, login_vinculo)

                            # LOG 30:
                            _dbg("WHATSAPP/WEBHOOK_NOTIFY_POLL_OK", {
                                "destino": "voluntario",
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "msg_type": "text",
                                "msg_id": msg_id,
                            })
                            continue

                        # =========================
                        # CASO 2: ÁUDIO
                        # =========================
                        if msg_type == "audio":
                            # LOG 31:
                            _dbg("WHATSAPP/WEBHOOK_AUDIO_BAIXANDO_META", {
                                "audio_id": audio_id,
                                "audio_mime_type": audio_mime_type,
                                "encontro_id": encontro_id,
                            })

                            filename = _wa_save_incoming_audio_from_meta(
                                media_id=audio_id,
                                original_mime_type=audio_mime_type
                            )

                            # LOG 32:
                            _dbg("WHATSAPP/WEBHOOK_AUDIO_BAIXADO_META", {
                                "arquivo_audio": filename,
                                "audio_id": audio_id,
                                "encontro_id": encontro_id,
                            })

                            cur.execute("""
                                INSERT INTO mensagens
                                  (encontro_id, tipo, arquivo_audio, telefone_origem, nome_origem,
                                   telefone_alvo, status, pendente_para, remetente_tipo)
                                VALUES
                                  (%s, 'audio', %s, %s, %s, %s, 'pendente', 'voluntario', 'whatsapp')
                            """, (
                                encontro_id,
                                filename,
                                _only_digits(wa_from),
                                wa_from_name or "Responsável",
                                telefone_legacy
                            ))
                            nova_msg_id = cur.lastrowid
                            cnx.commit()

                            # LOG 33:
                            _dbg("WHATSAPP/WEBHOOK_AUDIO_SALVO", {
                                "mensagem_id": nova_msg_id,
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "codigo_qr": codigo_qr,
                                "arquivo_audio": filename,
                            })

                            _notify_poll("voluntario", encontro_id, login_vinculo)

                            # LOG 34:
                            _dbg("WHATSAPP/WEBHOOK_NOTIFY_POLL_OK", {
                                "destino": "voluntario",
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "msg_type": "audio",
                                "msg_id": msg_id,
                            })
                            continue

                        # =========================
                        # CASO 3: FOTO / IMAGEM
                        # =========================
                        if msg_type == "image":
                            # LOG 35:
                            _dbg("WHATSAPP/WEBHOOK_IMAGE_BAIXANDO_META", {
                                "image_id": image_id,
                                "image_mime_type": image_mime_type,
                                "encontro_id": encontro_id,
                            })

                            filename = _wa_save_incoming_image_from_meta(
                                media_id=image_id,
                                original_mime_type=image_mime_type
                            )

                            # LOG 36:
                            _dbg("WHATSAPP/WEBHOOK_IMAGE_BAIXADA_META", {
                                "arquivo_foto": filename,
                                "image_id": image_id,
                                "encontro_id": encontro_id,
                            })

                            cur.execute("""
                                INSERT INTO mensagens
                                  (encontro_id, tipo, arquivo_foto, conteudo_texto, telefone_origem, nome_origem,
                                   telefone_alvo, status, pendente_para, remetente_tipo)
                                VALUES
                                  (%s, 'foto', %s, %s, %s, %s, %s, 'pendente', 'voluntario', 'whatsapp')
                            """, (
                                encontro_id,
                                filename,
                                image_caption or None,
                                _only_digits(wa_from),
                                wa_from_name or "Responsável",
                                telefone_legacy
                            ))
                            nova_msg_id = cur.lastrowid
                            cnx.commit()

                            # LOG 37:
                            _dbg("WHATSAPP/WEBHOOK_IMAGE_SALVA", {
                                "mensagem_id": nova_msg_id,
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "codigo_qr": codigo_qr,
                                "arquivo_foto": filename,
                                "image_caption": image_caption,
                            })

                            _notify_poll("voluntario", encontro_id, login_vinculo)

                            # LOG 38:
                            _dbg("WHATSAPP/WEBHOOK_NOTIFY_POLL_OK", {
                                "destino": "voluntario",
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "msg_type": "image",
                                "msg_id": msg_id,
                            })
                            continue

                        # =========================
                        # CASO 4: LOCALIZAÇÃO
                        # =========================
                        if msg_type == "location":
                            lat = float(loc_lat)
                            lng = float(loc_lng)

                            # 1) grava em localizacoes
                            cur.execute("""
                                INSERT INTO localizacoes
                                  (encontro_id, voluntario_id, voluntario_nome, voluntario_telefone,
                                   latitude, longitude, accuracy, ts_client)
                                VALUES
                                  (%s, NULL, %s, %s, %s, %s, NULL, NULL)
                            """, (
                                encontro_id,
                                wa_from_name or "Responsável",
                                _only_digits(wa_from),
                                lat,
                                lng,
                            ))

                            maps_link = f"https://maps.google.com/?q={lat},{lng}"
                            texto_loc = f"📍 Localização enviada pelo responsável:\n{maps_link}"

                            if loc_name:
                                texto_loc += f"\nNome: {loc_name}"
                            if loc_address:
                                texto_loc += f"\nEndereço: {loc_address}"

                            # 2) cria mensagem de texto para garantir exibição no chat
                            cur.execute("""
                                INSERT INTO mensagens
                                  (encontro_id, tipo, conteudo_texto, telefone_origem, nome_origem,
                                   telefone_alvo, status, pendente_para, remetente_tipo)
                                VALUES
                                  (%s, 'texto', %s, %s, %s, %s, 'pendente', 'voluntario', 'whatsapp')
                            """, (
                                encontro_id,
                                texto_loc,
                                _only_digits(wa_from),
                                wa_from_name or "Responsável",
                                telefone_legacy
                            ))
                            nova_msg_id = cur.lastrowid
                            cnx.commit()

                            # LOG 39:
                            _dbg("WHATSAPP/WEBHOOK_LOCATION_SALVA", {
                                "mensagem_id": nova_msg_id,
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "codigo_qr": codigo_qr,
                                "latitude": lat,
                                "longitude": lng,
                                "maps_link": maps_link,
                                "name": loc_name,
                                "address": loc_address,
                            })

                            _notify_poll("voluntario", encontro_id, login_vinculo)

                            # LOG 40:
                            _dbg("WHATSAPP/WEBHOOK_NOTIFY_POLL_OK", {
                                "destino": "voluntario",
                                "encontro_id": encontro_id,
                                "login_vinculo": login_vinculo,
                                "msg_type": "location",
                                "msg_id": msg_id,
                            })
                            continue

                    except Exception as e:
                        cnx.rollback()
                        _log_exc("Erro ao processar webhook do WhatsApp", e)

                        # LOG 41:
                        _dbg("WHATSAPP/WEBHOOK_PROCESSAMENTO_ERRO_CONTEXTO", {
                            "wa_from": wa_from,
                            "wa_from_name": wa_from_name,
                            "msg_type": msg_type,
                            "msg_id": msg_id,
                        })
                    finally:
                        try:
                            cur.close()
                        except Exception:
                            pass
                        try:
                            cnx.close()
                        except Exception:
                            pass

        # LOG 42:
        _dbg("WHATSAPP/WEBHOOK_POST_OUT", {"ok": True})
        return {"ok": True}

    except Exception as e:
        _log_exc("Erro em /webhook/meta_whatsapp", e)

        # LOG 43:
        _dbg("WHATSAPP/WEBHOOK_POST_OUT", {
            "ok": False,
            "error": repr(e)
        })
        return {"ok": False, "error": repr(e)}

# =========================
# TESTE WHATSAPP
# =========================

@app.get("/teste_whatsapp_completo")
def teste_whatsapp_completo():
    cnx, cur = _open_cursor()
    try:
        resp = _maybe_send_onboarding_to_whatsapp(cur, 8)
        cnx.commit()
        return resp
    finally:
        cur.close()
        cnx.close()


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("API_chat:app", host="0.0.0.0", port=8000, reload=True)
