# ================================================================
# database.py  —  Configuración de Base de Datos
# Liga Municipal de Basquetbol de Nochixtlán
#
# Configuración en .streamlit/secrets.toml:
#   DATABASE_URL   = "postgresql://postgres.[ref]:[pass]@
#                    aws-0-us-east-1.pooler.supabase.com:5432/postgres"
#   password_admin = "tu_contraseña_admin"
#
# Supabase usa Transaction Mode (puerto 5432 o 6543).
# Nunca hardcodear la URL — siempre leer de st.secrets.
# ================================================================

import streamlit as st
from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker
from models import Base


# ── 1. Resolver URL desde secrets ────────────────────────────────────────────
DATABASE_URL: str = st.secrets.get(
    "DATABASE_URL", "sqlite:///liga_nochixtlan.db"
)

# Neon / Heroku exportan "postgres://" — SQLAlchemy 2.x requiere "postgresql://"
if DATABASE_URL.startswith("postgres://"):
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

_IS_SQLITE = "sqlite" in DATABASE_URL


# ── 2. Engine por motor ───────────────────────────────────────────────────────
if _IS_SQLITE:
    engine = create_engine(
        DATABASE_URL,
        connect_args={"check_same_thread": False},
        echo=False,
    )
else:
    # Supabase / PostgreSQL
    # pool_pre_ping  → verifica la conexión antes de usarla (elimina cold-start)
    # pool_recycle   → rota conexiones cada 30 min (evita timeouts silenciosos)
    # keepalives     → previene desconexiones inesperadas del lado del servidor
    engine = create_engine(
        DATABASE_URL,
        pool_pre_ping=True,
        pool_recycle=1800,
        pool_size=5,
        max_overflow=10,
        pool_timeout=30,
        connect_args={
            "keepalives":          1,
            "keepalives_idle":     30,
            "keepalives_interval": 10,
            "keepalives_count":    5,
        },
        echo=False,
    )


# ── 3. PRAGMAs para SQLite local ─────────────────────────────────────────────
if _IS_SQLITE:
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=ON")
        cursor.execute("PRAGMA journal_mode=WAL")
        cursor.close()


# ── 4. Session factory ────────────────────────────────────────────────────────
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# ── 5. init_db — tablas + migraciones incrementales ──────────────────────────
def init_db() -> None:
    # Crea todas las tablas si no existen (idempotente).
    # Los try/except permiten aplicar migraciones en BDs existentes.
    try:
        Base.metadata.create_all(bind=engine)
    except Exception:
        pass

    with engine.connect() as conn:
        # v3 — columna is_test
        try:
            conn.execute(text(
                "ALTER TABLE seasons ADD COLUMN is_test BOOLEAN NOT NULL DEFAULT 0"
            ))
        except Exception:
            pass

        # v4 — columnas de liguilla
        for col_sql in [
            "ALTER TABLE matches ADD COLUMN phase VARCHAR(20) NOT NULL DEFAULT 'Fase Regular'",
            "ALTER TABLE matches ADD COLUMN playoff_round VARCHAR(20)",
            "ALTER TABLE matches ADD COLUMN game_number INTEGER NOT NULL DEFAULT 1",
        ]:
            try:
                conn.execute(text(col_sql))
            except Exception:
                pass

        # Índice único que incluye game_number
        try:
            conn.execute(text("DROP INDEX IF EXISTS uq_match_per_jornada"))
        except Exception:
            pass
        try:
            conn.execute(text(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS uq_match_per_jornada
                ON matches (season_id, jornada, home_team_id, away_team_id, game_number)
                """
            ))
        except Exception:
            pass

        try:
            conn.commit()
        except Exception:
            pass


# ── 6. get_db — context manager de sesión ────────────────────────────────────
def get_db():
    # Uso:  with get_db() as db:  ...
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
