# ================================================================
# app_diseno.py  —  Aplicación v5 "Supabase Ready"
# Liga Municipal de Basquetbol de Nochixtlán, Oaxaca
# Stack: Python · Streamlit · Supabase/PostgreSQL · SQLAlchemy
#
# Arquitectura de rendimiento:
#   1. _ensure_data_loaded()  — carga única en session_state
#   2. Batch query de líderes — 1 query por temporada (no N+1)
#   3. Páginas públicas       — leen de RAM (session_state)
#   4. Páginas admin          — consultan BD directo (siempre frescos)
#   5. Mutaciones admin       — llaman _invalidate_data() + st.cache_data.clear()
#
# Cambios v5:
#   - Sin mock data: DB vacía muestra mensajes limpios, no datos falsos
#   - _show_connection_error() — banner profesional si Supabase no responde
#   - get_db() con rollback automático en excepciones
#   - _ensure_data_loaded() con try/except — app nunca crashea al arrancar
#   - try/except en TODAS las operaciones de escritura admin
#   - Bug fix: season_selector() tenía return inalcanzable
#   - _invalidate_data() + cache_data.clear() tras cada mutación exitosa
#
# Navegación: Posiciones | Líderes | Calendario | Equipos | Admin
# Uso: streamlit run app_diseno.py
# ================================================================

import hashlib
from collections import defaultdict
from contextlib import contextmanager
from datetime import datetime, date, timedelta

import pandas as pd
import streamlit as st
from sqlalchemy import func, text as sa_text
from sqlalchemy.orm import Session
from streamlit_option_menu import option_menu

from database_test import SessionLocal, init_db
from models import (
    Base, Season, Team, Player, Match,
    PlayerMatchStat, TransferHistory,
)


# ── Constantes globales ───────────────────────────────────────────────────────
ADMIN_PASSWORD = st.secrets.get("password_admin", "admin123")
ADMIN_HASH = hashlib.sha256(ADMIN_PASSWORD.encode()).hexdigest()

CATEGORIES = ["Libre", "Veteranos", "Femenil", "3ra"]
VENUES = ["Cancha Bicentenario", "Cancha Techada", "Cancha III"]

MAX_ROSTER = 12
MAX_PERMISSIONS = 3
MAX_DEFAULTS_BAJA = 4

CATEGORY_WEEKDAY = {
    "Libre":     3,   # Jueves
    "Veteranos": 4,   # Viernes
    "Femenil":   4,   # Viernes
    "3ra":       5,   # Sábado
}
WEEKDAY_NAMES = {
    0: "Lunes", 1: "Martes", 2: "Miércoles",
    3: "Jueves", 4: "Viernes", 5: "Sábado", 6: "Domingo",
}

ROUND_ORDER = {"Cuartos": 1, "Semifinal": 2, "Final": 3}
ROUND_LABELS = {
    "Cuartos":   "⚔️ Cuartos de Final",
    "Semifinal": "🥊 Semifinales",
    "Final":     "🥇 Gran Final",
}


# ================================================================
# CSS — Tema Premium Deportivo + Mobile-First
# Colores: fondo #0d0d14 · naranja #F26B0F · dorado #FFD700
# ================================================================
PREMIUM_CSS = """
<style>

/* ── Reset base y fondo oscuro ── */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stMain"],
.main, .block-container {
    background-color: #0d0d14 !important;
    color: #F0F0F0 !important;
}

/* ── Ocultar sidebar completamente ── */
[data-testid="stSidebar"],
[data-testid="collapsedControl"],
[data-testid="stSidebarNavItems"] {
    display: none !important;
}

/* ── Contenedor principal ── */
.block-container {
    padding-top: 0.4rem !important;
    padding-bottom: 2rem !important;
    max-width: 1200px;
}

/* ── Navbar wrapper ── */
.nav-wrapper {
    background: linear-gradient(135deg, #0f0f1a 0%, #16162a 100%);
    border-bottom: 2px solid #F26B0F;
    padding: 0.5rem 1rem 0 1rem;
    margin-bottom: 1.5rem;
    border-radius: 0 0 14px 14px;
}

/* ── Logo del club ── */
.club-logo {
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding-bottom: 0.6rem;
}
.club-logo .ball { font-size: 1.7rem; }
.club-logo .info strong {
    display: block;
    font-size: 0.95rem;
    color: #F26B0F;
    font-weight: 900;
    text-transform: uppercase;
    letter-spacing: 0.04em;
    line-height: 1.1;
}
.club-logo .info small {
    font-size: 0.65rem;
    color: #888;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

/* ── Métricas — tarjetas doradas ── */
[data-testid="stMetric"] {
    background: linear-gradient(145deg, #12121e, #1c1c2e) !important;
    border: 1px solid #252535 !important;
    border-radius: 12px !important;
    padding: 0.9rem 1rem !important;
    transition: border-color 0.2s, transform 0.15s;
}
[data-testid="stMetric"]:hover {
    border-color: #F26B0F !important;
    transform: translateY(-2px);
}
[data-testid="stMetricLabel"] p {
    color: #999 !important;
    font-size: 0.73rem !important;
    text-transform: uppercase;
    letter-spacing: 0.07em;
}
[data-testid="stMetricValue"] {
    color: #FFD700 !important;
    font-size: 1.7rem !important;
    font-weight: 900 !important;
}

/* ── Dataframes / Tables — texto blanco blindado en dark mode ── */
[data-testid="stDataFrame"],
[data-testid="stTable"] {
    border: 1px solid #252535 !important;
    border-radius: 10px !important;
    overflow: hidden;
    background-color: #12121e !important;
}

/* Celdas td (st.table) — blanco puro */
[data-testid="stTable"] td,
[data-testid="stTable"] td * {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
    background-color: #12121e !important;
}

/* Encabezados th (st.table) — naranja */
[data-testid="stTable"] th,
[data-testid="stTable"] th * {
    color: #F26B0F !important;
    -webkit-text-fill-color: #F26B0F !important;
    font-weight: 700 !important;
    background-color: #0f0f1a !important;
    border-bottom: 1px solid #F26B0F !important;
}

/* st.dataframe: roles del canvas DOM */
[data-testid="stDataFrame"] [role="gridcell"],
[data-testid="stDataFrame"] [role="gridcell"] * {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
}
[data-testid="stDataFrame"] [role="columnheader"],
[data-testid="stDataFrame"] [role="columnheader"] * {
    color: #F26B0F !important;
    -webkit-text-fill-color: #F26B0F !important;
    font-weight: 700 !important;
}

/* ── Títulos ── */
h1 {
    color: #F26B0F !important;
    font-weight: 900 !important;
    letter-spacing: -0.01em;
    border-bottom: 1px solid #252535;
    padding-bottom: 0.3rem;
    margin-bottom: 1rem !important;
}
h2 { color: #FFD700 !important; font-weight: 700 !important; }
h3 { color: #E8E8E8 !important; font-weight: 600 !important; }

/* ── Botones primarios ── */
[data-testid="stButton"] > button[kind="primary"] {
    background: linear-gradient(135deg, #F26B0F, #c95500) !important;
    border: none !important;
    color: #fff !important;
    font-weight: 700 !important;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    font-size: 0.8rem !important;
    border-radius: 8px !important;
    transition: box-shadow 0.2s, transform 0.1s;
    min-height: 44px !important;
}
[data-testid="stButton"] > button[kind="primary"]:hover {
    box-shadow: 0 0 20px rgba(242, 107, 15, 0.45) !important;
    transform: scale(1.02);
}

/* ── Botones secundarios ── */
[data-testid="stButton"] > button[kind="secondary"] {
    background: transparent !important;
    border: 1px solid #F26B0F !important;
    color: #F26B0F !important;
    border-radius: 8px !important;
    min-height: 44px !important;
}

/* ── Inputs y selectbox ── */
[data-testid="stSelectbox"] > div > div,
[data-testid="stTextInput"] > div > div > input,
[data-testid="stNumberInput"] > div > div > input,
[data-testid="stTextArea"] textarea {
    background: #12121e !important;
    border-color: #252535 !important;
    color: #F0F0F0 !important;
    border-radius: 8px !important;
}
[data-testid="stSelectbox"] > div > div:hover {
    border-color: #F26B0F !important;
}

/* ── Expanders ── */
[data-testid="stExpander"] {
    border: 1px solid #252535 !important;
    border-radius: 10px !important;
    background: #12121e !important;
}
[data-testid="stExpander"] summary:hover {
    color: #F26B0F !important;
}

/* ── Alerts ── */
[data-testid="stAlert"] { border-radius: 8px !important; }
[data-testid="stAlert"][data-baseweb="notification"] {
    background: #12121e !important;
}

/* ── Separadores ── */
hr { border-color: #252535 !important; margin: 1.2rem 0 !important; }

/* ── Caption y texto secundario ── */
.stCaption, [data-testid="stCaptionContainer"] {
    color: #777 !important;
    font-size: 0.78rem !important;
}

/* ── Tabs ── */
[data-testid="stTabs"] [data-baseweb="tab"] {
    background: transparent !important;
    color: #999 !important;
    font-size: 0.83rem !important;
}
[data-testid="stTabs"] [data-baseweb="tab"][aria-selected="true"] {
    color: #F26B0F !important;
    border-bottom: 2px solid #F26B0F !important;
    font-weight: 700 !important;
}
[data-testid="stTabs"] [data-baseweb="tab-border"] {
    background-color: #252535 !important;
}

/* ── Containers con borde ── */
[data-testid="stVerticalBlockBorderWrapper"] {
    border: 1px solid #252535 !important;
    border-radius: 12px !important;
    background: #12121e !important;
}

/* ── Progress bar ── */
[data-testid="stProgress"] > div > div {
    background-color: #F26B0F !important;
}

/* ── Radio buttons ── */
[data-testid="stRadio"] [data-baseweb="radio"] [type="radio"]:checked + div {
    background-color: #F26B0F !important;
    border-color: #F26B0F !important;
}

/* ── Checkbox ── */
[data-testid="stCheckbox"] [data-baseweb="checkbox"] [type="checkbox"]:checked + div {
    background-color: #F26B0F !important;
    border-color: #F26B0F !important;
}

/* ── Spinner ── */
[data-testid="stSpinner"] > div { border-top-color: #F26B0F !important; }

/* ── option-menu horizontal: sobreescribir colores ── */
.stHorizontalBlock nav ul { background: transparent !important; }

/* ── Mobile-first: pantallas <= 640px ── */
@media (max-width: 640px) {
    .block-container {
        padding-left: 0.4rem !important;
        padding-right: 0.4rem !important;
    }
    .nav-wrapper { padding: 0.3rem 0.4rem 0 0.4rem; border-radius: 0; }
    h1 { font-size: 1.4rem !important; }
    [data-testid="stMetricValue"] { font-size: 1.4rem !important; }
    [data-testid="stButton"] > button { min-height: 48px !important; }
}

/* ── Footer ── */
.app-footer {
    text-align: center;
    padding: 2rem 0 1rem;
    color: #3a3a4a;
    font-size: 0.7rem;
    border-top: 1px solid #1a1a2a;
    margin-top: 3rem;
    text-transform: uppercase;
    letter-spacing: 0.1em;
}

</style>
"""


# ================================================================
# UTILIDADES DE BASE DE DATOS
# ================================================================
def _show_connection_error(exc: Exception) -> None:
    """Banner profesional de error de conexión — nunca crashea la app."""
    st.error(
        "🔴 **No se pudo conectar con la base de datos (Supabase)**\n\n"
        "La aplicación no puede cargar datos en este momento. Esto puede deberse a:\n"
        "- Un problema de red o el servicio de Supabase está caído\n"
        "- La variable `DATABASE_URL` en `.streamlit/secrets.toml` es incorrecta\n"
        "- La sesión de conexión expiró\n\n"
        f"**Detalle técnico:** `{type(exc).__name__}: {exc}`\n\n"
        "Recarga la página para intentar de nuevo. Si el problema persiste, "
        "verifica la consola de Supabase en [supabase.com](https://supabase.com)."
    )


@contextmanager
def get_db():
    # Context manager de sesión SQLAlchemy con manejo de errores.
    db = SessionLocal()
    try:
        yield db
    except Exception:
        db.rollback()
        raise
    finally:
        db.close()


def active_season(db: Session, category: str):
    return (
        db.query(Season)
        .filter(Season.category == category, Season.is_active == True)
        .first()
    )


def active_teams(db: Session, season_id: int):
    return (
        db.query(Team)
        .filter(Team.season_id == season_id, Team.status == "Activo")
        .order_by(Team.name)
        .all()
    )


def roster_count(db: Session, team_id: int) -> int:
    return (
        db.query(func.count(Player.id))
        .filter(Player.team_id == team_id, Player.is_active == True)
        .scalar()
    ) or 0


def season_selector(db: Session, category: str, key_prefix: str = ""):
    # Muestra selector de temporada con la activa como default.
    seasons = (
        db.query(Season)
        .filter(Season.category == category)
        .order_by(Season.is_active.desc(), Season.created_at.desc())
        .all()
    )
    if not seasons:
        return None
    season_map = {
        f"{'🟢 ' if s.is_active else '📦 '}{s.name}{' [PRUEBA]' if s.is_test else ''}": s
        for s in seasons
    }
    if len(seasons) == 1:
        return seasons[0]
    labels = list(season_map.keys())
    default_idx = next((i for i, s in enumerate(seasons) if s.is_active), 0)
    with st.expander("📜 Cambiar temporada", expanded=False):
        sel_label = st.selectbox(
            "Temporada", labels, index=default_idx,
            key=f"{key_prefix}_season_hist",
            label_visibility="collapsed",
        )
    return season_map[sel_label]


# ================================================================
# UTILIDADES GENERALES
# ================================================================
def short_name(full_name: str) -> str:
    # 'Carlos Alberto García Martínez' → 'Carlos García'
    if not full_name:
        return "—"
    parts = full_name.strip().split()
    if len(parts) <= 2:
        return full_name.strip()
    return f"{parts[0]} {parts[2]}"


def next_weekday_date(from_date: date, weekday: int) -> date:
    days_ahead = weekday - from_date.weekday()
    if days_ahead < 0:
        days_ahead += 7
    return from_date + timedelta(days=days_ahead)


# ================================================================
# AUTENTICACIÓN
# ================================================================
def is_admin() -> bool:
    return st.session_state.get("authenticated", False)


def login_widget_top() -> None:
    # Widget de login compacto para la navbar superior.
    if is_admin():
        if st.button("🔓 Salir", key="logout_btn_top", use_container_width=True):
            st.session_state["authenticated"] = False
            st.session_state["page"] = "Posiciones"
            st.rerun()
    else:
        with st.expander("🔐", expanded=False):
            with st.form("login_form_nav"):
                pwd = st.text_input(
                    "pwd", type="password",
                    placeholder="Contraseña…",
                    label_visibility="collapsed",
                )
                if st.form_submit_button("Entrar", use_container_width=True):
                    if hashlib.sha256(pwd.encode()).hexdigest() == ADMIN_HASH:
                        st.session_state["authenticated"] = True
                        st.rerun()
                    else:
                        st.error("Contraseña incorrecta.")


# ================================================================
# CAPA DE CARGA DE DATOS — ZERO LATENCY
#
# Estrategia:
#   • Al arrancar, _ensure_data_loaded() ejecuta UN conjunto de
#     batch queries y guarda todo en session_state["_app_data"].
#   • Las páginas públicas leen de RAM — cero consultas a Supabase.
#   • Las mutaciones admin llaman _invalidate_data() para forzar
#     recarga en el siguiente render.
# ================================================================

def _calc_standings_raw(db: Session, season_id: int) -> pd.DataFrame:
    # Calcula tabla de posiciones. Usado en carga inicial y en admin.
    # Sistema v2: PG×3 + PP×1 | WO = derrota (0 pts) | orden: Pts DESC, DP DESC
    teams = db.query(Team).filter(Team.season_id == season_id).all()
    if not teams:
        return pd.DataFrame()

    matches = (
        db.query(Match)
        .filter(
            Match.season_id == season_id,
            Match.phase == "Fase Regular",
            Match.status.in_(
                ["Jugado", "WO Local", "WO Visitante", "WO Doble"]),
        )
        .all()
    )

    stats: dict = {
        t.id: {"Equipo": t.name, "PJ": 0, "PG": 0, "PP": 0,
               "WO": 0, "PF": 0, "PC": 0}
        for t in teams
    }

    for m in matches:
        h, a = m.home_team_id, m.away_team_id
        hs, as_ = (m.home_score or 0), (m.away_score or 0)

        if m.status == "Jugado":
            for tid in (h, a):
                stats[tid]["PJ"] += 1
            stats[h]["PF"] += hs
            stats[h]["PC"] += as_
            stats[a]["PF"] += as_
            stats[a]["PC"] += hs
            if hs > as_:
                stats[h]["PG"] += 1
                stats[a]["PP"] += 1
            elif as_ > hs:
                stats[a]["PG"] += 1
                stats[h]["PP"] += 1
            else:
                stats[h]["PG"] += 1
                stats[a]["PG"] += 1

        elif m.status == "WO Local":
            # Local hace WO: visitante gana (PG×3), local pierde (WO, 0 pts)
            stats[h]["PJ"] += 1
            stats[h]["WO"] += 1
            stats[h]["PC"] += 20
            stats[a]["PJ"] += 1
            stats[a]["PG"] += 1
            stats[a]["PF"] += 20

        elif m.status == "WO Visitante":
            # Visitante hace WO: local gana (PG×3), visitante pierde (WO, 0 pts)
            stats[a]["PJ"] += 1
            stats[a]["WO"] += 1
            stats[a]["PC"] += 20
            stats[h]["PJ"] += 1
            stats[h]["PG"] += 1
            stats[h]["PF"] += 20

        elif m.status == "WO Doble":
            # Ambos hacen WO: ambos pierden, 0 pts cada uno
            stats[h]["PJ"] += 1
            stats[h]["WO"] += 1
            stats[a]["PJ"] += 1
            stats[a]["WO"] += 1

    rows = []
    for s in stats.values():
        # Sistema v2: PG×3 + PP×1. WO = derrota por default → 0 pts (no suma PP)
        pts = s["PG"] * 3 + s["PP"] * 1
        rows.append({**s, "DP": s["PF"] - s["PC"], "Pts": pts})

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df = df.sort_values(["Pts", "DP"], ascending=[
                        False, False]).reset_index(drop=True)
    df.insert(0, "#", range(1, len(df) + 1))
    return df[["#", "Equipo", "PJ", "PG", "PP", "WO", "PF", "PC", "DP", "Pts"]]


# @st.cache_data version para uso en admin (liguilla gen, etc.)
@st.cache_data(ttl=600, show_spinner=False)
def calculate_standings(_db: Session, season_id: int) -> pd.DataFrame:
    return _calc_standings_raw(_db, season_id)


def _batch_leaders(db: Session, season_id: int) -> dict:
    # UN solo query que reemplaza N+1 consultas por jugador.
    # Devuelve dict: player_id → {pname, tname, pts_all/reg/lig, trp_all/reg/lig, gp}
    results = (
        db.query(
            Player.id.label("pid"),
            Player.name.label("pname"),
            Team.name.label("tname"),
            Match.phase.label("phase"),
            func.sum(PlayerMatchStat.points).label("pts"),
            func.sum(PlayerMatchStat.triples).label("trp"),
            func.count(PlayerMatchStat.id).label("gp"),
        )
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(
            Match.season_id == season_id,
            PlayerMatchStat.played == True,
            PlayerMatchStat.team_id == Player.team_id,  # anti-traspaso
            Player.is_active == True,
        )
        .group_by(Player.id, Player.name, Team.name, Match.phase)
        .all()
    )

    agg: dict = {}
    for r in results:
        if r.pid not in agg:
            agg[r.pid] = {
                "pname": r.pname, "tname": r.tname,
                "pts_all": 0, "pts_reg": 0, "pts_lig": 0,
                "trp_all": 0, "trp_reg": 0, "trp_lig": 0,
            }
        pts = r.pts or 0
        trp = r.trp or 0
        agg[r.pid]["pts_all"] += pts
        agg[r.pid]["trp_all"] += trp
        if r.phase == "Fase Regular":
            agg[r.pid]["pts_reg"] += pts
            agg[r.pid]["trp_reg"] += trp
        elif r.phase == "Liguilla":
            agg[r.pid]["pts_lig"] += pts
            agg[r.pid]["trp_lig"] += trp
    return agg


def _build_leaders_lists(agg: dict) -> dict:
    # Construye las listas de top-10 a partir del dict de batch leaders.
    def make_list(val_key: str, display_key: str, limit: int = 10) -> list:
        rows = [
            {
                "Jugador": short_name(v["pname"]),
                "Equipo":  v["tname"],
                display_key: v[val_key],
            }
            for v in agg.values() if v[val_key] > 0
        ]
        return sorted(rows, key=lambda x: x[display_key], reverse=True)[:limit]

    return {
        "scorers_all":     make_list("pts_all", "PTS"),
        "scorers_reg":     make_list("pts_reg", "PTS"),
        "scorers_lig":     make_list("pts_lig", "PTS"),
        "triples_all":     make_list("trp_all", "3PT"),
        "triples_reg":     make_list("trp_reg", "3PT"),
        "triples_lig":     make_list("trp_lig", "3PT"),
    }


def _batch_records(db: Session, season_id: int) -> tuple:
    # Récords de puntos y triples en un solo partido (2 queries simples).
    rec_pts = (
        db.query(PlayerMatchStat, Player, Team, Match)
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(Match.season_id == season_id, PlayerMatchStat.played == True)
        .order_by(PlayerMatchStat.points.desc())
        .first()
    )
    rec_trp = (
        db.query(PlayerMatchStat, Player, Team, Match)
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(Match.season_id == season_id, PlayerMatchStat.played == True)
        .order_by(PlayerMatchStat.triples.desc())
        .first()
    )

    def fmt_rec(r, attr, label):
        if not r or getattr(r[0], attr, 0) == 0:
            return None
        stat, player, team, match = r
        return (
            f"**{short_name(player.name)}** · {team.name} · "
            f"**{getattr(stat, attr)} {label}** · Jornada {match.jornada}"
        )

    return fmt_rec(rec_pts, "points", "Puntos"), fmt_rec(rec_trp, "triples", "Triples")


def _matches_snapshot(db: Session, season_id: int) -> list:
    # Snapshot serializable de todos los partidos de la temporada.
    all_m = (
        db.query(Match)
        .filter(Match.season_id == season_id)
        .order_by(Match.jornada, Match.scheduled_date)
        .all()
    )
    team_map = {t.id: t.name for t in db.query(Team).filter(
        Team.season_id == season_id).all()}

    rows = []
    for m in all_m:
        rows.append({
            "id":           m.id,
            "jornada":      m.jornada,
            "vuelta":       m.vuelta,
            "phase":        m.phase,
            "playoff_round": m.playoff_round,
            "game_number":  m.game_number,
            "status":       m.status,
            "venue":        m.venue,
            "sched":        m.scheduled_date,
            "home_id":      m.home_team_id,
            "away_id":      m.away_team_id,
            "home_name":    team_map.get(m.home_team_id, "—"),
            "away_name":    team_map.get(m.away_team_id, "—"),
            "home_score":   m.home_score,
            "away_score":   m.away_score,
        })
    return rows


def _teams_snapshot(db: Session, season_id: int) -> list:
    return [
        {
            "id":       t.id,
            "name":     t.name,
            "status":   t.status,
            "perms":    t.permissions_used or 0,
            "wos":      t.defaults_count or 0,
        }
        for t in db.query(Team).filter(Team.season_id == season_id).order_by(Team.name).all()
    ]


def _team_player_stats(db: Session, season_id: int) -> dict:
    # Estadísticas individuales por equipo (batch).
    # Devuelve { team_id: { "team_name": str, "players": [...] } }
    results = (
        db.query(
            Player.id.label("pid"),
            Player.name.label("pname"),
            Player.number.label("num"),
            Player.team_id.label("tid"),
            Team.name.label("tname"),
            Match.phase.label("phase"),
            func.count(PlayerMatchStat.id).label("gp"),
            func.sum(PlayerMatchStat.points).label("pts"),
            func.sum(PlayerMatchStat.triples).label("trp"),
            # fouls removed v2
        )
        .join(Player, Player.id == PlayerMatchStat.player_id)
        .join(Team,   Team.id == PlayerMatchStat.team_id)
        .join(Match,  Match.id == PlayerMatchStat.match_id)
        .filter(
            Match.season_id == season_id,
            PlayerMatchStat.played == True,
            PlayerMatchStat.team_id == Player.team_id,
            Player.is_active == True,
        )
        .group_by(Player.id, Player.name, Player.number,
                  Player.team_id, Team.name, Match.phase)
        .all()
    )

    # Aggregate by team → player → phase
    teams_data: dict = {}
    for r in results:
        tid = r.tid
        if tid not in teams_data:
            teams_data[tid] = {"team_name": r.tname, "players_raw": {}}
        if r.pid not in teams_data[tid]["players_raw"]:
            teams_data[tid]["players_raw"][r.pid] = {
                "pid": r.pid, "name": r.pname, "number": r.num,
                "gp_all": 0, "pts_all": 0, "trp_all": 0,
                "gp_reg": 0, "pts_reg": 0, "trp_reg": 0,
                "gp_lig": 0, "pts_lig": 0, "trp_lig": 0,
            }
        p = teams_data[tid]["players_raw"][r.pid]
        gp = r.gp or 0
        pts = r.pts or 0
        trp = r.trp or 0
        p["gp_all"] += gp
        p["pts_all"] += pts
        p["trp_all"] += trp
        if r.phase == "Fase Regular":
            p["gp_reg"] += gp
            p["pts_reg"] += pts
            p["trp_reg"] += trp
        elif r.phase == "Liguilla":
            p["gp_lig"] += gp
            p["pts_lig"] += pts
            p["trp_lig"] += trp
    return teams_data


def _preload_category(db: Session, cat: str) -> dict | None:
    # Preload completo de una categoría en una sola sesión.
    season = active_season(db, cat)
    if not season:
        return None

    sid = season.id

    standings_df = _calc_standings_raw(db, sid)
    leaders_agg = _batch_leaders(db, sid)
    leaders_lists = _build_leaders_lists(leaders_agg)
    rec_pts, rec_trp = _batch_records(db, sid)
    matches = _matches_snapshot(db, sid)
    teams = _teams_snapshot(db, sid)
    team_stats = _team_player_stats(db, sid)

    # Snapshot de liguilla (subconjunto de matches)
    liguilla = [m for m in matches if m["phase"] == "Liguilla"]

    # Team map para lookup rápido
    team_map = {t["id"]: t["name"] for t in teams}

    # Métricas del equipo (para página Equipos)
    # Precalcular PJ, PF, PPG, 3PT por equipo y por fase
    team_metrics: dict = {}
    for m in matches:
        if m["status"] != "Jugado":
            continue
        for side, tid, score, opp_score in [
            ("h", m["home_id"], m["home_score"] or 0, m["away_score"] or 0),
            ("a", m["away_id"], m["away_score"] or 0, m["home_score"] or 0),
        ]:
            if tid not in team_metrics:
                team_metrics[tid] = {
                    "pj_all": 0, "pf_all": 0,
                    "pj_reg": 0, "pf_reg": 0,
                    "pj_lig": 0, "pf_lig": 0,
                }
            tm = team_metrics[tid]
            tm["pj_all"] += 1
            tm["pf_all"] += score
            if m["phase"] == "Fase Regular":
                tm["pj_reg"] += 1
                tm["pf_reg"] += score
            elif m["phase"] == "Liguilla":
                tm["pj_lig"] += 1
                tm["pf_lig"] += score

    # Triples por equipo (de los stats ya preloados)
    team_triples: dict = {}
    for tid, td in team_stats.items():
        team_triples[tid] = {
            "trp_all": sum(p["trp_all"] for p in td["players_raw"].values()),
            "trp_reg": sum(p["trp_reg"] for p in td["players_raw"].values()),
            "trp_lig": sum(p["trp_lig"] for p in td["players_raw"].values()),
        }

    return {
        "season_id":      sid,
        "season_name":    season.name,
        "season_year":    season.year,
        "season_is_test": season.is_test,
        "standings":      standings_df,
        "leaders":        leaders_lists,
        "rec_pts":        rec_pts,
        "rec_trp":        rec_trp,
        "matches":        matches,
        "teams":          teams,
        "team_map":       team_map,
        "team_stats":     team_stats,
        "team_metrics":   team_metrics,
        "team_triples":   team_triples,
        "liguilla":       liguilla,
    }


def _load_fresh_data() -> dict:
    # Carga todos los datos de todas las categorías en una sesión.
    result: dict = {}
    try:
        with get_db() as db:
            for cat in CATEGORIES:
                try:
                    result[cat] = _preload_category(db, cat)
                except Exception:
                    result[cat] = None
    except Exception as exc:
        # Si la conexión falla completamente, devolver dict vacío
        # El error ya se muestra en _ensure_data_loaded
        raise
    return result


def _ensure_data_loaded(force: bool = False) -> None:
    # Carga única: si los datos ya están en RAM, no hace nada.
    # force=True → recarga aunque existan datos (después de mutaciones admin).
    if not force and st.session_state.get("_data_loaded"):
        return

    with st.spinner("🏀 Conectando con Supabase y cargando la liga…"):
        try:
            init_db()
            st.session_state["_app_data"] = _load_fresh_data()
            st.session_state["_data_loaded"] = True
        except Exception as exc:
            st.session_state["_app_data"] = {cat: None for cat in CATEGORIES}
            st.session_state["_data_loaded"] = False
            _show_connection_error(exc)


def _invalidate_data() -> None:
    # Marca los datos como obsoletos para forzar recarga en el próximo render.
    # Llamar siempre después de cualquier mutación en admin.
    st.session_state["_data_loaded"] = False
    st.cache_data.clear()


# ================================================================
# ROUND-ROBIN + PLAYOFFS (lógica de calendario)
# ================================================================
def generate_round_robin_schedule(teams: list) -> list:
    # Algoritmo de rotación circular — Round-Robin Doble.
    # Retorna lista de (equipo_local, equipo_visitante, num_jornada).
    # Doble capa anti-duplicado: set seen + UniqueConstraint en BD.
    if len(teams) < 2:
        return []

    t = list(teams)
    if len(t) % 2 != 0:
        t.append(None)

    rounds = len(t) - 1
    half = len(t) // 2
    v1: list = []
    seen_v1: set = set()

    for r in range(rounds):
        for i in range(half):
            home = t[i]
            away = t[len(t) - 1 - i]
            if home is None or away is None:
                continue
            key = (home.id, away.id)
            if key not in seen_v1:
                seen_v1.add(key)
                v1.append((home, away, r + 1))
        t = [t[0]] + [t[-1]] + t[1:-1]

    seen_v2: set = set()
    v2: list = []
    for home, away, jorn in v1:
        key = (away.id, home.id)
        if key not in seen_v2:
            seen_v2.add(key)
            v2.append((away, home, jorn + rounds))

    return v1 + v2


def playoff_eligible_players(db: Session, team: Team, season_id: int) -> list:
    total = (
        db.query(func.count(Match.id))
        .filter(
            Match.season_id == season_id,
            Match.status == "Jugado",
            (Match.home_team_id == team.id) | (Match.away_team_id == team.id),
        )
        .scalar()
    ) or 0
    threshold = (total // 2) + 1

    result = []
    for p in db.query(Player).filter(
        Player.team_id == team.id, Player.is_active == True
    ).all():
        gp = (
            db.query(func.count(PlayerMatchStat.id))
            .join(Match, Match.id == PlayerMatchStat.match_id)
            .filter(
                PlayerMatchStat.player_id == p.id,
                PlayerMatchStat.team_id == team.id,
                Match.season_id == season_id,
                PlayerMatchStat.played == True,
            )
            .scalar()
        ) or 0
        result.append({
            "Jugador":    p.name,
            "#":          p.number,
            "PJ":         gp,
            "Requeridos": threshold,
            "Elegible":   "✅" if gp >= threshold else "❌",
        })
    return sorted(result, key=lambda x: x["Elegible"])


def _series_status(games: list, tid1: int, tid2: int,
                   name1: str, name2: str) -> tuple:
    # BO3 / BO1: devuelve (texto_resumen, winner_team_id | None)
    w1 = w2 = 0
    for g in games:
        if g["status"] != "Jugado":
            continue
        hs, as_ = g["home_score"] or 0, g["away_score"] or 0
        if g["home_id"] == tid1:
            if hs > as_:
                w1 += 1
            elif as_ > hs:
                w2 += 1
        else:
            if as_ > hs:
                w1 += 1
            elif hs > as_:
                w2 += 1

    total = len(games)
    if w1 >= 2 or (total == 1 and w1 == 1):
        return f"🏆 **{name1}** avanza ({w1}-{w2})", tid1
    if w2 >= 2 or (total == 1 and w2 == 1):
        return f"🏆 **{name2}** avanza ({w2}-{w1})", tid2
    if sum(1 for g in games if g["status"] == "Jugado") == 0:
        return f"🔵 {name1} vs {name2} — por jugar", None
    return f"⚡ {name1} **{w1}** — **{w2}** {name2}", None


# ================================================================
# COMPONENTES UI — HTML/CSS
# ================================================================
def _render_club_logo() -> None:
    st.markdown(
        """
        <div class="club-logo">
            <div class="ball">🏀</div>
            <div class="info">
                <strong>Liga Nochixtlán</strong>
                <small>Oaxaca · México</small>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_leaders_table(
    players: list,
    stat_key: str,
    col_label: str,
    limit: int = 10,
) -> None:
    """
    Tabla de líderes compacta — mobile-first, st.dataframe nativo.
    - Encabezados cortos: # / Jugador / Eq. / Pts o Tri / Eq.
    - CSS con padding mínimo y fuente 0.8rem para caber en 360px
    - Columnas small en todo excepto Jugador (medium)
    - Tripleros: columna adicional "Eq." (triples x 3)
    """
    if not players:
        st.info("Aún no hay estadísticas registradas para esta temporada.")
        return

    st.markdown("""
<style>
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
    font-size: 0.8rem !important;
    padding-left: 4px !important;
    padding-right: 4px !important;
    padding-top: 2px !important;
    padding-bottom: 2px !important;
}
[data-testid="stDataFrame"] > div { width: 100% !important; }
</style>
""", unsafe_allow_html=True)

    MEDALS     = {1: "🥇", 2: "🥈", 3: "🥉"}
    is_triples = (stat_key == "3PT")
    # Etiqueta corta para el encabezado de la columna de estadísticas
    stat_short = "Tri" if is_triples else "Pts"
    top        = players[:limit]

    rows = []
    for i, p in enumerate(top, start=1):
        val = int(p.get(stat_key, 0))
        row = {
            "#":        MEDALS.get(i, str(i)),
            "Jugador":  p.get("Jugador", "—"),
            "Eq.":      p.get("Equipo",  "—"),
            stat_short: val,
        }
        if is_triples:
            row["P.Eq."] = val * 3   # Puntos Equivalentes (triples × 3)
        rows.append(row)

    df = pd.DataFrame(rows)

    col_cfg = {
        "#":        st.column_config.TextColumn("#",       width="small"),
        "Jugador":  st.column_config.TextColumn("Jugador", width="medium"),
        "Eq.":      st.column_config.TextColumn("Eq.",     width="small"),
        stat_short: st.column_config.NumberColumn(stat_short, format="%d", width="small"),
    }
    if is_triples:
        col_cfg["P.Eq."] = st.column_config.NumberColumn(
            "P.Eq.", format="%d", width="small", help="Triples × 3",
        )

    st.dataframe(df, use_container_width=True, hide_index=True, column_config=col_cfg)
def render_stat_cards(
    players: list,
    stat_key: str,
    stat_label: str,
    icon: str,
    limit: int = 10,
) -> None:
    # Alias de compatibilidad — delega a render_leaders_table().
    # (tarjetas HTML eliminadas en v5, reemplazadas por tabla nativa)
    render_leaders_table(players, stat_key, stat_label, limit)


def render_record_banner(record_str: str, icon: str) -> None:
    if not record_str:
        return
    st.markdown(
        f"""
        <div style="
            background:linear-gradient(135deg,#1a0e00,#261800);
            border:1px solid #F26B0F;
            border-left:4px solid #FFD700;
            border-radius:8px;
            padding:0.75rem 1rem;
            margin:0.4rem 0 1rem;
            font-size:0.82rem;
            color:#E8E8E8;
        ">{icon} RÉCORD DE LA TEMPORADA &nbsp;—&nbsp; {record_str}</div>
        """,
        unsafe_allow_html=True,
    )


def _show_scoreboard(home_name: str, away_name: str,
                     home_pts: int, away_pts: int) -> None:
    winner_h = home_pts > away_pts
    winner_a = away_pts > home_pts
    col_h = "#27ae60" if winner_h else ("#e74c3c" if winner_a else "#fff")
    col_a = "#27ae60" if winner_a else ("#e74c3c" if winner_h else "#fff")

    st.markdown(
        f"""
        <div style="
            display:flex;align-items:center;justify-content:center;
            gap:1.5rem;padding:1rem 0.5rem;
            background:linear-gradient(135deg,#0f0f1a,#16162a);
            border-radius:12px;margin:0.6rem 0 1rem;
            border:1px solid #252535;
        ">
            <div style="text-align:right;flex:1;min-width:0">
                <div style="font-size:0.72rem;color:#888;text-transform:uppercase;
                            letter-spacing:1px;overflow:hidden;text-overflow:ellipsis;
                            white-space:nowrap">{home_name}</div>
                <div style="font-size:3.2rem;line-height:1;font-weight:900;
                            color:{col_h}">{home_pts}</div>
            </div>
            <div style="font-size:1.2rem;color:#444;font-weight:700;flex:0 0 auto">VS</div>
            <div style="text-align:left;flex:1;min-width:0">
                <div style="font-size:0.72rem;color:#888;text-transform:uppercase;
                            letter-spacing:1px;overflow:hidden;text-overflow:ellipsis;
                            white-space:nowrap">{away_name}</div>
                <div style="font-size:3.2rem;line-height:1;font-weight:900;
                            color:{col_a}">{away_pts}</div>
            </div>
        </div>
        <p style="text-align:center;font-size:0.7rem;color:#444;margin-top:-0.5rem">
            ✦ Marcador autocalculado ✦
        </p>
        """,
        unsafe_allow_html=True,
    )


# ================================================================
# CAPTURA — helpers data_editor
# ================================================================
LINEUP_COLS = ["player_id", "Jugador", "Asistencia", "Triples", "Puntos"]


def _build_lineup_df(players: list) -> pd.DataFrame:
    rows = [
        {
            "player_id":  p.id,
            "Jugador":    f"#{p.number} {short_name(p.name)}",
            "Asistencia": False, "Triples": 0, "Puntos": 0,
        }
        for p in players
    ]
    return pd.DataFrame(rows, columns=LINEUP_COLS)


# Columnas WO: solo Asistencia — Triples y Puntos deshabilitados
WO_COLS = ["player_id", "Jugador", "Asistencia"]

def _build_wo_df(players: list) -> pd.DataFrame:
    """DataFrame simplificado para modo WO: solo checkbox de asistencia."""
    rows = [
        {
            "player_id":  p.id,
            "Jugador":    f"#{p.number} {short_name(p.name)}",
            "Asistencia": False,
        }
        for p in players
    ]
    return pd.DataFrame(rows, columns=WO_COLS)


def _wo_col_config() -> dict:
    """Column config WO: Asistencia editable, resto deshabilitado."""
    return {
        "player_id":  st.column_config.NumberColumn("ID",  disabled=True, width="small"),
        "Jugador":    st.column_config.TextColumn(          disabled=True, width="medium"),
        "Asistencia": st.column_config.CheckboxColumn("✅ Asistió", width="small"),
    }


def _col_config() -> dict:
    return {
        "player_id":  st.column_config.NumberColumn("ID",       disabled=True, width="small"),
        "Jugador":    st.column_config.TextColumn(disabled=True, width="medium"),
        "Asistencia": st.column_config.CheckboxColumn(width="small"),
        "Triples":    st.column_config.NumberColumn(min_value=0, max_value=30, step=1, width="small"),
        "Puntos":     st.column_config.NumberColumn(min_value=0, max_value=99, step=1, width="small"),
    }


def _validate_lineup(df: pd.DataFrame, team_name: str = "") -> list:
    prefix = f"**[{team_name}]** " if team_name else ""
    errors = []
    for _, row in df.iterrows():
        nombre  = row["Jugador"]
        asistio = bool(row["Asistencia"])
        triples = int(row["Triples"])
        puntos  = int(row["Puntos"])
        tiene_stats = triples > 0 or puntos > 0

        if tiene_stats and not asistio:
            campos = []
            if puntos  > 0: campos.append(f"{puntos} pts")
            if triples > 0: campos.append(f"{triples} triples")
            errors.append(
                f"{prefix}**{nombre}** tiene estadísticas "
                f"({', '.join(campos)}) pero **Asistencia no está marcada**."
            )
        if asistio and triples > 0 and puntos < triples * 3:
            errors.append(
                f"{prefix}**{nombre}** — Puntos ({puntos}) < Triples×3 "
                f"({triples}×3={triples*3}). Faltan {triples*3-puntos} pts."
            )
    return errors


def _save_lineup(db: Session, match_id: int, team_id: int, df: pd.DataFrame) -> None:
    for _, row in df.iterrows():
        db.add(PlayerMatchStat(
            match_id=match_id, player_id=int(row["player_id"]),
            team_id=team_id,   played=bool(row["Asistencia"]),
            points=int(row["Puntos"]), triples=int(row["Triples"]),
        ))

# ================================================================
# PÁGINA: POSICIONES
# ================================================================


def _series_wins(games: list, tid1: int, tid2: int) -> tuple:
    """Cuenta victorias de cada equipo en la serie."""
    w1 = w2 = 0
    for g in games:
        if g["status"] != "Jugado":
            continue
        hs, as_ = g["home_score"] or 0, g["away_score"] or 0
        if g["home_id"] == tid1:
            if hs > as_:
                w1 += 1
            elif as_ > hs:
                w2 += 1
        else:
            if as_ > hs:
                w1 += 1
            elif hs > as_:
                w2 += 1
    return w1, w2


def _render_liguilla(liguilla: list, team_map: dict) -> None:
    """
    Bracket visual de eliminación usando st.components.v1.html().
    El HTML es un documento completo autocontenido — el navegador
    lo dibuja directamente sin pasar por el sanitizador de Streamlit.
    Colores 100% hardcoded: texto #FFFFFF garantizado en todo momento.
    """
    import streamlit.components.v1 as components
    from collections import defaultdict as _dd

    if not liguilla:
        st.info("Aún no hay partidos de Liguilla generados para esta temporada.")
        return

    # ── 1. Agrupar partidos por ronda y por par ───────────────────
    by_round = _dd(lambda: _dd(list))
    for g in liguilla:
        rnd = g["playoff_round"] or "—"
        key = tuple(sorted([g["home_id"], g["away_id"]]))
        by_round[rnd][key].append(g)

    rondas = sorted(by_round.keys(), key=lambda r: ROUND_ORDER.get(r, 9))

    # ── 2. Helpers de datos ───────────────────────────────────────
    def get_series(games, tid1, tid2):
        n1 = team_map.get(tid1, "Por definir")
        n2 = team_map.get(tid2, "Por definir")
        w1, w2 = _series_wins(games, tid1, tid2)
        _, winner_id = _series_status(games, tid1, tid2, n1, n2)
        played = sum(1 for g in games if g["status"] == "Jugado")
        if winner_id:
            badge = f"Serie {max(w1,w2)}&ndash;{min(w1,w2)}"
            badge_cls = "badge-done"
        elif played > 0:
            badge = f"En curso {w1}&ndash;{w2}"
            badge_cls = "badge-live"
        else:
            badge = "Por jugar"
            badge_cls = "badge-pending"
        w1_won = winner_id is not None and w1 > w2
        w2_won = winner_id is not None and w2 > w1
        return n1, n2, w1, w2, w1_won, w2_won, badge, badge_cls

    # ── 3. Generar tarjetas HTML para una ronda ───────────────────
    def round_cards_html(rnd):
        cards = ""
        for (tid1, tid2), games in by_round[rnd].items():
            n1, n2, w1, w2, n1w, n2w, badge, badge_cls = get_series(
                games, tid1, tid2)
            n1s = (n1[:15] + "…") if len(n1) > 15 else n1
            n2s = (n2[:15] + "…") if len(n2) > 15 else n2
            r1c = "row-win" if n1w else ("row-lose" if n2w else "row-neutral")
            r2c = "row-win" if n2w else ("row-lose" if n1w else "row-neutral")
            t1  = " &#127942;" if n1w else ""
            t2  = " &#127942;" if n2w else ""
            cards += f"""
            <div class="matchup">
              <div class="team-row {r1c}">
                <span class="tname">{n1s}{t1}</span>
                <span class="tscore">{w1}</span>
              </div>
              <div class="team-row {r2c}">
                <span class="tname">{n2s}{t2}</span>
                <span class="tscore">{w2}</span>
              </div>
              <div class="badge {badge_cls}">{badge}</div>
            </div>"""
        return cards if cards else '<p class="empty">Sin datos</p>'

    # ── 4. Calcular campeón ───────────────────────────────────────
    champion = "Por definir"
    if "Final" in by_round:
        for (tid1, tid2), games in by_round["Final"].items():
            n1 = team_map.get(tid1, "?")
            n2 = team_map.get(tid2, "?")
            _, winner_id = _series_status(games, tid1, tid2, n1, n2)
            if winner_id:
                champion = team_map.get(winner_id, "Por definir")

    champ_cls = "champ-known" if champion != "Por definir" else "champ-tbd"
    champ_label = champion if champion != "Por definir" else "&#8212; Por definir &#8212;"

    # ── 5. Columnas del bracket ───────────────────────────────────
    ROUND_NAMES = {
        "Cuartos":   "&#9876;&#65039; Cuartos de Final",
        "Semifinal": "&#129354; Semifinales",
        "Final":     "&#129351; Gran Final",
    }
    cols_html = ""
    for i, rnd in enumerate(rondas):
        rnd_label = ROUND_NAMES.get(rnd, rnd)
        connector = '<div class="connector"></div>' if i < len(rondas) - 1 else ""
        cols_html += f"""
        <div class="col">
          <div class="col-header">{rnd_label}</div>
          <div class="cards-wrap">
            {round_cards_html(rnd)}
          </div>
        </div>
        {connector}"""

    # ── 6. Documento HTML completo ────────────────────────────────
    html = f"""<!DOCTYPE html>
<html lang="es">
<head>
<meta charset="utf-8">
<style>
  *, *::before, *::after {{
    box-sizing: border-box;
    margin: 0;
    padding: 0;
  }}
  body {{
    background: #0d0d14;
    color: #ffffff;
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
    padding: 16px;
    overflow-x: auto;
  }}
  .bracket-title {{
    text-align: center;
    font-size: 1rem;
    font-weight: 900;
    color: #FFD700;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    margin-bottom: 20px;
  }}
  .bracket {{
    display: flex;
    align-items: flex-start;
    gap: 0;
  }}
  .col {{
    display: flex;
    flex-direction: column;
    align-items: center;
    min-width: 215px;
    flex: 1;
  }}
  .col-header {{
    font-size: 0.78rem;
    font-weight: 900;
    color: #F26B0F;
    letter-spacing: 0.07em;
    text-transform: uppercase;
    text-align: center;
    padding: 6px 10px 14px;
    border-bottom: 2px solid #F26B0F;
    width: 100%;
    margin-bottom: 14px;
  }}
  .cards-wrap {{
    display: flex;
    flex-direction: column;
    align-items: center;
    width: 100%;
    gap: 10px;
  }}
  .connector {{
    width: 32px;
    align-self: stretch;
    border-top: 2px solid rgba(242,107,15,0.3);
    border-bottom: 2px solid rgba(242,107,15,0.3);
    margin-top: 58px;
    margin-bottom: 58px;
    flex-shrink: 0;
  }}
  /* ── Tarjeta de matchup ── */
  .matchup {{
    background: #1a1c24;
    border: 2px solid #F26B0F;
    border-radius: 10px;
    padding: 10px;
    width: 205px;
    box-shadow: 2px 2px 10px rgba(0,0,0,0.6);
  }}
  .team-row {{
    display: flex;
    justify-content: space-between;
    align-items: center;
    border-radius: 6px;
    padding: 7px 10px;
    margin-bottom: 4px;
    border-left: 3px solid transparent;
  }}
  .row-neutral {{
    background: #1a1c24;
    border-left-color: #2a2a3a;
  }}
  .row-win {{
    background: #1c1600;
    border-left-color: #FFD700;
  }}
  .row-lose {{
    background: #111118;
    border-left-color: #2a2a3a;
  }}
  .tname {{
    font-size: 0.83rem;
    font-weight: bold;
    color: #ffffff;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    max-width: 145px;
  }}
  .row-win .tname  {{ color: #FFD700; font-weight: 900; }}
  .row-lose .tname {{ color: #666677; font-weight: 500; }}
  .tscore {{
    font-size: 1.05rem;
    font-weight: 900;
    color: #F26B0F;
    min-width: 18px;
    text-align: right;
  }}
  .row-win .tscore  {{ color: #FFD700; }}
  .row-lose .tscore {{ color: #444455; }}
  /* ── Badge de estado ── */
  .badge {{
    font-size: 0.68rem;
    font-weight: 700;
    text-align: center;
    margin-top: 7px;
    letter-spacing: 0.04em;
    padding: 2px 0;
  }}
  .badge-done    {{ color: #FFD700; }}
  .badge-live    {{ color: #F26B0F; }}
  .badge-pending {{ color: #666677; }}
  /* ── Campeón ── */
  .champ-box {{
    text-align: center;
    margin-top: 18px;
    padding: 14px 20px;
    border-radius: 12px;
    min-width: 200px;
    box-shadow: 2px 2px 12px rgba(0,0,0,0.7);
  }}
  .champ-known {{
    background: #1c1600;
    border: 2px solid #FFD700;
  }}
  .champ-tbd {{
    background: #1a1c24;
    border: 2px solid #2a2a3a;
  }}
  .champ-label {{
    font-size: 0.62rem;
    letter-spacing: 0.12em;
    text-transform: uppercase;
    color: #888;
    margin-bottom: 8px;
  }}
  .champ-name {{
    font-size: 1rem;
    font-weight: 900;
  }}
  .champ-known .champ-name {{ color: #FFD700; }}
  .champ-tbd   .champ-name {{ color: #666677; }}
  .empty {{ color: #666677; font-size: 0.8rem; text-align: center; padding: 20px; }}
</style>
</head>
<body>
  <div class="bracket-title">&#127952; Liguilla &mdash; Llaves de Eliminaci&oacute;n</div>
  <div class="bracket">
    {cols_html}
  </div>
  <div style="display:flex;justify-content:center;">
    <div class="champ-box {champ_cls}">
      <div class="champ-label">&#127942; Camp&eacute;on</div>
      <div class="champ-name">{champ_label}</div>
    </div>
  </div>
</body>
</html>"""

    # Altura dinámica: más rondas o partidos = más altura
    n_matchups  = sum(len(v) for v in by_round.values())
    base_height = 320 + n_matchups * 95
    components.html(html, height=min(base_height, 950), scrolling=False)

    # ── Detalle colapsable por ronda ──────────────────────────────
    st.markdown("---")
    for rnd in rondas:
        label = ROUND_LABELS.get(rnd, rnd)
        with st.expander(f"{label} — detalle de partidos", expanded=False):
            for (tid1, tid2), games in by_round[rnd].items():
                n1 = team_map.get(tid1, "?")
                n2 = team_map.get(tid2, "?")
                st.markdown(f"**{n1} vs {n2}**")
                for g in sorted(games, key=lambda x: x["game_number"]):
                    gdate = g["sched"].strftime("%d/%m %H:%M") if g["sched"] else "—"
                    if g["status"] == "Jugado":
                        st.markdown(
                            f"  ✅ Juego {g['game_number']}: "
                            f"**{g['home_name']} {g['home_score']}** — "
                            f"**{g['away_score']} {g['away_name']}** · {gdate}"
                        )
                    else:
                        st.markdown(
                            f"  🔵 Juego {g['game_number']}: "
                            f"{g['home_name']} vs {g['away_name']} · {gdate}"
                        )
                st.markdown("")
def page_standings() -> None:
    data = st.session_state["_app_data"]
    st.title("🏆 Posiciones")

    cat = st.selectbox("Categoría", CATEGORIES, key="stand_cat")
    d = data.get(cat)

    if not d:
        st.warning(f"No hay temporada activa para **{cat}**.")
        return

    sname = f"{d['season_name']}{' 🧪' if d['season_is_test'] else ''}"
    st.caption(f"📋 Temporada: **{sname}** — {cat} {d['season_year']}")

    hay_liguilla = bool(d.get("liguilla"))

    # Opciones de vista — Llaves solo aparece si hay datos de liguilla
    opciones = ["📊 Tabla General"]
    if hay_liguilla:
        opciones.append("🏆 Llaves de Liguilla")

    vista = st.segmented_control(
        "Vista", opciones, default="📊 Tabla General", key="stand_vista"
    )

    if vista == "📊 Tabla General" or vista is None:
        df = d["standings"]
        if df.empty:
            st.info("Aún no hay partidos registrados.")
            return

        st.markdown("""
<style>
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
    font-size: 0.85rem !important;
    padding: 3px 6px !important;
}
[data-testid="stDataFrame"] { width: 100% !important; }
</style>
""", unsafe_allow_html=True)

        df_display = df.rename(columns={"#": "Pos"})

        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Pos":    st.column_config.NumberColumn("Pos",  format="%d", width="small"),
                "Equipo": st.column_config.TextColumn("Equipo",              width="medium"),
                "PJ":     st.column_config.NumberColumn("PJ",  format="%d", width="small"),
                "PG":     st.column_config.NumberColumn("PG",  format="%d", width="small"),
                "PP":     st.column_config.NumberColumn("PP",  format="%d", width="small"),
                "WO":     st.column_config.NumberColumn("WO",  format="%d", width="small"),
                "PF":     st.column_config.NumberColumn("PF",  format="%d", width="small"),
                "PC":     st.column_config.NumberColumn("PC",  format="%d", width="small"),
                "DP":     st.column_config.NumberColumn("DP",  format="%d", width="small"),
                "Pts":    st.column_config.NumberColumn("Pts", format="%d", width="small"),
            },
        )
        st.caption(
            "PJ Jugados · PG Ganados · PP Perdidos · WO Default · "
            "PF Pts a Favor · PC Pts en Contra · DP Diferencia · "
            "Pts = PG×3 + PP×1"
        )
    else:
        _render_liguilla(d.get("liguilla", []), d["team_map"])


# ================================================================
# PÁGINA: LÍDERES ESTADÍSTICOS (tablas nativas st.dataframe)
# ================================================================
def page_leaders() -> None:
    data = st.session_state["_app_data"]
    st.title("🥇 Líderes Estadísticos")

    cat = st.selectbox("Categoría", CATEGORIES, key="lead_cat")
    d = data.get(cat)

    if not d:
        st.warning(f"No hay temporada activa para **{cat}**.")
        return

    sname = f"{d['season_name']}{' 🧪' if d['season_is_test'] else ''}"
    st.caption(f"📋 Temporada: **{sname}** — {cat} {d['season_year']}")

    phase_label = st.radio(
        "📊 Estadísticas de:",
        ["Fase Regular", "Liguilla", "Ambas fases"],
        horizontal=True, key="lead_phase",
    )

    phase_key_map = {
        "Fase Regular": ("scorers_reg", "triples_reg"),
        "Liguilla":     ("scorers_lig", "triples_lig"),
        "Ambas fases":  ("scorers_all", "triples_all"),
    }
    sc_key, tr_key = phase_key_map[phase_label]

    leaders  = d["leaders"]
    scorers  = leaders.get(sc_key, [])
    tripleros = leaders.get(tr_key, [])

    hay_datos = bool(scorers or tripleros)

    if not hay_datos:
        st.info(
            "⏳ Aún no hay estadísticas registradas para esta temporada. "
            "Captura el primer partido desde el Panel de Administración."
        )
        return

    # st.columns([1,1]) → lado a lado en escritorio, apilado en móvil (nativo)
    col1, col2 = st.columns([1, 1])

    with col1:
        st.subheader("🏀 Anotadores")
        render_leaders_table(scorers, "PTS", "Pts")
        render_record_banner(d.get("rec_pts"), "🏀")

    with col2:
        st.subheader("🎯 Tripleros")
        render_leaders_table(tripleros, "3PT", "Tri")
        render_record_banner(d.get("rec_trp"), "🎯")


# ================================================================
# PÁGINA: CALENDARIO
# ================================================================
def page_calendar() -> None:
    data = st.session_state["_app_data"]
    st.title("📅 Calendario de Juegos")

    cat = st.selectbox("Categoría", CATEGORIES, key="cal_cat")
    d = data.get(cat)

    if not d:
        st.warning(f"No hay temporada activa para **{cat}**.")
        return

    dia = WEEKDAY_NAMES[CATEGORY_WEEKDAY[cat]]
    sname = f"{d['season_name']}{' 🧪' if d['season_is_test'] else ''}"
    st.caption(
        f"📌 Categoría **{cat}** juega los **{dia}s** · "
        f"Temporada: **{sname}**"
    )

    all_matches = d["matches"]
    if not all_matches:
        st.info("Aún no hay partidos programados.")
        return

    # ── Filtro de fase ──────────────────────────────────────────────
    hay_liguilla = any(m["phase"] == "Liguilla" for m in all_matches)
    opciones_fase = ["Fase Regular"]
    if hay_liguilla:
        opciones_fase.append("🏆 Liguilla")

    fase_sel = st.radio(
        "Fase:", opciones_fase, horizontal=True, key="cal_fase"
    )
    fase_filtro = "Liguilla" if "Liguilla" in fase_sel else "Fase Regular"
    matches = [m for m in all_matches if m["phase"] == fase_filtro]

    if not matches:
        st.info(f"No hay partidos de {fase_filtro} registrados.")
        return

    rows = []
    for m in matches:
        fecha = m["sched"].strftime("%d/%m/%Y") if m["sched"] else "—"
        hora = m["sched"].strftime("%H:%M") if m["sched"] else "—"

        if m["status"] == "Pendiente":
            resultado = "⏳ Pendiente"
        elif m["status"] == "Cancelado":
            resultado = "🚫 Cancelado"
        elif m["status"] == "Jugado":
            resultado = f"{m['home_score']} - {m['away_score']}"
        elif m["status"] in ("WO Local", "WO Visitante", "WO Doble"):
            resultado = f"⚠️ {m['status']}"
        else:
            resultado = "🔵 Programado"

        fila: dict = {
            "J":         m["jornada"],
            "Local":     m["home_name"],
            "Visitante": m["away_name"],
            "Fecha":     fecha,
            "Hora":      hora,
            "Cancha":    m["venue"] or "—",
            "Resultado": resultado,
        }
        if fase_filtro == "Liguilla":
            fila["Ronda"] = m.get("playoff_round") or "—"
            fila["Juego"] = m.get("game_number", 1)
        rows.append(fila)

    df_cal = pd.DataFrame(rows)

    # Filtro por jornada / ronda
    if fase_filtro == "Fase Regular":
        jornadas = ["Todas"] + sorted(df_cal["J"].unique().tolist())
        sel_j = st.selectbox("Filtrar jornada", jornadas, key="cal_j")
        if sel_j != "Todas":
            df_cal = df_cal[df_cal["J"] == sel_j]
        idx_col = "J"
        rename_idx = "Jornada"
    else:
        rondas = ["Todas"] + [r for r in ["Cuartos", "Semifinal", "Final"]
                              if r in df_cal["Ronda"].values]
        sel_r = st.selectbox("Filtrar ronda", rondas, key="cal_ronda")
        if sel_r != "Todas":
            df_cal = df_cal[df_cal["Ronda"] == sel_r]
        idx_col = "Ronda"
        rename_idx = "Ronda"

    if df_cal.empty:
        st.info("No hay registros disponibles.")
    else:
        st.dataframe(df_cal, use_container_width=True, hide_index=True)


# ================================================================
# PÁGINA: EQUIPOS (estadísticas + liguilla)
# ================================================================
def page_teams() -> None:
    data = st.session_state["_app_data"]
    st.title("🛡️ Equipos")

    cat = st.selectbox("Categoría", CATEGORIES, key="teams_cat")
    d = data.get(cat)

    if not d:
        st.warning(f"No hay temporada activa para **{cat}**.")
        return

    sname = f"{d['season_name']}{' 🧪' if d['season_is_test'] else ''}"
    st.caption(f"📋 Temporada: **{sname}** — {cat} {d['season_year']}")

    # Solo estadísticas — Liguilla se mueve a Posiciones
    teams = d["teams"]
    if not teams:
        st.info("No hay equipos registrados en esta categoría.")
        return
    all_t = teams
    sel_name = st.selectbox(
        "Equipo", [t["name"] for t in all_t], key="ts_team")
    sel_team = next(t for t in all_t if t["name"] == sel_name)

    phase_label = st.radio(
        "Estadísticas de:",
        ["Fase Regular", "Liguilla", "Ambas fases"],
        horizontal=True, key="ts_phase",
    )
    pk = {"Fase Regular": "reg", "Liguilla": "lig",
          "Ambas fases": "all"}[phase_label]

    tm = d["team_metrics"].get(sel_team["id"], {})
    tt = d["team_triples"].get(sel_team["id"], {})
    ts = d["team_stats"].get(sel_team["id"], {})

    pj = tm.get(f"pj_{pk}", 0)
    pf = tm.get(f"pf_{pk}", 0)
    ppg = round(pf / pj, 1) if pj > 0 else 0.0
    trp_t = tt.get(f"trp_{pk}", 0) if tt else 0

    st.markdown(f"### 🏀 {sel_name}")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("🗓️ PJ",  pj,       help="Partidos Jugados")
    c2.metric("🏀 PF",  pf,        help="Puntos a Favor")
    c3.metric("📈 PPG", f"{ppg}",  help="Promedio por Partido")
    c4.metric("🎯 3PT", trp_t,     help="Triples del equipo")

    st.markdown("---")
    st.subheader("👥 Plantilla")

    if not ts:
        st.info("Sin partidos jugados aún para este equipo.")
    else:
        players_raw = ts.get("players_raw", {})
        rows = []
        for p in players_raw.values():
            gp  = p.get(f"gp_{pk}",  0)
            pts = p.get(f"pts_{pk}", 0)
            trp = p.get(f"trp_{pk}", 0)
            ppg_p = round(pts / gp, 1) if gp > 0 else 0.0
            rows.append({
                "#":       p["number"],
                "Jugador": p["name"],
                "PJ":      gp, "PTS": pts, "PPG": ppg_p,
                "3PT":     trp,
            })

        df_p = (
            pd.DataFrame(rows)
            .sort_values("PTS", ascending=False)
            .reset_index(drop=True)
        )
        df_p.insert(0, "Pos", ["🥇" if i == 0 else f"{i+1}°" for i in range(len(df_p))])
        df_p = df_p.rename(columns={"#": "Dorsal"})
        if df_p.empty:
            st.info("No hay registros disponibles.")
        else:
            st.dataframe(
                df_p,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Pos":     st.column_config.TextColumn("Pos",    width="small"),
                    "Dorsal":  st.column_config.NumberColumn("Dorsal", format="%d", width="small"),
                    "Jugador": st.column_config.TextColumn("Jugador", width="medium"),
                    "PJ":      st.column_config.NumberColumn("PJ",   format="%d", width="small"),
                    "PTS":     st.column_config.NumberColumn("PTS",  format="%d", width="small"),
                    "PPG":     st.column_config.NumberColumn("PPG",  format="%.1f", width="small"),
                    "3PT":     st.column_config.NumberColumn("3PT",  format="%d", width="small"),
                },
            )
        st.caption(
            f"{phase_label} · Solo estadísticas con **{sel_name}** (anti-traspaso activo)"
        )


# ================================================================
# ADMIN: SECCIÓN CAPTURA DE PARTIDO
# ================================================================
def _section_capture() -> None:
    cat = st.selectbox("Categoría", CATEGORIES, key="cap_cat")
    cap_tabs = st.tabs(["📥 Nuevo Resultado", "✏️ Editar Partido"])

    # ════════════════════════════════════════════════════════
    # TAB 0 — Nuevo Resultado
    # ════════════════════════════════════════════════════════
    with cap_tabs[0]:
        with get_db() as db:
            season = active_season(db, cat)
            if not season:
                st.warning("Sin temporada activa.")
                return
            pending = (
                db.query(Match)
                .filter(Match.season_id == season.id, Match.status == "Programado")
                .order_by(Match.jornada, Match.scheduled_date)
                .all()
            )
            match_opts = {}
            for m in pending:
                ht = db.query(Team).get(m.home_team_id)
                at = db.query(Team).get(m.away_team_id)
                fecha = m.scheduled_date.strftime("%d/%m/%y") if m.scheduled_date else "S/F"
                match_opts[f"J{m.jornada} | {ht.name} vs {at.name} ({fecha})"] = m.id

        if not match_opts:
            st.success("✅ Todos los partidos están capturados.")
            return

        sel_lbl  = st.selectbox("Partido", list(match_opts.keys()), key="cap_match")
        match_id = match_opts[sel_lbl]

        with get_db() as db:
            match     = db.query(Match).get(match_id)
            home_team = db.query(Team).get(match.home_team_id)
            away_team = db.query(Team).get(match.away_team_id)
            home_ps   = (db.query(Player)
                         .filter(Player.team_id == home_team.id, Player.is_active == True)
                         .order_by(Player.number).all())
            away_ps   = (db.query(Player)
                         .filter(Player.team_id == away_team.id, Player.is_active == True)
                         .order_by(Player.number).all())

        # ── Switch: partido normal vs. WO ──────────────────────────
        st.markdown("---")
        es_wo = st.toggle(
            "⚠️ Registrar como **Ganar por Default (W.O.)**",
            key="cap_es_wo",
        )

        # ══════════════════════════════════════════════════════
        # FLUJO WO — Partido por Default
        # ══════════════════════════════════════════════════════
        if es_wo:
            st.warning(
                "⚠️ **MODO DEFAULT ACTIVO** · Registra solo asistencia manual. "
                "Puntos y triples deshabilitados. El equipo ganador recibe "
                "**+3 pts** en tabla; el perdedor **0 pts**."
            )

            # ── Paso 1: Elegir equipo ganador ─────────────────────
            wo_winner = st.radio(
                "¿Quién GANA por default?",
                [f"🏠 {home_team.name}", f"✈️ {away_team.name}"],
                horizontal=True,
                key="cap_wo_winner",
            )
            winner_is_home = wo_winner.startswith("🏠")
            winner_team    = home_team if winner_is_home else away_team
            loser_team     = away_team if winner_is_home else home_team
            winner_ps      = home_ps  if winner_is_home else away_ps
            loser_ps       = away_ps  if winner_is_home else home_ps

            st.markdown("---")

            # ── Paso 2: Asistencia manual de ambos equipos ────────
            st.markdown(
                f"**Marca quién llegó a la cancha** "
                f"(solo los marcados contarán para elegibilidad Liguilla)"
            )

            wo_h_col, wo_a_col = st.columns(2)

            with wo_h_col:
                badge_h = "🏆 GANADOR" if winner_is_home else "❌ PERDEDOR"
                st.markdown(
                    f"**🏠 {home_team.name}** &nbsp; "
                    f"<span style='color:{'#FFD700' if winner_is_home else '#888'}"
                    f";font-size:0.75rem;font-weight:700'>{badge_h}</span>",
                    unsafe_allow_html=True,
                )
                home_wo = st.data_editor(
                    _build_wo_df(home_ps),
                    column_config=_wo_col_config(),
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    key="ed_wo_home",
                )

            with wo_a_col:
                badge_a = "🏆 GANADOR" if not winner_is_home else "❌ PERDEDOR"
                st.markdown(
                    f"**✈️ {away_team.name}** &nbsp; "
                    f"<span style='color:{'#FFD700' if not winner_is_home else '#888'}"
                    f";font-size:0.75rem;font-weight:700'>{badge_a}</span>",
                    unsafe_allow_html=True,
                )
                away_wo = st.data_editor(
                    _build_wo_df(away_ps),
                    column_config=_wo_col_config(),
                    use_container_width=True,
                    hide_index=True,
                    num_rows="fixed",
                    key="ed_wo_away",
                )

            st.markdown("---")

            # ── Paso 3: Elegir quién recibe los 20 pts (ganador) ──
            # Filtrar a los marcados como presentes en tiempo real
            winner_wo_df   = home_wo if winner_is_home else away_wo
            winner_present = winner_wo_df[winner_wo_df["Asistencia"] == True]

            st.markdown(f"**Asignar 20 pts reglamentarios** (equipo ganador: {winner_team.name})")

            if winner_present.empty:
                st.info(
                    "Marca al menos un jugador del equipo ganador como presente "
                    "para asignarle los 20 pts reglamentarios."
                )
                scorer_id = None
            else:
                # Construir opciones solo con los jugadores presentes
                present_ids = winner_present["player_id"].tolist()
                scorer_opts = {}
                for p in (home_ps if winner_is_home else away_ps):
                    if p.id in present_ids:
                        scorer_opts[f"#{p.number} {short_name(p.name)}"] = p.id
                scorer_opts["— Sin asignar puntos —"] = None

                scorer_lbl = st.selectbox(
                    "Jugador con 20 pts",
                    list(scorer_opts.keys()),
                    key="cap_wo_scorer",
                )
                scorer_id = scorer_opts[scorer_lbl]

            # ── Marcador visual ───────────────────────────────────
            h_score_disp = 20 if winner_is_home else 0
            a_score_disp = 0  if winner_is_home else 20
            _show_scoreboard(home_team.name, away_team.name,
                             h_score_disp, a_score_disp)

            # ── Guardar ───────────────────────────────────────────
            n_home_present = int(home_wo["Asistencia"].sum())
            n_away_present = int(away_wo["Asistencia"].sum())

            st.caption(
                f"Resumen: {home_team.name} **{n_home_present}** presentes · "
                f"{away_team.name} **{n_away_present}** presentes"
            )

            if st.button("💾 GUARDAR W.O.", type="primary",
                         use_container_width=True, key="btn_save_wo"):
                try:
                    with get_db() as db:
                        m  = db.query(Match).get(match_id)
                        ht = db.query(Team).get(m.home_team_id)
                        at = db.query(Team).get(m.away_team_id)

                        # Limpiar stats anteriores
                        db.query(PlayerMatchStat).filter(
                            PlayerMatchStat.match_id == match_id
                        ).delete()

                        # Status y marcador oficial
                        if winner_is_home:
                            m.status     = "WO Visitante"   # visitante hizo WO
                            m.home_score = 20
                            m.away_score = 0
                        else:
                            m.status     = "WO Local"       # local hizo WO
                            m.home_score = 0
                            m.away_score = 20
                        m.played_date = datetime.now()

                        # ── Registrar asistencias del equipo LOCAL ────
                        for _, row in home_wo.iterrows():
                            present = bool(row["Asistencia"])
                            if not present:
                                continue          # no llegó → no se registra
                            pts = 20 if (int(row["player_id"]) == scorer_id
                                         and winner_is_home) else 0
                            db.add(PlayerMatchStat(
                                match_id=match_id,
                                player_id=int(row["player_id"]),
                                team_id=ht.id,
                                played=True,
                                points=pts,
                                triples=0,
                            ))

                        # ── Registrar asistencias del equipo VISITANTE ─
                        for _, row in away_wo.iterrows():
                            present = bool(row["Asistencia"])
                            if not present:
                                continue
                            pts = 20 if (int(row["player_id"]) == scorer_id
                                         and not winner_is_home) else 0
                            db.add(PlayerMatchStat(
                                match_id=match_id,
                                player_id=int(row["player_id"]),
                                team_id=at.id,
                                played=True,
                                points=pts,
                                triples=0,
                            ))

                        db.commit()

                    _invalidate_data()
                    st.success(
                        f"✅ **W.O. guardado.** {winner_team.name} gana por default. "
                        f"{n_home_present + n_away_present} jugadores con asistencia registrada."
                    )
                    st.balloons()
                    st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al guardar W.O.: `{exc}`")

        # ══════════════════════════════════════════════════════
        # FLUJO NORMAL
        # ══════════════════════════════════════════════════════
        else:
            st.markdown(f"#### 🏠 {home_team.name}")
            home_edited = st.data_editor(
                _build_lineup_df(home_ps), column_config=_col_config(),
                use_container_width=True, hide_index=True,
                num_rows="fixed", key="ed_home_new",
            )
            st.markdown(f"#### ✈️ {away_team.name}")
            away_edited = st.data_editor(
                _build_lineup_df(away_ps), column_config=_col_config(),
                use_container_width=True, hide_index=True,
                num_rows="fixed", key="ed_away_new",
            )

            home_score = int(home_edited["Puntos"].sum())
            away_score = int(away_edited["Puntos"].sum())
            _show_scoreboard(home_team.name, away_team.name,
                             home_score, away_score)

            st.markdown("---")
            if st.button("💾 GUARDAR RESULTADOS", type="primary",
                         use_container_width=True, key="btn_save_new"):
                errs = (
                    _validate_lineup(home_edited, home_team.name) +
                    _validate_lineup(away_edited, away_team.name)
                )
                if errs:
                    st.error("❌ Corrige los errores antes de guardar:")
                    for e in errs:
                        st.markdown(f"  • {e}")
                else:
                    try:
                        with get_db() as db:
                            m  = db.query(Match).get(match_id)
                            ht = db.query(Team).get(m.home_team_id)
                            at = db.query(Team).get(m.away_team_id)
                            db.query(PlayerMatchStat).filter(
                                PlayerMatchStat.match_id == match_id
                            ).delete()
                            _save_lineup(db, match_id, ht.id, home_edited)
                            _save_lineup(db, match_id, at.id, away_edited)
                            m.status      = "Jugado"
                            m.home_score  = home_score
                            m.away_score  = away_score
                            m.played_date = datetime.now()
                            db.commit()
                        _invalidate_data()
                        st.success("✅ Partido guardado en Supabase.")
                        st.balloons()
                        st.rerun()
                    except Exception as exc:
                        st.error(f"🔴 Error al guardar el partido: `{exc}`")

    # ════════════════════════════════════════════════════════
    # TAB 1 — Editar partido ya capturado
    # ════════════════════════════════════════════════════════
    with cap_tabs[1]:
        with get_db() as db:
            season = active_season(db, cat)
            if not season:
                st.warning("Sin temporada activa.")
                return
            finished = (
                db.query(Match)
                .filter(
                    Match.season_id == season.id,
                    Match.status.in_(
                        ["Jugado", "WO Local", "WO Visitante", "WO Doble"]),
                )
                .order_by(Match.jornada.desc(), Match.scheduled_date.desc())
                .all()
            )
            edit_opts = {}
            for m in finished:
                ht = db.query(Team).get(m.home_team_id)
                at = db.query(Team).get(m.away_team_id)
                fecha = m.scheduled_date.strftime("%d/%m/%y") if m.scheduled_date else "S/F"
                edit_opts[
                    f"J{m.jornada} | {ht.name} vs {at.name} ({fecha}) — {m.status}"
                ] = m.id

        if not edit_opts:
            st.info("No hay partidos finalizados para editar.")
            return

        edit_lbl = st.selectbox("Partido a editar", list(edit_opts.keys()),
                                key="edit_match")
        edit_mid = edit_opts[edit_lbl]

        with get_db() as db:
            m2        = db.query(Match).get(edit_mid)
            home_team = db.query(Team).get(m2.home_team_id)
            away_team = db.query(Team).get(m2.away_team_id)
            existing  = {
                s.player_id: s
                for s in db.query(PlayerMatchStat)
                .filter(PlayerMatchStat.match_id == edit_mid).all()
            }
            home_ps = (db.query(Player)
                       .filter(Player.team_id == home_team.id, Player.is_active == True)
                       .order_by(Player.number).all())
            away_ps = (db.query(Player)
                       .filter(Player.team_id == away_team.id, Player.is_active == True)
                       .order_by(Player.number).all())

        def build_edit_df(players):
            rows = []
            for p in players:
                s = existing.get(p.id)
                rows.append({
                    "player_id":  p.id,
                    "Jugador":    f"#{p.number} {short_name(p.name)}",
                    "Asistencia": s.played  if s else False,
                    "Triples":    s.triples if s else 0,
                    "Puntos":     s.points  if s else 0,
                })
            return pd.DataFrame(rows, columns=LINEUP_COLS)

        st.markdown(f"#### 🏠 {home_team.name}")
        h_edit = st.data_editor(
            build_edit_df(home_ps), column_config=_col_config(),
            use_container_width=True, hide_index=True,
            num_rows="fixed", key="ed_home_edit",
        )
        st.markdown(f"#### ✈️ {away_team.name}")
        a_edit = st.data_editor(
            build_edit_df(away_ps), column_config=_col_config(),
            use_container_width=True, hide_index=True,
            num_rows="fixed", key="ed_away_edit",
        )

        e_hs = int(h_edit["Puntos"].sum())
        e_as = int(a_edit["Puntos"].sum())
        _show_scoreboard(home_team.name, away_team.name, e_hs, e_as)

        st.markdown("---")
        if st.button("💾 ACTUALIZAR PARTIDO", type="primary",
                     use_container_width=True, key="btn_update"):
            errs = (
                _validate_lineup(h_edit, home_team.name) +
                _validate_lineup(a_edit, away_team.name)
            )
            if errs:
                st.error("❌ Corrige los errores:")
                for e in errs:
                    st.markdown(f"  • {e}")
            else:
                try:
                    with get_db() as db:
                        m  = db.query(Match).get(edit_mid)
                        ht = db.query(Team).get(m.home_team_id)
                        at = db.query(Team).get(m.away_team_id)
                        db.query(PlayerMatchStat).filter(
                            PlayerMatchStat.match_id == edit_mid
                        ).delete()
                        _save_lineup(db, edit_mid, ht.id, h_edit)
                        _save_lineup(db, edit_mid, at.id, a_edit)
                        m.home_score  = e_hs
                        m.away_score  = e_as
                        m.played_date = datetime.now()
                        db.commit()
                    _invalidate_data()
                    st.success("✅ Partido actualizado en Supabase.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al actualizar el partido: `{exc}`")
# ================================================================
# GENERADOR DE PDF — ELEGIBILIDAD PARA LIGUILLA
# ================================================================

def _calc_elegibilidad_categoria(db, season_id: int) -> list:
    """
    Calcula la elegibilidad de TODOS los equipos de una categoría.
    Devuelve lista de dicts con estructura:
      { "equipo": str, "total_partidos": int, "min_requerido": int,
        "jugadores": [ {"Jugador", "Asistencias", "Mínimo", "Estatus"} ] }
    """
    from sqlalchemy import func as _func

    equipos = (
        db.query(Team)
        .filter(Team.season_id == season_id, Team.status == "Activo")
        .order_by(Team.name)
        .all()
    )

    resultado = []
    for equipo in equipos:
        total_partidos = (
            db.query(_func.count(Match.id))
            .filter(
                Match.season_id == season_id,
                Match.status.in_(["Jugado", "WO Local",
                                   "WO Visitante", "WO Doble"]),
                (Match.home_team_id == equipo.id) |
                (Match.away_team_id == equipo.id),
            )
            .scalar()
        ) or 0

        min_requerido = (total_partidos // 2) + 1

        jugadores_db = (
            db.query(Player)
            .filter(Player.team_id == equipo.id, Player.is_active == True)
            .order_by(Player.name)
            .all()
        )

        filas = []
        for p in jugadores_db:
            asistencias = (
                db.query(_func.count(PlayerMatchStat.id))
                .join(Match, Match.id == PlayerMatchStat.match_id)
                .filter(
                    PlayerMatchStat.player_id == p.id,
                    PlayerMatchStat.team_id == equipo.id,
                    Match.season_id == season_id,
                    PlayerMatchStat.played == True,
                )
                .scalar()
            ) or 0
            elegible = (asistencias >= min_requerido) and (total_partidos > 0)
            filas.append({
                "Jugador":     p.name,
                "Asistencias": asistencias,
                "Mínimo":      min_requerido,
                "Estatus":     "ELEGIBLE" if elegible else "NO ELEGIBLE",
            })

        resultado.append({
            "equipo":          equipo.name,
            "total_partidos":  total_partidos,
            "min_requerido":   min_requerido,
            "jugadores":       filas,
        })

    return resultado


def _fecha_es(dt=None) -> str:
    """Fecha en español puro sin depender del locale del servidor."""
    from datetime import datetime as _dt
    if dt is None:
        dt = _dt.now()
    meses = {
        1: "enero",    2: "febrero", 3: "marzo",     4: "abril",
        5: "mayo",     6: "junio",   7: "julio",      8: "agosto",
        9: "septiembre", 10: "octubre", 11: "noviembre", 12: "diciembre",
    }
    return f"{dt.day} de {meses[dt.month]} de {dt.year}"


def _generar_pdf_general_elegibilidad(
    categoria: str,
    temporada_nombre: str,
    equipos_data: list,   # salida de _calc_elegibilidad_categoria
) -> bytes:
    """
    Reporte General de Elegibilidad para toda la categoria.
    - Encabezado global + bloque por equipo
    - Pie de pagina con numeracion "Pagina X de Y" en espanol
    - Fecha 100% en espanol via diccionario manual
    - Firmas y sello al final
    """
    from io import BytesIO
    from reportlab.lib.pagesizes import letter
    from reportlab.lib.units import cm
    from reportlab.lib import colors
    from reportlab.platypus import (
        SimpleDocTemplate, Paragraph, Spacer, Table,
        TableStyle, HRFlowable, KeepTogether,
    )
    from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
    from reportlab.lib.enums import TA_CENTER, TA_LEFT
    from reportlab.pdfgen import canvas as _canvas_mod

    # ── Fecha 100% espanol ────────────────────────────────────────
    fecha_str = _fecha_es()   # ej. "15 de marzo de 2026"

    # ── Numeracion "Pagina X de Y" ────────────────────────────────
    # Se usa una subclase de Canvas para conocer el total al imprimir.
    class _PageNumCanvas(_canvas_mod.Canvas):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self._saved_page_states = []

        def showPage(self):
            self._saved_page_states.append(dict(self.__dict__))
            self._startPage()

        def save(self):
            total = len(self._saved_page_states)
            for state in self._saved_page_states:
                self.__dict__.update(state)
                self._draw_footer(total)
                super().showPage()
            super().save()

        def _draw_footer(self, total_pages: int):
            self.saveState()
            self.setFont("Helvetica", 7)
            self.setFillColor(colors.grey)
            self.drawCentredString(
                letter[0] / 2, 1.2 * cm,
                f"Pagina {self.getPageNumber()} de {total_pages}  |  "
                f"Liga Municipal de Basquetbol de Nochixtlan  |  {fecha_str}"
            )
            self.restoreState()

    buf = BytesIO()
    doc = SimpleDocTemplate(
        buf,
        pagesize=letter,
        leftMargin=2*cm, rightMargin=2*cm,
        topMargin=2*cm,  bottomMargin=2.5*cm,
    )

    styles = getSampleStyleSheet()

    # ── Estilos ───────────────────────────────────────────────────
    s_title = ParagraphStyle(
        "GT", parent=styles["Heading1"],
        fontSize=13, leading=16, alignment=TA_CENTER,
        textColor=colors.black, fontName="Helvetica-Bold", spaceAfter=3,
    )
    s_info = ParagraphStyle(
        "GI", parent=styles["Normal"],
        fontSize=9, textColor=colors.black,
        fontName="Helvetica", spaceAfter=2,
    )
    s_team_header = ParagraphStyle(
        "GTH", parent=styles["Heading2"],
        fontSize=11, textColor=colors.black,
        fontName="Helvetica-Bold", spaceBefore=10, spaceAfter=3,
    )
    s_summary = ParagraphStyle(
        "GSUM", parent=styles["Normal"],
        fontSize=8, textColor=colors.Color(0.3, 0.3, 0.3),
        fontName="Helvetica-Oblique", spaceAfter=4,
    )
    s_footer_txt = ParagraphStyle(
        "GF", parent=styles["Normal"],
        fontSize=7, alignment=TA_CENTER,
        textColor=colors.grey, fontName="Helvetica",
    )

    story = []

    # ════════════════════════════════════════════════════════
    # PORTADA / ENCABEZADO GLOBAL
    # ════════════════════════════════════════════════════════
    story.append(Paragraph(
        "LIGA MUNICIPAL DE BASQUETBOL DE NOCHIXTLAN", s_title))
    story.append(Paragraph(
        "REPORTE GENERAL DE ELEGIBILIDAD PARA LIGUILLA", s_title))
    story.append(HRFlowable(
        width="100%", thickness=2, color=colors.black, spaceAfter=6))

    story.append(Paragraph(f"<b>Fecha de emision:</b> {fecha_str}", s_info))
    story.append(Paragraph(f"<b>Categoria:</b> {categoria}", s_info))
    story.append(Paragraph(f"<b>Temporada:</b> {temporada_nombre}", s_info))
    story.append(Paragraph(
        f"<b>Total de equipos:</b> {len(equipos_data)}", s_info))

    total_jug  = sum(len(e["jugadores"]) for e in equipos_data)
    total_eleg = sum(
        sum(1 for j in e["jugadores"] if j["Estatus"] == "ELEGIBLE")
        for e in equipos_data
    )
    story.append(Paragraph(
        f"<b>Total de jugadores en plantilla:</b> {total_jug}", s_info))
    story.append(Paragraph(
        f"<b>Total elegibles para Liguilla:</b> {total_eleg} de {total_jug}", s_info))
    story.append(Spacer(1, 0.5*cm))
    story.append(HRFlowable(
        width="100%", thickness=0.5, color=colors.grey, spaceAfter=8))

    # ════════════════════════════════════════════════════════
    # BLOQUE POR EQUIPO
    # ════════════════════════════════════════════════════════
    COL_WIDTHS = [0.8*cm, 6.5*cm, 2.8*cm, 2.5*cm, 3*cm]
    HEADER_ROW = ["#", "Jugador", "Asistencias", "Minimo req.", "Estatus"]

    for equipo_info in equipos_data:
        equipo_nombre  = equipo_info["equipo"]
        total_partidos = equipo_info["total_partidos"]
        min_req        = equipo_info["min_requerido"]
        jugadores      = equipo_info["jugadores"]
        n_eleg         = sum(1 for j in jugadores if j["Estatus"] == "ELEGIBLE")
        n_total        = len(jugadores)

        block = []

        # Encabezado del equipo
        block.append(Paragraph(f"Equipo: {equipo_nombre}", s_team_header))
        block.append(HRFlowable(
            width="100%", thickness=1, color=colors.black, spaceAfter=3))
        block.append(Paragraph(
            f"Partidos jugados: {total_partidos}   |   "
            f"Minimo requerido: {min_req}  "
            f"( {total_partidos} // 2 + 1 )   |   "
            f"Elegibles: {n_eleg} de {n_total}",
            s_summary,
        ))

        if not jugadores:
            block.append(Paragraph("Sin jugadores en cedula.", s_summary))
        else:
            data = [HEADER_ROW]
            for i, j in enumerate(jugadores, start=1):
                # Estatus limpio en espanol, sin emojis
                estatus_raw = j["Estatus"]
                estatus_txt = "ELEGIBLE" if estatus_raw == "ELEGIBLE" else "NO ELEGIBLE"
                data.append([
                    str(i),
                    j["Jugador"],
                    str(j["Asistencias"]),
                    str(j["Minimo"]) if "Minimo" in j else str(j.get("Mínimo", min_req)),
                    estatus_txt,
                ])

            tbl = Table(data, colWidths=COL_WIDTHS, repeatRows=1)
            estatus_colors = [
                ("TEXTCOLOR",
                 (4, row_i + 1), (4, row_i + 1),
                 colors.Color(0.1, 0.5, 0.1)
                 if data[row_i + 1][4] == "ELEGIBLE"
                 else colors.Color(0.7, 0.1, 0.1))
                for row_i in range(len(jugadores))
            ]
            tbl.setStyle(TableStyle([
                ("BACKGROUND",    (0, 0), (-1, 0),  colors.black),
                ("TEXTCOLOR",     (0, 0), (-1, 0),  colors.white),
                ("FONTNAME",      (0, 0), (-1, 0),  "Helvetica-Bold"),
                ("FONTSIZE",      (0, 0), (-1, 0),  8),
                ("ALIGN",         (0, 0), (-1, 0),  "CENTER"),
                ("TOPPADDING",    (0, 0), (-1, 0),  4),
                ("BOTTOMPADDING", (0, 0), (-1, 0),  4),
                ("FONTNAME",      (0, 1), (-1, -1), "Helvetica"),
                ("FONTSIZE",      (0, 1), (-1, -1), 8),
                ("ALIGN",         (0, 1), (0, -1),  "CENTER"),
                ("ALIGN",         (1, 1), (1, -1),  "LEFT"),
                ("ALIGN",         (2, 1), (-1, -1), "CENTER"),
                ("TOPPADDING",    (0, 1), (-1, -1), 3),
                ("BOTTOMPADDING", (0, 1), (-1, -1), 3),
                ("ROWBACKGROUNDS",(0, 1), (-1, -1),
                 [colors.white, colors.Color(0.95, 0.95, 0.95)]),
                ("FONTNAME",      (4, 1), (4, -1),  "Helvetica-Bold"),
                ("GRID",          (0, 0), (-1, -1), 0.3, colors.grey),
                ("BOX",           (0, 0), (-1, -1), 0.8, colors.black),
                *estatus_colors,
            ]))
            block.append(tbl)

        block.append(Spacer(1, 0.4*cm))
        story.append(KeepTogether(block))

    # ════════════════════════════════════════════════════════
    # CIERRE: REGLA LEGAL + FIRMAS
    # ════════════════════════════════════════════════════════
    story.append(Spacer(1, 0.6*cm))
    story.append(HRFlowable(
        width="100%", thickness=1, color=colors.black, spaceAfter=6))
    story.append(Paragraph(
        "<i>Regla de elegibilidad: el jugador debe haber asistido al menos al "
        "50%+1 de los partidos jugados por su equipo. "
        "Formula: Minimo requerido = (Partidos del equipo // 2) + 1.</i>",
        s_footer_txt,
    ))
    story.append(Spacer(1, 2*cm))

    firma_data = [
        ["________________________________", "", "________________________________"],
        ["Firma del Presidente de la Liga",  "", "Sello Oficial de la Liga"],
    ]
    firma_tbl = Table(firma_data, colWidths=[6*cm, 3*cm, 6*cm])
    firma_tbl.setStyle(TableStyle([
        ("FONTNAME",  (0, 0), (-1, -1), "Helvetica"),
        ("FONTSIZE",  (0, 0), (-1, -1), 9),
        ("ALIGN",     (0, 0), (-1, -1), "CENTER"),
        ("TOPPADDING",(0, 1), (-1, 1),  2),
    ]))
    story.append(firma_tbl)

    doc.build(story, canvasmaker=_PageNumCanvas)
    return buf.getvalue()

def _section_management() -> None:
    cat = st.selectbox("Categoría", CATEGORIES, key="mgmt_cat")

    def load_teams():
        with get_db() as db:
            s = active_season(db, cat)
            if not s:
                return [], None
            return (
                db.query(Team).filter(Team.season_id ==
                                      s.id).order_by(Team.name).all()
            ), s

    tabs = st.tabs([
        "🏢 Equipos", "👥 Cédula", "🔄 Traspasos",
        "🏆 Elegibilidad", "📋 Permisos & WOs",
    ])

    # ── Equipos ──────────────────────────────────────────────────────────
    with tabs[0]:
        teams, season = load_teams()
        if not season:
            st.warning("No hay temporada activa.")
            return

        if teams:
            with get_db() as db:
                df_t = pd.DataFrame([
                    {
                        "Equipo":    t.name, "Estado": t.status,
                        "Jugadores": roster_count(db, t.id),
                        "Permisos":  t.permissions_used or 0,
                        "WOs":       t.defaults_count or 0,
                    }
                    for t in teams
                ])
            if df_t.empty:
                st.info("No hay registros disponibles.")
            else:
                st.dataframe(
                    df_t,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Equipo":     st.column_config.TextColumn("Equipo",     width="medium"),
                        "Estado":     st.column_config.TextColumn("Estado",     width="small"),
                        "Jugadores":  st.column_config.NumberColumn("Jugadores", format="%d", width="small"),
                        "Permisos":   st.column_config.NumberColumn("Permisos",  format="%d", width="small"),
                        "WOs":        st.column_config.NumberColumn("WOs",       format="%d", width="small"),
                    },
                )
        else:
            st.info("Aún no hay equipos.")

        if teams:
            st.markdown("---")
            st.subheader("🗑️ Eliminar / Dar de Baja")
            del_team_name = st.selectbox(
                "Equipo", [t.name for t in teams], key="del_team_sel"
            )
            if st.button("🗑️ Ejecutar", key="btn_del_team", type="secondary"):
                try:
                    with get_db() as db:
                        s = active_season(db, cat)
                        team = db.query(Team).filter(
                            Team.name == del_team_name, Team.season_id == s.id
                        ).first()
                        if team:
                            match_count = (
                                db.query(func.count(Match.id))
                                .filter(
                                    (Match.home_team_id == team.id) |
                                    (Match.away_team_id == team.id)
                                )
                                .scalar()
                            ) or 0
                            if match_count == 0:
                                db.query(Player).filter(
                                    Player.team_id == team.id
                                ).update({"team_id": None, "is_active": False})
                                db.delete(team)
                                db.commit()
                                _invalidate_data()
                                st.success(f"✅ **{del_team_name}** eliminado.")
                            else:
                                team.status = "Dado de Baja"
                                db.commit()
                                _invalidate_data()
                                st.warning(
                                    f"⚠️ **{del_team_name}** dado de baja. "
                                    f"Tenía {match_count} partido(s) — historial preservado."
                                )
                            st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al procesar el equipo: `{exc}`")

        st.markdown("---")
        st.subheader("➕ Nuevo equipo")
        with st.form("form_new_team", clear_on_submit=True):
            nombre_eq = st.text_input("Nombre del equipo")
            sub_eq = st.form_submit_button("Registrar", type="primary")
        if sub_eq:
            nombre_eq = nombre_eq.strip()
            if not nombre_eq:
                st.error("El nombre no puede estar vacío.")
            else:
                try:
                    with get_db() as db:
                        s = active_season(db, cat)
                        dup = db.query(Team).filter(
                            Team.season_id == s.id, Team.name == nombre_eq
                        ).first()
                        if dup:
                            st.error(f"Ya existe '{nombre_eq}'.")
                        else:
                            db.add(Team(
                                name=nombre_eq, category=cat,
                                season_id=s.id, status="Activo",
                            ))
                            db.commit()
                            _invalidate_data()
                            st.success(f"✅ '{nombre_eq}' registrado.")
                            st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al guardar el equipo: `{exc}`")

    # ── Cédula ────────────────────────────────────────────────────────────
    with tabs[1]:
        teams, season = load_teams()
        if not teams:
            st.warning("Primero registra equipos.")
            return

        sel_tname = st.selectbox(
            "Equipo", [t.name for t in teams], key="ced_team_sel")
        sel_team = next(t for t in teams if t.name == sel_tname)

        with get_db() as db:
            players = (
                db.query(Player)
                .filter(Player.team_id == sel_team.id, Player.is_active == True)
                .order_by(Player.number).all()
            )
            count = len(players)

        pct = count / MAX_ROSTER
        st.metric("Estado de cédula", f"{count} / {MAX_ROSTER}",
                  delta=f"{MAX_ROSTER - count} disponibles" if count < MAX_ROSTER else "COMPLETA",
                  delta_color="normal" if count < MAX_ROSTER else "off")
        st.progress(min(pct, 1.0))

        if players:
            # ── Calcular elegibilidad para Liguilla ──────────────
            with get_db() as db:
                s_elig = active_season(db, cat)
                if s_elig:
                    total_match = (
                        db.query(func.count(Match.id))
                        .filter(
                            Match.season_id == s_elig.id,
                            Match.status.in_(["Jugado", "WO Local",
                                              "WO Visitante", "WO Doble"]),
                            (Match.home_team_id == sel_team.id) |
                            (Match.away_team_id == sel_team.id),
                        )
                        .scalar()
                    ) or 0
                    threshold = (total_match // 2) + 1

                    # Asistencias por jugador
                    asist_map = {}
                    for p in players:
                        gp = (
                            db.query(func.count(PlayerMatchStat.id))
                            .join(Match, Match.id == PlayerMatchStat.match_id)
                            .filter(
                                PlayerMatchStat.player_id == p.id,
                                PlayerMatchStat.team_id  == sel_team.id,
                                Match.season_id == s_elig.id,
                                PlayerMatchStat.played == True,
                            )
                            .scalar()
                        ) or 0
                        asist_map[p.id] = gp
                else:
                    total_match = 0
                    threshold   = 1
                    asist_map   = {p.id: 0 for p in players}

            st.caption(
                f"Partidos del equipo: **{total_match}** · "
                f"Mínimo para Liguilla: **{threshold}** partido(s) jugado(s)"
            )

            rows_ced = []
            for p in players:
                gp       = asist_map.get(p.id, 0)
                elegible = gp >= threshold if total_match > 0 else False
                badge    = "✅ Elegible" if elegible else f"❌ Falta {threshold - gp}"
                rows_ced.append({
                    "Dorsal":   p.number,
                    "Nombre":   p.name,
                    "Alta":     str(p.joined_team_date or "—"),
                    "PJ":       gp,
                    "Liguilla": badge,
                })
            df_cedula = pd.DataFrame(rows_ced)
            st.dataframe(
                df_cedula,
                use_container_width=True,
                hide_index=True,
                column_config={
                    "Dorsal":   st.column_config.NumberColumn("Dorsal",   format="%d", width="small"),
                    "Nombre":   st.column_config.TextColumn("Nombre",              width="medium"),
                    "Alta":     st.column_config.TextColumn("Alta",                width="small"),
                    "PJ":       st.column_config.NumberColumn("PJ",       format="%d", width="small"),
                    "Liguilla": st.column_config.TextColumn("Liguilla",            width="small"),
                },
            )
            st.markdown("#### Dar de baja")
            baja_map = {f"#{p.number} — {p.name}": p.id for p in players}
            b_sel = st.selectbox("Jugador", list(
                baja_map.keys()), key="baja_sel")
            b_mot = st.text_input("Motivo (opcional)", key="baja_mot")
            if st.button("Dar de Baja", key="btn_baja", type="secondary"):
                pid = baja_map[b_sel]
                try:
                    with get_db() as db:
                        p = db.query(Player).get(pid)
                        db.add(TransferHistory(
                            player_id=p.id, from_team_id=p.team_id,
                            to_team_id=None, reason=b_mot or "Baja administrativa",
                        ))
                        p.team_id = None
                        p.is_active = False
                        db.commit()
                    _invalidate_data()
                    st.success(f"✅ {b_sel} dado de baja.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al dar de baja: `{exc}`")

        st.markdown("---")
        st.subheader("➕ Agregar jugador")
        if count >= MAX_ROSTER:
            st.warning(
                f"⛔ Cédula completa ({MAX_ROSTER}/{MAX_ROSTER}). "
                "Da de baja un jugador primero."
            )
        else:
            with st.form("form_player", clear_on_submit=True):
                c1, c2, c3 = st.columns([2, 2, 1])
                with c1:
                    p_nom = st.text_input("Nombre(s)")
                with c2:
                    p_ape = st.text_input("Apellido(s)")
                with c3:
                    p_num = st.number_input("Dorsal", 0, 99, 0)
                sub_p = st.form_submit_button("➕ Registrar", type="primary")
            if sub_p:
                nombre = f"{p_nom.strip()} {p_ape.strip()}".strip()
                if not nombre:
                    st.error("Nombre vacío.")
                else:
                    try:
                        with get_db() as db:
                            db.add(Player(
                                name=nombre, number=p_num, category=cat,
                                team_id=sel_team.id, is_active=True,
                                joined_team_date=date.today(),
                            ))
                            db.commit()
                        _invalidate_data()
                        st.success(f"✅ {nombre} (#{p_num}) registrado.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"🔴 Error al registrar jugador: `{exc}`")

    # ── Traspasos ─────────────────────────────────────────────────────────
    with tabs[2]:
        st.info(
            "Al traspasar, las estadísticas del jugador **se reinician a 0** "
            "con el nuevo equipo. El historial anterior queda preservado en la BD."
        )
        teams, season = load_teams()
        if not season or len(teams) < 2:
            st.warning("Se necesitan al menos 2 equipos.")
            return

        from_name = st.selectbox(
            "Equipo origen", [t.name for t in teams], key="tr_from")
        with get_db() as db:
            s = active_season(db, cat)
            ft = db.query(Team).filter(
                Team.name == from_name, Team.season_id == s.id
            ).first()
            fp = (
                db.query(Player)
                .filter(Player.team_id == ft.id, Player.is_active == True)
                .all()
            )
            player_opts = {f"#{p.number} — {p.name}": p.id for p in fp}

        if not player_opts:
            st.info("El equipo no tiene jugadores activos.")
        else:
            p_label = st.selectbox(
                "Jugador a traspasar", list(player_opts.keys()), key="tr_player"
            )
            to_opts = [t.name for t in teams if t.name != from_name]
            to_name = st.selectbox("Equipo destino", to_opts, key="tr_to")
            motivo = st.text_input("Motivo (opcional)", key="tr_mot")

            if st.button("🔄 Confirmar Traspaso", type="primary", key="btn_transfer"):
                p_id = player_opts[p_label]
                try:
                    with get_db() as db:
                        s3 = active_season(db, cat)
                        player = db.query(Player).get(p_id)
                        tt = db.query(Team).filter(
                            Team.name == to_name, Team.season_id == s3.id
                        ).first()
                        db.add(TransferHistory(
                            player_id=player.id,
                            from_team_id=player.team_id,
                            to_team_id=tt.id,
                            reason=motivo or "Traspaso",
                        ))
                        player.team_id = tt.id
                        player.joined_team_date = date.today()
                        db.commit()
                    _invalidate_data()
                    st.success(f"✅ {p_label} → **{to_name}**.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al realizar el traspaso: `{exc}`")

    # ── Elegibilidad para Liguilla (Reporte General) ──────────────────
    with tabs[3]:
        st.subheader("🏆 Elegibilidad para Liguilla")
        st.caption("Vista y reporte de toda la categoría — calculado equipo por equipo.")

        teams, season = load_teams()
        if not teams:
            st.info("Sin equipos registrados.")
            return
        if not season:
            st.info("Sin temporada activa.")
            return

        # ── Calcular elegibilidad de TODOS los equipos ────────────────
        with get_db() as db:
            s = active_season(db, cat)
            equipos_data = _calc_elegibilidad_categoria(db, s.id)

        if not equipos_data:
            st.info("Sin datos para mostrar.")
            return

        # ── Métricas globales ─────────────────────────────────────────
        total_jug  = sum(len(e["jugadores"]) for e in equipos_data)
        total_eleg = sum(
            sum(1 for j in e["jugadores"] if j["Estatus"] == "ELEGIBLE")
            for e in equipos_data
        )
        mg1, mg2, mg3 = st.columns(3)
        mg1.metric("Equipos",         len(equipos_data))
        mg2.metric("Total jugadores", total_jug)
        mg3.metric("Elegibles",       f"{total_eleg} / {total_jug}")

        st.markdown("---")

        # ── Vista previa: tabla global con filtro por equipo ──────────
        todos_df_rows = []
        for e in equipos_data:
            for j in e["jugadores"]:
                todos_df_rows.append({
                    "Equipo":      e["equipo"],
                    "Jugador":     j["Jugador"],
                    "Asistencias": j["Asistencias"],
                    "Mínimo":      j["Mínimo"],
                    "Estatus":     "✅ Elegible" if j["Estatus"] == "ELEGIBLE"
                                   else "❌ No Elegible",
                })

        df_global = pd.DataFrame(todos_df_rows)

        # Filtro por equipo (opcional)
        opciones_eq = ["Todos los equipos"] + [e["equipo"] for e in equipos_data]
        eq_filtro = st.selectbox(
            "Filtrar vista por equipo", opciones_eq, key="eleg_eq_filtro"
        )
        df_vista = (df_global if eq_filtro == "Todos los equipos"
                    else df_global[df_global["Equipo"] == eq_filtro])

        st.markdown("""
<style>
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
    font-size: 0.85rem !important;
    padding: 3px 6px !important;
}
</style>""", unsafe_allow_html=True)

        st.dataframe(
            df_vista,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Equipo":      st.column_config.TextColumn("Equipo",  width="medium"),
                "Jugador":     st.column_config.TextColumn("Jugador", width="medium"),
                "Asistencias": st.column_config.NumberColumn(
                                   "Asist.", format="%d", width="small"),
                "Mínimo":      st.column_config.NumberColumn(
                                   "Mín.",   format="%d", width="small"),
                "Estatus":     st.column_config.TextColumn("Estatus", width="small"),
            },
        )
        st.caption("Regla: Mínimo = (Partidos del equipo // 2) + 1  — calculado de forma independiente por equipo.")

        st.markdown("---")

        # ── Botón de descarga PDF general ────────────────────────────
        if st.button("📥 Generar Reporte General de Elegibilidad (PDF)",
                     key="btn_gen_pdf_general", type="primary",
                     use_container_width=True):
            try:
                with st.spinner("Generando PDF..."):
                    pdf_bytes = _generar_pdf_general_elegibilidad(
                        categoria=cat,
                        temporada_nombre=s.name,
                        equipos_data=equipos_data,
                    )
                fname = f"elegibilidad_general_{cat}_{s.name.replace(' ','_')}.pdf"
                st.download_button(
                    label="⬇️ Descargar Reporte General (PDF)",
                    data=pdf_bytes,
                    file_name=fname,
                    mime="application/pdf",
                    key="dl_pdf_general",
                )
                st.success(
                    f"✅ PDF listo · {len(equipos_data)} equipos · "
                    f"{total_eleg}/{total_jug} jugadores elegibles."
                )
            except Exception as exc:
                st.error(f"🔴 Error al generar PDF: `{exc}`")
    # ── Permisos & WOs ────────────────────────────────────────────────────
    with tabs[4]:
        teams, season = load_teams()
        if teams:
            with get_db() as db:
                df_disc = pd.DataFrame([
                    {
                        "Equipo":   t.name,
                        "Permisos": f"{t.permissions_used or 0} / {MAX_PERMISSIONS}",
                        "WOs":      t.defaults_count or 0,
                        "Estado":   t.status,
                    }
                    for t in teams
                ])
            if df_disc.empty:
                st.info("No hay registros disponibles.")
            else:
                st.dataframe(
                    df_disc,
                    use_container_width=True,
                    hide_index=True,
                    column_config={
                        "Equipo":    st.column_config.TextColumn("Equipo",   width="medium"),
                        "Permisos":  st.column_config.TextColumn("Permisos", width="small"),
                        "WOs":       st.column_config.NumberColumn("WOs", format="%d", width="small"),
                        "Estado":    st.column_config.TextColumn("Estado",   width="small"),
                    },
                )

        st.markdown("---")
        col_p, col_d = st.columns(2)

        with col_p:
            st.subheader("📋 Permiso")
            teams, season = load_teams()
            if teams:
                perm_n = st.selectbox(
                    "Equipo", [t.name for t in teams], key="perm_n")
                if st.button("Registrar Permiso", key="btn_perm"):
                    try:
                        with get_db() as db:
                            s = active_season(db, cat)
                            t = db.query(Team).filter(
                                Team.name == perm_n, Team.season_id == s.id
                            ).first()
                            if (t.permissions_used or 0) >= MAX_PERMISSIONS:
                                st.error(f"⛔ Ya agotó {MAX_PERMISSIONS} permisos.")
                            else:
                                t.permissions_used = (t.permissions_used or 0) + 1
                                db.commit()
                                _invalidate_data()
                                st.success("✅ Permiso registrado.")
                                st.rerun()
                    except Exception as exc:
                        st.error(f"🔴 Error: `{exc}`")

        with col_d:
            st.subheader("⚠️ WO / Default")
            teams, season = load_teams()
            if teams:
                wo_n = st.selectbox(
                    "Equipo", [t.name for t in teams], key="wo_n")
                if st.button("Registrar WO", key="btn_wo", type="secondary"):
                    try:
                        with get_db() as db:
                            s = active_season(db, cat)
                            t = db.query(Team).filter(
                                Team.name == wo_n, Team.season_id == s.id
                            ).first()
                            t.defaults_count = (t.defaults_count or 0) + 1
                            if t.defaults_count >= MAX_DEFAULTS_BAJA:
                                t.status = "Dado de Baja"
                                st.warning(
                                    f"⛔ {wo_n} dado de baja automáticamente.")
                            db.commit()
                            _invalidate_data()
                        st.success("✅ WO registrado.")
                        st.rerun()
                    except Exception as exc:
                        st.error(f"🔴 Error: `{exc}`")


# ================================================================
# ADMIN: SECCIÓN CALENDARIO
# ================================================================
def _section_calendar_admin() -> None:
    cat = st.selectbox("Categoría", CATEGORIES, key="adm_cal_cat")
    dia = WEEKDAY_NAMES[CATEGORY_WEEKDAY[cat]]
    st.info(f"📌 Categoría **{cat}** juega los **{dia}s**")

    with get_db() as db:
        season = active_season(db, cat)
        if not season:
            st.warning("Sin temporada activa.")
            return
        teams = active_teams(db, season.id)
        all_m = (
            db.query(Match).filter(Match.season_id == season.id)
            .order_by(Match.jornada, Match.scheduled_date).all()
        )
        match_ids = [m.id for m in all_m]
        match_snap = {
            m.id: {
                "jornada": m.jornada, "vuelta": m.vuelta,
                "status":  m.status,  "venue":  m.venue,
                "sched":   m.scheduled_date,
                "home_id": m.home_team_id, "away_id": m.away_team_id,
                "phase":   m.phase, "playoff_round": m.playoff_round,
                "game_number": m.game_number,
            }
            for m in all_m
        }
        team_map = {t.id: t.name for t in db.query(Team).all()}

    if "cal_editing_mid" not in st.session_state:
        st.session_state["cal_editing_mid"] = None

    cal_tabs = st.tabs([
        "📋 Ver / Editar",
        "➕ Partido Manual",
        "🔧 Generar Calendario",
        "🏆 Generar Liguilla",
        "📱 WhatsApp",
    ])

    # ── Ver / Editar ──────────────────────────────────────────────────────
    with cal_tabs[0]:
        if not match_ids:
            st.info("No hay partidos. Usa 'Generar Calendario' o 'Partido Manual'.")
        else:
            cf1, cf2 = st.columns(2)
            with cf1:
                jornadas = sorted({mi["jornada"]
                                  for mi in match_snap.values()})
                j_sel = st.selectbox(
                    "Jornada", ["Todas"] + jornadas, key="cal_j_fil")
            with cf2:
                st_fil = st.selectbox("Estado", [
                    "Todos", "Programado", "Pendiente", "Cancelado",
                    "Jugado", "WO Local", "WO Visitante", "WO Doble",
                ], key="cal_st_fil")

            filtered = [
                mid for mid in match_ids
                if (j_sel == "Todas" or match_snap[mid]["jornada"] == j_sel)
                and (st_fil == "Todos" or match_snap[mid]["status"] == st_fil)
            ]

            if not filtered:
                st.info("Sin partidos con ese filtro.")
            else:
                BADGE = {
                    "Programado": "🔵", "Pendiente": "⏳", "Cancelado": "🚫",
                    "Jugado":     "✅", "WO Local": "⚠️", "WO Visitante": "⚠️",
                    "WO Doble":   "🚫",
                }
                for mid in filtered:
                    mi = match_snap[mid]
                    hn = team_map.get(mi["home_id"], "—")
                    an = team_map.get(mi["away_id"], "—")
                    sched_str = mi["sched"].strftime(
                        "%d/%m/%Y %H:%M") if mi["sched"] else "Sin fecha"
                    badge = BADGE.get(mi["status"], "❓")

                    c_info, c_btn = st.columns([5, 1])
                    with c_info:
                        st.markdown(
                            f"{badge} **J{mi['jornada']}** · **{hn}** vs **{an}** "
                            f"| {sched_str} | {mi['venue'] or 'Sin cancha'} | _{mi['status']}_"
                        )
                    with c_btn:
                        if st.button("✏️", key=f"edit_btn_{mid}", use_container_width=True):
                            st.session_state["cal_editing_mid"] = (
                                None if st.session_state["cal_editing_mid"] == mid else mid
                            )
                            st.rerun()

                    if st.session_state["cal_editing_mid"] == mid:
                        with st.container(border=True):
                            st.markdown(
                                f"**Editando: J{mi['jornada']} · {hn} vs {an}**")
                            default_d = (
                                mi["sched"].date() if mi["sched"]
                                else next_weekday_date(date.today(), CATEGORY_WEEKDAY[cat])
                            )
                            default_t = (
                                mi["sched"].time() if mi["sched"]
                                else datetime.strptime("20:00", "%H:%M").time()
                            )
                            c1, c2, c3, c4 = st.columns(4)
                            with c1:
                                new_d = st.date_input(
                                    "Fecha", default_d, key=f"cal_d_{mid}")
                            with c2:
                                new_t = st.time_input(
                                    "Hora",  default_t, key=f"cal_t_{mid}")
                            with c3:
                                new_v = st.selectbox(
                                    "Cancha", VENUES,
                                    index=VENUES.index(
                                        mi["venue"]) if mi["venue"] in VENUES else 0,
                                    key=f"cal_v_{mid}",
                                )
                            with c4:
                                s_opts = ["Programado",
                                          "Pendiente", "Cancelado"]
                                new_s = st.selectbox(
                                    "Estado", s_opts,
                                    index=s_opts.index(
                                        mi["status"]) if mi["status"] in s_opts else 0,
                                    key=f"cal_s_{mid}",
                                )

                            bc1, bc2 = st.columns(2)
                            with bc1:
                                if st.button("💾 Guardar", key=f"cal_save_{mid}", type="primary"):
                                    try:
                                        with get_db() as db:
                                            mo = db.query(Match).get(mid)
                                            mo.scheduled_date = datetime.combine(
                                                new_d, new_t)
                                            mo.venue = new_v
                                            mo.status = new_s
                                            db.commit()
                                        _invalidate_data()
                                        st.session_state["cal_editing_mid"] = None
                                        st.success("✅ Actualizado.")
                                        st.rerun()
                                    except Exception as exc:
                                        st.error(f"🔴 Error: `{exc}`")
                            with bc2:
                                if st.button("🗑️ Eliminar", key=f"cal_del_{mid}", type="secondary"):
                                    try:
                                        with get_db() as db:
                                            mo = db.query(Match).get(mid)
                                            db.query(PlayerMatchStat).filter(
                                                PlayerMatchStat.match_id == mid
                                            ).delete()
                                            db.delete(mo)
                                            db.commit()
                                        _invalidate_data()
                                        st.session_state["cal_editing_mid"] = None
                                        st.warning("🗑️ Partido eliminado.")
                                        st.rerun()
                                    except Exception as exc:
                                        st.error(f"🔴 Error: `{exc}`")

    # ── Partido manual ────────────────────────────────────────────────────
    with cal_tabs[1]:
        if not teams:
            st.warning("Registra equipos primero.")
        else:
            with st.form("form_manual_match"):
                c1, c2 = st.columns(2)
                with c1:
                    home_n = st.selectbox(
                        "Local",    [t.name for t in teams], key="man_h")
                    jorn = st.number_input("Jornada", 1, 100, 1)
                    md = st.date_input("Fecha", next_weekday_date(
                        date.today(), CATEGORY_WEEKDAY[cat]))
                    mv = st.selectbox("Cancha", VENUES)
                with c2:
                    away_n = st.selectbox(
                        "Visitante", [t.name for t in teams], key="man_a")
                    gn = st.number_input("# Juego", 1, 3, 1)
                    mt = st.time_input(
                        "Hora", datetime.strptime("20:00", "%H:%M").time())
                    ph = st.selectbox("Fase", ["Fase Regular", "Liguilla"])
                    pr = st.selectbox("Ronda Liguilla", [
                                      "", "Cuartos", "Semifinal", "Final"])
                sub_m = st.form_submit_button(
                    "➕ Agregar Partido", type="primary")

            if sub_m:
                if home_n == away_n:
                    st.error("Local y visitante no pueden ser el mismo.")
                else:
                    with get_db() as db:
                        s = active_season(db, cat)
                        ht = db.query(Team).filter(
                            Team.name == home_n, Team.season_id == s.id).first()
                        at = db.query(Team).filter(
                            Team.name == away_n, Team.season_id == s.id).first()
                        try:
                            db.add(Match(
                                season_id=s.id, home_team_id=ht.id, away_team_id=at.id,
                                jornada=jorn, vuelta=1, phase=ph,
                                playoff_round=pr if pr else None,
                                game_number=gn,
                                scheduled_date=datetime.combine(md, mt),
                                venue=mv, status="Programado",
                            ))
                            db.commit()
                            _invalidate_data()
                            st.success("✅ Partido agregado.")
                            st.rerun()
                        except Exception as e:
                            db.rollback()
                            st.error(f"Error: {e}")

    # ── Generar Round-Robin ───────────────────────────────────────────────
    with cal_tabs[2]:
        if not teams:
            st.warning("Registra equipos primero.")
        else:
            st.info(f"Round-Robin Doble con {len(teams)} equipos.")
            with get_db() as db:
                season = active_season(db, cat)
                existing_count = (
                    db.query(func.count(Match.id))
                    .filter(Match.season_id == season.id).scalar()
                ) or 0

            if existing_count > 0:
                st.warning(
                    f"⚠️ Ya existen {existing_count} partidos. "
                    "Solo se añaden los nuevos (sin duplicar)."
                )

            start_date = st.date_input(
                "Inicio de temporada",
                next_weekday_date(date.today(), CATEGORY_WEEKDAY[cat]),
                key="cal_gen_start",
            )
            default_time = st.time_input("Hora", datetime.strptime(
                "20:00", "%H:%M").time(), key="cal_gen_time")
            default_venue = st.selectbox("Cancha", VENUES, key="cal_gen_venue")

            if st.button("🔧 Generar Calendario", type="primary", key="btn_gen_cal"):
                schedule = generate_round_robin_schedule(teams)
                if not schedule:
                    st.error("No se pudo generar el calendario.")
                else:
                    added = 0
                    with get_db() as db:
                        s = active_season(db, cat)
                        for home, away, jorn in schedule:
                            game_date = next_weekday_date(
                                start_date + timedelta(weeks=(jorn - 1)),
                                CATEGORY_WEEKDAY[cat],
                            )
                            try:
                                db.add(Match(
                                    season_id=s.id,
                                    home_team_id=home.id, away_team_id=away.id,
                                    jornada=jorn,
                                    vuelta=1 if jorn <= (
                                        len(schedule) // 2) else 2,
                                    scheduled_date=datetime.combine(
                                        game_date, default_time),
                                    venue=default_venue, status="Programado",
                                ))
                                db.flush()
                                added += 1
                            except Exception:
                                db.rollback()
                        db.commit()
                    _invalidate_data()
                    st.success(f"✅ {added} partidos generados.")
                    st.rerun()

    # ── Generar Liguilla ──────────────────────────────────────────────────
    with cal_tabs[3]:
        with get_db() as db:
            season = active_season(db, cat)
            if not season:
                st.warning("Sin temporada activa.")
            else:
                standings_df = calculate_standings(db, season.id)
                n_classif = st.number_input(
                    "Equipos clasificados", 4, 8, 4, step=2, key="lig_n")
                rounds_map = {
                    4: ["Semifinal", "Final"],
                    6: ["Cuartos", "Semifinal", "Final"],
                    8: ["Cuartos", "Semifinal", "Final"],
                }
                bo = st.selectbox(
                    "Formato", ["BO1 (1 juego)", "BO3 (mejor de 3)"], key="lig_bo")
                games_per_series = 1 if "BO1" in bo else 3
                start_lig = st.date_input(
                    "Inicio de Liguilla",
                    next_weekday_date(date.today(), CATEGORY_WEEKDAY[cat]),
                    key="lig_start",
                )
                lig_time = st.time_input("Hora", datetime.strptime(
                    "20:00", "%H:%M").time(), key="lig_time")
                lig_venue = st.selectbox("Cancha", VENUES, key="lig_venue")

                if standings_df.empty:
                    st.info("No hay clasificación. Captura partidos primero.")
                else:
                    top_names = standings_df.head(int(n_classif))[
                        "Equipo"].tolist()
                    st.markdown(f"**Clasificados:** {', '.join(top_names)}")

                    if st.button("🏆 Generar Liguilla", type="primary", key="btn_gen_lig"):
                        classif_teams = [
                            t for t in teams if t.name in top_names]
                        added = 0
                        with get_db() as db:
                            s = active_season(db, cat)
                            rnds = rounds_map.get(len(classif_teams), [
                                                  "Semifinal", "Final"])
                            jorn_counter = (
                                db.query(func.max(Match.jornada))
                                .filter(Match.season_id == s.id).scalar() or 0
                            ) + 1
                            n = len(classif_teams)
                            matchups = [
                                (classif_teams[i], classif_teams[n - 1 - i])
                                for i in range(n // 2)
                            ]
                            for round_name in rnds:
                                for gn in range(1, games_per_series + 1):
                                    for home_t, away_t in matchups:
                                        game_dt = datetime.combine(
                                            start_lig +
                                            timedelta(weeks=(gn - 1)), lig_time
                                        )
                                        try:
                                            db.add(Match(
                                                season_id=s.id,
                                                home_team_id=home_t.id, away_team_id=away_t.id,
                                                jornada=jorn_counter, phase="Liguilla",
                                                playoff_round=round_name, game_number=gn,
                                                scheduled_date=game_dt, venue=lig_venue,
                                                status="Programado",
                                            ))
                                            db.flush()
                                            added += 1
                                        except Exception:
                                            db.rollback()
                                jorn_counter += 1
                            db.commit()
                        _invalidate_data()
                        st.success(f"✅ Liguilla generada: {added} partidos.")
                        st.rerun()

    # ── WhatsApp ──────────────────────────────────────────────────────────
    with cal_tabs[4]:
        if not match_ids:
            st.info("No hay partidos generados.")
        else:
            j_wa = st.selectbox(
                "Jornada",
                sorted({mi["jornada"] for mi in match_snap.values()}),
                key="wa_j",
            )
            wa_matches = [
                mid for mid in match_ids if match_snap[mid]["jornada"] == j_wa]
            lines = [f"🏀 *JORNADA {j_wa} — {cat.upper()}*\n"]
            for mid in wa_matches:
                mi = match_snap[mid]
                hn = team_map.get(mi["home_id"], "—")
                an = team_map.get(mi["away_id"], "—")
                fecha = (
                    mi["sched"].strftime("%A %d/%m a las %H:%M").capitalize()
                    if mi["sched"] else "Por confirmar"
                )
                venue = mi["venue"] or "Por confirmar"
                lines.append(f"⚔️ {hn} vs {an}\n📅 {fecha}\n📍 {venue}\n")
            st.text_area("Texto para WhatsApp", "\n".join(
                lines), height=280, key="wa_text")
            st.caption("Copia y pega en tu grupo de WhatsApp.")


# ================================================================
# ADMIN: SECCIÓN GESTOR DE TEMPORADAS
# ================================================================
def _delete_season(season_id: int) -> tuple:
    # Elimina una temporada y todos sus datos en cascada.
    # Orden seguro: PlayerMatchStat → Matches → Players → Teams → Season
    deleted_stats = deleted_matches = deleted_teams = 0
    with get_db() as db:
        match_ids_list = [
            m.id for m in db.query(Match.id).filter(Match.season_id == season_id)
        ]
        if match_ids_list:
            deleted_stats = (
                db.query(PlayerMatchStat)
                .filter(PlayerMatchStat.match_id.in_(match_ids_list))
                .delete(synchronize_session=False)
            )
        deleted_matches = (
            db.query(Match).filter(Match.season_id == season_id)
            .delete(synchronize_session=False)
        )
        team_ids_list = [
            t.id for t in db.query(Team.id).filter(Team.season_id == season_id)
        ]
        if team_ids_list:
            db.query(Player).filter(
                Player.team_id.in_(team_ids_list)
            ).update({"team_id": None, "is_active": False}, synchronize_session=False)
        deleted_teams = (
            db.query(Team).filter(Team.season_id == season_id)
            .delete(synchronize_session=False)
        )
        db.query(Season).filter(Season.id == season_id).delete(
            synchronize_session=False)
        db.commit()
    _invalidate_data()
    return deleted_stats, deleted_matches, deleted_teams


def _section_season_manager() -> None:
    st.subheader("📋 Historial de Temporadas")

    with get_db() as db:
        all_seasons = (
            db.query(Season)
            .order_by(Season.category, Season.is_active.desc(), Season.created_at.desc())
            .all()
        )
        rows_hist = [
            {
                "Categoría": s.category,
                "Temporada": s.name,
                "Año":       s.year,
                "Estado":    "🟢 Activa" if s.is_active else "📦 Cerrada",
                "Tipo":      "🧪 Prueba" if s.is_test else "🏆 Oficial",
                "ID":        s.id,
            }
            for s in all_seasons
        ]

    if rows_hist:
        df_hist = pd.DataFrame(rows_hist).drop(columns=["ID"])
        st.dataframe(
            df_hist,
            use_container_width=True,
            hide_index=True,
            column_config={
                "Categoría": st.column_config.TextColumn("Categoría", width="small"),
                "Temporada": st.column_config.TextColumn("Temporada", width="medium"),
                "Año":       st.column_config.NumberColumn("Año", format="%d", width="small"),
                "Estado":    st.column_config.TextColumn("Estado",    width="small"),
                "Tipo":      st.column_config.TextColumn("Tipo",      width="small"),
            },
        )
    else:
        st.info("No hay registros disponibles.")

    st.markdown("---")

    # ── Eliminar temporada ────────────────────────────────────────────────
    with st.expander("🗑️ Eliminar una temporada (irreversible)", expanded=False):
        st.error(
            "⚠️ Se borrarán TODOS los datos: partidos, stats, equipos y jugadores.")
        if rows_hist:
            del_opts = {
                f"[{r['Categoría']}] {r['Temporada']} ({r['Año']}) {r['Estado']}": r["ID"]
                for r in rows_hist
            }
            del_lbl = st.selectbox("Temporada a eliminar", list(del_opts.keys()),
                                   key="del_season_sel")
            confirm = st.checkbox("Confirmo que quiero borrar esta temporada.",
                                  key="del_season_confirm")
            if st.button("🗑️ ELIMINAR DEFINITIVAMENTE", key="btn_del_season",
                         type="secondary", disabled=not confirm):
                sid = del_opts[del_lbl]
                stats, matches, teams = _delete_season(sid)
                st.warning(
                    f"✅ Eliminada. Borrados: {matches} partidos, "
                    f"{stats} stats, {teams} equipos."
                )
                st.rerun()

    # ── Crear nueva temporada ─────────────────────────────────────────────
    st.subheader("➕ Nueva Temporada")
    with st.form("form_new_season"):
        c1, c2, c3 = st.columns([3, 2, 1])
        with c1:
            new_name = st.text_input(
                "Nombre", placeholder="Torneo Apertura 2026")
        with c2:
            new_cat = st.selectbox("Categoría", CATEGORIES)
        with c3:
            new_year = st.number_input("Año", 2020, 2100, datetime.now().year)
        opts1, opts2 = st.columns(2)
        with opts1:
            clone_cedulas = st.checkbox(
                "Clonar equipos y jugadores", value=True)
        with opts2:
            is_test_new = st.checkbox("Marcar como Prueba 🧪")
        sub_new = st.form_submit_button("🚀 Crear y Activar", type="primary")

    if sub_new:
        if not new_name.strip():
            st.error("El nombre no puede estar vacío.")
            st.stop()

        old_season_name = None
        cloned_teams = 0
        cloned_players = 0

        try:
            with get_db() as db:
                old_season = active_season(db, new_cat)
                if old_season:
                    old_season_name = old_season.name

                new_season = Season(
                    name=new_name.strip(), category=new_cat,
                    year=int(new_year), is_active=True, is_test=is_test_new,
                )
                db.add(new_season)
                db.flush()

                if old_season:
                    old_season.is_active = False

                if clone_cedulas and old_season:
                    old_teams = db.query(Team).filter(
                        Team.season_id == old_season.id).all()
                    for ot in old_teams:
                        nt = Team(
                            name=ot.name, category=ot.category,
                            season_id=new_season.id, status="Activo",
                            permissions_used=0, defaults_count=0,
                        )
                        db.add(nt)
                        db.flush()
                        old_players = (
                            db.query(Player)
                            .filter(Player.team_id == ot.id, Player.is_active == True)
                            .all()
                        )
                        for op in old_players:
                            db.add(Player(
                                name=op.name, number=op.number,
                                category=op.category, team_id=nt.id,
                                is_active=True, joined_team_date=date.today(),
                            ))
                            cloned_players += 1
                        cloned_teams += 1

                db.commit()
            _invalidate_data()

            tipo_str = "🧪 PRUEBA" if is_test_new else "🏆 OFICIAL"
            st.success(
                f"✅ Temporada **{new_name}** ({tipo_str}) creada para **{new_cat}**."
            )
            if old_season_name:
                st.info(f"📦 Temporada anterior **'{old_season_name}'** archivada.")
            if clone_cedulas and old_season_name:
                st.success(
                    f"📋 Clonados: **{cloned_teams}** equipos y "
                    f"**{cloned_players}** jugadores."
                )
            elif not clone_cedulas:
                st.warning(
                    "Sin clonar: la nueva temporada no tiene equipos. "
                    "Regístralos en **Gestión**."
                )
            st.rerun()
        except Exception as exc:
            st.error(f"🔴 Error al crear la temporada en Supabase: `{exc}`")

    st.markdown("---")

    # ── Reactivar temporada ───────────────────────────────────────────────
    with st.expander("♻️ Reactivar una temporada cerrada", expanded=False):
        st.warning(
            "Reactivar desactivará la temporada activa actual de esa categoría.")
        with get_db() as db:
            closed = (
                db.query(Season)
                .filter(Season.is_active == False)
                .order_by(Season.category, Season.created_at.desc())
                .all()
            )
            closed_opts = {
                f"[{s.category}] {s.name} ({s.year}){'  🧪' if s.is_test else ''}": s.id
                for s in closed
            }

        if not closed_opts:
            st.info("No hay temporadas cerradas disponibles.")
        else:
            react_lbl = st.selectbox("Temporada a reactivar", list(closed_opts.keys()),
                                     key="react_sel")
            if st.button("♻️ Reactivar", key="btn_react", type="secondary"):
                try:
                    with get_db() as db:
                        target = db.query(Season).get(closed_opts[react_lbl])
                        target_cat = target.category
                        target_name = target.name
                        current = active_season(db, target_cat)
                        if current:
                            current.is_active = False
                        target.is_active = True
                        db.commit()
                    _invalidate_data()
                    st.success(f"✅ **{target_name}** reactivada para {target_cat}.")
                    st.rerun()
                except Exception as exc:
                    st.error(f"🔴 Error al reactivar: `{exc}`")


# ================================================================
# PÁGINA: ADMIN (consolidado, requiere login)
# ================================================================
def page_admin() -> None:
    if not is_admin():
        # Login centrado, sin acceso al panel
        st.title("🔐 Acceso Administrador")
        col_gap, col_form, col_gap2 = st.columns([1, 2, 1])
        with col_form:
            with st.container(border=True):
                st.markdown(
                    "<div style='text-align:center;font-size:3rem;margin-bottom:0.5rem'>🔐</div>",
                    unsafe_allow_html=True,
                )
                st.markdown("#### Panel Administrativo")
                st.caption(
                    "Introduce la contraseña de administrador para continuar.")
                with st.form("login_form_admin"):
                    pwd = st.text_input(
                        "Contraseña", type="password",
                        placeholder="••••••••",
                        label_visibility="collapsed",
                    )
                    if st.form_submit_button("🔓 Ingresar", type="primary",
                                             use_container_width=True):
                        if hashlib.sha256(pwd.encode()).hexdigest() == ADMIN_HASH:
                            st.session_state["authenticated"] = True
                            st.rerun()
                        else:
                            st.error("Contraseña incorrecta.")
        return

    # Panel admin (autenticado)
    st.title("⚙️ Panel de Administración")
    st.caption(
        "🟢 Sesión activa · "
        "Todos los cambios se guardan en Supabase y refrescan la caché automáticamente."
    )

    if st.button("🔓 Cerrar Sesión", key="logout_admin", type="secondary"):
        st.session_state["authenticated"] = False
        st.session_state["page"] = "Posiciones"
        st.rerun()

    st.markdown("---")

    # Orden del flujo de trabajo: Temporadas → Gestión → Calendario → Captura
    # El índice 0 (Temporadas) es el punto de entrada natural del admin.
    st.markdown("""
<style>
[data-testid="stDataFrame"] td,
[data-testid="stDataFrame"] th {
    color: #FFFFFF !important;
    -webkit-text-fill-color: #FFFFFF !important;
    font-size: 0.85rem !important;
    background-color: #1A1C24 !important;
    border-bottom: 1px solid #31333F !important;
    padding: 3px 6px !important;
}
[data-testid="stDataFrame"] { width: 100% !important; }
</style>
""", unsafe_allow_html=True)

    admin_tabs = st.tabs([
        "🏆 Temporadas",   # 0 — crear / gestionar torneos
        "🛠️ Gestión",      # 1 — equipos, cédulas, traspasos
        "🗓️ Calendario",   # 2 — programar jornadas
        "⚡ Captura",      # 3 — registrar resultados
    ])

    with admin_tabs[0]:   # 🏆 Temporadas
        _section_season_manager()
    with admin_tabs[1]:   # 🛠️ Gestión
        _section_management()
    with admin_tabs[2]:   # 🗓️ Calendario
        _section_calendar_admin()
    with admin_tabs[3]:   # ⚡ Captura de Partido
        _section_capture()


# ================================================================
# MAIN — Navbar Top + Routing
# ================================================================
def main() -> None:
    st.set_page_config(
        page_title="Liga Nochixtlán",
        page_icon="🏀",
        layout="wide",
        initial_sidebar_state="collapsed",
    )

    # Inyectar tema premium
    st.markdown(PREMIUM_CSS, unsafe_allow_html=True)

    # Carga única (solo la primera vez por sesión)
    _ensure_data_loaded()


    # ── Definición de páginas ─────────────────────────────────────────────
    NAV_OPTIONS = ["Posiciones", "Líderes", "Calendario", "Equipos", "Admin"]
    NAV_ICONS = ["trophy",     "person",  "calendar3",  "shield",  "lock"]

    if "page" not in st.session_state:
        st.session_state["page"] = "Posiciones"
    if st.session_state["page"] not in NAV_OPTIONS:
        st.session_state["page"] = "Posiciones"

    # ── Navbar superior ───────────────────────────────────────────────────
    st.markdown('<div class="nav-wrapper">', unsafe_allow_html=True)

    logo_col, nav_col, login_col = st.columns([1.6, 7.2, 1.4])

    with logo_col:
        _render_club_logo()

    with nav_col:
        selected = option_menu(
            menu_title=None,
            options=NAV_OPTIONS,
            icons=NAV_ICONS,
            orientation="horizontal",
            default_index=NAV_OPTIONS.index(st.session_state["page"]),
            styles={
                "container": {
                    "padding":          "0 !important",
                    "background-color": "transparent",
                    "margin":           "0",
                },
                "icon": {
                    "color":     "#F26B0F",
                    "font-size": "14px",
                },
                "nav-link": {
                    "font-size":      "12.5px",
                    "text-align":     "center",
                    "margin":         "0 2px",
                    "padding":        "6px 10px",
                    "border-radius":  "6px",
                    "--hover-color":  "#1a1a2e",
                    "color":          "#bbb",
                    "white-space":    "nowrap",
                },
                "nav-link-selected": {
                    "background-color": "#F26B0F",
                    "color":            "#fff",
                    "font-weight":      "700",
                },
                "menu": {"background-color": "transparent"},
            },
        )
        st.session_state["page"] = selected

    with login_col:
        login_widget_top()

    st.markdown("</div>", unsafe_allow_html=True)

    # ── Routing ───────────────────────────────────────────────────────────
    p = st.session_state["page"]

    if p == "Posiciones":
        page_standings()
    elif p == "Líderes":
        page_leaders()
    elif p == "Calendario":
        page_calendar()
    elif p == "Equipos":
        page_teams()
    elif p == "Admin":
        page_admin()

    # ── Footer ────────────────────────────────────────────────────────────
    st.markdown(
        '<div class="app-footer">'
        'Liga Municipal de Basquetbol · Nochixtlán, Oaxaca · © 2025'
        '</div>',
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()
