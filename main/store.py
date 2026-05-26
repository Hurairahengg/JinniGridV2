"""
store.py — Database layer for Mother.
SQLite by default, swap DATABASE_URL for Postgres later. No migrations system —
create_all on boot. Add Alembic the day you need a real schema change.
"""

import os
import json
from datetime import datetime, timezone
from contextlib import contextmanager
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Boolean,
    DateTime, JSON, ForeignKey, Index, desc,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

DATABASE_URL = os.environ.get("DATABASE_URL", "sqlite:///mother.db")

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {},
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
Base = declarative_base()


# ═══════════════════════════════════════════════════════════════
# MODELS
# ═══════════════════════════════════════════════════════════════
class Worker(Base):
    __tablename__ = "workers"
    id              = Column(String, primary_key=True)       # e.g. "vm1"
    name            = Column(String)
    broker          = Column(String)
    account         = Column(String)
    version         = Column(String)
    state           = Column(String, default="OFFLINE")      # OFFLINE/IDLE/RUNNING/ERROR/DEAD
    last_heartbeat  = Column(DateTime)
    last_balance    = Column(Float)
    last_equity     = Column(Float)
    open_positions  = Column(Integer, default=0)
    mem_bars        = Column(Integer, default=0)
    last_bar_ts     = Column(Integer)
    created_at      = Column(DateTime, default=lambda: datetime.now(timezone.utc))


class Trade(Base):
    __tablename__ = "trades"
    id                = Column(Integer, primary_key=True, autoincrement=True)
    worker_id         = Column(String, ForeignKey("workers.id"), index=True)
    ticket            = Column(Integer, index=True)
    symbol            = Column(String)
    direction         = Column(Integer)
    status            = Column(String, index=True)           # open / closed
    entry_time        = Column(DateTime)
    exit_time         = Column(DateTime)
    actual_entry      = Column(Float)
    actual_exit       = Column(Float)
    sl_price          = Column(Float)
    lots              = Column(Float)
    risk_used         = Column(Float)
    balance_at_entry  = Column(Float)
    net_pnl           = Column(Float)
    gross_pnl         = Column(Float)
    commission        = Column(Float)
    pnl_points        = Column(Float)
    r_multiple        = Column(Float)
    hit_sl            = Column(Boolean)
    bars_held         = Column(Integer)
    bars_window       = Column(JSON)
    signal_idx        = Column(Integer)
    validator_verdict = Column(JSON)
    raw_payload       = Column(JSON)


Index("ix_trades_worker_status", Trade.worker_id, Trade.status)


class Log(Base):
    __tablename__ = "logs"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    worker_id = Column(String, index=True)
    ts        = Column(DateTime, default=lambda: datetime.now(timezone.utc), index=True)
    level     = Column(String)                               # INFO/WARN/ERROR
    message   = Column(String)
    context   = Column(JSON)


class AuditEvent(Base):
    """Audit trail for operator + system actions."""
    __tablename__ = "events"
    id        = Column(Integer, primary_key=True, autoincrement=True)
    worker_id = Column(String, index=True)
    ts        = Column(DateTime, default=lambda: datetime.now(timezone.utc))
    type      = Column(String)                               # cmd.start / config.update / worker.dead etc.
    actor     = Column(String)                               # "operator" / "system"
    detail    = Column(JSON)


# ═══════════════════════════════════════════════════════════════
# INIT
# ═══════════════════════════════════════════════════════════════
def init_db():
    Base.metadata.create_all(engine)


@contextmanager
def db_session():
    s = SessionLocal()
    try:
        yield s
        s.commit()
    except Exception:
        s.rollback()
        raise
    finally:
        s.close()


# ═══════════════════════════════════════════════════════════════
# CRUD — workers
# ═══════════════════════════════════════════════════════════════
def upsert_worker(worker_id, **fields):
    with db_session() as s:
        w = s.get(Worker, worker_id)
        if w is None:
            w = Worker(id=worker_id, **fields)
            s.add(w)
        else:
            for k, v in fields.items():
                setattr(w, k, v)
        return _row_to_dict(w)


def set_worker_state(worker_id, state):
    with db_session() as s:
        w = s.get(Worker, worker_id)
        if w:
            w.state = state


def update_heartbeat(worker_id, payload):
    with db_session() as s:
        w = s.get(Worker, worker_id)
        if w is None:
            return
        w.last_heartbeat = datetime.now(timezone.utc)
        w.state          = payload.get("state", w.state)
        w.last_balance   = payload.get("balance", w.last_balance)
        w.last_equity    = payload.get("equity", w.last_equity)
        w.open_positions = payload.get("open_positions", w.open_positions)
        w.mem_bars       = payload.get("mem_bars", w.mem_bars)
        w.last_bar_ts    = payload.get("last_bar_ts", w.last_bar_ts)


def list_workers():
    with db_session() as s:
        return [_row_to_dict(w) for w in s.query(Worker).order_by(Worker.id).all()]


def get_worker(worker_id):
    with db_session() as s:
        w = s.get(Worker, worker_id)
        return _row_to_dict(w) if w else None


# ═══════════════════════════════════════════════════════════════
# CRUD — trades
# ═══════════════════════════════════════════════════════════════
def insert_open_trade(worker_id, t):
    with db_session() as s:
        row = Trade(
            worker_id=worker_id,
            ticket=t.get("ticket"),
            symbol=t.get("symbol"),
            direction=t.get("dir"),
            status="open",
            entry_time=_parse_dt(t.get("entry_time")),
            actual_entry=t.get("actual_entry"),
            sl_price=t.get("sl_price"),
            lots=t.get("lots"),
            risk_used=t.get("risk_used"),
            balance_at_entry=t.get("balance_at_entry"),
            bars_window=t.get("bars_window"),
            signal_idx=t.get("signal_idx_in_window"),
            raw_payload=t,
        )
        s.add(row)
        s.flush()
        return row.id


def close_trade(worker_id, t, verdict):
    with db_session() as s:
        row = (
            s.query(Trade)
            .filter_by(worker_id=worker_id, ticket=t.get("ticket"), status="open")
            .order_by(desc(Trade.id))
            .first()
        )
        if row is None:
            # late close with no open match — insert as closed directly
            row = Trade(
                worker_id=worker_id, ticket=t.get("ticket"), symbol=t.get("symbol"),
                direction=t.get("dir"), status="closed",
            )
            s.add(row)

        row.status            = "closed"
        row.exit_time         = _parse_dt(t.get("exit_time"))
        row.actual_exit       = t.get("actual_exit")
        row.net_pnl           = t.get("net_pnl")
        row.gross_pnl         = t.get("gross_pnl")
        row.commission        = t.get("commission")
        row.pnl_points        = t.get("pnl_points")
        row.r_multiple        = t.get("r_multiple")
        row.hit_sl            = t.get("hit_sl")
        row.bars_held         = t.get("bars_held")
        row.bars_window       = t.get("bars_window") or row.bars_window
        row.validator_verdict = verdict
        row.raw_payload       = t
        s.flush()
        return row.id


def list_trades(worker_id=None, limit=100, status=None):
    with db_session() as s:
        q = s.query(Trade)
        if worker_id:
            q = q.filter_by(worker_id=worker_id)
        if status:
            q = q.filter_by(status=status)
        rows = q.order_by(desc(Trade.id)).limit(limit).all()
        return [_row_to_dict(r) for r in rows]


def portfolio_stats():
    """Quick aggregate across all closed trades."""
    with db_session() as s:
        rows = s.query(Trade).filter_by(status="closed").all()
        n = len(rows)
        if n == 0:
            return {"n_trades": 0, "net_pnl": 0.0, "win_rate": 0.0, "wins": 0, "losses": 0}
        wins = sum(1 for r in rows if (r.net_pnl or 0) > 0)
        net  = sum((r.net_pnl or 0) for r in rows)
        return {
            "n_trades": n,
            "net_pnl":  round(net, 2),
            "win_rate": round(100.0 * wins / n, 2),
            "wins":     wins,
            "losses":   n - wins,
        }


# ═══════════════════════════════════════════════════════════════
# CRUD — logs / events
# ═══════════════════════════════════════════════════════════════
def insert_log(worker_id, level, message, context=None):
    with db_session() as s:
        s.add(Log(worker_id=worker_id, level=level, message=message, context=context))


def list_logs(worker_id=None, limit=200, level=None):
    with db_session() as s:
        q = s.query(Log)
        if worker_id:
            q = q.filter_by(worker_id=worker_id)
        if level:
            q = q.filter_by(level=level)
        rows = q.order_by(desc(Log.id)).limit(limit).all()
        return [_row_to_dict(r) for r in rows]


def insert_event(worker_id, type_, actor, detail=None):
    with db_session() as s:
        s.add(AuditEvent(worker_id=worker_id, type=type_, actor=actor, detail=detail or {}))


def prune_logs(keep_per_worker=10000):
    """Crude rolling cap; call periodically."""
    with db_session() as s:
        worker_ids = [w.id for w in s.query(Worker).all()]
        for wid in worker_ids:
            ids = [r.id for r in s.query(Log.id).filter_by(worker_id=wid)
                   .order_by(desc(Log.id)).offset(keep_per_worker).all()]
            if ids:
                s.query(Log).filter(Log.id.in_(ids)).delete(synchronize_session=False)


# ═══════════════════════════════════════════════════════════════
# helpers
# ═══════════════════════════════════════════════════════════════
def _row_to_dict(row):
    if row is None:
        return None
    d = {}
    for c in row.__table__.columns:
        v = getattr(row, c.name)
        if isinstance(v, datetime):
            v = v.isoformat()
        d[c.name] = v
    return d


def _parse_dt(v):
    if v is None or isinstance(v, datetime):
        return v
    try:
        return datetime.fromisoformat(str(v).replace("Z", "+00:00"))
    except Exception:
        return None