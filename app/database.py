from pathlib import Path

from sqlalchemy import create_engine, event, inspect, text
from sqlalchemy.orm import sessionmaker, declarative_base
from app.config import get_settings

settings = get_settings()


def _resolve_database_url(database_url: str) -> str:
    """Anchor relative SQLite paths to the backend, not the shell cwd."""
    prefix = "sqlite:///"
    if not database_url.startswith(prefix) or database_url == "sqlite:///:memory:":
        return database_url
    database_path = database_url[len(prefix):]
    if database_path.startswith("/") or ":/" in database_path:
        return database_url
    resolved = (Path(__file__).resolve().parents[1] / database_path).resolve()
    return f"sqlite:///{resolved.as_posix()}"


def _configure_sqlite_engine(engine) -> None:
    """Tune SQLite for short-lived concurrent write transactions.

    - journal_mode=WAL: readers and writers no longer block each other. In the
      default rollback-journal mode a writer needs EXCLUSIVE to commit while
      concurrent readers hold SHARED, so a burst of concurrent agent lifecycles
      stalled unrelated HTTP requests for tens of seconds.
    - busy_timeout=10000: bounds the wait for transient write-lock contention
      instead of failing immediately with 'database is locked'.
    - synchronous=NORMAL: the recommended durability/latency profile for WAL.

    Re-asserting WAL per connection is idempotent and cheap; for in-memory
    databases the pragma is a harmless no-op. Non-SQLite engines (e.g.
    PostgreSQL) are never touched.
    """
    @event.listens_for(engine, "connect")
    def _set_sqlite_pragmas(dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        try:
            cursor.execute("PRAGMA journal_mode=WAL")
            cursor.execute("PRAGMA synchronous=NORMAL")
            cursor.execute("PRAGMA busy_timeout=10000")
        finally:
            cursor.close()


database_url = _resolve_database_url(settings.DATABASE_URL)
connect_args = {"check_same_thread": False} if database_url.startswith("sqlite") else {}
engine = create_engine(database_url, connect_args=connect_args, pool_pre_ping=True)
if database_url.startswith("sqlite"):
    _configure_sqlite_engine(engine)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def init_db():
    from app import models_db
    Base.metadata.create_all(bind=engine)
    _apply_lightweight_migrations()
    from app.models_db import LedgerHeadDB, OptimizationCycleDB, OptimizationCycleSequenceDB
    from audit.audit_ledger import GENESIS_HASH
    db = SessionLocal()
    try:
        if db.get(LedgerHeadDB, 1) is None:
            db.add(LedgerHeadDB(id=1, seq=-1, current_hash=GENESIS_HASH))
        # Backfill databases created before cycle sequencing. SQLite's rowid
        # preserves insertion order for this table (its primary key is text).
        if engine.dialect.name == "sqlite":
            db.execute(text("UPDATE optimization_cycles SET sequence = rowid WHERE sequence IS NULL"))
        else:
            legacy_cycles = db.query(OptimizationCycleDB).filter(OptimizationCycleDB.sequence.is_(None)).order_by(
                OptimizationCycleDB.created_at, OptimizationCycleDB.id
            ).all()
            next_legacy_sequence = db.query(OptimizationCycleDB.sequence).order_by(
                OptimizationCycleDB.sequence.desc()
            ).first()
            next_legacy_sequence = (next_legacy_sequence[0] if next_legacy_sequence and next_legacy_sequence[0] else 0)
            for cycle in legacy_cycles:
                next_legacy_sequence += 1
                cycle.sequence = next_legacy_sequence
        max_sequence = db.query(OptimizationCycleDB.sequence).order_by(
            OptimizationCycleDB.sequence.desc()
        ).first()
        current_sequence = max_sequence[0] if max_sequence and max_sequence[0] is not None else 0
        sequence_head = db.get(OptimizationCycleSequenceDB, 1)
        if sequence_head is None:
            db.add(OptimizationCycleSequenceDB(id=1, current_sequence=current_sequence))
        elif sequence_head.current_sequence < current_sequence:
            sequence_head.current_sequence = current_sequence
        db.commit()
    finally:
        db.close()


def _apply_lightweight_migrations():
    inspector = inspect(engine)
    existing_tables = set(inspector.get_table_names())
    desired_columns = {
        "payment_events": {
            "source": "VARCHAR DEFAULT 'PRODUCTION' NOT NULL",
            "mode": "VARCHAR",
            "simulation_id": "VARCHAR",
        },
        "decisions": {
            "attempt_number": "INTEGER DEFAULT 1 NOT NULL",
            "source": "VARCHAR DEFAULT 'PRODUCTION' NOT NULL",
            "mode": "VARCHAR",
            "simulation_id": "VARCHAR",
            "expected_recovery": "FLOAT",
            "expected_utility": "FLOAT",
            "selected_channel": "VARCHAR",
            "nerv": "FLOAT",
            "intervention_cost": "FLOAT",
            "selected_strategy": "VARCHAR",
            "alternatives_json": "JSON",
            "portfolio_rank": "INTEGER",
            "optimization_cycle_id": "VARCHAR",
            "actual_recovered_amount": "FLOAT",
            "outcome_timestamp": "DATETIME",
        },
        "optimization_cycles": {
            "expected_nerv": "FLOAT DEFAULT 0.0 NOT NULL",
            "intervention_spend": "FLOAT DEFAULT 0.0 NOT NULL",
            "sequence": "INTEGER",
        },
        "portfolio_opportunities": {
            "selected_channel": "VARCHAR",
            "nerv": "FLOAT",
            "intervention_cost": "FLOAT",
            "realized_utility": "FLOAT",
            "learning_update_json": "JSON",
        },
        "audit_records": {
            "attempt_number": "INTEGER DEFAULT 1 NOT NULL",
            "source": "VARCHAR DEFAULT 'PRODUCTION' NOT NULL",
            "mode": "VARCHAR",
            "simulation_id": "VARCHAR",
            "failure_class": "VARCHAR",
            "recommended_window": "VARCHAR",
            "selected_strategy": "VARCHAR",
            "expected_recovery": "FLOAT",
            "expected_utility": "FLOAT",
            "portfolio_rank": "INTEGER",
            "actual_recovered_amount": "FLOAT",
            "selected_channel": "VARCHAR",
            "nerv": "FLOAT",
            "intervention_cost": "FLOAT",
        },
        "provider_refunds": {
            "audit_seq": "INTEGER",
            "audit_hash": "VARCHAR",
        },
    }

    with engine.begin() as conn:
        for table_name, columns in desired_columns.items():
            if table_name not in existing_tables:
                continue
            existing_columns = {col["name"] for col in inspector.get_columns(table_name)}
            for column_name, column_sql in columns.items():
                if column_name in existing_columns:
                    continue
                conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_sql}"))
