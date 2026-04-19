from __future__ import annotations

import os
import sqlite3
from contextlib import closing
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field


APP_TZ = timezone(timedelta(hours=8))
DB_PATH = Path(os.getenv("DB_PATH", "waitlist.db"))
ALLOWED_ORIGINS = os.getenv(
    "ALLOWED_ORIGINS",
    ",".join([
        "https://partnerlottery.netlify.app",
        "https://partnertake.netlify.app",
        "http://localhost:5500",
        "http://127.0.0.1:5500",
    ]),
).split(",")

app = FastAPI(title="Partner Waitlist API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[origin.strip() for origin in ALLOWED_ORIGINS if origin.strip()],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class TakeTicketRequest(BaseModel):
    surname: str = Field(min_length=1, max_length=20)
    party_size: int = Field(ge=1, le=20)
    phone: str = Field(min_length=1, max_length=30)


class ManualTicketRequest(BaseModel):
    surname: str = Field(min_length=1, max_length=20)
    party_size: int = Field(ge=1, le=20)
    phone: str = Field(min_length=1, max_length=30)


class TicketResponse(BaseModel):
    id: int
    no: int
    surname: str
    party_size: int
    phone: str
    status: str
    source: str
    created_at: str
    queue_ahead: int = 0


class StatusResponse(BaseModel):
    date_key: str
    current_call: Optional[TicketResponse]
    waiting_count: int
    waiting_list: list[TicketResponse]


class BasicResponse(BaseModel):
    ok: bool = True
    message: str = "ok"


class NextCallResponse(BaseModel):
    ok: bool = True
    current_call: TicketResponse
    waiting_count: int


class RepeatCallResponse(BaseModel):
    ok: bool = True
    current_call: Optional[TicketResponse]
    message: str


def now_local() -> datetime:
    return datetime.now(APP_TZ)


def today_key() -> str:
    return now_local().strftime("%Y-%m-%d")


def now_str() -> str:
    return now_local().strftime("%Y-%m-%d %H:%M:%S")


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with closing(get_conn()) as conn:
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS tickets (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_key TEXT NOT NULL,
                no INTEGER NOT NULL,
                surname TEXT NOT NULL,
                party_size INTEGER NOT NULL,
                phone TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'waiting',
                source TEXT NOT NULL DEFAULT 'customer',
                created_at TEXT NOT NULL,
                called_at TEXT,
                UNIQUE(date_key, no)
            );

            CREATE INDEX IF NOT EXISTS idx_tickets_date_status_id
            ON tickets(date_key, status, id);

            CREATE INDEX IF NOT EXISTS idx_tickets_date_phone
            ON tickets(date_key, phone);
            """
        )
        conn.commit()


@app.on_event("startup")
def on_startup() -> None:
    init_db()


def row_to_ticket(row: sqlite3.Row, queue_ahead: int = 0) -> TicketResponse:
    return TicketResponse(
        id=row["id"],
        no=row["no"],
        surname=row["surname"],
        party_size=row["party_size"],
        phone=row["phone"],
        status=row["status"],
        source=row["source"],
        created_at=row["created_at"],
        queue_ahead=queue_ahead,
    )


def get_current_call(conn: sqlite3.Connection, date_key: str) -> Optional[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM tickets
        WHERE date_key = ? AND status = 'called'
        ORDER BY called_at DESC, id DESC
        LIMIT 1
        """,
        (date_key,),
    ).fetchone()


def get_waiting_rows(conn: sqlite3.Connection, date_key: str) -> list[sqlite3.Row]:
    return conn.execute(
        """
        SELECT *
        FROM tickets
        WHERE date_key = ? AND status = 'waiting'
        ORDER BY id ASC
        """,
        (date_key,),
    ).fetchall()


def get_next_number(conn: sqlite3.Connection, date_key: str) -> int:
    row = conn.execute(
        "SELECT COALESCE(MAX(no), 0) AS max_no FROM tickets WHERE date_key = ?",
        (date_key,),
    ).fetchone()
    return int(row["max_no"]) + 1


def normalize_phone(phone: str) -> str:
    return "".join(ch for ch in phone.strip() if ch.isdigit()) or phone.strip()


@app.get("/")
def root() -> dict[str, Any]:
    return {
        "ok": True,
        "name": "Partner Waitlist API",
        "date_key": today_key(),
        "docs": "/docs",
        "allowed_origins": [origin.strip() for origin in ALLOWED_ORIGINS if origin.strip()],
    }


@app.get("/api/status", response_model=StatusResponse)
def api_status() -> StatusResponse:
    date_key = today_key()
    with closing(get_conn()) as conn:
        current_row = get_current_call(conn, date_key)
        waiting_rows = get_waiting_rows(conn, date_key)

        waiting_list = [
            row_to_ticket(row, queue_ahead=index)
            for index, row in enumerate(waiting_rows)
        ]

        return StatusResponse(
            date_key=date_key,
            current_call=row_to_ticket(current_row) if current_row else None,
            waiting_count=len(waiting_list),
            waiting_list=waiting_list,
        )


@app.post("/api/tickets", response_model=TicketResponse)
def take_ticket(payload: TakeTicketRequest) -> TicketResponse:
    date_key = today_key()
    phone = normalize_phone(payload.phone)

    with closing(get_conn()) as conn:
        duplicated = conn.execute(
            """
            SELECT *
            FROM tickets
            WHERE date_key = ? AND phone = ?
            LIMIT 1
            """,
            (date_key, phone),
        ).fetchone()
        if duplicated:
            raise HTTPException(status_code=409, detail="這支電話今天已取過號")

        no = get_next_number(conn, date_key)
        cur = conn.execute(
            """
            INSERT INTO tickets (date_key, no, surname, party_size, phone, status, source, created_at)
            VALUES (?, ?, ?, ?, ?, 'waiting', 'customer', ?)
            """,
            (
                date_key,
                no,
                payload.surname.strip(),
                payload.party_size,
                phone,
                now_str(),
            ),
        )
        conn.commit()

        row = conn.execute("SELECT * FROM tickets WHERE id = ?", (cur.lastrowid,)).fetchone()
        queue_ahead = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM tickets
            WHERE date_key = ? AND status = 'waiting' AND id < ?
            """,
            (date_key, cur.lastrowid),
        ).fetchone()["cnt"]

        return row_to_ticket(row, queue_ahead=int(queue_ahead))


@app.post("/api/admin/tickets", response_model=TicketResponse)
def manual_add_ticket(payload: ManualTicketRequest) -> TicketResponse:
    date_key = today_key()
    phone = normalize_phone(payload.phone)

    with closing(get_conn()) as conn:
        no = get_next_number(conn, date_key)
        cur = conn.execute(
            """
            INSERT INTO tickets (date_key, no, surname, party_size, phone, status, source, created_at)
            VALUES (?, ?, ?, ?, ?, 'waiting', 'manual', ?)
            """,
            (
                date_key,
                no,
                payload.surname.strip(),
                payload.party_size,
                phone,
                now_str(),
            ),
        )
        conn.commit()

        row = conn.execute("SELECT * FROM tickets WHERE id = ?", (cur.lastrowid,)).fetchone()
        queue_ahead = conn.execute(
            """
            SELECT COUNT(*) AS cnt
            FROM tickets
            WHERE date_key = ? AND status = 'waiting' AND id < ?
            """,
            (date_key, cur.lastrowid),
        ).fetchone()["cnt"]

        return row_to_ticket(row, queue_ahead=int(queue_ahead))


@app.post("/api/admin/next", response_model=NextCallResponse)
def call_next() -> NextCallResponse:
    date_key = today_key()
    with closing(get_conn()) as conn:
        next_row = conn.execute(
            """
            SELECT *
            FROM tickets
            WHERE date_key = ? AND status = 'waiting'
            ORDER BY id ASC
            LIMIT 1
            """,
            (date_key,),
        ).fetchone()

        if not next_row:
            raise HTTPException(status_code=404, detail="目前沒有等待中的客人")

        call_time = now_str()

        conn.execute(
            "UPDATE tickets SET status = 'called', called_at = ? WHERE id = ?",
            (call_time, next_row["id"]),
        )
        conn.commit()

        called_row = conn.execute("SELECT * FROM tickets WHERE id = ?", (next_row["id"],)).fetchone()
        waiting_count = conn.execute(
            "SELECT COUNT(*) AS cnt FROM tickets WHERE date_key = ? AND status = 'waiting'",
            (date_key,),
        ).fetchone()["cnt"]

        return NextCallResponse(
            current_call=row_to_ticket(called_row),
            waiting_count=int(waiting_count),
        )


@app.get("/api/admin/repeat", response_model=RepeatCallResponse)
def repeat_current_call() -> RepeatCallResponse:
    date_key = today_key()
    with closing(get_conn()) as conn:
        current_row = get_current_call(conn, date_key)

        if not current_row:
            return RepeatCallResponse(
                current_call=None,
                message="目前尚未叫號",
            )

        repeat_time = now_str()

        conn.execute(
            "UPDATE tickets SET called_at = ? WHERE id = ?",
            (repeat_time, current_row["id"]),
        )
        conn.commit()

        updated_row = conn.execute(
            "SELECT * FROM tickets WHERE id = ?",
            (current_row["id"],),
        ).fetchone()

        return RepeatCallResponse(
            current_call=row_to_ticket(updated_row),
            message=f"已重新叫號：{updated_row['no']}",
        )


@app.post("/api/admin/clear", response_model=BasicResponse)
def clear_today() -> BasicResponse:
    date_key = today_key()
    with closing(get_conn()) as conn:
        conn.execute("DELETE FROM tickets WHERE date_key = ?", (date_key,))
        conn.commit()
    return BasicResponse(message="今日資料已清空")


@app.get("/api/admin/queue", response_model=list[TicketResponse])
def admin_queue() -> list[TicketResponse]:
    date_key = today_key()
    with closing(get_conn()) as conn:
        rows = get_waiting_rows(conn, date_key)
        return [row_to_ticket(row, queue_ahead=index) for index, row in enumerate(rows)]


if __name__ == "__main__":
    import uvicorn

    port = int(os.getenv("PORT", "8000"))
    uvicorn.run("app:app", host="0.0.0.0", port=port, reload=False)
