import collections
import asyncio

from typing import List, AsyncIterator, Optional, NamedTuple
from dataclasses import dataclass, field
from datetime import datetime

from sanic import Sanic
from sanic.log import logger
from sanic.response import empty, json
from sanic.request import Request

import aiosqlite
import pydantic


app = Sanic("Ticket Vault", load_env="TICKET_VAULT_")
app.db = None
app.ticket_totals = {}
app.transaction_count = 0


@dataclass
class TransactionWaiter:
    waiter_count: int = 0
    event: asyncio.Event = field(default_factory=asyncio.Event)


app.transaction_waiters = collections.defaultdict(TransactionWaiter)


# noinspection PyUnusedLocal
@app.listener("before_server_start")
async def init_state(_, loop):
    app.db = await aiosqlite.connect(app.config.DB_PATH)

    await app.db.execute(
        "CREATE TABLE IF NOT EXISTS transactions ("
        "  id        INTEGER PRIMARY KEY,"
        "  timestamp TEXT NOT NULL,"
        "  by        TEXT NOT NULL,"
        "  who       TEXT NOT NULL,"
        "  amount    INTEGER NOT NULL,"
        "  note      TEXT"
        ")"
    )

    async with app.db.execute(
        "SELECT who, SUM(amount) FROM transactions GROUP BY who"
    ) as cursor:
        async for who, value in cursor:
            app.ticket_totals[who] = value

    async with app.db.execute("SELECT COUNT(*) FROM transactions") as cursor:
        async for row_count, in cursor:
            app.total_transactions = row_count


# noinspection PyUnusedLocal
@app.listener("after_server_stop")
async def reset_state(_, loop):
    if app.db is not None:
        await app.db.close()

    app.db = None
    app.ticket_totals = {}
    app.transaction_count = 0


# noinspection PyMethodParameters
class RegistrationModel(pydantic.BaseModel):
    timestamp: datetime
    by: str
    who: str

    @pydantic.validator("by")
    def by_must_not_be_empty(cls: "RegistrationModel", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @pydantic.validator("who")
    def who_must_not_be_empty(cls: "Transaction", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @property
    def as_tuple(self):
        return Transaction(
            timestamp=datetime_to_iso(self.timestamp),
            by=self.by,
            who=self.who,
            amount=0,
            note="Initial registration",
        )


@app.route("/ticket_holders/<who:[A-z]+>", methods=("POST",))
async def register_ticket_holder(request: Request, who: str):
    if who in app.ticket_totals:
        return json({"message": f'"{who}" is already a registered ticket holder.'})

    try:
        registration = RegistrationModel(who=who, **request.json)
    except pydantic.ValidationError as e:
        return json({"errors": e.errors()}, status=400)

    await app.db.execute(
        "INSERT INTO transactions (timestamp, by, who, amount, note) "
        "VALUES (?, ?, ?, ?, ?)",
        registration.as_tuple,
    )

    logger.info(f'Registered ticket holder "{registration.who}"')

    app.ticket_totals[who] = 0
    app.transaction_count += 1
    notify_transaction_waiters()

    return json(
        app.ticket_totals,
        headers={"x-transaction-count": app.transaction_count},
        status=201,
    )


# noinspection PyMethodParameters
class RenameHolderModel(pydantic.BaseModel):
    timestamp: datetime
    by: str
    who: str
    to: str

    @pydantic.validator("by")
    def by_must_not_be_empty(cls: "RegistrationModel", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @pydantic.validator("who")
    def who_must_not_be_empty(cls: "Transaction", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @pydantic.validator("to")
    def to_must_not_be_empty(cls: "Transaction", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @property
    def as_tuple(self):
        return Transaction(
            timestamp=datetime_to_iso(self.timestamp),
            by=self.by,
            who=self.to,
            amount=0,
            note=f'Ticket holder renamed from "{self.who}"',
        )


@app.route("/ticket_holders/<who:[A-z]+>", methods=("PATCH",))
async def rename_ticket_holder(request: Request, who: str):
    if who not in app.ticket_totals:
        return json(
            {"message": f'"{who}" is not a registered ticket holder.'}, status=404
        )

    try:
        rename = RenameHolderModel(who=who, **request.json)
    except pydantic.ValidationError as e:
        return json({"errors": e.errors()}, status=400)

    async with app.db.cursor() as cursor:
        await cursor.execute(
            "UPDATE transactions SET who = ? WHERE who = ?",
            (rename.to, rename.who),
        )
        await cursor.execute(
            "INSERT INTO transactions (timestamp, by, who, amount, note) "
            "VALUES (?, ?, ?, ?, ?)",
            rename.as_tuple,
        )

    logger.info(f'Renamed ticket holder "{rename.who}" to "{rename.to}"')

    app.ticket_totals[rename.to] = app.ticket_totals.pop(rename.who)
    app.transaction_count += 1
    notify_transaction_waiters()

    return json(
        app.ticket_totals,
        headers={"x-transaction-count": app.transaction_count},
    )


@app.route("/tickets", methods=("GET",))
async def query_tickets(request: Request):
    if "last-transaction-count" in request.args:
        await transaction_greater_than(
            count=int(request.args["last-transaction-count"][0])
        )

    return json(
        app.ticket_totals, headers={"x-transaction-count": app.transaction_count}
    )


# noinspection PyUnusedLocal
@app.route("/transactions", methods=("GET",))
async def get_transactions(request: Request):
    return json(
        [t._asdict() async for t in gen_transactions(ascending=False)],
        headers={"x-transaction-count": app.transaction_count},
    )


# noinspection PyUnusedLocal
@app.route("/transactions", methods=("HEAD",))
async def head_transactions(request: Request):
    return empty(headers={"x-transaction-count": app.transaction_count})


# noinspection PyMethodParameters
class TransactionModel(pydantic.BaseModel):
    timestamp: datetime
    by: str
    who: str
    amount: int
    note: Optional[str] = None

    @pydantic.validator("amount")
    def amount_must_not_be_zero(cls: "Transaction", v: int) -> int:
        if v == 0:
            raise ValueError("amount must not be zero")
        return v

    @pydantic.validator("by")
    def by_must_not_be_empty(cls: "Transaction", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @pydantic.validator("who")
    def who_must_not_be_empty(cls: "Transaction", v: str) -> str:
        return must_not_be_empty(cls=cls, v=v)

    @pydantic.validator("who")
    def who_must_be_registered(cls: "Transaction", v: str) -> str:
        if v not in app.ticket_totals:
            raise ValueError(f"{v} is not a registered ticket holder")
        return v

    @property
    def as_tuple(self):
        return Transaction(
            timestamp=datetime_to_iso(self.timestamp),
            by=self.by,
            who=self.who,
            amount=self.amount,
            note=self.note,
        )


class TransactionListModel(pydantic.BaseModel):
    transactions: List[TransactionModel]


@app.route("/transactions", methods=("POST",))
async def post_transactions(request: Request):
    # Example transaction list:
    #   [{"timestamp": "2021-03-02T04:41:14Z",
    #     "by": "Helen",
    #     "who": "Link",
    #     "amount": -50,
    #     "note": "baby doll"},
    #    {"timestamp": "2021-03-02T04:51:00Z",
    #     "by": "Matt",
    #     "who": "Andrew",
    #     "amount": 1,
    #     "note": "brushed teeth"}]
    try:
        transaction_list = TransactionListModel(transactions=request.json)
    except pydantic.ValidationError as e:
        return json({"errors": e.errors()}, status=400)

    await app.db.executemany(
        "INSERT INTO transactions (timestamp, by, who, amount, note) "
        "VALUES (?, ?, ?, ?, ?)",
        tuple(t.as_tuple for t in transaction_list.transactions),
    )

    ticket_adjustments = {who: 0 for who in app.ticket_totals}
    for t in transaction_list.transactions:
        ticket_adjustments[t.who] += t.amount

    logger.info(
        "Adjusting ticket amounts: "
        + ", ".join(
            f"{who}: {app.ticket_totals[who]} -> {app.ticket_totals[who] + amount}"
            for who, amount in sorted(ticket_adjustments.items(), key=lambda i: i[0])
        )
    )

    for who, amount in ticket_adjustments.items():
        app.ticket_totals[who] += amount

    app.transaction_count += len(transaction_list.transactions)
    notify_transaction_waiters()

    return json(
        app.ticket_totals,
        headers={"x-transaction-count": app.transaction_count},
        status=201,
    )


#
# Helpers
#


class Transaction(NamedTuple):
    timestamp: str
    by: str
    who: str
    amount: int
    note: Optional[str] = None


class TransactionWithId(NamedTuple):
    id: str
    timestamp: str
    by: str
    who: str
    amount: int
    note: Optional[str] = None


def datetime_to_iso(dt: datetime) -> str:
    return f"{dt.replace(microsecond=0, tzinfo=None).isoformat()}Z"


async def gen_transactions(ascending: bool = False) -> AsyncIterator[TransactionWithId]:
    async with app.db.execute(
        f"SELECT * FROM transactions "
        f"ORDER BY timestamp {'ASC' if ascending else 'DESC'}"
    ) as cursor:
        async for fields in cursor:
            yield TransactionWithId(*fields)


async def transaction_greater_than(count: int):
    # TODO: This could be simplified a bit, since no client should really be waiting on
    #       on a distant future count. Either the client has seen the latest transaction
    #       or it hasn't.
    waiters = app.transaction_waiters
    while app.transaction_count <= count:
        waiters[count].waiter_count += 1
        try:
            await waiters[count].event.wait()
        finally:
            waiters[count].waiter_count -= 1
            if waiters[count].waiter_count == 0:
                waiters.pop(count)


def notify_transaction_waiters():
    for count_last_seen, waiter in app.transaction_waiters.items():
        if app.transaction_count > count_last_seen:
            waiter.event.set()


#
# Validators
#


# noinspection PyUnusedLocal
def must_not_be_empty(cls, v: str) -> str:
    if not v:
        raise ValueError("must not be empty")
    return v


if __name__ == "__main__":
    app.run(
        host="127.0.0.1",
        auto_reload=app.config.get("DEBUG", False),
        port=app.config.PORT,
        workers=app.config.WORKERS,
        debug=app.config.get("DEBUG", False),
    )
