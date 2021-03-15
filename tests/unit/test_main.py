import asyncio
import json


def custom_headers(headers) -> dict:
    return {key: value for key, value in headers.items() if key.startswith("x-")}


def status_and_headers_and_json(result):
    return result.status_code, custom_headers(result.headers), result.json()


async def create_basic_transactions(test_cli):
    r = await test_cli.post(
        "/ticket_holders/Elliot",
        data=json.dumps({"timestamp": "2021-03-02T04:40:00Z", "by": "Magda"}),
    )
    assert status_and_headers_and_json(r) == (
        201,
        {"x-transaction-count": "1"},
        {"Elliot": 0},
    )

    r = await test_cli.post(
        "/ticket_holders/Darlene",
        data=json.dumps({"timestamp": "2021-03-02T04:41:00Z", "by": "Edward"}),
    )
    assert status_and_headers_and_json(r) == (
        201,
        {"x-transaction-count": "2"},
        {"Elliot": 0, "Darlene": 0},
    )

    r = await test_cli.post(
        "/transactions",
        data=json.dumps(
            [
                {
                    "timestamp": "2021-03-02T04:42:00Z",
                    "by": "Edward",
                    "who": "Darlene",
                    "amount": 500,
                    "note": "Assembled her first PC!",
                },
                {
                    "timestamp": "2021-03-02T04:43:00Z",
                    "by": "Magda",
                    "who": "Elliot",
                    "amount": 5,
                    "note": "Brushed teeth",
                },
            ]
        ),
    )
    assert status_and_headers_and_json(r) == (
        201,
        {"x-transaction-count": "4"},
        {"Elliot": 5, "Darlene": 500},
    )


async def test_lifecycle(test_cli):
    r = await test_cli.get("/tickets")
    assert status_and_headers_and_json(r) == (200, {"x-transaction-count": "0"}, {})

    await create_basic_transactions(test_cli)

    r = await test_cli.head("/transactions")
    assert (r.status_code, custom_headers(r.headers), r.text) == (
        204,
        {"x-transaction-count": "4"},
        "",
    )

    r = await test_cli.get("/transactions")
    assert (r.status_code, custom_headers(r.headers), r.json()) == (
        200,
        {"x-transaction-count": "4"},
        [
            {
                "id": 4,
                "timestamp": "2021-03-02T04:43:00Z",
                "by": "Magda",
                "who": "Elliot",
                "amount": 5,
                "note": "Brushed teeth",
            },
            {
                "id": 3,
                "timestamp": "2021-03-02T04:42:00Z",
                "by": "Edward",
                "who": "Darlene",
                "amount": 500,
                "note": "Assembled her first PC!",
            },
            {
                "id": 2,
                "timestamp": "2021-03-02T04:41:00Z",
                "by": "Edward",
                "who": "Darlene",
                "amount": 0,
                "note": "Initial registration",
            },
            {
                "id": 1,
                "timestamp": "2021-03-02T04:40:00Z",
                "by": "Magda",
                "who": "Elliot",
                "amount": 0,
                "note": "Initial registration",
            },
        ],
    )

    r = await test_cli.get("/tickets")
    assert status_and_headers_and_json(r) == (
        200,
        {"x-transaction-count": "4"},
        {
            "Darlene": 500,
            "Elliot": 5,
        },
    )


async def test_long_polling(test_cli):
    """
    Long polling is a process in which the HTTP client doesn't receive a reply until
    some condition is met. Multiple long-polls are allowed simultaneously, and with
    normal polls. Each long-poll might be waiting for a different transaction amount.
    """

    async def get_tickets_immediately():
        """
        Without the "last-transaction-count" parameter, the ticket counts should
        return immediately.
        """
        r = await test_cli.get(f"/tickets")
        assert status_and_headers_and_json(r) == (
            200,
            {"x-transaction-count": "0"},
            {},
        )

    async def get_tickets_before_rewards():
        """
        The first two transactions are the creation of the initial ticket holders. This
        long-poll should return once transaction 2 has completed. At this point, the
        ticket holders should exist but they haven't received any tickets yet.
        """
        r = await test_cli.get(f"/tickets?last-transaction-count=1")
        assert status_and_headers_and_json(r) == (
            200,
            {"x-transaction-count": "2"},
            {
                "Darlene": 0,
                "Elliot": 0,
            },
        )

    async def get_tickets_after_rewards():
        """
        The last two transactions are adding a ticket amount to both ticket holders.
        This long-poll should return once transaction 4 has completed. At this point,
        the ticket holders should both have tickets.
        """
        r = await test_cli.get(f"/tickets?last-transaction-count=3")
        assert status_and_headers_and_json(r) == (
            200,
            {"x-transaction-count": "4"},
            {
                "Darlene": 500,
                "Elliot": 5,
            },
        )

    # Issue all three ticket requests in parallel and wait for all to complete.
    await asyncio.gather(
        get_tickets_immediately(),
        get_tickets_before_rewards(),
        get_tickets_after_rewards(),
        create_basic_transactions(test_cli),
    )


async def test_rename_ticket_holder(test_cli):
    """
    A ticket holder can be renamed with a PATCH request. All previous transactions will
    be updated to reflect the new name, and a 0-amount transaction will be added with
    a note about the renaming.
    """

    await create_basic_transactions(test_cli)

    r = await test_cli.patch(
        "/ticket_holders/Darlene",
        data=json.dumps(
            {"timestamp": "2021-03-02T04:44:00Z", "by": "Edward", "to": "Dar"}
        ),
    )
    assert status_and_headers_and_json(r) == (
        200,
        {"x-transaction-count": "5"},
        {
            "Dar": 500,
            "Elliot": 5,
        },
    )

    # Get all of the transactions to ensure that the renaming is accurately reflected
    # in the database.

    r = await test_cli.get("/transactions")
    assert (r.status_code, custom_headers(r.headers), r.json()) == (
        200,
        {"x-transaction-count": "5"},
        [
            {
                "id": 5,
                "timestamp": "2021-03-02T04:44:00Z",
                "by": "Edward",
                "who": "Dar",
                "amount": 0,
                "note": 'Ticket holder renamed from "Darlene"',
            },
            {
                "id": 4,
                "timestamp": "2021-03-02T04:43:00Z",
                "by": "Magda",
                "who": "Elliot",
                "amount": 5,
                "note": "Brushed teeth",
            },
            {
                "id": 3,
                "timestamp": "2021-03-02T04:42:00Z",
                "by": "Edward",
                "who": "Dar",
                "amount": 500,
                "note": "Assembled her first PC!",
            },
            {
                "id": 2,
                "timestamp": "2021-03-02T04:41:00Z",
                "by": "Edward",
                "who": "Dar",
                "amount": 0,
                "note": "Initial registration",
            },
            {
                "id": 1,
                "timestamp": "2021-03-02T04:40:00Z",
                "by": "Magda",
                "who": "Elliot",
                "amount": 0,
                "note": "Initial registration",
            },
        ],
    )


# TODO: test more negative cases


async def test_rejected_zero_amount(test_cli):
    await create_basic_transactions(test_cli)

    r = await test_cli.post(
        "/transactions",
        data=json.dumps(
            [
                {
                    "timestamp": "2021-03-02T04:45:00Z",
                    "by": "Edward",
                    "who": "Darlene",
                    "amount": 0,
                    "note": "Bad transaction",
                },
            ]
        ),
    )
    assert r.status_code == 400
