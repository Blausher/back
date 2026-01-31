import asyncio
from contextlib import asynccontextmanager

from models.advertisement import Advertisement
from models.user import User
from repositories import advertisements as ads_repo
from repositories import users as users_repo


class DummyTransaction:
    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False


class DummyConnection:
    def __init__(self, row):
        self.row = row
        self.executed = []
        self.fetched = []

    def transaction(self):
        return DummyTransaction()

    async def execute(self, query, *args):
        self.executed.append((query, args))

    async def fetchrow(self, query, *args):
        self.fetched.append((query, args))
        return self.row


def test_user_repository_create(monkeypatch):
    async def run():
        expected = {"id": 7, "is_verified_seller": True}
        connection = DummyConnection(expected)

        @asynccontextmanager
        async def conn_stub():
            yield connection

        monkeypatch.setattr(users_repo, "get_pg_connection", conn_stub)

        repo = users_repo.UserRepository()
        user = await repo.create(user_id=7, is_verified_seller=True)

        assert isinstance(user, User)
        assert user.id == 7
        assert user.is_verified_seller is True

    asyncio.run(run())


def test_advertisement_repository_create(monkeypatch):
    async def run():
        expected = {
            "seller_id": 7,
            "is_verified_seller": True,
            "item_id": 10,
            "name": "Desk",
            "description": "Wooden desk",
            "category": 2,
            "images_qty": 1,
        }
        connection = DummyConnection(expected)

        @asynccontextmanager
        async def conn_stub():
            yield connection

        monkeypatch.setattr(ads_repo, "get_pg_connection", conn_stub)

        repo = ads_repo.AdvertisementRepository()
        ad = await repo.create(
            seller_id=expected["seller_id"],
            item_id=expected["item_id"],
            name=expected["name"],
            description=expected["description"],
            category=expected["category"],
            images_qty=expected["images_qty"],
        )

        assert isinstance(ad, Advertisement)
        assert ad.item_id == expected["item_id"]
        assert ad.seller_id == expected["seller_id"]
        assert ad.is_verified_seller is True

        assert connection.fetched

    asyncio.run(run())
