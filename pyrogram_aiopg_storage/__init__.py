__author__ = 'Dmytro Smyk'
__version__ = '0.2'

import time
from enum import Enum
from itertools import chain
from string import digits
from typing import Any, List, Tuple, Dict, Optional

from aiopg.sa import Engine, SAConnection
from pyrogram import raw, utils
from pyrogram.storage import Storage
from sqlalchemy import MetaData, Table, Column, Integer, Boolean, String, BIGINT, Index, \
    select, PrimaryKeyConstraint
from sqlalchemy.dialects.postgresql import BYTEA, insert, dialect as psql_dialect
from sqlalchemy.sql.ddl import CreateTable, CreateIndex, DropTable, DropIndex


DIGITS = set(digits)


class PeerType(Enum):
    """ Pyrogram peer types """
    USER = 'user'
    BOT = 'bot'
    GROUP = 'group'
    CHANNEL = 'channel'
    SUPERGROUP = 'supergroup'


def get_input_peer(peer):
    """ This function is almost blindly copied from pyrogram sqlite storage"""
    peer_id, peer_type, access_hash = peer['id'], peer['type'], peer['access_hash']

    if peer_type in {PeerType.USER.value, PeerType.BOT.value}:
        return raw.types.InputPeerUser(user_id=peer_id, access_hash=access_hash)

    if peer_type == PeerType.GROUP.value:
        return raw.types.InputPeerChat(chat_id=-peer_id)

    if peer_type in {PeerType.CHANNEL.value, PeerType.SUPERGROUP.value}:
        return raw.types.InputPeerChannel(
            channel_id=utils.get_channel_id(peer_id),
            access_hash=access_hash
        )

    raise ValueError(f"Invalid peer type: {peer['type']}")


async def fetchone(conn: SAConnection, query: str) -> Optional[Dict]:
    """ Small helper - fetches a single row from provided query """
    cursor = await conn.execute(query)
    row = await cursor.fetchone()
    return dict(row) if row else None


class PostgreSQLStorage(Storage):
    """
    Implementation of PostgreSQL storage.

    Example usage:

    >>> import aiopg
    >>> from pyrogram import Client
    >>>
    >>> db_pool = await aiopg.sa.create_engine(...)
    >>> session = PostgreSQLStorage(db_pool=db_pool, user_id=..., phone=...)
    >>> pyrogram = Client(session_name=session)
    >>> await pyrogram.connect()
    >>> ...

    """

    USERNAME_TTL = 8 * 60 * 60  # pyrogram constant

    def __init__(self,
                 db_pool: Engine,
                 user_id: int,
                 phone: str):
        """
        :param db_pool: prepared aiopg database pool
        :param user_id: application user identifier
        :param phone: telegram session phone
        """
        # normalize input phone and build a unique suffix id which will be used for our tables
        normalized_phone = ''.join([x for x in phone if x in DIGITS])
        self._session_id = f'{user_id}_{normalized_phone}'
        self._session_data = None
        self._pool: Engine = db_pool

        super().__init__(name=self._session_id)

        # define SQLAlchemy models
        self._meta = MetaData()

        self._t_session = Table(
            f'pyrogram_session_{self._session_id}', self._meta,

            Column('dc_id', Integer, primary_key=True),
            Column('test_mode', Boolean),
            Column('auth_key', BYTEA),
            Column('date', Integer, nullable=False),
            Column('user_id', Integer),
            Column('is_bot', Boolean),
            Column('phone', String(length=50))
        )

        self._t_peers = Table(
            f'peers_{self._session_id}', self._meta,

            Column('id', BIGINT),
            Column('access_hash', BIGINT),
            Column('type', String, nullable=False),
            Column('username', String),
            Column('phone_number', String),
            Column('last_update_on', Integer),
            PrimaryKeyConstraint('id', name=f'pk_peers_{self._session_id}')
        )

        # indexes
        self._i_peers_username = Index(
            f'idx_peers_username_{self._session_id}',
            self._t_peers.c.username
        )
        self._i_peers_phone_number = Index(
            f'idx_peers_phone_number_{self._session_id}',
            self._t_peers.c.phone_number
        )

    async def open(self):
        """ Initialize pyrogram session"""
        async with self._pool.acquire() as conn:
            # check if pyro tables are already exist
            if not await self._is_session_table_exists(conn):
                # if no - execute all necessary DDL
                await self._execute_pyrogram_ddl(conn)

            async with self._pool.acquire() as conn:
                session_data = await fetchone(conn=conn, query=select(self._t_session.c))
                # postgresql returns bytea type as a `memoryview` so we convert
                # it to bytes, because `bytes` type is expected by pyrogram
                auth_key = bytes(session_data['auth_key']) if session_data['auth_key'] else None
                session_data['auth_key'] = auth_key
                self._session_data = session_data

    async def _is_session_table_exists(self, conn: SAConnection):
        """ Check if pyrogram tables are already exists using postgresql internal table """
        query = f"""
            SELECT COUNT(*) AS cnt
            FROM pg_tables
            WHERE tablename = '{self._t_session.name}'
        """
        result = await fetchone(conn=conn, query=query)
        return bool(result['cnt'])

    async def _execute_pyrogram_ddl(self, conn):
        """ Create initial structures """
        await conn.execute(str(CreateTable(self._t_session)))
        await conn.execute(str(CreateTable(self._t_peers)))
        await conn.execute(str(CreateIndex(self._i_peers_username)))
        await conn.execute(str(CreateIndex(self._i_peers_phone_number)))
        await conn.execute(insert(self._t_session).values([2, None, None, 0, None, None]))

    async def save(self):
        """ On save we update the date """
        await self.date(int(time.time()))

    async def close(self):
        """ Nothing to do on close """
        pass

    async def delete(self):
        """ Delete all the tables and indexes """
        async with self._pool.acquire() as conn:
            await conn.execute(str(DropIndex(self._i_peers_phone_number)))
            await conn.execute(str(DropIndex(self._i_peers_username)))
            await conn.execute(str(DropTable(self._t_session)))
            await conn.execute(str(DropTable(self._t_peers)))

    async def update_peers(self, peers: List[Tuple[int, int, str, str, str]]):
        """ Copied and adopted from pyro sqlite storage"""
        if not peers:
            return

        now = int(time.time())
        deduplicated_peers = []
        seen_ids = set()

        # deduplicate peers to avoid possible `CardinalityViolation` error
        for peer in peers:
            peer_id, *_ = peer
            if peer_id in seen_ids:
                continue
            seen_ids.add(peer_id)
            # enrich peer with timestamp and append
            deduplicated_peers.append(tuple(chain(peer, (now,))))

        # construct insert query
        insert_query = insert(self._t_peers).values(deduplicated_peers)
        final_query = (
            insert_query.on_conflict_do_update(
                constraint=self._t_peers.primary_key.name,
                set_={
                    'access_hash': insert_query.excluded.access_hash,
                    'username': insert_query.excluded.username,
                    'phone_number': insert_query.excluded.phone_number,
                    'last_update_on': insert_query.excluded.last_update_on
                }
            )
            .compile(dialect=psql_dialect())
            .statement
        )

        async with self._pool.acquire() as conn:
            await conn.execute(final_query)

    async def get_peer_by_id(self, peer_id: int):
        query = (
            select([self._t_peers.c.id, self._t_peers.c.access_hash, self._t_peers.c.type])
            .where(self._t_peers.c.id == peer_id)
        )
        async with self._pool.acquire() as conn:
            try:
                r = await fetchone(conn, query)
            except:
                raise KeyError(f"ID not found: {peer_id}")

        if r is None:
            raise KeyError(f"ID not found: {peer_id}")

        return get_input_peer(r)

    async def get_peer_by_username(self, username: str):
        query = (
            select([self._t_peers.c.id,
                    self._t_peers.c.access_hash,
                    self._t_peers.c.type,
                    self._t_peers.c.last_update_on])
            .where(self._t_peers.c.username == username)
        )

        async with self._pool.acquire() as conn:
            r = await fetchone(conn, query)

        if r is None:
            raise KeyError(f"Username not found: {username}")

        if int(time.time() - r['last_update_on']) > self.USERNAME_TTL:
            raise KeyError(f"Username expired: {username}")

        return get_input_peer(r)

    async def get_peer_by_phone_number(self, phone_number: str):
        query = (
            select([self._t_peers.c.id, self._t_peers.c.access_hash, self._t_peers.c.type])
            .where(self._t_peers.c.phone_number == phone_number)
        )
        async with self._pool.acquire() as conn:
            r = await fetchone(conn, query)

        if r is None:
            raise KeyError(f"Phone number not found: {phone_number}")

        return get_input_peer(r)

    async def _set(self, column, value: Any):
        query = self._t_session.update().values({column: value})
        async with self._pool.acquire() as conn:
            await conn.execute(query)
        self._session_data[column] = value  # update local copy

    async def _accessor(self, column, value: Any = object):
        return self._session_data[column] if value == object else await self._set(column, value)

    async def dc_id(self, value: int = object):
        return await self._accessor('dc_id', value)

    async def test_mode(self, value: bool = object):
        return await self._accessor('test_mode', value)

    async def auth_key(self, value: bytes = object):
        return await self._accessor('auth_key', value)

    async def date(self, value: int = object):
        return await self._accessor('date', value)

    async def user_id(self, value: int = object):
        return await self._accessor('user_id', value)

    async def is_bot(self, value: bool = object):
        return await self._accessor('is_bot', value)

