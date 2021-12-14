Pyrogram aiopg storage
======================

Motivation
----------

I need to work with multiple telegram session and keep all of them in a single PostgreSQL database.
I use aiopg with SQLAlchemy hence I've written a wrapper for this stack.
Maybe someone also requires such

Usage
-----

```python

import aiopg.sa
from pyrogram import Client
from pyrogram_aiopg_storage import PostgreSQLStorage

db_pool = await aiopg.sa.create_engine(...)
session = PostgreSQLStorage(db_pool=db_pool, user_id=..., phone=...)
pyrogram = Client(session_name=session)
await pyrogram.connect()

```