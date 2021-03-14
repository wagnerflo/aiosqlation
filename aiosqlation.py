import aiosqlite
import asyncio
import functools
import sqlite3

class Transaction:
    def __init__(self, db, connection, type, timeout):
        self._db = db
        self._connection = connection
        self._type = type
        self._timeout = timeout
        self._release_connection = False

    async def __aenter__(self):
        start_time = time()

        while True:
            try:
                if self._connection is None:
                    self._connection = await self._db.aquire_connection()
                    self._release_connection = True

                await self._connection.execute(
                    'BEGIN {} TRANSACTION'.format(
                        '' if self._type is None else self._type.name
                    )
                )

            except OperationalError as e:
                if e.args != ('database is locked',):
                    raise

                if ( self._timeout is not None and
                     time() >= start_time + self._timeout ):
                    if self._release_connection:
                        self._db.release_connection(self._connection)
                    raise TimeoutError() from None

                await async_sleep(.5)

            else:
                return self._connection

    async def __aexit__(self, exc_type, exc, tb):
        if self._connection._connection.in_transaction:
            if exc is None:
                await self._connection.commit()
            else:
                await self._connection.rollback()

        if self._release_connection:
            self._db.release_connection(self._connection)

class Transaction:
    DEFERRED = 'DEFERRED'
    IMMEDIATE = 'IMMEDIATE'
    EXCLUSIVE = 'EXCLUSIVE'

class Connection(aiosqlite.Connection):
    def transaction(self, transaction_type):
        return Transaction(self, transaction_type)

    def deferred(self):
        return Transaction(self, Transaction.DEFERRED)

    def immediate(self):
        return Transaction(self, Transaction.IMMEDIATE)

    def exclusive(self):
        return Transaction(self, Transaction.EXCLUSIVE)

class ConnectionWaiter:
    def __init__(self, pool, transaction_type=None):
        self.pool = pool
        self.connection = None
        self.transaction_type = transaction_type

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        self.connection = await self.pool.acquire_connection()
        if transaction_type is None:
            return self.connection
        else:
            return self.connection

    async def __aexit__(self, exc_type, exc, tb):
        self.pool.return_connection(self.connection)

class ConnectionPool:
    def __init__(self, database, *, iter_chunk_size=64, maxsize=8, **kws):
        if isinstance(database, bytes):
            loc = database.decode('utf-8')
        else:
            loc = str(database)

        self._connector = functools.partial(sqlite3.connect, loc, **kws)
        self._iter_chunk_size = iter_chunk_size
        self._queue = asyncio.Queue(maxsize=maxsize)
        self._size = 0
        self._closed = False

    @property
    def maxsize(self):
        return self._queue.maxsize

    @property
    def size(self):
        return self._size

    @property
    def available(self):
        return self._queue.qsize()

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.close()

    async def create_connection(self):
        self._size = self._size + 1
        connection = await Connection(self._connector, self._iter_chunk_size)
        await connection.execute('PRAGMA journal_mode=WAL')
        await connection.execute('PRAGMA busy_timeout=0')
        return connection

    async def acquire_connection(self):
        if self._closed:
            raise PoolClosed()

        if self._queue.empty() and self._size < self._queue.maxsize:
            return await self.create_connection()
        else:
            return await self._queue.get()

    def return_connection(self, connection):
        self._queue.put_nowait(connection)

    def connect(self):
        return ConnectionWaiter(self)

    def transaction(self):
        pass

    async def close(self):
        if self._closed:
            return

        # indicate that pool is closing; makes sure acquire_connection
        # will give out no more connections
        self._closed = True

        # repeatedly pick all connections from the queue
        connections = [ self._queue.get() for _ in range(self._size) ]

        # as soon as a connection appears, close it
        for connection in asyncio.as_completed(connections):
            await (await connection).close()

        self._size = 0

__all__ = (
    'ConnectionPool',
)
