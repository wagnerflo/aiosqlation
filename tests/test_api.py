from unittest import IsolatedAsyncioTestCase
from aiosqlation import *

CONNECTOR = 'file::memory:?cache=shared'
MAXSIZE = 4

class TestPool(IsolatedAsyncioTestCase):
    async def test_lifecycle_manual(self):
        pool = ConnectionPool(CONNECTOR, maxsize=MAXSIZE)
        self.assertEqual(pool.maxsize, MAXSIZE)
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.available, 0)
        connection = await pool.connect()
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.available, 0)
        pool.return_connection(connection)
        self.assertEqual(pool.size, 1)
        self.assertEqual(pool.available, 1)
        await pool.close()
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.available, 0)

    async def test_lifecycle_with(self):
        async with ConnectionPool(CONNECTOR, maxsize=MAXSIZE) as pool:
            self.assertEqual(pool.maxsize, MAXSIZE)
            self.assertEqual(pool.size, 0)
            self.assertEqual(pool.available, 0)
            async with pool.connect() as connection:
                self.assertEqual(pool.size, 1)
                self.assertEqual(pool.available, 0)
            self.assertEqual(pool.size, 1)
            self.assertEqual(pool.available, 1)
        self.assertEqual(pool.size, 0)
        self.assertEqual(pool.available, 0)
