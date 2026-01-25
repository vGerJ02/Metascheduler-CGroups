'''Database helper class.'''

import os
import sqlite3
from pathlib import Path
from datetime import datetime
from time import sleep
from typing import List
from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.interfaces.scheduler import Scheduler
from api.utils.singleton import Singleton
from api.interfaces.queue import Queue

DEFAULT_DATABASE_FILE = ''
if os.environ.get('TESTING'):
    DEFAULT_DATABASE_FILE = Path('./db/test_db.sqlite3')
else:
    DEFAULT_DATABASE_FILE = Path('./db/db.sqlite3')


class DatabaseHelper(metaclass=Singleton):
    '''Database helper class.

    This class is responsible for handling all database operations.

    Attributes:
        _db_file (Path): The path to the database file.
        _con (sqlite3.Connection): The connection to the database.
        _cur (sqlite3.Cursor): The cursor to the database.

    Methods:
        TODO AT THE END OF THE PROJECT
    '''

    def __init__(self, schedulers: List[Scheduler] = None, database_file: Path = None) -> None:
        '''Initializes the database helper.'''

        if os.environ.get('TESTING') == 'true':
            from api.classes.apache_hadoop import ApacheHadoop
            from api.classes.sge import SGE
            schedulers = []
            schedulers.append(ApacheHadoop())
            schedulers.append(SGE())
        if not schedulers:
            raise ValueError(
                'At least one scheduler must be provided in database initialization')
        self._db_file = database_file or DEFAULT_DATABASE_FILE
        self._con = sqlite3.connect(self._db_file)
        self._con.execute('PRAGMA foreign_keys = ON')
        self._cur = self._con.cursor()
        self._create_tables()
        self._insert_default_queues(schedulers)

    def _insert_default_queues(self, schedulers: List[Scheduler]) -> None:
        '''Inserts the default queues into the database.'''

        for scheduler in schedulers:
            self._cur.execute(
                'SELECT * FROM queues WHERE name = ?', (scheduler.name,))
            row = self._cur.fetchone()
            if row is None:
                self._cur.execute(
                    'INSERT INTO queues (name) VALUES (?)', (scheduler.name,))
                self._con.commit()

    def _create_tables(self) -> None:
        '''Creates the tables in the database.'''

        self._create_queues_table()
        self._create_jobs_table()
        self._create_job_metrics_table()

    def _create_queues_table(self) -> None:
        '''Creates the queues table in the database.'''

        self._cur.execute('''CREATE TABLE IF NOT EXISTS queues
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL)''')
        self._con.commit()

    def _create_jobs_table(self) -> None:
        '''Creates the jobs table in the database.'''

        self._cur.execute('''CREATE TABLE IF NOT EXISTS jobs
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
            queue_id INTEGER NOT NULL,
            name TEXT NOT NULL,
            created_at DATETIME NOT NULL,
            owner TEXT NOT NULL,
            status TEXT NOT NULL,
            path TEXT NOT NULL,
            options TEXT NOT NULL,
            scheduler_job_id INTEGER,
            pwd TEXT,
            scheduler_type TEXT NOT NULL, 
            FOREIGN KEY(queue_id) REFERENCES queues(id))''')
        self._con.commit()

    def _create_job_metrics_table(self) -> None:
        '''Creates the job metrics table in the database.'''

        self._cur.execute('''CREATE TABLE IF NOT EXISTS job_metrics
            (id INTEGER PRIMARY KEY AUTOINCREMENT,
            job_id INTEGER NOT NULL,
            collected_at DATETIME NOT NULL,
            cpu_usage REAL NOT NULL,
            ram_usage REAL NOT NULL,
            disk_read_bytes REAL NOT NULL DEFAULT 0,
            disk_write_bytes REAL NOT NULL DEFAULT 0,
            FOREIGN KEY(job_id) REFERENCES jobs(id) ON DELETE CASCADE)''')
        self._con.commit()
        self._ensure_job_metrics_columns()

    def _ensure_job_metrics_columns(self) -> None:
        '''Ensure job_metrics has columns needed for newer metrics.'''
        self._cur.execute('PRAGMA table_info(job_metrics)')
        columns = {row[1] for row in self._cur.fetchall()}
        if 'disk_read_bytes' not in columns:
            self._cur.execute(
                'ALTER TABLE job_metrics ADD COLUMN disk_read_bytes REAL NOT NULL DEFAULT 0'
            )
        if 'disk_write_bytes' not in columns:
            self._cur.execute(
                'ALTER TABLE job_metrics ADD COLUMN disk_write_bytes REAL NOT NULL DEFAULT 0'
            )
        self._con.commit()

    def _refresh_connection(self) -> None:
        '''Refreshes the connection to the database.'''

        self._con = sqlite3.connect(self._db_file)
        self._con.execute('PRAGMA foreign_keys = ON')
        self._cur = self._con.cursor()

    def reset_database_for_testing(self) -> None:
        '''Resets the database for testing purposes.'''

        self._refresh_connection()
        self._cur.execute('DROP TABLE IF EXISTS job_metrics')
        self._cur.execute('DROP TABLE IF EXISTS jobs')
        self._cur.execute('DROP TABLE IF EXISTS queues')
        self._con.commit()
        self._create_tables()
        from api.classes.apache_hadoop import ApacheHadoop
        from api.classes.sge import SGE
        self._insert_default_queues([ApacheHadoop(), SGE()])

    def insert_job(self, job: Job) -> None:
        '''Inserts a job into the database.'''

        try:
            self._refresh_connection()
            self._cur.execute(
                'INSERT INTO jobs (queue_id, name, created_at, owner, status, path, options, scheduler_job_id, pwd, scheduler_type) VALUES (?, ?, ?, ?, ?, ?, ?, NULL, ?, ?)',
                (job.queue, job.name, job.created_at, job.owner,
                 job.status.value, str(job.path), job.options, str(job.pwd), job.scheduler_type))

            self._con.commit()
        except sqlite3.IntegrityError as e:
            raise Exception(f'Queue {job.queue} not found') from e

    def get_queue_id(self, queue_name: str) -> int:
        '''Gets the ID of a queue by its name.'''

        self._refresh_connection()
        self._cur.execute(
            'SELECT id FROM queues WHERE name = ?', (queue_name,))
        row = self._cur.fetchone()
        if row is None:
            raise Exception(f'Queue {queue_name} not found')
        return row[0]

    def get_jobs(self, status: JobStatus = None, queue: int = None, owner: str = None) -> List[Job]:
        '''Gets all jobs from the database that match the given criteria.'''

        self._refresh_connection()
        query = 'SELECT * FROM jobs'
        params = []
        if owner and owner != 'root':
            query += ' WHERE owner = ?'
            params.append(owner)
            if status:
                query += ' AND status = ?'
                params.append(status.value)
            if queue:
                query += ' AND queue_id = ?'
                params.append(queue)
        else:
            query += ' WHERE 1=1'
            if status:
                query += ' AND status = ?'
                params.append(status.value)
            if queue:
                query += ' AND queue_id = ?'
                params.append(queue)

        self._cur.execute(query, params)
        rows = self._cur.fetchall()
        jobs = []
        for row in rows:
            job = Job(
                id_=row[0],
                queue=row[1],
                name=row[2],
                created_at=row[3],
                owner=row[4],
                status=row[5],
                path=row[6],
                options=row[7],
                scheduler_job_id=row[8],
                pwd=row[9],
                scheduler_type=row[10]
            )
            jobs.append(job)
        return jobs

    def get_job(self, job_id: int, owner: str) -> Job:
        '''Gets a job by its ID.'''

        self._refresh_connection()
        self._cur.execute(
            'SELECT * FROM jobs WHERE id = ? AND owner = ?', (job_id, owner,))
        row = self._cur.fetchone()
        if row is None:
            raise Exception('Job not found')
        return Job(
                id_=row[0],
                queue=row[1],
                name=row[2],
                created_at=row[3],
                owner=row[4],
                status=row[5],
                path=row[6],
                options=row[7],
                scheduler_job_id=row[8],
                pwd=row[9],
                scheduler_type=row[10]
            )

    def update_job(self, job_id: int, owner: str, job: Job) -> None:
        '''Updates a job in the database.'''

        self._refresh_connection()
        self._cur.execute(
            'UPDATE jobs SET name = ?, queue_id = ?, status = ?, path = ?, options = ?, scheduler_type = ? WHERE id = ? AND owner = ?', (job.name, job.queue, job.status.value, str(job.path), job.options, job.scheduler_type, job_id, owner))
        self._con.commit()

    def delete_job(self, job_id: int, owner: str) -> None:
        '''Deletes a job from the database.'''

        self._refresh_connection()
        self._cur.execute(
            'DELETE FROM jobs WHERE id = ? AND owner = ?', (job_id, owner))
        self._con.commit()

    def get_queues(self) -> List[Queue]:
        '''Gets all queues from the database.'''

        self._refresh_connection()
        self._cur.execute('SELECT id, name FROM queues')
        rows = self._cur.fetchall()
        return [Queue(*row) for row in rows]

    def set_job_scheduler_id(self, job_id: int, owner: str, scheduler_job_id: int) -> None:
        '''Sets the scheduler job ID of a job.'''
        self._refresh_connection()
        self._cur.execute(
            'UPDATE jobs SET scheduler_job_id = ? WHERE id = ? AND owner = ?', (scheduler_job_id, job_id, owner))
        self._con.commit()

    def insert_job_metric(
        self,
        job_id: int,
        cpu_usage: float,
        ram_usage: float,
        disk_read_bytes: float = 0.0,
        disk_write_bytes: float = 0.0,
        collected_at: datetime | None = None
    ) -> None:
        '''Insert a metric sample for a job.'''
        self._refresh_connection()
        collected_at = collected_at or datetime.utcnow()
        try:
            self._cur.execute(
                'INSERT INTO job_metrics (job_id, collected_at, cpu_usage, ram_usage, disk_read_bytes, disk_write_bytes) VALUES (?, ?, ?, ?, ?, ?)',
                (job_id, collected_at.isoformat(), cpu_usage, ram_usage, disk_read_bytes, disk_write_bytes)
            )
            self._con.commit()
        except sqlite3.IntegrityError as exc:
            raise Exception(f'Job {job_id} not found') from exc

    def get_job_metrics(self, job_id: int) -> List[dict]:
        '''Return all stored metric samples for a job ordered by collection time.'''
        self._refresh_connection()
        self._cur.execute(
            'SELECT id, job_id, collected_at, cpu_usage, ram_usage, disk_read_bytes, disk_write_bytes FROM job_metrics WHERE job_id = ? ORDER BY collected_at ASC',
            (job_id,)
        )
        rows = self._cur.fetchall()
        return [
            {
                'id': row[0],
                'job_id': row[1],
                'collected_at': row[2],
                'cpu_usage': row[3],
                'ram_usage': row[4],
                'disk_read_bytes': row[5],
                'disk_write_bytes': row[6],
            } for row in rows
        ]

