from datetime import datetime
from pathlib import Path
import os

from api.constants.scheduler_type import SchedulerType
from api.constants.job_status import JobStatus


class Job:
    '''
    Job interface

    '''

    def __init__(self, id_: int = None, queue: int = -1, name: str = None,
                 created_at: datetime = None, owner: str = None,
                 status: JobStatus = JobStatus.TO_BE_QUEUED, path: Path = None, scheduler_type: str = '',
                 options: str = '', scheduler_job_id: int = None, scheduler_job_ref: str = None,
                 pwd: Path = None, quiet: bool = False):
        self.id_ = id_
        self.queue = queue
        self.name = name
        self.created_at = datetime.now() if created_at is None else created_at
        self.owner = owner
        if isinstance(status, JobStatus):
            self.status = status
        else:
            self.status = JobStatus(status)
        self.path = path
        self.options = options
        self.scheduler_job_id = scheduler_job_id
        self.scheduler_job_ref = scheduler_job_ref
        # self.pwd = "/home/metascheduler"
        self.pwd = str(pwd) if pwd else self._default_pwd()
        self.scheduler_type = self._validate_scheduler_code(scheduler_type)
        self.quiet = quiet

    def __str__(self):
        return f"Job(id={self.id_}, queue={self.queue}, name='{self.name}', " \
               f"created_at='{self.created_at}', owner='{self.owner}', " \
               f"status='{self.status.name}', path='{self.path}', " \
               f"scheduler_type='{self.scheduler_type}', options='{self.options}', " \
               f"scheduler_job_id={self.scheduler_job_id}, scheduler_job_ref='{self.scheduler_job_ref}', " \
               f"pwd='{self.pwd}')"

    @staticmethod
    def _default_pwd() -> str:

        ssh_user = os.getenv("SSH_USER")
        if ssh_user:
            return f"/home/{ssh_user}"
        return str(Path.home())

    @staticmethod
    def _validate_scheduler_code(code: str) -> str:
        '''
        Private method that validates whether the code is 'S' or 'H'. Stores only the letter.
        '''
        valid_codes = {'S', 'H'}
        if not isinstance(code, str):
            raise ValueError("Scheduler type must be a string.")

        code = code.strip().upper()
        if code not in valid_codes:
            raise ValueError(f"Scheduler type '{code}' not recognized. Only 'S' or 'H' allowed.")

        return code
