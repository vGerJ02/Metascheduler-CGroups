from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from api.constants.job_status import JobStatus
from api.interfaces.job import Job
from api.utils.database_helper import DatabaseHelper

router = APIRouter(
    prefix='/jobs',
    tags=['Jobs'],
)


class PostJobModel(BaseModel):
    name: str
    queue: int
    owner: str
    path: str
    scheduler_type: str
    options: str = ''
    qsub_options: str = ''
    pwd: str = None


class PutJobModel(BaseModel):
    name: str = None
    queue: int = None
    status: JobStatus = None
    path: str = None
    #scheduler_type = None
    options: str = None
    qsub_options: str | None = None


@router.get('')
def read_jobs(owner: str, status: JobStatus = None, queue: int = None):
    return DatabaseHelper().get_jobs(owner=owner, status=status, queue=queue)


@router.get('/{job_id}')
def read_job(job_id: int, owner: str):
    try:
        job = DatabaseHelper().get_job(job_id, owner)
        return DatabaseHelper().get_job(job_id, owner)
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e)) from e

@router.get('/{job_id}/metrics')
def read_job_metrics(job_id: int, owner: str):
    try:
        read_job(job_id, owner)
        return DatabaseHelper().get_job_metrics(job_id)
    except HTTPException as exc:
        raise exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.get('/{job_id}/metrics/nodes')
def read_job_node_metrics(job_id: int, owner: str):
    try:
        read_job(job_id, owner)
        return DatabaseHelper().get_job_node_metrics(job_id)
    except HTTPException as exc:
        raise exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

@router.get('/{job_id}/metrics')
def read_job_metrics(job_id: int, owner: str):
    try:
        read_job(job_id, owner)
        return DatabaseHelper().get_job_metrics(job_id)
    except HTTPException as exc:
        raise exc
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.post('', status_code=201)
def create_job(job: PostJobModel):
    try:
        DatabaseHelper().insert_job(Job(name=job.name, queue=job.queue,
                                        owner=job.owner, path=job.path, scheduler_type= job.scheduler_type,
                                        options=job.options, qsub_options=job.qsub_options, pwd=job.pwd))
        return {'status': 'success', 'message': 'Job created successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.put('/{job_id}')
def update_job(job_id: int, owner: str, job: PutJobModel):
    stored_job = read_job(job_id, owner)
    if stored_job.status.value is not JobStatus.TO_BE_QUEUED.value:
        raise HTTPException(
            status_code=400, detail='Only TO_BE_QUEUED jobs can be updated 🚫')
    try:
        DatabaseHelper().update_job(job_id, owner, Job(name=job.name or stored_job.name, queue=job.queue or stored_job.queue,
                                                       status=job.status or stored_job.status, path=job.path or stored_job.path,
                                                        scheduler_type=job.scheduler_type or stored_job,
                                                       options=job.options or stored_job.options,
                                                       qsub_options=job.qsub_options if job.qsub_options is not None else stored_job.qsub_options))
        return {'status': 'success', 'message': 'Job updated successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def update_job_status(job_id: int, owner: str, status: JobStatus):
    ''' Update the status of a job '''
    stored_job = read_job(job_id, owner)
    if stored_job.status.value is status.value:
        return {'status': 'success', 'message': 'Job status not changed'}
    try:
        DatabaseHelper().update_job_status(job_id, owner, status)
        return {'status': 'success', 'message': 'Job updated successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def set_job_scheduler_job_id(job_id: int, owner: str, scheduler_job_id: int):
    ''' Set the scheduler job ID of a job '''
    read_job(job_id, owner)
    try:
        DatabaseHelper().set_job_scheduler_id(job_id, owner, scheduler_job_id)
        return {'status': 'success', 'message': 'Job updated successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


def set_job_scheduler_job_ref(job_id: int, owner: str, scheduler_job_ref: str):
    ''' Set the scheduler job reference of a job '''
    read_job(job_id, owner)
    try:
        DatabaseHelper().set_job_scheduler_ref(job_id, owner, scheduler_job_ref)
        return {'status': 'success', 'message': 'Job updated successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e


@router.delete('/{job_id}')
def delete_job(job_id: int, owner: str):
    stored_job = read_job(job_id, owner)
    if stored_job.status.value is not JobStatus.TO_BE_QUEUED.value:
        raise HTTPException(
            status_code=400, detail='Only TO_BE_QUEUED jobs can be deleted 🚫')
    try:
        DatabaseHelper().delete_job(job_id, owner)
        return {'status': 'success', 'message': 'Job deleted successfully ✅'}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e)) from e
