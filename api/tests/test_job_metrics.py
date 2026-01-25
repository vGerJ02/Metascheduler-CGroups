from datetime import datetime

from api.tests.test_jobs import test_job_1
from api.utils.database_helper import DatabaseHelper


def test_store_and_fetch_metrics(client):
    response = client.post('/jobs', json=test_job_1)
    assert response.status_code == 201

    captured_at = datetime(2024, 1, 1, 12, 0, 0)
    DatabaseHelper().insert_job_metric(
        job_id=1,
        cpu_usage=42.5,
        ram_usage=512.0,
        disk_read_bytes=123.0,
        disk_write_bytes=456.0,
        collected_at=captured_at,
    )

    metrics = DatabaseHelper().get_job_metrics(1)
    assert len(metrics) == 1
    assert metrics[0]['job_id'] == 1
    assert metrics[0]['cpu_usage'] == 42.5
    assert metrics[0]['ram_usage'] == 512.0
    assert metrics[0]['disk_read_bytes'] == 123.0
    assert metrics[0]['disk_write_bytes'] == 456.0
    assert metrics[0]['collected_at'] == captured_at.isoformat()
