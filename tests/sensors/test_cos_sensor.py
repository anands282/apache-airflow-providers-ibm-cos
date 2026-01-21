from unittest.mock import MagicMock, patch

from airflow_provider_ibm_cos.sensors.cos import IBMCloudObjectSensor


@patch("airflow_provider_ibm_cos.sensors.cos.IBMCloudObjectStorageHook")
def test_sensor_poke_success(mock_hook):
    instance = mock_hook.return_value
    instance.list_keys.return_value = ["file1.csv"]

    sensor = IBMCloudObjectSensor(
        task_id="test_sensor",
        bucket="my-bucket",
        prefix="data/",
    )

    result = sensor.poke(context={})

    assert result is True
    instance.list_keys.assert_called_once_with("my-bucket", "data/")


@patch("airflow_provider_ibm_cos.sensors.cos.IBMCloudObjectStorageHook")
def test_sensor_poke_failure(mock_hook):
    instance = mock_hook.return_value
    instance.list_keys.return_value = []

    sensor = IBMCloudObjectSensor(
        task_id="test_sensor",
        bucket="my-bucket",
        prefix="missing/",
    )

    result = sensor.poke(context={})

    assert result is False
    instance.list_keys.assert_called_once_with("my-bucket", "missing/")
