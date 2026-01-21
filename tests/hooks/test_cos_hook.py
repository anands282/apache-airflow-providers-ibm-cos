import pytest
from unittest.mock import MagicMock, patch

from airflow.models import Connection
from airflow_provider_ibm_cos.hooks.cos import IBMCloudObjectStorageHook


@pytest.fixture
def mock_connection():
    return Connection(
        conn_id="ibm_cos_default",
        login="test_access_key",
        password="test_secret_key",
        extra={
            "endpoint_url": "https://s3.test.cloud-object-storage.appdomain.cloud"
        },
    )


@patch("airflow_provider_ibm_cos.hooks.cos.client")
def test_get_client(mock_boto_client, mock_connection, monkeypatch):
    monkeypatch.setattr(
        IBMCloudObjectStorageHook,
        "get_connection",
        lambda self, _: mock_connection,
    )

    hook = IBMCloudObjectStorageHook()
    client = hook.get_client()

    mock_boto_client.assert_called_once()
    assert client == mock_boto_client.return_value


@patch("airflow_provider_ibm_cos.hooks.cos.client")
def test_list_keys_with_results(mock_boto_client, mock_connection, monkeypatch):
    mock_cos = MagicMock()
    mock_cos.list_objects_v2.return_value = {
        "Contents": [
            {"Key": "data/file1.csv"},
            {"Key": "data/file2.csv"},
        ]
    }

    mock_boto_client.return_value = mock_cos

    monkeypatch.setattr(
        IBMCloudObjectStorageHook,
        "get_connection",
        lambda self, _: mock_connection,
    )

    hook = IBMCloudObjectStorageHook()
    keys = hook.list_keys("my-bucket", "data/")

    assert keys == ["data/file1.csv", "data/file2.csv"]
    mock_cos.list_objects_v2.assert_called_once_with(
        Bucket="my-bucket", Prefix="data/"
    )


@patch("airflow_provider_ibm_cos.hooks.cos.client")
def test_list_keys_empty_bucket(mock_boto_client, mock_connection, monkeypatch):
    mock_cos = MagicMock()
    mock_cos.list_objects_v2.return_value = {}

    mock_boto_client.return_value = mock_cos

    monkeypatch.setattr(
        IBMCloudObjectStorageHook,
        "get_connection",
        lambda self, _: mock_connection,
    )

    hook = IBMCloudObjectStorageHook()
    keys = hook.list_keys("empty-bucket", "missing/")

    assert keys == []
