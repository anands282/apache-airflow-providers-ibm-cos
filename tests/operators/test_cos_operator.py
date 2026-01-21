from unittest.mock import MagicMock, patch

from airflow_provider_ibm_cos.operators.cos import IBMCloudObjectUploadOperator


@patch("airflow_provider_ibm_cos.operators.cos.IBMCloudObjectStorageHook")
def test_upload_operator(mock_hook):
    mock_client = MagicMock()
    mock_hook.return_value.get_client.return_value = mock_client

    operator = IBMCloudObjectUploadOperator(
        task_id="upload_test",
        bucket="my-bucket",
        key="data/file.csv",
        filename="/tmp/file.csv",
    )

    operator.execute(context={})

    mock_client.upload_file.assert_called_once_with(
        "/tmp/file.csv",
        "my-bucket",
        "data/file.csv",
    )
