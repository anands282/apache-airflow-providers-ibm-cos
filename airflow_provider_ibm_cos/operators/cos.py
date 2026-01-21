from airflow.models import BaseOperator
from airflow_provider_ibm_cos.hooks.cos import IBMCloudObjectStorageHook


class IBMCloudObjectUploadOperator(BaseOperator):
    def __init__(
        self,
        bucket: str,
        key: str,
        filename: str,
        ibm_cos_conn_id: str = "ibm_cos_default",
        **kwargs,
        ):
            super().__init__(**kwargs)
            self.bucket = bucket
            self.key = key
            self.filename = filename
            self.ibm_cos_conn_id = ibm_cos_conn_id


    def execute(self, context):
        hook = IBMCloudObjectStorageHook(self.ibm_cos_conn_id)
        cos = hook.get_client()
        cos.upload_file(self.filename, self.bucket, self.key)