from airflow.hooks.base import BaseHook
from ibm_boto3 import client
from ibm_botocore.client import Config


class IBMCloudObjectStorageHook(BaseHook):
    conn_name_attr = "ibm_cos_conn_id"
    default_conn_name = "ibm_cos_default"
    conn_type = "ibm_cos"
    hook_name = "IBM Cloud Object Storage"


    def __init__(self, ibm_cos_conn_id: str = default_conn_name):
        super().__init__()
        self.conn_id = ibm_cos_conn_id


    def get_client(self):
        conn = self.get_connection(self.conn_id)
        extra = conn.extra_dejson


        return client(
            "s3",
            aws_access_key_id=conn.login,
            aws_secret_access_key=conn.password,
            endpoint_url=extra.get("endpoint_url"),
            config=Config(signature_version="oauth")
        )


    def list_keys(self, bucket: str, prefix: str):
        cos = self.get_client()
        response = cos.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [obj["Key"] for obj in response.get("Contents", [])]