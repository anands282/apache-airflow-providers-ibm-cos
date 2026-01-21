from airflow.sensors.base import BaseSensorOperator
from airflow_provider_ibm_cos.hooks.cos import IBMCloudObjectStorageHook


class IBMCloudObjectSensor(BaseSensorOperator):
    def __init__(
        self,
        bucket: str,
        prefix: str,
        ibm_cos_conn_id: str = "ibm_cos_default",
        **kwargs,
        ):
            super().__init__(**kwargs)
            self.bucket = bucket
            self.prefix = prefix
            self.ibm_cos_conn_id = ibm_cos_conn_id


    def poke(self, context):
        hook = IBMCloudObjectStorageHook(self.ibm_cos_conn_id)
        keys = hook.list_keys(self.bucket, self.prefix)
        self.log.info("Found %d objects", len(keys))
        return len(keys) > 0