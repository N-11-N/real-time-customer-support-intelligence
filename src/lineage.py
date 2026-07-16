import os
from datetime import datetime, timezone
from openlineage.client import OpenLineageClient
from openlineage.client.event_v2 import RunEvent, RunState, Run, Job
from openlineage.client.transport.file import FileConfig, FileTransport
from openlineage.client.uuid import generate_new_uuid


class PipelineLineage:
    def __init__(self, path="lineage_events/openlineage.log"):
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.client = OpenLineageClient(transport=FileTransport(FileConfig(log_file_path=path)))
        self.run = Run(runId=str(generate_new_uuid()))
        self.job = Job(namespace="support_intelligence", name="end_to_end_pipeline")

    def emit(self, state: RunState):
        self.client.emit(RunEvent(
            eventType=state,
            eventTime=datetime.now(timezone.utc).isoformat(),
            run=self.run,
            job=self.job,
            producer="https://github.com/customer-support-intelligence",
        ))
