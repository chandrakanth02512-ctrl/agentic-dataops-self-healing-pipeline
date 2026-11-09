import os

import pytest
from agentic_dataops.session import local_spark


@pytest.fixture(scope="session")
def spark():
    # A loopback address works reliably in Codespaces and CI runners.
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    session = local_spark(delta=True)
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()
