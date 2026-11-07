"""Check Fabric's native runtime without installing or replacing its libraries."""

import sys
from importlib.metadata import version


def validate_fabric_runtime(spark, *, delta_version=None, python_version=None):
    delta_version = delta_version or version("delta-spark")
    python_version = python_version or sys.version_info[:3]
    if (not spark.version.startswith("4.1.")
            or not delta_version.startswith("4.2.")
            or tuple(python_version[:2]) != (3, 13)):
        raise RuntimeError(
            "Fabric Runtime 2.0 requires native Spark 4.1.x, Delta 4.2.x and Python 3.13; "
            "attach the approved AgenticDataOps_Environment without replacing built-in libraries"
        )
    return {"spark": spark.version, "delta": delta_version,
            "python": ".".join(str(part) for part in python_version)}
