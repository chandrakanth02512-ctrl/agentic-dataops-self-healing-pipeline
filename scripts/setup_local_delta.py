"""Download the selected Delta jars using normal HTTPS proxy and Maven checksums.

Spark supplies the shared JVM dependencies. Fabric supplies Delta itself; do not run
this installer inside Fabric. Cached jars are checked on every invocation.
"""

import hashlib
from importlib.metadata import version
from pathlib import Path
from urllib.request import urlopen


DESTINATION = Path(__file__).resolve().parents[1] / ".cache" / "delta"


def main():
    DESTINATION.mkdir(parents=True, exist_ok=True)
    delta_version = version("delta-spark")
    if delta_version not in ("4.0.0", "4.2.0"):
        raise RuntimeError(f"Unsupported local Delta version: {delta_version}")
    artifacts = ["delta-spark_2.13", "delta-storage"]
    if delta_version == "4.2.0":
        artifacts += ["delta-kernel-api", "delta-kernel-defaults", "delta-kernel-unitycatalog"]
    for artifact in artifacts:
        filename = f"{artifact}-{delta_version}.jar"
        url = f"https://repo.maven.apache.org/maven2/io/delta/{artifact}/{delta_version}/{filename}"
        # Maven publishes SHA-1 sidecars for this release, fetched over TLS.
        with urlopen(url + ".sha1", timeout=60) as response:
            expected = response.read().decode().strip().split()[0]
        destination = DESTINATION / filename
        if destination.exists():
            payload = destination.read_bytes()
            if hashlib.sha1(payload).hexdigest() == expected:
                print(f"Verified {filename}")
                continue
        with urlopen(url, timeout=120) as response:
            payload = response.read()
        if hashlib.sha1(payload).hexdigest() != expected:
            raise RuntimeError(f"Maven checksum mismatch: {filename}")
        temporary = destination.with_suffix(".download")
        temporary.write_bytes(payload)
        temporary.replace(destination)
        print(f"Installed and verified {filename}")


if __name__ == "__main__":
    main()
