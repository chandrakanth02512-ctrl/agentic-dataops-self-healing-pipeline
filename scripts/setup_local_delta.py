"""Download the two Delta jars using the normal HTTPS proxy and Maven checksums.

Spark already supplies Scala and ANTLR. Fabric supplies Delta itself; do not run
this installer inside Fabric. Cached jars are checked on every invocation.
"""

import hashlib
from pathlib import Path
from urllib.request import urlopen


DESTINATION = Path(__file__).resolve().parents[1] / ".cache" / "delta"


def main():
    DESTINATION.mkdir(parents=True, exist_ok=True)
    for artifact in ("delta-spark_2.13", "delta-storage"):
        filename = f"{artifact}-4.0.0.jar"
        url = f"https://repo.maven.apache.org/maven2/io/delta/{artifact}/4.0.0/{filename}"
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
