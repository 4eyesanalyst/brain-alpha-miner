"""Encrypts the miner's working state so it can live in a public repository.

    python vault.py pack      miner/  ->  miner_state.enc   (encrypted, safe to commit)
    python vault.py unpack    miner_state.enc  ->  miner/
    python vault.py newkey    print a fresh key (store it as MINER_STATE_KEY)

The key comes from the MINER_STATE_KEY environment variable (a GitHub secret in the cloud;
a Windows user variable locally). Without the key the file is unreadable, so your alpha
formulas stay private even though the code is public.
"""
import io
import sys
import tarfile
from pathlib import Path

from cryptography.fernet import Fernet

from brain import _credential

ROOT = Path(__file__).parent
STATE_DIR = ROOT / "miner"
VAULT = ROOT / "miner_state.enc"
SKIP = {"miner.lock", "new_ready.md"}
LOG_TAIL_LINES = 5000


def _fernet():
    return Fernet(_credential("MINER_STATE_KEY").encode())


def pack():
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for path in sorted(STATE_DIR.iterdir()):
            if path.name in SKIP or path.suffix == ".tmp" or not path.is_file():
                continue
            data = path.read_bytes()
            if path.name == "miner.log":  # keep the log from growing without bound
                data = b"\n".join(data.splitlines()[-LOG_TAIL_LINES:]) + b"\n"
            info = tarfile.TarInfo(path.name)
            info.size = len(data)
            tar.addfile(info, io.BytesIO(data))
    VAULT.write_bytes(_fernet().encrypt(buf.getvalue()))
    print(f"packed {STATE_DIR.name}/ -> {VAULT.name} ({VAULT.stat().st_size // 1024} KB)")


def unpack():
    if not VAULT.exists():
        print("no saved state yet; starting fresh")
        return
    STATE_DIR.mkdir(exist_ok=True)
    data = _fernet().decrypt(VAULT.read_bytes())
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
        tar.extractall(STATE_DIR, filter="data")
    print(f"unpacked {VAULT.name} -> {STATE_DIR.name}/")


if __name__ == "__main__":
    command = sys.argv[1] if len(sys.argv) > 1 else ""
    if command == "pack":
        pack()
    elif command == "unpack":
        unpack()
    elif command == "newkey":
        print(Fernet.generate_key().decode())
    else:
        sys.exit(__doc__)
