from __future__ import annotations
import subprocess
import shlex
from dataclasses import dataclass
from datetime import datetime

@dataclass
class RunResult:
    started_at: datetime
    ended_at: datetime
    returncode: int
    stdout: str
    stderr: str
    cmd: str

def run_cmd(cmd: list[str], cwd: str | None = None, env: dict | None = None, timeout_s: int | None = None) -> RunResult:
    started = datetime.now()
    p = subprocess.run(
        cmd,
        cwd=cwd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout_s,
    )
    ended = datetime.now()
    return RunResult(
        started_at=started,
        ended_at=ended,
        returncode=p.returncode,
        stdout=p.stdout or "",
        stderr=p.stderr or "",
        cmd=" ".join(shlex.quote(x) for x in cmd),
    )
