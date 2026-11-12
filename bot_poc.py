"""Single-player compatibility entry for the managed, authenticated lifecycle."""
from pathlib import Path
import subprocess
import sys


def simulate_single_bot():
    return subprocess.call([sys.executable,str(Path(__file__).with_name('storm.py')),'locust',
                            '--users','1','--spawn-rate','1','--run-time','10s',*sys.argv[1:]])


if __name__=='__main__':
    raise SystemExit(simulate_single_bot())
