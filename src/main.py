import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from historical import run_historical
from realtime import run_realtime

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python src/main.py [historical|realtime]")
        sys.exit(1)
    mode = sys.argv[1]
    if mode == "historical": run_historical()
    elif mode == "realtime": run_realtime()
    else: sys.exit(1)
