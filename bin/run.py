import os,sys

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(BASE_DIR)
from core import AGcollect

if __name__ == "__main__":
    client = AGcollect.Collect(sys.argv)
