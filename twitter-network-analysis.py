import os
import sys
from orchestrator import Orchestrator

PROJECT_PATH = os.path.abspath('')
INPUT_PATH = os.path.join(PROJECT_PATH, 'input/')
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'output/')


def main():
    project_name = sys.argv[1]

    o = Orchestrator(project_name, INPUT_PATH, OUTPUT_PATH)
    o.execute()


if __name__ == '__main__':
    main()
