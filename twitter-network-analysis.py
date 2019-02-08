import os
from orchestrator import Orchestrator

PROJECT_PATH = os.path.dirname(os.path.realpath(__file__))
INPUT_PATH = os.path.join(PROJECT_PATH, 'input/')
OUTPUT_PATH = os.path.join(PROJECT_PATH, 'output/')
PROJECT_NAME = 'uk_healthcare_infomap_small'


def main():
    print()
    o = Orchestrator(PROJECT_NAME, INPUT_PATH, OUTPUT_PATH)
    results = o.execute()

    return results


if __name__ == "__main__":
    main()
