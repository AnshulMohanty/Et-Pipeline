# cli/ingest_file.py

import sys, json, uuid, os

from backend.app.ingest_pipeline import parse_file_bytes

def make_job_from_file(path):

    docs = parse_file_bytes(path)

    job = {"job_id": str(uuid.uuid4()), "documents": docs}

    print(json.dumps(job))

if __name__ == "__main__":

    if len(sys.argv) < 2:

        print("usage: python cli/ingest_file.py path")

        sys.exit(2)

    print(make_job_from_file(sys.argv[1]))

