#!/usr/bin/env python3

import argparse
import json
import sys
from concurrent.futures import ProcessPoolExecutor
from typing import Any
from tqdm import tqdm


def recursive_deserialize(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: recursive_deserialize(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [recursive_deserialize(v) for v in obj]

    if isinstance(obj, str):
        try:
            decoded = json.loads(obj)
        except (json.JSONDecodeError, TypeError):
            return obj

        # Only expand serialized objects/arrays.
        if not isinstance(decoded, (dict, list)):
            return obj

        return recursive_deserialize(decoded)

    return obj


def process_line(line: str) -> str:
    data = json.loads(line)
    # data = recursive_deserialize(data)
    data["tools"] = recursive_deserialize(data["tools"])
    return json.dumps(data, ensure_ascii=False)# , separators=(",", ":"))


def main():
    parser = argparse.ArgumentParser(
        description="Recursively deserialize embedded JSON in a JSONL file."
    )
    parser.add_argument("input", help="Input JSONL file, or - for stdin")
    parser.add_argument("-o", "--output", help="Output JSONL file, default: stdout")
    parser.add_argument(
        "-j", "--jobs",
        type=int,
        default=None,
        help="Number of worker processes (default: CPU count)",
    )
    parser.add_argument(
        "--chunksize",
        type=int,
        default=100,
        help="ProcessPool map chunksize (default: 100)",
    )
    args = parser.parse_args()

    fin = sys.stdin if args.input == "-" else open(
        args.input, "r", encoding="utf-8"
    )
    fout = (
        open(args.output, "w", encoding="utf-8")
        if args.output
        else sys.stdout
    )

    try:
        lines = (line for line in fin if line.strip())

        with ProcessPoolExecutor(max_workers=args.jobs) as executor:
            results = executor.map(
                process_line,
                lines,
                chunksize=args.chunksize,
            )
            for result in tqdm(results):
                fout.write(result)
                fout.write("\n")
    finally:
        if fin is not sys.stdin:
            fin.close()
        if fout is not sys.stdout:
            fout.close()


if __name__ == "__main__":
    main()
