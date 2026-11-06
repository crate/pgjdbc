"""Compares the failing tests of a stock-suite Maven log with the list of expected failures.

Usage: check.py <maven log> <expected failures>      fail on any difference
       check.py --write <maven log> <expected failures>  rewrite the list from the log
"""
import re
import sys

ANSI = re.compile(r"\x1b\[[0-9;]*m")
TIMESTAMP = re.compile(r"^\S+Z ")
CLASS = re.compile(r"<<< (?:FAILURE|ERROR)! - in (\S+)")
TEST = re.compile(r"^\[ERROR\] (.+?)\s+Time elapsed: .*<<< (?:FAILURE|ERROR)!$")

HEADER = """\
# Stock pgJDBC tests that fail against PostgreSQL because the fork behaves differently
# for crate. The Stock baseline workflow fails when the source-distribution build and
# this list disagree, in either direction. A change that fails or fixes stock tests
# updates this list: python3 .github/stock-baseline/check.py --write <maven log> <this file>
"""


def failures(log):
    found, cls = set(), None
    with open(log, errors="replace") as f:
        for raw in f:
            line = TIMESTAMP.sub("", ANSI.sub("", raw.rstrip("\n").split("\t")[-1])).strip()
            m = CLASS.search(line)
            if m:
                cls = m.group(1)
                continue
            m = TEST.match(line)
            if m and cls:
                found.add(f"{cls}#{m.group(1)}")
    return found


def expected(path):
    with open(path) as f:
        return {line.strip() for line in f if line.strip() and not line.startswith("#")}


def main(args):
    write = args[:1] == ["--write"]
    log, path = args[1:] if write else args
    failed = failures(log)
    if write:
        with open(path, "w") as f:
            f.write(HEADER + "".join(f"{t}\n" for t in sorted(failed)))
        print(f"{len(failed)} expected failures written to {path}")
        return 0
    listed = expected(path)
    new, fixed = sorted(failed - listed), sorted(listed - failed)
    for t in new:
        print(f"::error::fails but is not in the list: {t}")
    for t in fixed:
        print(f"::error::passes but is still in the list: {t}")
    print(f"{len(failed)} failing, {len(listed)} listed, {len(new)} new, {len(fixed)} no longer failing")
    return 1 if new or fixed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
