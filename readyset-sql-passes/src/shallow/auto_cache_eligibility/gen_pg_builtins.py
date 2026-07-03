#!/usr/bin/env python3
"""Generate the PostgreSQL builtin-function allowlist for shallow-cache eligibility.

A shallow cache is auto-created only for queries that call builtin functions; a
call to anything outside this list is treated as a user-defined function (UDF)
and the query is proxied instead of cached.  The authoritative source for the
builtins of a given server is its own catalog: every function the planner can
resolve by bare name lives in the `pg_catalog` schema of `pg_proc`.  We spin up
a throwaway container per supported major version. BUILTINS is unioned across
versions -- the fail-safe direction, since a name present in any supported major
is a builtin and one missing from a minor never causes a wrongful skip.
IMMUTABLE_BUILTINS is intersected instead: a name is cacheable only if every one
of its overloads is immutable in every major, so gaining a non-immutable overload
anywhere excludes it.

Usage:
    ./gen_pg_builtins.py [VERSION ...]        # defaults to the supported majors
Writes pg_builtins.rs next to this script (BUILTINS + IMMUTABLE_BUILTINS
slices).  Requires Docker.
"""

import re
import subprocess
import sys
import time
from pathlib import Path

DEFAULT_VERSIONS = ["13", "14", "15", "16", "17"]

# All builtin functions are the pg_proc rows in the pg_catalog namespace. No
# prokind filter: aggregates ('a'), window ('w') and procedures ('p') are all
# callable names we must recognize as builtin. This list is what tells a
# non-immutable *builtin* apart from a *UDF* (a name the catalog doesn't know).
BUILTINS_QUERY = (
    "SELECT DISTINCT lower(p.proname) "
    "FROM pg_catalog.pg_proc p "
    "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
    "WHERE n.nspname = 'pg_catalog'"
)

# IMMUTABLE functions (provolatile = 'i') are the definitive safe-to-cache
# builtins: the result depends only on the arguments, never on session, clock,
# or table state. Under default-deny this is the base allowlist -- a builtin
# that is not immutable is denied unless a curated list or opt-in permits it.
#
# A function NAME is immutable only if EVERY overload of that name is immutable.
# `date_trunc`, for example, is immutable for (text, timestamp) but STABLE for
# (text, timestamptz); a `DISTINCT ... WHERE provolatile='i'` would admit the
# name on the strength of the immutable overload and wrongly cache the stable
# one. Group by name and require `bool_and(provolatile='i')` so a name with any
# non-immutable overload is excluded.
IMMUTABLE_QUERY = (
    "SELECT lower(p.proname) "
    "FROM pg_catalog.pg_proc p "
    "JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace "
    "WHERE n.nspname = 'pg_catalog' "
    "GROUP BY lower(p.proname) "
    "HAVING bool_and(p.provolatile = 'i')"
)

# Sanity bands: a stock pg_catalog has ~2400-2700 distinct function names, of
# which ~1900-2300 are immutable. Far outside these means the query or
# container is broken; fail rather than ship a truncated list.
MIN_EXPECTED = 2200
MAX_EXPECTED = 3200
IMMUTABLE_MIN_EXPECTED = 1600
IMMUTABLE_MAX_EXPECTED = 2600


def collect(version: str) -> tuple[set[str], set[str]]:
    container = f"rs-pgbuiltins-{version}"
    subprocess.run(["docker", "rm", "-f", container],
                   stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(f"  postgres:{version}: starting container", file=sys.stderr)
    subprocess.run(
        ["docker", "run", "-d", "--rm", "--name", container,
         "-e", "POSTGRES_HOST_AUTH_METHOD=trust", f"postgres:{version}"],
        check=True, stdout=subprocess.DEVNULL,
    )
    try:
        for _ in range(120):
            ready = subprocess.run(
                ["docker", "exec", container, "pg_isready", "-U", "postgres"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            time.sleep(1)
        else:
            raise SystemExit(f"postgres:{version} never became ready")
        def run_query(query: str) -> set[str]:
            out = subprocess.run(
                ["docker", "exec", container, "psql", "-U", "postgres", "-tAc", query],
                check=True, capture_output=True, text=True,
            )
            return {line.strip() for line in out.stdout.splitlines() if line.strip()}

        all_names = run_query(BUILTINS_QUERY)
        immutable_names = run_query(IMMUTABLE_QUERY)
    finally:
        subprocess.run(["docker", "rm", "-f", container],
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    print(
        f"  postgres:{version}: {len(all_names)} builtins, "
        f"{len(immutable_names)} immutable",
        file=sys.stderr,
    )
    return all_names, immutable_names


def write_rust_list(
    out_path: Path, slices: list[tuple[str, set[str]]], header_lines: list[str]
) -> None:
    """Emit one or more sorted `pub(super) static <NAME>: &[&str]` slices.

    Only identifier-shaped names are kept: a function callable by bare name is an
    identifier, and this keeps the generated Rust literal safe without escaping.
    """
    with out_path.open("w") as f:
        for line in header_lines:
            f.write(f"// {line}\n")
        f.write("// GENERATED -- do not edit by hand.\n")
        for name, names in slices:
            safe = sorted(n for n in names if re.fullmatch(r"[a-z_][a-z0-9_]*", n))
            dropped = len(names) - len(safe)
            if dropped:
                print(f"  {name}: dropped {dropped} non-identifier name(s)", file=sys.stderr)
            f.write("#[rustfmt::skip]\n")
            f.write(f"pub(super) static {name}: &[&str] = &[\n")
            for n in safe:
                f.write(f'    "{n}",\n')
            f.write("];\n")
            print(f"  wrote {len(safe)} names to {name}", file=sys.stderr)


def main() -> None:
    versions = sys.argv[1:] or DEFAULT_VERSIONS
    # BUILTINS is unioned across majors: a name present in any supported version
    # is a builtin, the fail-safe direction for the builtin-vs-UDF test (a name
    # missing from one minor never causes a wrongful skip). IMMUTABLE is
    # INTERSECTED: a name is treated as safe-to-cache only if it is immutable in
    # EVERY supported major, so a name that gains a non-immutable overload in
    # some version is never wrongly cached -- the fail-safe direction here is to
    # under-admit, since default-deny only under-caches when the list is short.
    all_union: set[str] = set()
    immutable_intersection: set[str] | None = None
    for version in versions:
        all_names, immutable_names = collect(version)
        all_union |= all_names
        immutable_intersection = (
            immutable_names
            if immutable_intersection is None
            else immutable_intersection & immutable_names
        )
    immutable_intersection = immutable_intersection or set()

    if not (MIN_EXPECTED <= len(all_union) <= MAX_EXPECTED):
        raise SystemExit(
            f"builtins union has {len(all_union)} names, outside sanity band "
            f"[{MIN_EXPECTED}, {MAX_EXPECTED}]; refusing to write"
        )
    if not (IMMUTABLE_MIN_EXPECTED <= len(immutable_intersection) <= IMMUTABLE_MAX_EXPECTED):
        raise SystemExit(
            f"immutable intersection has {len(immutable_intersection)} names, outside "
            f"sanity band [{IMMUTABLE_MIN_EXPECTED}, {IMMUTABLE_MAX_EXPECTED}]; refusing to write"
        )
    # Immutable is a subset of all builtins; guard against a query mixup.
    assert immutable_intersection <= all_union, "immutable set is not a subset of all builtins"

    header = [
        "PostgreSQL builtin function names for shallow-cache eligibility.",
        "Source: pg_catalog.pg_proc across PostgreSQL majors "
        + ", ".join(versions)
        + " (BUILTINS unioned, IMMUTABLE_BUILTINS intersected).",
        "BUILTINS: every pg_catalog function (builtin-vs-UDF test).",
        "IMMUTABLE_BUILTINS: names whose every overload is provolatile='i', in "
        "every major (default-deny cacheable base).",
        "Regenerate: ./gen_pg_builtins.py",
    ]
    write_rust_list(
        Path(__file__).with_name("pg_builtins.rs"),
        [("BUILTINS", all_union), ("IMMUTABLE_BUILTINS", immutable_intersection)],
        header,
    )


if __name__ == "__main__":
    main()
