from __future__ import annotations

import argparse
from pathlib import Path

import uvicorn

from .api import build_app
from .config import ProjectPaths
from .service import run_project_generation


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Revenue Forecast Control Tower")
    subparsers = parser.add_subparsers(dest="command", required=True)

    run_all = subparsers.add_parser("run-all", help="Generate data, outputs, and dashboard snapshot")
    run_all.add_argument("--months", type=int, default=18)
    run_all.add_argument("--accounts", type=int, default=240)
    run_all.add_argument("--regions", type=int, default=6)
    run_all.add_argument("--seed", type=int, default=23)

    serve = subparsers.add_parser("serve", help="Run the FastAPI service locally")
    serve.add_argument("--host", default="127.0.0.1")
    serve.add_argument("--port", type=int, default=8010)

    return parser.parse_args()


def main() -> None:
    args = parse_args()
    root = Path.cwd()
    paths = ProjectPaths.from_root(root)

    if args.command == "run-all":
        run_project_generation(paths, months=args.months, account_count=args.accounts, region_count=args.regions, seed=args.seed)
        return

    if args.command == "serve":
        uvicorn.run(build_app(root), host=args.host, port=args.port)
        return

    raise ValueError(f"Unsupported command: {args.command}")


if __name__ == "__main__":
    main()
