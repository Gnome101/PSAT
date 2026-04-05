"""Proxy upgrade monitor worker — long-running scanner process."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

from services.monitoring.proxy_watcher import (
    DEFAULT_POLL_INTERVAL,
    DEFAULT_SCAN_INTERVAL,
    run_poll_loop,
    run_scan_loop,
)

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

logger = logging.getLogger(__name__)

DEFAULT_RPC_URL = os.environ.get("ETH_RPC", "https://ethereum-rpc.publicnode.com")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Proxy upgrade monitor worker")
    parser.add_argument("--rpc-url", default=DEFAULT_RPC_URL, help="Ethereum RPC URL")
    parser.add_argument(
        "--interval",
        type=float,
        default=None,
        help="Scan/poll interval in seconds (default depends on mode)",
    )
    parser.add_argument(
        "--poll",
        action="store_true",
        help="Run storage-slot polling loop instead of event-based scanning",
    )
    args = parser.parse_args()

    # Graceful shutdown
    def handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down", signum)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    if args.poll:
        interval = args.interval if args.interval is not None else DEFAULT_POLL_INTERVAL
        logger.info("Proxy poll monitor starting (rpc=%s, interval=%ss)", args.rpc_url, interval)
        run_poll_loop(args.rpc_url, interval)
    else:
        interval = args.interval if args.interval is not None else DEFAULT_SCAN_INTERVAL
        logger.info("Proxy monitor starting (rpc=%s, interval=%ss)", args.rpc_url, interval)
        run_scan_loop(args.rpc_url, interval)


if __name__ == "__main__":
    main()
