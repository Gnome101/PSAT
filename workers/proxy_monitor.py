"""Proxy upgrade monitor worker — long-running scanner process."""

from __future__ import annotations

import argparse
import logging
import os
import signal
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[1] / ".env")

# Ensure project root on sys.path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from services.monitoring.proxy_watcher import DEFAULT_SCAN_INTERVAL, run_scan_loop

logger = logging.getLogger(__name__)

DEFAULT_RPC_URL = os.environ.get("ETH_RPC", "https://ethereum-rpc.publicnode.com")


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Proxy upgrade monitor worker")
    parser.add_argument("--rpc-url", default=DEFAULT_RPC_URL, help="Ethereum RPC URL")
    parser.add_argument(
        "--interval",
        type=float,
        default=DEFAULT_SCAN_INTERVAL,
        help=f"Scan interval in seconds (default: {DEFAULT_SCAN_INTERVAL})",
    )
    args = parser.parse_args()

    # Graceful shutdown
    def handle_signal(signum, frame):
        logger.info("Received signal %s, shutting down", signum)
        sys.exit(0)

    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    logger.info("Proxy monitor starting (rpc=%s, interval=%ss)", args.rpc_url, args.interval)
    run_scan_loop(args.rpc_url, args.interval)


if __name__ == "__main__":
    main()
