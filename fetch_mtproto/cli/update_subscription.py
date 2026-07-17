"""Rebuild and serve the local V2Ray subscription (CLI)."""

from __future__ import annotations

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export working V2Ray links to subscription.txt and serve it locally."
    )
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()

    config = load_config(required=False)
    db, _mt, catalog = open_catalogs(config)
    try:
        count = catalog.update_subscription()
        output = catalog.subscription_path.resolve()
        print(f"Wrote {count} unique server(s) to {output}")

        handler = partial(SimpleHTTPRequestHandler, directory=str(output.parent))
        server = ThreadingHTTPServer((args.host, args.port), handler)
        display_host = "127.0.0.1" if args.host in {"0.0.0.0", "::"} else args.host
        print(
            f"NekoRay subscription URL: "
            f"http://{display_host}:{args.port}/{output.name}"
        )
        print("Keep this process running while NekoRay updates the subscription.")
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("\nStopped local subscription server.")
        finally:
            server.server_close()
    finally:
        db.close()


if __name__ == "__main__":
    main()
