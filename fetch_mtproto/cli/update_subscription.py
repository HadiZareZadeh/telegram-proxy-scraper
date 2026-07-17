"""Rebuild and serve the local V2Ray subscription (CLI)."""

from __future__ import annotations

import argparse
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.subscription_server import (
    print_subscription_urls,
    resolve_server_settings,
)


def main() -> None:
    config = load_config(required=False)
    default_host, default_port = resolve_server_settings(config)

    parser = argparse.ArgumentParser(
        description="Export working V2Ray links to subscription.txt and serve it locally."
    )
    parser.add_argument("--host", default=default_host)
    parser.add_argument("--port", type=int, default=default_port)
    args = parser.parse_args()

    db, _mt, catalog = open_catalogs(config)
    try:
        count = catalog.update_subscription()
        output = catalog.subscription_path.resolve()
        print(f"Wrote {count} unique server(s) to {output}")

        handler = partial(SimpleHTTPRequestHandler, directory=str(output.parent))
        server = ThreadingHTTPServer((args.host, args.port), handler)
        print_subscription_urls(
            bind_host=args.host, port=args.port, filename=output.name
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
