"""Run Xray TUN mode with automatic rotation through fastest V2Ray servers."""

from __future__ import annotations

from fetch_mtproto.catalogs import open_catalogs
from fetch_mtproto.config_loader import load_config
from fetch_mtproto.v2ray.tun import run_tun_loop, tun_settings_from_config


def main() -> None:
    config = load_config(required=False)
    settings = tun_settings_from_config(config)
    if settings is None:
        raise SystemExit(
            "Xray binary not found. Install Xray-core and set xray.bin in config.yaml "
            "(install xray on PATH, set xray.bin in config.yaml, or run setup to install it in xray/)."
        )

    db, _mt, catalog = open_catalogs(config)
    try:
        raise SystemExit(run_tun_loop(catalog, settings))
    finally:
        db.close()


if __name__ == "__main__":
    main()
