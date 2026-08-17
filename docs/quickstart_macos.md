# macOS Quickstart

This guide installs the `0.1.0a1` Mac Developer Preview and local development
profile. It uses only unsigned
Binance public market-data endpoints. No API key or account is supported.

## Requirements

- Apple Silicon Mac
- macOS user logged in to an Aqua session
- Python 3.12
- An author-controlled reverse-DNS service label

Verify the release hashes against `release/0.1.0a1/manifest.json`, then install
the wheel in a dedicated environment:

```bash
shasum -a 256 dist/*
python3.12 -m venv "$HOME/.venvs/binance-market-data-recorder"
. "$HOME/.venvs/binance-market-data-recorder/bin/activate"
python -m pip install dist/binance_market_data_recorder-0.1.0a1-py3-none-any.whl
binance-market-recorder --version
binance-market-recorder doctor
binance-market-recorder config show
binance-market-recorder status
```

The default data root is:

```text
~/Library/Application Support/BinanceMarketDataRecorder/
```

It is intentionally outside Desktop, Documents, iCloud Drive, `/tmp`, and the
source repository.

## Configure

Create a user-owned mode-0600 TOML file outside the repository:

```toml
[recorder]
data_root = "/Users/you/Library/Application Support/BinanceMarketDataRecorder"
log_level = "INFO"
rotation_seconds = 60.0
rotation_bytes = 134217728
durability_interval_seconds = 1.0
ingress_queue_capacity = 8192
max_frame_bytes = 16777216
heartbeat_seconds = 5.0
sleep_gap_threshold_seconds = 30.0
prevent_sleep = false
```

```bash
chmod 600 "$HOME/.config/binance-market-data-recorder.toml"
export BINANCE_MARKET_RECORDER_CONFIG_FILE="$HOME/.config/binance-market-data-recorder.toml"
```

Spot and USD-M core streams are intentionally fixed to BTCUSDT diff depth,
aggregate trades, and book ticker in this preview. They are not credentialed
configuration fields.

## Install the user LaunchAgent

Choose a label in a namespace you control. Do not copy an example publisher
namespace:

```bash
export AUTHOR_CONTROLLED_LABEL='your.owned.namespace.BinanceMarketDataRecorder'
binance-market-recorder launchd install \
  --label "$AUTHOR_CONTROLLED_LABEL" \
  --author-controls-namespace
binance-market-recorder launchd status
binance-market-recorder status
```

No root privileges or LaunchDaemon are used. Manage the service with:

```bash
binance-market-recorder launchd stop
binance-market-recorder launchd start
binance-market-recorder launchd status
```

## Stop and uninstall

```bash
binance-market-recorder launchd stop
binance-market-recorder launchd uninstall
```

Uninstall removes the plist and LaunchAgent metadata only. Raw data, manifests,
reports, Catalog, and logs remain in the configured data root.

External archive registration and safe removal are covered in
[operations](operations.md). Read [known limitations](known_limitations.md)
before collecting data. The approved VPS production and future local pull
archive architecture is documented separately in [VPS operations](vps_operations.md)
and [Archive Transfer Contract](archive_transfer_contract.md); it is not
implemented by this quickstart.
