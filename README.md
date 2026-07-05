# Modbus TCP proxy

`modbus-proxy-rs` is a Modbus TCP layer 7 reverse proxy. It lets multiple
clients talk to Modbus devices that only support a single client, or a very
small number of concurrent clients.

## Quick start

1. Download and extract the latest release for your platform.
2. Create `modbus-config.yml`:

```yaml
devices:
  - modbus:
      url: plc1.acme.org:502
      connect_delay_ms: 0
    listen:
      bind: 0.0.0.0:9000
```

3. Run `./modbus-proxy-rs -c ./modbus-config.yml` and point your clients to `localhost:9000`.

-----

This repo is a fork of [`tiagocoutinho/modbus-proxy-rs`](https://github.com/tiagocoutinho/modbus-proxy-rs) that I tried to improve purely via agentic coding (all gemini 3.1 pro medium thinking, with a few "extend", "test", "review", "fix/improve" iterations).

Note:
> The changes in this fork have only been verified via manual and automatic testing. I did not read the code myself. Use at your own risk.

Changes in this fork:

- Safer proxying with transaction ID checks, backend timeouts, reconnects, and better error logging.
- New Test Suite for mismatches, timeouts, and multiplexed concurrency.
- More detailed connection and traffic logs.
- Project polish: release workflow, and local devcontainer/editor setup, license

-----

## Behavior

- Multiple client connections can share one backend Modbus TCP connection per
  configured device.
- Requests to the same backend are serialized on a first-come, first-served
  request/reply basis to avoid cross-talk between clients.
- Backend connections are opened lazily, reused while clients are active, and
  closed again when the last client disconnects.
- Backend replies are validated against the request transaction ID before they
  are forwarded to the client.
- Backend reads use a 5 second timeout so a stalled device does not block the
  proxy forever.
- Backend errors are logged and the proxy reconnects on the next request.
- Multiple configured devices run concurrently, each with its own listener and
  backend connection state.

## Installation

### From git releases

Download the archive that matches your platform from the
[latest release](https://github.com/dominikandreas/modbus-proxy-rs/releases/latest):

- `modbus-proxy-rs-x86_64-pc-windows-msvc.zip`: 64-bit Windows
- `modbus-proxy-rs-x86_64-unknown-linux-gnu.tar.gz`: most 64-bit Linux systems
- `modbus-proxy-rs-x86_64-unknown-linux-musl.tar.gz`: static 64-bit Linux build
- `modbus-proxy-rs-aarch64-unknown-linux-gnu.tar.gz`: ARM64 Linux
- `modbus-proxy-rs-armv7-unknown-linux-gnueabihf.tar.gz`: 32-bit ARM Linux

Each release also includes `SHA256SUMS.txt` for checksum verification.

On Linux:

```bash
tar -xzf modbus-proxy-rs-x86_64-unknown-linux-gnu.tar.gz
cd modbus-proxy-rs-x86_64-unknown-linux-gnu
./modbus-proxy-rs -c ./modbus-config.yml
```

On Windows PowerShell:

```powershell
Expand-Archive .\modbus-proxy-rs-x86_64-pc-windows-msvc.zip -DestinationPath .
cd .\modbus-proxy-rs-x86_64-pc-windows-msvc
.\modbus-proxy-rs.exe -c .\modbus-config.yml
```

### from source

Installation requrires the rust toolchain.

Install from crates.io:

```bash
cargo install modbus-proxy-rs
```

Or build locally:

```bash
cargo build --release
```


## Configuration

The proxy reads its configuration from a file passed with `-c` /
`--config-file`.

Each device entry needs:

- `modbus.url`: the backend Modbus TCP device address
- `modbus.connect_delay_ms`: optional delay after opening the backend TCP
  connection and before forwarding the first request
- `listen.bind`: the local listening address clients should connect to

Configuration files can be written in YAML, TOML, or JSON.

Example in YAML:

```yaml
devices:
  - modbus:
      url: plc1.acme.org:502
      connect_delay_ms: 0
    listen:
      bind: 0.0.0.0:9000
```

Some devices accept TCP connections before their Modbus endpoint is ready for
the first request. Set `connect_delay_ms` to wait after connecting to the
backend. Huawei SUN2000 dongles commonly need around `2000`.

Example in TOML:

```toml
[[devices]]
modbus.url = "plc1.acme.org:502"
listen.bind = "0.0.0.0:9000"
```

Example in JSON:

```json
{
  "devices": [
    {
      "modbus": { "url": "plc1.acme.org:502" },
      "listen": { "bind": "0.0.0.0:9000" }
    }
  ]
}
```

You can expose more than one backend by adding more entries:

```yaml
devices:
  - modbus:
      url: plc1.acme.org:502
    listen:
      bind: 0.0.0.0:9000
  - modbus:
      url: plc2.acme.org:502
    listen:
      bind: 0.0.0.0:9001
```

## Running

Start the proxy with:

```bash
modbus-proxy-rs -c ./modbus-config.yml
```

Clients should then connect to the configured `listen.bind` address instead of
directly to the Modbus device.

The server shuts down cleanly on `Ctrl+C`, and on Unix also reacts to
`SIGTERM`.

## Logging

Logging is controlled with `RUST_LOG`.

```bash
RUST_LOG=info modbus-proxy-rs -c ./modbus-config.yml
```

Useful levels:

- `warn`: backend connection problems and retries
- `info`: client connect/disconnect events and request/reply sizes
- `debug`: connection lifecycle and per-request frame dumps
- `trace`: raw backend write/read frame dumps

## Docker

This project ships with a [Dockerfile](./Dockerfile).

Build the image:

```bash
docker build -t modbus-proxy .
```

The final image is a minimal `scratch` image that runs as a non-root user and
starts with:

```bash
./modbus-proxy-rs -c /etc/modbus-proxy.yml
```

If you have a local `config.yml`:

```yaml
devices:
  - modbus:
      url: plc1.acme.org:502
    listen:
      bind: 0.0.0.0:502
```

Run the container with:

```bash
docker run --init --rm -p 5020:502 -v $PWD/config.yml:/etc/modbus-proxy.yml modbus-proxy
```

Or pass a different configuration path:

```bash
docker run --init --rm -p 5020:502 -v $PWD/config.yml:/config.yml modbus-proxy -c /config.yml
```

Clients can then connect to `<your-hostname-or-ip>:5020`.

If you configure multiple devices, publish each corresponding bind port with an
additional `-p <host port>:<container port>` mapping.

## Development

For a ready-to-use cloud environment, open the repository in https://github.dev/dominikandreas/modbus-proxy-rs, connect to a codespace (`ctr` + `shift` + `p`, 'codespaces')
and choose the default dev container. 

The container already includes the Rust
toolchain and VS Code integration, so you can start hacking and run `cargo
test` right away.

For local containerized development, open the repo in VS Code and select
`Dev Containers: Reopen in Container`. This uses the checked-in
`.devcontainer/` configuration and gives you the same setup locally.

Run the test suite with:

```bash
cargo test
```

The repository also includes:

- a `.devcontainer/` setup for containerized development
- `.vscode/launch.json` and `.vscode/tasks.json` for local debugging/tasks

## Credits

### tiagocoutinho/modbus-proxy-rs

- Tiago Coutinho <coutinhotiago@gmail.com>

From which this repository is forked. He implemented the original core logic, I only vibe coded on top.

### Contributors

- [@dominikandreas](https://github.com/dominikandreas)
- [@coutinhotiago](https://github.com/tiagocoutinho)
