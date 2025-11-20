# Sombra CLI (npm)

Install the native `sombra` command-line interface directly from npm:

```bash
# global install
npm install -g sombra-cli

# temporary invocation
npx sombra -- --help
```

The package ships a thin Node.js wrapper (`sombra`) that shells out to the
precompiled binary distributed with each GitHub release of
[`maskdotdev/sombra-db`](https://github.com/maskdotdev/sombra-db). During
`npm install` (or `pnpm/bun install`) a postinstall hook downloads the proper
archive for your platform, extracts the binary into `dist/`, and wires the
`sombra` executable to that path.

## Supported targets

| Platform        | `process.platform` / `process.arch` | Release triple                | Output binary |
| --------------- | ----------------------------------- | ----------------------------- | ------------- |
| macOS (Apple)   | `darwin` / `arm64`                  | `aarch64-apple-darwin`        | `sombra`      |
| macOS (Intel)   | `darwin` / `x64`                    | `x86_64-apple-darwin`         | `sombra`      |
| Linux (x64)     | `linux` / `x64`                     | `x86_64-unknown-linux-gnu`    | `sombra`      |
| Linux (arm64)   | `linux` / `arm64`                   | `aarch64-unknown-linux-gnu`   | `sombra`      |
| Windows (x64)   | `win32` / `x64`                     | `x86_64-pc-windows-msvc`      | `sombra.exe`  |

Each published GitHub release must include artifacts named
`sombra-cli-v<VERSION>-<triple>.tar.gz` to match the matrix above (for example,
`sombra-cli-v0.3.6-aarch64-apple-darwin.tar.gz`). The npm package version should
stay in lockstep with the Rust CLI version so users can install a specific tag
via `npm install sombra-cli@x.y.z`.

## Environment overrides

The installer understands a few environment variables to support mirrors,
testing, or offline environments:

- `SOMBRA_CLI_SKIP_DOWNLOAD=1` &mdash; skip the download step entirely.
- `SOMBRA_CLI_VERSION` &mdash; override the version that is used in the asset
  name (defaults to the package version).
- `SOMBRA_CLI_TAG` &mdash; override the Git tag portion of the release URL
  (defaults to `v<SOMBRA_CLI_VERSION>`).
- `SOMBRA_CLI_REPO` &mdash; change the GitHub repository slug
  (defaults to `maskdotdev/sombra-db`).
- `SOMBRA_CLI_BASE_URL` &mdash; replace the `https://github.com/<repo>/releases/download/<tag>`
  prefix, useful for custom mirrors.
- `SOMBRA_CLI_ASSET` &mdash; override the asset filename completely.
- `SOMBRA_CLI_DOWNLOAD_URL` &mdash; provide the full URL to fetch (bypasses the
  repo/tag/asset computation).

Use these knobs to point the installer at staging artifacts or to host the
prebuilt CLI on your own infrastructure.
