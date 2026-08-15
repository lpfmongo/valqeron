# Show available commands
default:
    @just --list

# Run all tests
test:
    cargo install cargo-llvm-cov --locked
    cargo llvm-cov

# Run all loom tests
test-loom:
    RUSTFLAGS="--cfg loom" cargo test -p valqeron-infrastructure --lib loom_tests

# Check Rust code formatting without modifying files
format-check:
    cargo fmt --all -- --check

# Format all Rust code
format:
    cargo fmt --all

# Run Clippy lints
lint:
    cargo clippy --workspace --all-targets --all-features -- -D warnings

# Assert async containment: core and infrastructure must stay tokio/tonic-free.
deps-check:
    #!/usr/bin/env bash
    set -euo pipefail
    for crate in valqeron-core valqeron-infrastructure; do
        for dep in tokio tonic; do
            if cargo tree -p "$crate" -e normal | grep -q " ${dep} v"; then
                echo "FAIL: ${crate} depends on ${dep}" >&2
                exit 1
            fi
        done
        echo "OK: ${crate} is async-free"
    done

# Run every fuzz target for the requested bounded time.
fuzz-all:
    just --justfile crates/identifiers/Justfile fuzz-all

# Installation is a separate lifecycle from the engine binary itself; this
# recipe is the development stand-in for future packaging. The service
# definition is a static, machine-local file under scripts/install/ — copy the
# .example next to it once and edit the CHANGE-ME paths. The recipe builds the
# release binary, copies the definition into place, and re-registers it
# without ever leaving two engines running at once (the old instance is fully
# torn down before the new one starts).
#
# Install the engine as a login service (launchd/systemd user) and start it
engine-install:
    #!/usr/bin/env bash
    set -euo pipefail
    cargo build --release -p valqeron-engine
    install_dir="{{justfile_directory()}}/scripts/install"
    require_definition() {
        if [ ! -f "$1" ]; then
            echo "missing service definition: $1" >&2
            echo "create your machine-local copy first:" >&2
            echo "  cp $1.example $1" >&2
            echo "then edit the CHANGE-ME paths in it" >&2
            exit 1
        fi
        if grep -q "/CHANGE-ME" "$1"; then
            echo "$1 still contains CHANGE-ME placeholder paths; edit them first" >&2
            exit 1
        fi
    }
    case "$(uname -s)" in
    Darwin)
        label="io.valqeron.engine"
        src="$install_dir/$label.plist"
        require_definition "$src"
        plutil -lint "$src" >/dev/null
        # launchd does not create the StandardOut/ErrorPath parent directory;
        # create the default one (mirrors ProjectDirs("io","valqeron","valqeron")).
        mkdir -p "$HOME/Library/LaunchAgents" \
            "$HOME/Library/Application Support/io.valqeron.valqeron"
        plist="$HOME/Library/LaunchAgents/$label.plist"
        cp "$src" "$plist"
        uid="$(id -u)"
        # bootout is asynchronous: it initiates SIGTERM + teardown and returns.
        # Wait until launchd has reaped the old process and dropped the
        # registration before bootstrapping again — otherwise the old and new
        # engines overlap (the loser of the db lock exits with code 3 and
        # KeepAlive respawns it every ThrottleInterval, i.e. process churn).
        # The 90s cap outlives launchd's own SIGKILL backstop (ExitTimeOut 60s).
        launchctl bootout "gui/$uid/$label" 2>/dev/null || true
        deadline=$((SECONDS + 90))
        while launchctl print "gui/$uid/$label" >/dev/null 2>&1; do
            if [ "$SECONDS" -ge "$deadline" ]; then
                echo "timed out waiting for the previous engine service to unload" >&2
                exit 1
            fi
            sleep 0.2
        done
        launchctl bootstrap "gui/$uid" "$plist"
        echo "installed launchd agent: $plist"
        echo "the engine starts now and at every login"
        echo "inspect with:  launchctl print gui/$uid/$label"
        echo "logs:          see StandardErrorPath in $plist"
        ;;
    Linux)
        unit="valqeron-engine.service"
        src="$install_dir/$unit"
        require_definition "$src"
        unit_dir="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user"
        # The unit sandbox (ReadWritePaths) expects the default data dir; the
        # socket dir is handled by RuntimeDirectory=.
        mkdir -p "$unit_dir" "${XDG_DATA_HOME:-$HOME/.local/share}/valqeron"
        cp "$src" "$unit_dir/$unit"
        systemctl --user daemon-reload
        systemctl --user enable "$unit"
        # restart, not `enable --now`: an already-running engine must be
        # replaced so the new build takes effect. systemd serializes the stop
        # (bounded by TimeoutStopSec) before the start — never two engines.
        systemctl --user restart "$unit"
        echo "installed systemd user unit: $unit_dir/$unit"
        echo "the engine starts now and at every login"
        echo "inspect with:  systemctl --user status valqeron-engine"
        ;;
    *)
        echo "unsupported platform: $(uname -s) (launchd/systemd only)" >&2
        exit 1
        ;;
    esac

# The machine-local definition under scripts/install/ stays.
#
# Stop the engine login service and remove its installed definition
engine-uninstall:
    #!/usr/bin/env bash
    set -euo pipefail
    case "$(uname -s)" in
    Darwin)
        label="io.valqeron.engine"
        plist="$HOME/Library/LaunchAgents/$label.plist"
        uid="$(id -u)"
        # bootout is asynchronous; wait for launchd to reap the process and
        # drop the registration so "uninstalled" means "no engine running".
        launchctl bootout "gui/$uid/$label" 2>/dev/null || true
        deadline=$((SECONDS + 90))
        while launchctl print "gui/$uid/$label" >/dev/null 2>&1; do
            if [ "$SECONDS" -ge "$deadline" ]; then
                echo "timed out waiting for the engine service to unload" >&2
                exit 1
            fi
            sleep 0.2
        done
        if [ -e "$plist" ]; then
            rm -f "$plist"
            echo "removed $plist"
        else
            echo "nothing to remove: $plist does not exist"
        fi
        echo "engine service uninstalled"
        ;;
    Linux)
        unit="${XDG_CONFIG_HOME:-$HOME/.config}/systemd/user/valqeron-engine.service"
        systemctl --user disable --now valqeron-engine.service 2>/dev/null || true
        if [ -e "$unit" ]; then
            rm -f "$unit"
            echo "removed $unit"
        else
            echo "nothing to remove: $unit does not exist"
        fi
        systemctl --user daemon-reload
        echo "engine service uninstalled"
        ;;
    *)
        echo "unsupported platform: $(uname -s) (launchd/systemd only)" >&2
        exit 1
        ;;
    esac
