#!/usr/bin/env bash
# =============================================================================
# Duck Lineage Quickstart Demo
# =============================================================================
# Starts Marquez, runs an example ETL pipeline in DuckDB, and opens an
# interactive DuckDB session with lineage tracking enabled.
#
# Usage:
#   bash test/demo.sh              # Full demo (default)
#   bash test/demo.sh --down       # Tear down infrastructure
#   bash test/demo.sh --clean      # Tear down + remove downloaded DuckDB
#   bash test/demo.sh -i           # Interactive DuckDB session (skip seeding)
#   bash test/demo.sh --no-seed    # Start infra but don't run example pipeline
#   bash test/demo.sh -h           # Show help
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
DUCKDB_VERSION="v1.4.4"
MARQUEZ_API_PORT=5000
MARQUEZ_ADMIN_PORT=5001
MARQUEZ_UI_PORT=3000
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DUCKDB_DIR="${SCRIPT_DIR}/.duckdb"
DUCKDB_BIN="${DUCKDB_DIR}/duckdb"
DEMO_DB="${SCRIPT_DIR}/.demo.duckdb"
IS_LOCAL_BUILD=false
EXT_PATH=""

# ---------------------------------------------------------------------------
# Colored output helpers
# ---------------------------------------------------------------------------
RED='\033[0;31m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
BOLD='\033[1m'
NC='\033[0m' # No color

info()    { echo -e "${BLUE}[info]${NC} $*"; }
success() { echo -e "${GREEN}[ok]${NC} $*"; }
error()   { echo -e "${RED}[error]${NC} $*" >&2; }
warn()    { echo -e "${YELLOW}[warn]${NC} $*"; }

# ---------------------------------------------------------------------------
# Usage / help
# ---------------------------------------------------------------------------
usage() {
    cat <<EOF
${BOLD}Duck Lineage Quickstart Demo${NC}

Usage: $(basename "$0") [OPTIONS]

Options:
  --down             Tear down demo infrastructure
  --clean            Tear down + remove downloaded DuckDB binary
  -i, --interactive  Skip seeding, just open interactive DuckDB session
  --no-seed          Start infrastructure but don't run example pipeline
  --seed-only        Run full pipeline then exit (no interactive REPL)
  -h, --help         Show this help message

Examples:
  make demo          # Run the full demo
  make demo-down     # Stop containers
  make demo-clean    # Stop containers + delete DuckDB binary
EOF
}

# ---------------------------------------------------------------------------
# Flag parsing
# ---------------------------------------------------------------------------
ACTION="run"         # run | down | clean | interactive | seed-only
SEED=true

while [[ $# -gt 0 ]]; do
    case "$1" in
        --down)
            ACTION="down"; shift ;;
        --clean)
            ACTION="clean"; shift ;;
        -i|--interactive)
            ACTION="interactive"; shift ;;
        --seed-only)
            ACTION="seed-only"; shift ;;
        --no-seed)
            SEED=false; shift ;;
        -h|--help)
            usage; exit 0 ;;
        *)
            error "Unknown option: $1"
            usage; exit 1 ;;
    esac
done

# ---------------------------------------------------------------------------
# Core functions
# ---------------------------------------------------------------------------

check_prerequisites() {
    local missing=()
    command -v docker >/dev/null 2>&1    || missing+=("docker")
    command -v docker >/dev/null 2>&1 && docker compose version >/dev/null 2>&1 || missing+=("docker compose")
    command -v curl >/dev/null 2>&1      || missing+=("curl")

    if [[ ${#missing[@]} -gt 0 ]]; then
        error "Missing prerequisites: ${missing[*]}"
        echo "Please install the missing tools and try again."
        exit 1
    fi
    success "Prerequisites OK (docker, docker compose, curl)"
}

ensure_duckdb() {
    # Check for locally built DuckDB (has extension compiled in)
    local build_bin="${SCRIPT_DIR}/../build/release/duckdb"
    if [[ -x "${build_bin}" ]]; then
        DUCKDB_BIN="$(cd "$(dirname "${build_bin}")" && pwd)/duckdb"
        local ext_file="${SCRIPT_DIR}/../build/release/extension/duck_lineage/duck_lineage.duckdb_extension"
        if [[ -f "${ext_file}" ]]; then
            IS_LOCAL_BUILD=true
            EXT_PATH="$(cd "$(dirname "${ext_file}")" && pwd)/duck_lineage.duckdb_extension"
            success "Using locally built DuckDB: ${DUCKDB_BIN} (extension: ${EXT_PATH})"
        else
            success "Using locally built DuckDB: ${DUCKDB_BIN}"
        fi
        return
    fi

    # Check PATH
    if command -v duckdb >/dev/null 2>&1; then
        DUCKDB_BIN="$(command -v duckdb)"
        success "Using system DuckDB: ${DUCKDB_BIN}"
        return
    fi

    # Check local download
    if [[ -x "${DUCKDB_BIN}" ]]; then
        success "Using downloaded DuckDB: ${DUCKDB_BIN}"
        return
    fi

    info "DuckDB not found — downloading ${DUCKDB_VERSION}..."

    local os arch filename
    os="$(uname -s | tr '[:upper:]' '[:lower:]')"
    arch="$(uname -m)"

    case "${os}" in
        linux)
            case "${arch}" in
                x86_64)  filename="duckdb_cli-linux-amd64.zip" ;;
                aarch64) filename="duckdb_cli-linux-aarch64.zip" ;;
                *)       error "Unsupported Linux architecture: ${arch}"; exit 1 ;;
            esac
            ;;
        darwin)
            filename="duckdb_cli-osx-universal.zip"
            ;;
        *)
            error "Unsupported OS: ${os}"; exit 1 ;;
    esac

    local url="https://github.com/duckdb/duckdb/releases/download/${DUCKDB_VERSION}/${filename}"
    mkdir -p "${DUCKDB_DIR}"

    info "Downloading ${url}"
    curl -fsSL -o "${DUCKDB_DIR}/${filename}" "${url}"
    unzip -oq "${DUCKDB_DIR}/${filename}" -d "${DUCKDB_DIR}"
    rm -f "${DUCKDB_DIR}/${filename}"
    chmod +x "${DUCKDB_BIN}"

    if [[ ! -x "${DUCKDB_BIN}" ]]; then
        error "Failed to download DuckDB"; exit 1
    fi
    success "Downloaded DuckDB ${DUCKDB_VERSION} to ${DUCKDB_BIN}"
}

start_marquez() {
    local profile_flag=""
    if [[ "${1:-}" == "--with-ui" ]]; then
        profile_flag="--profile ui"
        info "Starting Marquez (API + UI)..."
    else
        info "Starting Marquez..."
    fi
    cd "${SCRIPT_DIR}" && docker compose ${profile_flag} up -d
    cd - >/dev/null

    info "Waiting for Marquez to be ready..."
    local max_retries=60
    local i=0
    local spinner='|/-\'
    while [[ $i -lt $max_retries ]]; do
        if curl -f -s "http://localhost:${MARQUEZ_API_PORT}/ping" >/dev/null 2>&1; then
            echo ""
            success "Marquez is ready!"
            return
        fi
        local si=$((i % 4))
        printf "\r  ${BLUE}[${spinner:$si:1}]${NC} Waiting for Marquez... (%d/%d)" "$((i + 1))" "$max_retries"
        sleep 2
        i=$((i + 1))
    done
    echo ""
    error "Marquez failed to start after $((max_retries * 2)) seconds"
    error "Check logs with: cd test && docker compose logs marquez"
    exit 1
}

stop_marquez() {
    info "Stopping demo infrastructure..."
    cd "${SCRIPT_DIR}" && docker compose --profile ui down -v
    cd - >/dev/null
    rm -f "${DEMO_DB}" "${DEMO_DB}.wal"
    success "Infrastructure stopped"
}

run_seed() {
    info "Running ETL pipeline..."
    rm -f "${DEMO_DB}" "${DEMO_DB}.wal"

    if [[ "${IS_LOCAL_BUILD}" == true ]]; then
        # Local build: load extension from build dir, strip INSTALL/LOAD from SQL
        local tmp_sql
        tmp_sql="$(mktemp)"
        sed '/^INSTALL duck_lineage/d; /^LOAD duck_lineage/d' "${SCRIPT_DIR}/demo-example.sql" > "${tmp_sql}"
        "${DUCKDB_BIN}" "${DEMO_DB}" -unsigned \
            -cmd "LOAD '${EXT_PATH}'" < "${tmp_sql}"
        rm -f "${tmp_sql}"
    else
        "${DUCKDB_BIN}" "${DEMO_DB}" < "${SCRIPT_DIR}/demo-example.sql"
    fi

    info "Waiting for lineage events to land in Marquez..."
    sleep 3
    success "ETL pipeline complete — lineage events sent to Marquez"
}

open_interactive() {
    echo ""
    echo -e "${BOLD}Starting interactive DuckDB session with lineage tracking...${NC}"
    echo -e "  Every query you run will be tracked in Marquez."
    echo -e "  Type ${BOLD}.quit${NC} to exit."
    echo ""

    local load_cmds=()
    if [[ "${IS_LOCAL_BUILD}" == true ]]; then
        load_cmds+=(-unsigned -cmd "LOAD '${EXT_PATH}'")
    else
        load_cmds+=(-cmd "INSTALL duck_lineage FROM community" -cmd "LOAD duck_lineage")
    fi

    # Use rlwrap if available — fixes input in terminals where DuckDB's
    # built-in line editor doesn't work (e.g. VS Code integrated terminal)
    local wrapper=()
    if command -v rlwrap >/dev/null 2>&1; then
        wrapper=(rlwrap)
    fi

    "${wrapper[@]}" "${DUCKDB_BIN}" "${DEMO_DB}" \
        "${load_cmds[@]}" \
        -cmd "SET duck_lineage_url = 'http://localhost:${MARQUEZ_API_PORT}/api/v1/lineage'" \
        -cmd "SET duck_lineage_namespace = 'demo'" \
        -cmd "SET duck_lineage_debug = true"
}

print_banner() {
    echo ""
    echo -e "${BOLD}╔══════════════════════════════════════════════╗${NC}"
    echo -e "${BOLD}║        Duck Lineage Quickstart Demo          ║${NC}"
    echo -e "${BOLD}╚══════════════════════════════════════════════╝${NC}"
    echo ""
}

print_success_box() {
    echo ""
    echo -e "${GREEN}╔══════════════════════════════════════════════╗${NC}"
    echo -e "${GREEN}║              Demo is ready!                  ║${NC}"
    echo -e "${GREEN}╠══════════════════════════════════════════════╣${NC}"
    echo -e "${GREEN}║${NC}                                              ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}  Marquez UI: ${BOLD}http://localhost:${MARQUEZ_UI_PORT}${NC}            ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}  Namespace:  ${BOLD}demo${NC}                            ${GREEN}║${NC}"
    echo -e "${GREEN}║${NC}                                              ${GREEN}║${NC}"
    echo -e "${GREEN}╚══════════════════════════════════════════════╝${NC}"
    echo ""
}

# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

case "${ACTION}" in
    down)
        stop_marquez
        exit 0
        ;;
    clean)
        stop_marquez
        if [[ -d "${DUCKDB_DIR}" ]]; then
            info "Removing downloaded DuckDB binary..."
            rm -rf "${DUCKDB_DIR}"
            success "Cleaned up ${DUCKDB_DIR}"
        fi
        exit 0
        ;;
    interactive)
        check_prerequisites
        start_marquez --with-ui
        ensure_duckdb
        open_interactive
        echo ""
        warn "Don't forget to stop the demo infrastructure: make demo-down"
        exit 0
        ;;
    seed-only)
        print_banner
        check_prerequisites
        start_marquez
        ensure_duckdb
        run_seed
        print_success_box
        exit 0
        ;;
    run)
        print_banner
        check_prerequisites
        start_marquez --with-ui
        ensure_duckdb

        if [[ "${SEED}" == true ]]; then
            run_seed
        fi

        print_success_box
        open_interactive

        echo ""
        warn "Don't forget to stop the demo infrastructure: make demo-down"
        ;;
esac
