#!/usr/bin/env bash
set -euo pipefail

###############################################################################
#  demo_new_cli.sh  —  MDF Agent CLI UX demo
#
#  Showcases: global config, direct publish with HTTPS upload to Globus,
#             status from memory, repo mode, curation workflow, search.
#
#  Prerequisites:
#    1.  pip install -e .       (so `mdf` is on PATH)
#    2.  mdf login              (Globus auth for staging)
#
#  Usage:  bash cs/aws/v2/demo_new_cli.sh
###############################################################################

blue()   { printf "\033[1;34m%s\033[0m\n" "$*"; }
green()  { printf "\033[1;32m%s\033[0m\n" "$*"; }
dim()    { printf "\033[2m%s\033[0m\n" "$*"; }
banner() { echo; printf "\033[1;36m══ %s ══\033[0m\n" "$*"; echo; }
pause()  { dim "(enter to continue)"; read -r; }

# ── Create sample data ───────────────────────────────────────────────────────

DATA_DIR=$(mktemp -d)
REPO_DIR=$(mktemp -d)
trap 'rm -rf "${DATA_DIR}" "${REPO_DIR}"' EXIT

cat > "${DATA_DIR}/xrd_scan_001.csv" <<'CSV'
two_theta,intensity,d_spacing
10.5,120,8.42
21.3,450,4.17
31.7,890,2.82
38.2,340,2.35
44.5,670,2.03
50.1,210,1.82
CSV

cat > "${DATA_DIR}/xrd_scan_002.csv" <<'CSV'
two_theta,intensity,d_spacing
10.5,115,8.42
21.4,460,4.16
31.6,910,2.83
38.3,355,2.35
44.4,680,2.04
50.0,225,1.82
CSV

cat > "${DATA_DIR}/metadata.json" <<'JSON'
{
  "instrument": "Rigaku SmartLab",
  "wavelength_angstrom": 1.5406,
  "scan_type": "theta-2theta",
  "sample": "Fe3Al intermetallic",
  "temperature_K": 298
}
JSON

green "Sample data created:"
ls -lh "${DATA_DIR}"
echo

###############################################################################
banner "1. GLOBAL CONFIG — set defaults once, use everywhere"
###############################################################################

mdf config set defaults.service staging
mdf config set user.organization argonne
mdf config set user.publisher "Materials Data Facility"

blue "Config:"
mdf config show

pause

###############################################################################
banner "2. DIRECT PUBLISH — files upload to Globus, no repo needed"
###############################################################################

dim '$ mdf publish ./data/ \'
dim '    --title "Fe3Al XRD Characterization" \'
dim '    --author "Doe, Jane" --author "Smith, Bob" \'
dim '    --description "Theta-2theta XRD scans ..." \'
dim '    --test --submit'
echo

mdf publish "${DATA_DIR}/" \
  --title "Fe3Al XRD Characterization" \
  --author "Doe, Jane" --author "Smith, Bob" \
  --description "Theta-2theta XRD scans of Fe3Al intermetallic" \
  --test --submit

echo
blue "Saved to config:"
mdf config get last_publish

pause

###############################################################################
banner "3. STATUS — remembers what you just published"
###############################################################################

dim '$ mdf status    # no args — reads last_publish from config'
echo
mdf status

pause

###############################################################################
banner "4. REPO MODE — init / add / commit / publish"
###############################################################################

mdf init "${REPO_DIR}" \
  --title "High-Entropy Alloy Tensile Data" \
  --author "Kumar, Raj"

cat > "${REPO_DIR}/tensile_data.csv" <<'CSV'
strain_pct,stress_mpa,temp_k
0.1,210,298
0.5,450,298
1.0,680,298
2.0,890,298
5.0,1050,298
CSV

(cd "${REPO_DIR}" && mdf add tensile_data.csv)
(cd "${REPO_DIR}" && mdf commit -m "Initial experimental data")
echo
(cd "${REPO_DIR}" && mdf publish --test --submit)

echo
blue "Config tracks the latest:"
mdf config get last_publish

pause

###############################################################################
banner "5. CURATION — list pending, then approve"
###############################################################################

mdf pending

echo
SOURCE_ID=$(mdf config get last_publish | python3 -c "
import json, sys
print(json.load(sys.stdin).get('source_id', ''))
" 2>/dev/null || true)

if [[ -n "${SOURCE_ID}" ]]; then
  blue "Approving: ${SOURCE_ID}"
  mdf approve "${SOURCE_ID}" --notes "Data looks great"
fi

pause

###############################################################################
banner "6. SEARCH"
###############################################################################

dim '$ mdf search "XRD"'
mdf search "XRD" || true

echo
dim '$ mdf search "tensile"'
mdf search "tensile" || true

###############################################################################
banner "DONE"
###############################################################################

green "What we demonstrated:"
echo "  1. mdf config set/show       — set defaults once"
echo "  2. mdf publish ./data/       — direct publish, real Globus upload"
echo "  3. mdf status                — remembers last publish"
echo "  4. mdf init/add/commit/pub   — repo mode still works"
echo "  5. mdf pending / approve     — curation at top level"
echo "  6. mdf search                — find datasets"
echo
dim "Config file:"
mdf config path
