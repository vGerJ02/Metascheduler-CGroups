#!/usr/bin/env bash
set -euo pipefail

WORK_DIR="${TMPDIR:-/tmp}/ms_openmp_bench_${USER:-user}_$$"
SRC_FILE="${WORK_DIR}/openmp_spin.c"
BIN_FILE="${WORK_DIR}/openmp_spin"
DURATION_SECONDS="${DURATION_SECONDS:-10}"
THREADS="${OMP_NUM_THREADS:-}"

cleanup() {
  rm -rf "${WORK_DIR}"
}
trap cleanup EXIT

mkdir -p "${WORK_DIR}"

cat > "${SRC_FILE}" <<'EOF'
#include <omp.h>
#include <stdio.h>
#include <stdlib.h>

int main(int argc, char **argv) {
    int duration = 10;
    if (argc > 1) {
        duration = atoi(argv[1]);
        if (duration <= 0) duration = 10;
    }

    double start = omp_get_wtime();
    volatile double sink = 0.0;

    #pragma omp parallel reduction(+:sink)
    {
        double local = 0.0;
        while ((omp_get_wtime() - start) < duration) {
            for (int i = 1; i < 50000; i++) {
                local += 1.0 / (double)i;
            }
        }
        sink += local;
    }

    printf("OpenMP benchmark finished in ~%d seconds, sink=%f\n", duration, sink);
    return 0;
}
EOF

if command -v gcc >/dev/null 2>&1; then
  gcc -O2 -fopenmp "${SRC_FILE}" -o "${BIN_FILE}"
elif command -v cc >/dev/null 2>&1; then
  cc -O2 -fopenmp "${SRC_FILE}" -o "${BIN_FILE}"
else
  echo "No C compiler found (gcc/cc)." >&2
  exit 1
fi

if [[ -n "${THREADS}" ]]; then
  export OMP_NUM_THREADS="${THREADS}"
fi

"${BIN_FILE}" "${DURATION_SECONDS}"

