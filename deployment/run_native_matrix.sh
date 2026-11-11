#!/bin/sh
# Run the F6 native matrix and save logs/exit files for freeze_candidate.py.
set -u
root=${1:?usage: run_native_matrix.sh STAGING_ROOT}
cd "$root" || exit 2
mkdir -p results

run() {
    mode=$1
    shift
    echo "===== $mode ====="
    if [ "$#" -gt 0 ]; then
        env "$@" ctest --test-dir "$mode" --output-on-failure \
            >"results/ctest-$mode.log" 2>&1
    else
        ctest --test-dir "$mode" --output-on-failure \
            >"results/ctest-$mode.log" 2>&1
    fi
    code=$?
    echo "$code" >"results/ctest-$mode.exit"
    tail -5 "results/ctest-$mode.log"
    return "$code"
}

run release || exit 1
run asan ASAN_OPTIONS=detect_leaks=0:halt_on_error=1 \
    UBSAN_OPTIONS=halt_on_error=1:print_stacktrace=1 || exit 1
# Linux 6.x with vm.mmap_rnd_bits=32 breaks the TSan runtime mapping.
# setarch -R disables ASLR for this test process tree only.
echo "===== tsan ====="
env TSAN_OPTIONS=halt_on_error=1 setarch "$(uname -m)" -R \
    ctest --test-dir tsan --output-on-failure \
    >results/ctest-tsan.log 2>&1
code=$?
echo "$code" >results/ctest-tsan.exit
tail -5 results/ctest-tsan.log
exit "$code"
