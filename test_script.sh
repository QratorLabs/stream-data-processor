#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Entering build directory: ${SCRIPT_DIR}/build ..."
mkdir -p "${SCRIPT_DIR}/build"
pushd "${SCRIPT_DIR}/build" || exit
cmake .. -DENABLE_TESTS=ON -DCLANG_TIDY_LINT=OFF
make test_main
ctest --verbose
popd || exit 1
