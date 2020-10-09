#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Entering build directory: ${SCRIPT_DIR}/build ..."
mkdir -p "${SCRIPT_DIR}/build"
pushd "${SCRIPT_DIR}/build" || exit
cmake .. -DENABLE_TESTS=ON
make test_main
./test/test_main
popd || exit 1
