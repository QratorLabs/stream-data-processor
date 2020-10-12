#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Checking formatting..."
find "${SCRIPT_DIR}/src" "${SCRIPT_DIR}/examples" -name \*.h -print0 -o -name \*.cpp --print0 | xargs -0 clang-format --dry-run --Werror --ferror-limit=1
echo "Entering build directory: ${SCRIPT_DIR}/build ..."
mkdir -p "${SCRIPT_DIR}/build"
pushd "${SCRIPT_DIR}/build" || exit
cmake .. -DENABLE_TESTS=ON -DCLANG_TIDY_LINT=ON
make test_main
ctest --verbose
popd || exit 1
