#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
find "${SCRIPT_DIR}/src" "${SCRIPT_DIR}/examples" -name \*.h -print -o -name \*.cpp | xargs clang-format -i

