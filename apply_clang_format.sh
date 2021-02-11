#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
find "${SCRIPT_DIR}/src" -name \*.h -print -o -name \*.cpp -print | xargs clang-format -i --verbose
