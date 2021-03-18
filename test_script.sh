#!/bin/bash

SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
echo "Entering build directory: ${SCRIPT_DIR}/build ..."
mkdir -p "${SCRIPT_DIR}/build"
pushd "${SCRIPT_DIR}/build" || exit 1

if [[ "$OSTYPE" == "linux-musl" ]] || ! command -v clang++ &> /dev/null || ! command -v clang &> /dev/null; then
  cmake .. -D"ENABLE_TESTS"=ON -D"CLANG_TIDY_LINT"=OFF \
      -D"CMAKE_EXPORT_COMPILE_COMMANDS"=ON -D"BUILD_SHARED_LIBS"=OFF
else
  cmake .. -D"ENABLE_TESTS"=ON -D"CLANG_TIDY_LINT"=OFF \
      -D"CMAKE_EXPORT_COMPILE_COMMANDS"=ON -D"BUILD_SHARED_LIBS"=OFF \
      -D"CMAKE_CXX_COMPILER"=clang++ -D"CMAKE_C_COMPILER"=clang

  if [[ "$OSTYPE" == "darwin"* ]]; then
    export ASAN_OPTIONS="detect_leaks=0"
  else
    export ASAN_OPTIONS="detect_leaks=1"
  fi
fi

NPROC=1
if [[ "$OSTYPE" == "darwin"* ]]; then
  NPROC=$(sysctl -n hw.logicalcpu)
else
  NPROC=$(nproc)
fi

make test_main -j$(( NPROC / 2 + 1 ))

if ! command -v clang-tidy &> /dev/null; then
  echo "clang-tidy could not be found. Skipping tidy check..."
else
  if [[ "$OSTYPE" == "darwin"* ]]; then
    sed -i.bu 's|-I/usr/local/include|-isystem /usr/local/include|g' compile_commands.json
    sed -i.bu -E 's|-I([^[:space:]]*)/third_party|-isystem \1/third_party|g' compile_commands.json
    sed -i.bu -E 's|-I([^[:space:]]*)/build|-isystem \1/build|g' compile_commands.json
  else
    sed -i 's|-I/usr/local/include|-isystem /usr/local/include|g' compile_commands.json
    sed -i 's|-I/usr/include|-isystem /usr/include|g' compile_commands.json
    sed -i 's|-I\(\S*\)/third_party|-isystem \1/third_party|g' compile_commands.json
    sed -i 's|-I\(\S*\)/build|-isystem \1/build|g' compile_commands.json
  fi

  clang-tidy -p . --warnings-as-errors $(find ../src -iname "*.cpp")
fi

if ! command -v clang-format &> /dev/null; then
  echo "clang-format could not be found. Skipping format check..."
else
  clang-format --dry-run --Werror $(find ../src -iname "*.cpp" -o -iname "*.h")
fi

ctest --verbose
popd || exit 1
