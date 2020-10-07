#!/bin/bash

rm -rf build
mkdir build
cmake -S . -B build
cmake --build build --target test_main
./build/test/test_main
