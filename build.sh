#!/bin/bash

if [ "$1" = "rebuild" ]; then
    rm -rf build
fi

mkdir -p build &&
    cd build &&
    cmake .. &&
    make -j60 &&
    cd ..