#! /bin/bash
for fname in $(ls ./protocols/*.proto); do
    protoc -I./protocols --python_out=./sorna/proto $fname
done

# Temporary patch
for fname in $(ls ./sorna/proto/*_pb2.py); do
    sed -i "s%\([ \t]\+\)\(syntax=\)%\1# \2%" $fname
done
