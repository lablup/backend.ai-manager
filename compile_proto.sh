#! /bin/bash
protoc -I./protocols --python_out=./sorna/proto ./protocols/api.proto

# Temporary patch
sed -i "s%\([ \t]\+\)\(syntax=\)%\1# \2%" ./sorna/proto/api_pb2.py
