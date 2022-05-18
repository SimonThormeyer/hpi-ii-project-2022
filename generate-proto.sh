#!/bin/bash

for PROTOTYPE in corporate organization person
  do
    protoc --proto_path=proto --python_out=build/gen proto/bakdata/$PROTOTYPE/v1/$PROTOTYPE.proto
  done
