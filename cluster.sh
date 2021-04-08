#!/bin/bash

find playground/ -name '*.toml' | parallel ./target/debug/server
