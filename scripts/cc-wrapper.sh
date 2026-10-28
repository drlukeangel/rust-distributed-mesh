#!/usr/bin/env bash
#
# cc-wrapper.sh — the C and C++ compiler cargo hands build scripts (.cargo/config.toml [env] CC, CXX):
# `sccache cc` when sccache is in PATH, plain `cc` otherwise (sccache is optional: README.md, "Build
# and test"; scripts/rustc-wrapper.sh names it missing once per cargo run). Run as c++-wrapper.sh (the
# link beside it) it compiles C++ with `c++`.
case "${0##*/}" in
    c++*) compiler=c++ ;;
    *) compiler=cc ;;
esac
if command -v sccache >/dev/null 2>&1; then
    exec sccache "$compiler" "$@"
fi
exec "$compiler" "$@"
