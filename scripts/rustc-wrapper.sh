#!/usr/bin/env bash
#
# rustc-wrapper.sh — Per-crate rustc wrapper: external crates through sccache (one cache for every
# worktree), workspace crates through plain rustc. Shared with rafka-v2.
#
# Decision rule: Cargo invokes this wrapper as `<wrapper> <rustc> <args...>`. If RDM_RECOMPILE_VENDORS=1
# is set, the wrapper exports SCCACHE_RECACHE=1 and executes plain rustc (`$@`) for everything to refresh the build.
# Otherwise, it inspects ARGS for the crate's source file (the first argument ending in `.rs`). If the source file
# resides under an external dependency tree ($CARGO_HOME/registry/src/, $CARGO_HOME/git/checkouts/, or any path
# root declared under [patch.crates-io] in Cargo.toml, such as /mnt/e/iroh, /mnt/e/iroh-gossip, /mnt/e/netwatch,
# or /mnt/e/noq parsed dynamically at run time), it delegates compilation to sccache (`exec sccache "$@"`) to benefit
# from object caching; otherwise, for workspace member crates, it directly execs plain rustc (`exec "$@"`) to preserve
# rustc's early-rmeta artifact notifications and restore inter-crate pipelining.
#
# sccache and mold are optional (README.md, "Build and test"). With mold in PATH every compile links
# through it on Linux (`-C link-arg=-fuse-ld=mold`); without it, the platform's default linker links. Without
# sccache every crate compiles with plain rustc (and C/C++ with plain cc/c++, scripts/cc-wrapper.sh).
# A missing tool is named once per cargo run, on stderr, by the first compile that finds it missing.
# The lane gate requires both (scripts/rdm-gate-at.sh).

set -euo pipefail

have_sccache=0; command -v sccache >/dev/null 2>&1 && have_sccache=1
# mold is the Linux linker (the hosts the committed config named it for); elsewhere it is never looked for.
have_mold=0; no_mold=0
case "${OSTYPE:-}" in
    linux*) if command -v mold >/dev/null 2>&1; then have_mold=1; else no_mold=1; fi ;;
esac

# Locate the crate source file argument (the first argument ending in .rs)
src_file=""
for arg in "$@"; do
    case "$arg" in
        *.rs)
            src_file="$arg"
            break
            ;;
    esac
done

# If no source file was passed (e.g. rustc -vV, --print, or version queries), exec plain rustc
if [ -z "$src_file" ]; then
    exec "$@"
fi

link_args=()
[ "$have_mold" -eq 1 ] && link_args=(-C link-arg=-fuse-ld=mold)

# Name a missing tool once per cargo run, on cargo's own stderr (/proc/<cargo pid>/fd/2): a line on
# this compile's stderr goes into cargo's per-crate output cache and is replayed on every later build
# of that crate, after the tool is installed too. Cargo is this wrapper's parent: its pid and start
# time key one marker directory, made atomically (mkdir), so of the compiles cargo starts in parallel
# exactly one prints; markers of cargo runs that have ended are removed. A compile a build script runs
# (proc-macro2's probe) prints nothing. Cargo's stderr is written only where an append cannot overwrite
# cargo's own lines: a terminal, a pipe, or a file cargo holds open for append (`2>>`); a file opened
# with `2>` gets no line. Without /proc (macOS) the line goes on this compile's stderr.
if [ "$have_sccache" -eq 0 ] || [ "$no_mold" -eq 1 ]; then
    to=""; started=""
    if [ -r "/proc/$PPID/stat" ]; then
        fd2="/proc/$PPID/fd/2"
        if [ "$(cat "/proc/$PPID/comm" 2>/dev/null)" = cargo ] && [ -w "$fd2" ]; then
            if [ -p "$fd2" ] || [ -c "$fd2" ]; then
                to="$fd2"
            elif [ -f "$fd2" ]; then
                flags="$(sed -n 's/^flags:[[:space:]]*//p' "/proc/$PPID/fdinfo/2" 2>/dev/null || true)"
                if [ -n "$flags" ] && (( (8#$flags & 8#2000) != 0 )); then to="$fd2"; fi
            fi
            started="$(sed 's/.*) //' "/proc/$PPID/stat" 2>/dev/null | cut -d' ' -f20 || true)"
        fi
    else
        to=/dev/stderr
        started="$(ps -o lstart= -p "$PPID" 2>/dev/null | tr -s ' ' '_' || true)"
    fi
    notes="${TMPDIR:-/tmp}/rdm-build-notes.$(id -u)"
    if [ -n "$to" ] && mkdir -p "$notes" 2>/dev/null && mkdir "$notes/$PPID.$started" 2>/dev/null; then
        for m in "$notes"/*; do
            pid="${m##*/}"; pid="${pid%%.*}"
            kill -0 "$pid" 2>/dev/null || rmdir "$m" 2>/dev/null || true
        done
        missing=""
        [ "$have_sccache" -eq 1 ] || missing="sccache is not in PATH: crates compile with plain rustc, C and C++ with plain cc and c++"
        [ "$no_mold" -eq 0 ] || missing="${missing:+$missing; }mold is not in PATH: the default linker links"
        printf 'rdm build: %s (both optional: README.md, "Build and test")\n' "$missing" >> "$to" 2>/dev/null || true
    fi
fi

if [ "$have_sccache" -eq 0 ]; then
    exec "$@" ${link_args[@]+"${link_args[@]}"}
fi

# If forced recompile of vendors is requested, refresh cache and bypass wrapper
if [ "${RDM_RECOMPILE_VENDORS:-0}" = "1" ]; then
    export SCCACHE_RECACHE=1
    exec "$@" ${link_args[@]+"${link_args[@]}"}
fi

cargo_home="${CARGO_HOME:-$HOME/.cargo}"
is_external=0

# Vendored forks live OUTSIDE every worktree as [patch.crates-io] path deps
# (Cargo.toml: iroh, iroh-base, iroh-gossip at /mnt/e/...). Their source path is
# the same from every lane, so they cache like a registry crate — and until this
# line they compiled on every cold build (P139/U256: iroh 18.8 s + iroh-gossip
# 5.4 s on every compact bin's critical path). Luke 2026-09-04: vendors are never
# compiled twice; --recompile-vendors is the only exception.
# cargo hands rustc the CANONICAL source path: /mnt/e is a symlink (to ${HOME}/mnt-e on
# node1), so a literal /mnt/e/* match never fires and the vendors compiled on every cold build
# anyway (proof walls 03:58: iroh 13.3 s / 14.5 s on two consecutive cold builds). Compare
# canonical to canonical.
src_canon="$(readlink -f -- "$src_file" 2>/dev/null || printf '%s' "$src_file")"
vendor_canon="$(readlink -f /mnt/e 2>/dev/null || printf '%s' /mnt/e)"
if [[ "$src_file" == "$cargo_home/registry/src/"* || "$src_file" == "$cargo_home/git/checkouts/"* || "$src_file" == /mnt/e/* || "$src_canon" == "$vendor_canon/"* ]]; then
    is_external=1
else
    # Find Cargo.toml relative to this script location to extract [patch.crates-io] path roots dynamically
    script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd -P)"
    manifest="$script_dir/../Cargo.toml"
    if [ -f "$manifest" ]; then
        while IFS= read -r patch_root; do
            [ -z "$patch_root" ] && continue
            if [[ "$patch_root" != /* ]]; then
                patch_root="$(cd "$script_dir/.." && cd "$patch_root" 2>/dev/null && pwd -P || echo "$patch_root")"
            fi
            patch_root="$(readlink -f -- "$patch_root" 2>/dev/null || printf '%s' "$patch_root")"
            if [[ "$src_canon" == "$patch_root/"* || "$src_canon" == "$patch_root" ]]; then
                is_external=1
                break
            fi
        done < <(sed -n '/\[patch\.crates-io\]/,/^\s*\[/ { s/.*path[[:space:]]*=[[:space:]]*"\([^"]*\)".*/\1/p }' "$manifest")
    fi
fi

if [ "$is_external" -eq 0 ]; then
    exec "$@" ${link_args[@]+"${link_args[@]}"}
fi

# OUT_DIR is the ONE lane-varying input to an external crate's compile. Every other
# path-bearing input is either excluded from sccache's key (--out-dir, -L, --extern:
# their FILE CONTENTS are hashed instead) or path-stable across lanes (rustc's cwd is
# the package root under ~/.cargo/registry or /mnt/e; the source paths likewise). But
# cargo sets OUT_DIR=<target-dir>/<profile>/build/<pkg>-<hash>/out, and a crate that
# reads it at compile time — `env!("OUT_DIR")`, `include!(concat!(env!("OUT_DIR"), ..))`
# (serde_core, serde, serde_derive, thiserror, thiserror_impl, ring, enum_assoc, …) —
# bakes that literal into its HIR, so its crate hash (SVH) differs per target dir; every
# downstream crate's SVH includes its upstreams' SVHs, so the whole graph above serde
# differs byte-for-byte per lane and sccache misses on all of it (measured 2026-09-06:
# two fresh target dirs, same source, same cwd — 0 % hits for 264 external crates;
# 112 leaf artifacts identical, 18 differing by exactly three 16-byte hashes each).
#
# Fix: hand rustc a PATH-STABLE OUT_DIR — an immutable, content-addressed copy of the
# build script's output under $RDM_OUTDIR_STORE. Identical build-script output ⇒
# identical hash ⇒ identical literal in every lane ⇒ identical rlib ⇒ cache hit.
# Different output (a build script that embeds its own path) ⇒ a different copy, never a
# false hit. The copy is created once via temp+rename (a concurrent lane either sees the
# finished dir or loses the rename and uses the winner's), and never modified: cargo's
# dep-info freshness reads the copied files' mtimes, which `cp -a` preserves. The real
# out dir stays where cargo put it (-L native= and downstream build scripts still see
# it); only the rustc invocation's OUT_DIR moves.
if [ -n "${OUT_DIR:-}" ] && [ -d "$OUT_DIR" ]; then
    store="${RDM_OUTDIR_STORE:-$HOME/.cache/rafka-outdir}"
    if mkdir -p "$store" 2>/dev/null; then
        # Copy first, then hash the copy with the real out-dir prefix ABSTRACTED: a build
        # script may write its own absolute out-dir path into a generated file
        # (cranelift-assembler-x64's generated-files.rs lists `<out>/assembler.rs`), which
        # would otherwise give every lane a different content hash for identical output.
        # Text files carrying the prefix get a placeholder before hashing and the final
        # stable path after; binaries are left alone (`grep -I`). The archive is
        # deterministic: sorted names, fixed mtime/owner, normalized mode (a umask
        # difference between two lanes must not split the key).
        real_out="$OUT_DIR"
        placeholder='@RAFKA_OUT_DIR@'
        dest=""
        tmp="$(mktemp -d "$store/.tmp.XXXXXXXX" 2>/dev/null || true)"
        if [ -n "$tmp" ] && cp -a "$real_out/." "$tmp/" 2>/dev/null; then
            carriers="$(grep -rIlF -- "$real_out" "$tmp" 2>/dev/null || true)"
            if [ -n "$carriers" ]; then
                printf '%s\n' "$carriers" | xargs -d '\n' sed -i "s#$(printf '%s' "$real_out" | sed 's/[#\\&]/\\&/g')#$placeholder#g" 2>/dev/null || carriers=""
            fi
            h="$(cd "$tmp" && LC_ALL=C tar --sort=name --mtime='@0' --owner=0 --group=0 --numeric-owner \
                    --mode='u+rw,go+r,go-w' -cf - . 2>/dev/null | sha256sum | cut -c1-32)" || h=""
            if [ -n "$h" ]; then
                dest="$store/$h"
                if [ -d "$dest" ]; then
                    rm -rf "$tmp"
                else
                    if [ -n "$carriers" ]; then
                        printf '%s\n' "$carriers" | xargs -d '\n' sed -i "s#$placeholder#$(printf '%s' "$dest" | sed 's/[#\\&]/\\&/g')#g" 2>/dev/null || true
                    fi
                    mv -T "$tmp" "$dest" 2>/dev/null || rm -rf "$tmp"
                fi
            else
                rm -rf "$tmp"
            fi
        else
            [ -n "$tmp" ] && rm -rf "$tmp"
        fi
        if [ -n "$dest" ]; then
            if [ -d "$dest" ]; then
                # A build script may hand rustc its out-dir path under its OWN name via
                # `cargo:rustc-env=` (cranelift-codegen: ISLE_DIR; mime_guess:
                # MIME_TYPES_GENERATED_PATH — which reqwest, iroh, tower-http and the whole
                # datafusion family sit above). Every env value carrying the real out-dir
                # prefix moves with it; anything else is left alone.
                real_out="$OUT_DIR"
                while IFS= read -r -d '' kv; do
                    name="${kv%%=*}"
                    value="${kv#*=}"
                    case "$name" in
                        ''|*[!A-Za-z0-9_]*) continue ;;
                    esac
                    if [ "$value" = "$real_out" ] || [[ "$value" == "$real_out/"* ]]; then
                        export "$name=$dest${value#"$real_out"}"
                    fi
                done < <(env -0)
                export OUT_DIR="$dest"
            fi
        fi
    fi
fi

exec sccache "$@" ${link_args[@]+"${link_args[@]}"}
