# Migration plan: import topology-spike into rafka-V2-new-mesh

**Status:** PLAN ONLY — do NOT execute the push without explicit user confirmation (step 7).

---

## 0. Verified facts (basis for this plan)

| Fact | Verified value |
|------|---------------|
| Spike repo | `E:/dev/rafka-topology-spike` — local-only, no remotes, 22 commits, unrelated history (`git init` clean room) |
| Mesh repo | `E:/dev/rafka-V2-new-mesh` — full history, HEAD `c82fa87` (`chore(stateful-node-restart): add validation evidence + stop tracking dist/`) |
| Mesh current branch | `sprint-stateful-node-restart` |
| Mesh remotes | `origin` → https://github.com/drlukeangel/rafkav2 ; `public` → https://github.com/drlukeangel/rust-distributed-mesh.git |
| Tracked files in mesh-not-spike | **0** — spike's tracked set is a strict superset of the mesh's tracked set |
| Tracked files in spike-not-mesh | **145** — new crates, tabs, verify scripts, screenshots, `repeater/`, soak scripts, etc. |
| Shared tracked files with different content | **11** — listed and audited in §1 below |
| `[patch.crates-io]` paths | Identical in both `Cargo.toml` files (`E:/iroh/iroh`, `E:/iroh/iroh-base`, `E:/iroh-gossip`, `E:/noq/noq`, `E:/noq/noq-proto`, `E:/noq/noq-udp`) |
| Workspace members diff | Spike adds `"crates/rafka-entity-cache"` and `"repeater"` to `[workspace].members` |
| `R:` drive / path in spike | Not present — no spike files reference an `R:` path or `CARGO_TARGET_DIR=R:`; no `/XD R:` needed |
| Original-unique untracked content (mesh only) | `docs/blog/10-extending-the-mesh-with-node-caches.md`, `docs/blog/11-a-certificate-that-rides-the-gossip.md`, `docs/blog/screenshots/` (4 images), `docs/blog/README.md` (modified unstaged) |
| Spike `.claude/` status | Present on disk in spike, gitignored in spike (`.gitignore` line `.claude/`) — but NOT gitignored in the mesh `.gitignore`; see §3 |

---

## 1. Why overlay-copy (not force-push the spike)

The spike was created with a fresh `git init`; its history is **unrelated** to the mesh's. Connecting the two repos and force-pushing would:

- Irreversibly overwrite GitHub history on both `origin` and `public`.
- Lose the mesh's untracked blog 10, 11, and screenshots that the spike never contained.

A `--allow-unrelated-histories` merge creates a noisy disconnected-DAG merge commit on a public repo.

The overlay-copy approach keeps the mesh's full git history intact, adds the spike's working-tree content as a single clean commit on top, and leaves all untracked mesh files untouched. `robocopy /E` without `/PURGE` or `/MIR` never deletes destination-only files — so the untracked blog 10/11/screenshots survive automatically.

---

## 2. Audit of the 11 shared files with different content

Running `join` on the `ls-files -s` output from both repos reveals 11 shared tracked files whose committed blobs differ. Each is audited below. In every case the spike's version is **correct to import** — it contains the spike's new feature additions on top of the mesh's content — with one exception: `.gitignore`.

| File | Direction | What changed |
|------|-----------|-------------|
| `Cargo.toml` | spike is newer | Adds `"crates/rafka-entity-cache"` and `"repeater"` to workspace members — correct |
| `Cargo.lock` | spike is newer | Lock file reflecting new crates — correct |
| `admin-ui/Cargo.toml` | spike is newer | Adds `rafka-entity-cache` dependency — correct |
| `admin-ui/src/main.rs` | spike is newer | Adds `mod topology_cache`, `node_caches`/`channel_events` imports, `ca: Arc<iroh::SecretKey>` field, `/api/ca` route — correct |
| `admin-ui/web/src/App.tsx` | spike is newer | Adds Caches/Channels/TopologyCache tab routing — correct |
| `admin-ui/web/src/SpawnBar.tsx` | spike is newer | Spawn bar additions for multi-mesh / cert flows — correct |
| `admin-ui/web/src/api.ts` | spike is newer | API client additions for new endpoints — correct |
| `admin-ui/web/src/tabs/Topology.tsx` | spike is newer | Extended topology tab with cache overlay — correct |
| `crates/rafka-node-base/Cargo.toml` | spike is newer | Adds `rafka-entity-cache` dependency — correct |
| `crates/rafka-node-base/src/lib.rs` | spike is newer | Exports `topology_cache`, `node_cache`, `cert` modules — correct |
| **`.gitignore`** | **spike is OLDER/SIMPLER** | **REVERSION RISK — see §3** |

**Verification command** (re-run before importing to confirm no new regressions):

```bash
join \
  <(git -C "E:/dev/rafka-V2-new-mesh"    ls-files -s | awk '{print $4, $2}' | sort) \
  <(git -C "E:/dev/rafka-topology-spike" ls-files -s | awk '{print $4, $2}' | sort) \
  | awk '$2 != $3 {print $1}'
```

Expected output: the same 11 filenames listed above, nothing more.

---

## 3. Files to EXCLUDE from the overlay copy

### 3a. `.gitignore` — explicit exclusion required

The spike's `.gitignore` is a minimal 7-line file:

```
/target
*.log
/data
/data-*
**/node_modules/
.claude/
.antigravitycli/
```

The mesh's `.gitignore` is much more complete — it covers `*.jsonl`, `flamegraph-*.folded`, `*.pdb`, `admin-ui-*.log`, `admin-ui-panic.log`, `.vscode/`, `.idea/`, `docs/superpowers/`, `data/`, `/target/` (with trailing slash), and more. Overlaying the spike's `.gitignore` would silently remove all those rules, causing previously-ignored files to appear as untracked and risk accidental commits of build artifacts.

**Resolution: exclude `.gitignore` from the copy.** The mesh's version is strictly richer. Add `.gitignore` to the `robocopy /XF` list.

After the copy, manually merge any new spike-only rules into the mesh `.gitignore` if needed (the spike adds `/data-*` and `.claude/` — check if the mesh already covers them):

```powershell
# Check mesh .gitignore for spike-only entries
Select-String -Path "E:/dev/rafka-V2-new-mesh/.gitignore" -Pattern "data-\*|\.claude"
```

If `/data-*` is absent from the mesh `.gitignore`, add it manually before committing (spike added it to prevent committing per-node data directories).

### 3b. `docs/blog/README.md` — explicit exclusion required

The spike's `docs/blog/README.md` is the nine-part version (entries 1–9 only). The mesh has an unstaged modification that updates it to the eleven-part version (adds entries for posts 10 and 11 and rewrites the introduction paragraph). Overlaying the spike copy would silently revert this edit.

**Resolution: exclude `docs/blog/README.md` from the copy.** The spike has zero blog files not already in the mesh (`comm -13` on `docs/blog` is empty), so skipping the entire `docs\blog` directory from the copy loses nothing from the spike.

Add `docs\blog` to the `robocopy /XD` list.

### 3c. `.claude/` — explicit exclusion required

The spike's `.gitignore` lists `.claude/` as ignored, but the mesh's `.gitignore` does NOT. If `robocopy` copies `.claude/` from the spike into the mesh, and `git add .` is used in step 6, git in the mesh will stage it (it is not ignored there). The `.claude/` directory contains agent session state — not appropriate to commit.

**Resolution: add `.claude` to the `robocopy /XD` list.**

### 3d. Standard runtime excludes

| Path | Reason |
|------|--------|
| `.git` | Never copy — would replace mesh's git history |
| `target` | Cargo build artifacts — gitignored, large, reproducible |
| `node_modules` | JS deps — gitignored |
| `data`, `data-cust*`, `data-cv*`, `data-mm*` | Runtime node data directories — not tracked |
| `pr-fix`, `temp_sim`, `poc-tmp` | Spike-only scratch directories |
| `*.log` | Runtime logs — gitignored |
| `admin-ui\web\dist` | React build output — gitignored in both repos via `admin-ui/web/.gitignore` line 11; verify spike has it: `Test-Path "E:/dev/rafka-topology-spike/admin-ui/web/dist"` — if True, add to `/XD` |

---

## 4. Pre-flight checks

```powershell
# 4a. Record pre-import HEAD for rollback reference
git -C "E:/dev/rafka-V2-new-mesh" log --oneline -1
# Must show: c82fa87 chore(stateful-node-restart): ...

# 4b. Confirm 0 tracked files in mesh-not-spike
$meshFiles  = (git -C "E:/dev/rafka-V2-new-mesh"    ls-files) -split "`n" | Sort-Object
$spikeFiles = (git -C "E:/dev/rafka-topology-spike" ls-files) -split "`n" | Sort-Object
$lost = $meshFiles | Where-Object { $_ -notin $spikeFiles }
if ($lost) { Write-Error "STOP: mesh has tracked files the spike is missing: $lost" }
else        { Write-Host "OK: 0 tracked files in mesh-not-spike" }

# 4c. Confirm shared-file diff still matches the expected 11-file list (no surprises)
# Run the join command from §2 and compare against the known 11 files

# 4d. Confirm untracked blog originals present
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/10-extending-the-mesh-with-node-caches.md"   # True
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/11-a-certificate-that-rides-the-gossip.md"   # True
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/screenshots/10-cache-tab.png"                # True

# 4e. Confirm mesh working tree has no staged changes that could collide
git -C "E:/dev/rafka-V2-new-mesh" diff --cached --name-only
# Expected: empty

# 4f. Check if spike's admin-ui/web/dist exists (add to /XD if True)
Test-Path "E:/dev/rafka-topology-spike/admin-ui/web/dist"
```

---

## 5. The overlay copy — exact command

```powershell
robocopy `
  "E:\dev\rafka-topology-spike" `
  "E:\dev\rafka-V2-new-mesh" `
  /E `
  /XD .git target node_modules .claude `
      data data-cust1 data-cust1-mesh2 data-cust2 data-cust2-mesh2 `
      data-cust3 data-cust3-mesh2 data-cv1 data-cv2 data-mm-a data-mm-b `
      pr-fix temp_sim poc-tmp docs\blog `
  /XF .gitignore "*.log" "*.err.log" `
  /NP /TEE
```

**What this does:**
- Copies all spike source files, new crates, UI tabs, verify scripts, screenshots, and `repeater/` into the mesh workspace.
- Does NOT copy `.git`, build artifacts, runtime data, `.claude/` agent state, or the two files that would revert mesh content (`.gitignore`, `docs/blog/README.md` via `/XD docs\blog`).
- Does NOT delete any destination-only files — the mesh's untracked blog 10/11/screenshots survive.

**One-liner version (for reference):**

```powershell
robocopy "E:\dev\rafka-topology-spike" "E:\dev\rafka-V2-new-mesh" /E /XD .git target node_modules .claude data data-cust1 data-cust1-mesh2 data-cust2 data-cust2-mesh2 data-cust3 data-cust3-mesh2 data-cv1 data-cv2 data-mm-a data-mm-b pr-fix temp_sim poc-tmp docs\blog /XF .gitignore "*.log" "*.err.log" /NP /TEE
```

---

## 6. Post-copy verification

```powershell
# 6a. Blog files still present (robocopy excluded docs\blog entirely)
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/10-extending-the-mesh-with-node-caches.md"   # True
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/11-a-certificate-that-rides-the-gossip.md"   # True
Test-Path "E:/dev/rafka-V2-new-mesh/docs/blog/screenshots/10-cache-tab.png"                # True

# 6b. .gitignore preserved (excluded from copy — mesh version unchanged)
git -C "E:/dev/rafka-V2-new-mesh" diff .gitignore
# Expected: empty (no diff — mesh .gitignore was not touched)

# 6c. New spike files are now present
Test-Path "E:/dev/rafka-V2-new-mesh/crates/rafka-entity-cache/src/lib.rs"   # True
Test-Path "E:/dev/rafka-V2-new-mesh/repeater/src/main.rs"                   # True
Test-Path "E:/dev/rafka-V2-new-mesh/crates/rafka-node-base/src/cert.rs"     # True

# 6d. Review which pre-existing tracked files were modified by the copy
git -C "E:/dev/rafka-V2-new-mesh" diff --stat
# Expected: the 10 modified tracked files from §2 (all except .gitignore, which was excluded)
# Scan this list: confirm every filename is on the §2 "correct to import" list
# Any filename NOT on that list = unexpected reversion — stop and investigate

# 6e. Check if /data-* needs to be added to mesh .gitignore
Select-String -Path "E:/dev/rafka-V2-new-mesh/.gitignore" -Pattern "data-\*"
# If no match: add /data-* to mesh .gitignore before committing
```

---

## 7. Build verification

```powershell
cd "E:/dev/rafka-V2-new-mesh"
cargo build --workspace 2>&1 | Tee-Object -FilePath build-import.log
```

Expected: clean build. The `[patch.crates-io]` paths are identical in both repos. The two new workspace members (`crates/rafka-entity-cache` and `repeater`) are now present in the overlaid `Cargo.toml`.

If the build fails, check `build-import.log` for the first error. The most likely cause is a use/import in the spike code that depends on a crate version not yet present in the mesh's `Cargo.lock` — compare the lock file diff with `git diff Cargo.lock`.

---

## 8. Commit

Review what git sees before staging:

```powershell
git -C "E:/dev/rafka-V2-new-mesh" status
```

Confirm:
- Modified tracked files match the 10 "correct to import" files from §2 (`.gitignore` excluded — should not appear modified).
- New untracked files are the 145 spike-not-mesh files from `comm -13`.
- No `.claude/` directory appears (it was excluded by `/XD .claude`).
- `docs/blog/` shows only its original untracked state (10, 11, screenshots still untracked; README still modified-unstaged).

Stage and commit — use targeted `git add` per-path, not `git add .`, to avoid accidentally staging `.claude/` or other unintended files:

```powershell
cd "E:/dev/rafka-V2-new-mesh"

# Stage modified tracked files (the 10 legitimate modifications from §2)
git add Cargo.toml Cargo.lock .gitignore
git add admin-ui/Cargo.toml admin-ui/src/main.rs
git add admin-ui/web/src/App.tsx admin-ui/web/src/SpawnBar.tsx admin-ui/web/src/api.ts
git add admin-ui/web/src/tabs/Topology.tsx
git add crates/rafka-node-base/Cargo.toml crates/rafka-node-base/src/lib.rs

# Note: .gitignore was excluded from copy, so it will NOT appear in git diff.
# Remove it from the add command if git status confirms it's clean.

# Stage new files by directory (safer than git add .)
git add crates/rafka-entity-cache/
git add repeater/
git add admin-ui/src/topology_cache.rs admin-ui/src/sim_cache.rs
git add admin-ui/web/src/tabs/Caches.tsx admin-ui/web/src/tabs/Channels.tsx admin-ui/web/src/tabs/TopologyCache.tsx
git add crates/rafka-node-base/src/cert.rs crates/rafka-node-base/src/node_cache.rs crates/rafka-node-base/src/topology_cache.rs
git add docs/plans/mesh-v2/verify/
git add docs/node-lifecycle-soak.html
git add CACHE-ROADMAP.md
git add soak30.sh soak_bytype.json status_check.py windows-findings.md

# Spot-check: blog files must NOT be staged
git status docs/blog/
# Expected: README.md still "modified not staged", 10/11/screenshots still "untracked"

# Review full staged set before committing
git diff --cached --stat

# Commit
git commit -m "feat(entity-cache): import topology-spike — rafka-entity-cache + repeater + cert layer + cache/channel UI tabs

Import working tree from isolated topology spike (22 commits, no shared history).
Adds: crates/rafka-entity-cache (generic node cache engine with durability, keyset gossip,
      12 passing unit tests), repeater/ (cross-mesh trust-agnostic relay), admin-ui
      Caches + Channels + TopologyCache tabs, node-admin cert module (CA sign/verify,
      NodeCert carried in gossip digest, verified on join), multi-mesh bootstrap
      (two admins, shared root CA), birth-injection topology fill, verify scripts
      and evidence screenshots.
Workspace Cargo.toml gains two new members: rafka-entity-cache and repeater.
No tracked mesh file is removed or regressed; blog posts 10 and 11 are preserved
as-is in the destination (not tracked by spike, not touched by this commit)."
```

---

## 9. Push to both remotes — REQUIRES EXPLICIT USER CONFIRMATION

**Do NOT run these commands until the user explicitly confirms the push. Both remotes are public GitHub repositories.**

Review the import commit before pushing:

```powershell
git -C "E:/dev/rafka-V2-new-mesh" show HEAD --stat | head -30
```

Once confirmed:

```powershell
# Push to origin (rafkav2)
git -C "E:/dev/rafka-V2-new-mesh" push origin sprint-stateful-node-restart

# Push to public (rust-distributed-mesh)
git -C "E:/dev/rafka-V2-new-mesh" push public sprint-stateful-node-restart
```

Both pushes are fast-forward — no rewrite, no force. The remote branches advance from `c82fa87` to the new import commit.

---

## 10. Rollback

If the import commit needs to be undone before pushing:

```powershell
# Pre-import SHA — confirmed above as c82fa87
git -C "E:/dev/rafka-V2-new-mesh" reset --hard c82fa87
```

This discards the import commit from the local branch. The untracked blog files survive `reset --hard` because git does not touch untracked files.

**If the push to either remote has already been executed:** a `reset --hard` + force-push would rewrite public history. That is destructive and should not be done unilaterally. Preference: do not push until the build and a smoke-test of the admin-ui pass locally.

---

## Summary

**File written to:** `E:/dev/rafka-topology-spike/docs/migration/to-rafka-v2-mesh.md`

**Recommended one-line copy command:**
```
robocopy "E:\dev\rafka-topology-spike" "E:\dev\rafka-V2-new-mesh" /E /XD .git target node_modules .claude data data-cust1 data-cust1-mesh2 data-cust2 data-cust2-mesh2 data-cust3 data-cust3-mesh2 data-cv1 data-cv2 data-mm-a data-mm-b pr-fix temp_sim poc-tmp docs\blog /XF .gitignore "*.log" "*.err.log" /NP /TEE
```

**Biggest risk to double-check:** The 11 shared tracked files with differing content. The `join` blob-SHA audit (§2) confirms 10 of the 11 are legitimate forward additions from the spike. The 11th — `.gitignore` — is a genuine reversion (spike's version is simpler and would remove flamegraph, .vscode, *.jsonl, and other rules). It is excluded from the copy by `/XF .gitignore`. Rerun the `join` command in §2 before executing to confirm no new diverged files have appeared since this plan was written.
