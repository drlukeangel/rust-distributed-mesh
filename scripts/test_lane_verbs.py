"""The lane verbs, each run against a scratch estate: a bare origin, a main checkout and one lane, all
under a scratch $HOME (every verb finds the lanes, the main checkout and the deploy directory from
$HOME), with this checkout's scripts committed into it. The lane's gate is a stub that records the tree
in green-trees and leaves a mark, so a landing that gates again is seen; `gh` is a stub that records its
arguments; the lane's prebuilt rdm-docgen is a stub that writes the generated page from the sources.

    python3 scripts/test_lane_verbs.py
"""
import os
from pathlib import Path
import shutil
import signal
import subprocess
import time
import tempfile
import unittest

SCRIPTS = Path(__file__).resolve().parent
LANE = '9'
BRANCH = 'i143-1-scratch'

# seat-rules.md item 7, verbatim from lane 4's (the lead's text, #3203), the lane its only variable.
ITEM_7 = ("7. End the unit with its evidence, which the lead reviews before anything lands: run "
          "`bash {lane_dir}/scripts/post-land-gate.sh {lane}` yourself, not under the turn lock (it rebases on "
          "origin/main and proves the tip; resolve rebase conflicts yourself: generated docs/reference/rdm/*.md by "
          "regenerating with rdm-docgen, INDEX/PRD rows by keeping both sides); report its GREEN line; the cells you "
          "ran with their results; and Jaeger links (http://192.168.68.99:16687/trace/<trace id>, found in ClickHouse "
          "otel.otel_traces on 192.168.68.99:18123, which the gate exports to) for the traces that show your fix "
          "working. A RED is read and fixed in this lane, never re-run to see if it passes; a red that is another "
          "known issue's is reported as that issue.")

GATE_STUB = """#!/usr/bin/env bash
# The scratch lane's gate: green, recorded as the real fast gate records it, and marked as having run.
cd "$1" && echo "DONE 0s" && echo "$(git rev-parse HEAD^{tree}) $(date -Is) $(git rev-parse --short HEAD)" >> "$HOME/rust-distributed-mesh-deploy/green-trees"
touch "$HOME/gate-ran"
"""

DOCGEN_STUB = """#!/usr/bin/env bash
# The scratch lane's rdm-docgen: spans.md is generated from the sources, one line per source file.
while [ $# -gt 0 ]; do case "$1" in --root) root=$2; shift 2;; --out) out=$2; shift 2;; *) shift;; esac; done
mkdir -p "$out"; { echo "<!-- generated -->"; for f in "$root"/src/*.txt; do echo "$(basename "$f")=$(cat "$f")"; done; } > "$out/spans.md"
"""

GH_STUB = """#!/usr/bin/env bash
echo "$*" >> "$HOME/gh.log"
case "$1 $2" in "issue view") echo OPEN;; esac
"""


def generated(sources):
    return '<!-- generated -->\n' + ''.join(f'{name}={text}\n' for name, text in sorted(sources.items()))


class Estate:
    """A scratch $HOME holding origin.git, rust-distributed-mesh (main), rdm-lanes/<lane> and the deploy dir."""

    def __init__(self):
        self.home = Path(tempfile.mkdtemp(prefix='lane-verbs-'))
        self.origin = self.home / 'origin.git'
        self.main = self.home / 'rust-distributed-mesh'
        self.lane = self.home / 'rdm-lanes' / LANE
        self.deploy = self.home / 'rust-distributed-mesh-deploy'
        self.state = self.home / 'lane-queue' / 'rdm' / LANE
        bin_dir = self.home / 'bin'
        for d in (bin_dir, self.deploy, self.state / 'inbox'):
            d.mkdir(parents=True)
        (bin_dir / 'gh').write_text(GH_STUB)
        (bin_dir / 'gh').chmod(0o755)
        self.env = {k: v for k, v in os.environ.items() if not k.startswith(('RDM_', 'GIT_', 'CARGO_TARGET_DIR'))}
        self.env.update(HOME=str(self.home), PATH=f'{bin_dir}:{os.environ["PATH"]}',
                        GIT_AUTHOR_NAME='t', GIT_AUTHOR_EMAIL='t@t', GIT_COMMITTER_NAME='t', GIT_COMMITTER_EMAIL='t@t')
        subprocess.run(['git', 'init', '-q', '--bare', '-b', 'main', str(self.origin)], check=True, env=self.env)
        seed = self.home / 'seed'
        self.git(None, 'clone', '-q', str(self.origin), str(seed))
        shutil.copytree(SCRIPTS, seed / 'scripts', ignore=shutil.ignore_patterns('__pycache__'))
        (seed / 'scripts' / 'rdm-gate-at.sh').write_text(GATE_STUB)
        (seed / '.gitignore').write_text('target/\n')
        self.sources = {'a.txt': 'a0', 'b.txt': 'b0'}
        for name, text in self.sources.items():
            self.write(seed, f'src/{name}', text + '\n')
        self.write(seed, 'docs/reference/rdm/spans.md', generated(self.sources))
        self.git(seed, 'add', '-A')
        self.git(seed, 'commit', '-q', '-m', 'base')
        self.git(seed, 'push', '-q', 'origin', 'HEAD:main')
        self.git(None, 'clone', '-q', str(self.origin), str(self.main))
        self.git(None, 'clone', '-q', str(self.origin), str(self.lane))
        self.git(self.lane, 'switch', '-q', '-c', BRANCH, 'origin/main')
        docgen = self.lane / 'target' / 'debug' / 'rdm-docgen'
        docgen.parent.mkdir(parents=True)
        docgen.write_text(DOCGEN_STUB)
        docgen.chmod(0o755)
        (self.lane / 'target' / '.lane-path').write_text(f'{self.lane}\n')
        (self.state / 'branch').write_text(BRANCH + '\n')
        self.pusher = seed

    def close(self):
        shutil.rmtree(self.home, ignore_errors=True)

    def git(self, cwd, *args):
        out = subprocess.run(['git', *args], cwd=cwd or self.home, env=self.env, capture_output=True, text=True)
        assert out.returncode == 0, f'git {args}: {out.stderr}'
        return out.stdout.strip()

    @staticmethod
    def write(repo, path, text):
        p = Path(repo) / path
        p.parent.mkdir(parents=True, exist_ok=True)
        p.write_text(text)

    def commit(self, repo, msg, files):
        for path, text in files.items():
            self.write(repo, path, text)
        self.git(repo, 'add', '-A')
        self.git(repo, 'commit', '-q', '-m', msg)

    def change(self, repo, msg, **sources):
        """A commit changing source files and regenerating spans.md from the repo's own sources."""
        have = {p.name: p.read_text().strip() for p in sorted((Path(repo) / 'src').glob('*.txt'))}
        have.update(sources)
        self.commit(repo, msg, {**{f'src/{k}': v + '\n' for k, v in sources.items()}, 'docs/reference/rdm/spans.md': generated(have)})

    def move_main(self, msg, **sources):
        """main moves: a commit pushed to origin from another checkout."""
        self.git(self.pusher, 'pull', '-q', '--rebase', 'origin', 'main')
        self.change(self.pusher, msg, **sources)
        self.git(self.pusher, 'push', '-q', 'origin', 'HEAD:main')

    def run(self, script, *args):
        out = subprocess.run(['bash', str(self.lane / 'scripts' / script), *args], cwd=self.home, env=self.env,
                             capture_output=True, text=True, timeout=300)
        return out.returncode, out.stdout + out.stderr

    def origin_main(self):
        return self.git(self.lane, 'ls-remote', str(self.origin), 'refs/heads/main').split()[0]

    def green_trees(self):
        p = self.deploy / 'green-trees'
        return p.read_text() if p.exists() else ''


class Verbs(unittest.TestCase):
    def setUp(self):
        self.e = Estate()
        self.addCleanup(self.e.close)

    def prove(self):
        """The seat's own proof: post-land-gate.sh on the lane's tip, GREEN, the gate's mark then cleared."""
        rc, out = self.e.run('post-land-gate.sh', LANE)
        self.assertEqual(rc, 0, out)
        self.assertIn(f'GREEN lane {LANE}', out)
        (self.e.home / 'gate-ran').unlink()
        return out

    def land(self):
        comment = self.e.home / 'close.md'
        comment.write_text('landed @SHA@\n')
        return self.e.run('lane-land.sh', LANE, BRANCH, '1', str(comment))

    def test_dispatch_renders_item_7_with_the_lanes_own_post_land_gate(self):
        design = self.e.home / 'design.md'
        design.write_text('the design\n')
        rc, out = self.e.run('lane-dispatch.sh', '--lane', LANE, '--issue', '1', '--name', 'scratch', '--title', 'Scratch', '--design', str(design))
        self.assertEqual(rc, 0, out)
        rules = (self.e.state / 'seat-rules.md').read_text().splitlines()
        self.assertIn(ITEM_7.format(lane_dir=self.e.lane, lane=LANE), rules, 'item 7, verbatim, names this lane\'s post-land-gate.sh')
        item_3 = next(l for l in rules if l.startswith('3. '))
        self.assertIn('Never land, never push, never switch branch', item_3)
        self.assertNotIn('gate', item_3, 'a seat proves its own tip: item 3 no longer forbids the gate')

    def test_post_land_gate_regenerates_a_conflict_only_in_the_generated_pages(self):
        self.e.change(self.e.lane, 'branch: a', **{'a.txt': 'a1'})
        self.e.move_main('main: b', **{'b.txt': 'b1'})
        rc, out = self.e.run('post-land-gate.sh', LANE)
        self.assertEqual(rc, 0, out)
        self.assertIn(f'GREEN lane {LANE}', out)
        self.assertIn('regenerated docs/reference/rdm', out)
        self.assertEqual(self.e.git(self.e.lane, 'rev-parse', 'HEAD~1'), self.e.origin_main(), 'the branch now sits on main')
        self.assertEqual((self.e.lane / 'docs/reference/rdm/spans.md').read_text(), generated({'a.txt': 'a1', 'b.txt': 'b1'}))
        self.assertEqual(self.e.git(self.e.lane, 'status', '--porcelain', '--untracked-files=no'), '')

    def test_post_land_gate_refuses_by_name_a_conflict_outside_the_generated_pages(self):
        self.e.change(self.e.lane, 'branch: a', **{'a.txt': 'a1'})
        tip = self.e.git(self.e.lane, 'rev-parse', 'HEAD')
        self.e.move_main('main: a', **{'a.txt': 'a2'})
        rc, out = self.e.run('post-land-gate.sh', LANE)
        self.assertEqual(rc, 4, out)
        self.assertIn('REFUSED: rebase on origin/main conflicts in src/a.txt', out)
        self.assertEqual(self.e.git(self.e.lane, 'rev-parse', 'HEAD'), tip, 'the rebase is aborted')
        self.assertFalse((self.e.home / 'gate-ran').exists(), 'no gate runs')

    def test_post_land_gate_records_the_proven_ranges_patch_id_with_its_green(self):
        self.e.change(self.e.lane, 'branch: a', **{'a.txt': 'a1'})
        self.prove()
        tree = self.e.git(self.e.lane, 'rev-parse', 'HEAD^{tree}')
        base = self.e.git(self.e.lane, 'rev-parse', 'origin/main')
        pid = subprocess.run(f'git diff {base} HEAD | git patch-id --stable', shell=True, cwd=self.e.lane, env=self.e.env,
                             capture_output=True, text=True).stdout.split()[0]
        rows = (self.e.deploy / 'green-ranges').read_text().splitlines()
        self.assertTrue(rows[-1].startswith(f'{tree} {pid} {base} '), rows)

    def test_a_green_stands_when_main_moved_only_in_other_files_and_lands_without_a_gate(self):
        self.e.commit(self.e.lane, 'branch: a', {'src/a.txt': 'a1\n'})
        self.prove()
        proven = self.e.git(self.e.lane, 'rev-parse', 'HEAD^{tree}')
        self.e.commit(self.e.pusher, 'main: c', {'src/c.txt': 'c0\n'})
        self.e.git(self.e.pusher, 'push', '-q', 'origin', 'HEAD:main')
        moved = self.e.origin_main()
        rc, out = self.land()
        self.assertEqual(rc, 0, out)
        self.assertIn('LANDED-AND-RECORDED', out)
        self.assertFalse((self.e.home / 'gate-ran').exists(), f'the landing ran no second gate: {out}')
        landed = self.e.origin_main()
        self.assertEqual(self.e.git(self.e.lane, 'rev-parse', f'{landed}~1'), moved, 'the branch landed on the moved main')
        tree = self.e.git(self.e.lane, 'rev-parse', f'{landed}^{{tree}}')
        self.assertIn(f'stands-on {proven}', next(l for l in self.e.green_trees().splitlines() if l.startswith(tree + ' ')))
        self.assertIn('issue close 1', (self.e.home / 'gh.log').read_text())

    def test_a_green_does_not_stand_when_main_moved_in_the_branchs_files(self):
        self.e.commit(self.e.lane, 'branch: a', {'src/a.txt': 'a1\n'})
        self.prove()
        tip = self.e.git(self.e.lane, 'rev-parse', 'HEAD')
        self.e.commit(self.e.pusher, 'main: d', {'src/d.txt': 'd0\n', 'src/a.txt': 'a0\n\nmain line\n'})
        self.e.git(self.e.pusher, 'push', '-q', 'origin', 'HEAD:main')
        moved = self.e.origin_main()
        rc, out = self.land()
        self.assertEqual(rc, 8, out)
        self.assertIn('prove it again', out)
        self.assertIn('src/a.txt', out, 'the refusal names the file both moved')
        self.assertEqual(self.e.origin_main(), moved, 'nothing landed')
        self.assertEqual(self.e.git(self.e.lane, 'rev-parse', 'HEAD'), tip, 'the lane is left as it was')

    def test_a_tip_never_proven_does_not_stand(self):
        self.e.commit(self.e.lane, 'branch: a', {'src/a.txt': 'a1\n'})
        self.e.commit(self.e.pusher, 'main: c', {'src/c.txt': 'c0\n'})
        self.e.git(self.e.pusher, 'push', '-q', 'origin', 'HEAD:main')
        rc, out = self.land()
        self.assertEqual(rc, 8, out)
        self.assertIn('no GREEN post-land-gate.sh', out)


    def dispatch(self, *extra):
        design = self.e.home / 'design.md'
        design.write_text('the design\n')
        return self.e.run('lane-dispatch.sh', '--lane', LANE, '--issue', '1', '--name', 'scratch', '--title', 'Scratch',
                          '--design', str(design), *extra)

    def gh_log(self):
        p = self.e.home / 'gh.log'
        return p.read_text() if p.exists() else ''

    def test_dispatch_uses_rdms_repo_by_default_and_records_it(self):
        rc, out = self.dispatch()
        self.assertEqual(rc, 0, out)
        rdm = 'drlukeangel/rust-distributed-mesh'
        self.assertEqual((self.e.state / 'repo').read_text().strip(), rdm)
        log = self.gh_log()
        self.assertIn(f'issue view 1 --repo {rdm}', log)
        self.assertIn(f'issue comment 1 --repo {rdm}', log)
        self.assertNotIn('rafka-v2', log)
        self.assertIn(f'You are the seat for {rdm} issue #1', (self.e.state / 'seat-rules.md').read_text())
        self.assertIn(f'https://github.com/{rdm}/issues/1', (self.e.state / 'plan.md').read_text())

    def test_dispatch_repo_flag_names_a_unit_still_on_rafka_v2(self):
        rc, out = self.dispatch('--repo', 'drlukeangel/rafka-v2')
        self.assertEqual(rc, 0, out)
        self.assertEqual((self.e.state / 'repo').read_text().strip(), 'drlukeangel/rafka-v2')
        self.assertIn('issue view 1 --repo drlukeangel/rafka-v2', self.gh_log())
        self.assertIn('You are the seat for drlukeangel/rafka-v2 issue #1', (self.e.state / 'seat-rules.md').read_text())

    def land_proven(self):
        self.e.commit(self.e.lane, 'branch: a', {'src/a.txt': 'a1\n'})
        self.prove()
        rc, out = self.land()
        self.assertEqual(rc, 0, out)

    def test_land_closes_in_the_lanes_recorded_repo(self):
        (self.e.state / 'repo').write_text('drlukeangel/rust-distributed-mesh\n')
        self.land_proven()
        self.assertIn('issue close 1 --repo drlukeangel/rust-distributed-mesh', self.gh_log())

    def test_land_in_a_lane_with_no_repo_file_closes_in_rafka_v2(self):
        self.assertFalse((self.e.state / 'repo').exists())
        self.land_proven()
        self.assertIn('issue close 1 --repo drlukeangel/rafka-v2', self.gh_log())

    def test_a_gate_waits_for_a_held_turn_lock(self):
        turn = self.e.home / 'turn.lock'
        env = {**self.e.env, 'RDM_TURN_LOCK': str(turn)}
        holder = subprocess.Popen(['flock', '-o', str(turn), 'sleep', '600'], env=env, start_new_session=True)
        gate = None
        try:
            time.sleep(1)
            gate = subprocess.Popen(['bash', str(SCRIPTS / 'rdm-gate-at.sh'), str(self.e.lane)], cwd=self.e.home, env=env,
                                    stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, start_new_session=True)
            time.sleep(4)
            self.assertIsNone(gate.poll(), 'the gate is still waiting, not run or aborted, while the turn lock is held')
            holder_pg = os.getpgid(holder.pid)
            os.killpg(holder_pg, signal.SIGKILL)
            holder.wait()
            out, _ = gate.communicate(timeout=120)
            self.assertIn('WAITING: the box\'s turn lock', out)
        finally:
            for p in (holder, gate):
                if p is not None and p.poll() is None:
                    os.killpg(os.getpgid(p.pid), signal.SIGKILL)
                    p.wait()


if __name__ == '__main__':
    unittest.main()
