"""The landing checker rejects runtime auth codes by code kind, including in fixture directories."""
import contextlib
import io
from pathlib import Path
import runpy
import subprocess
import sys
import unittest
from unittest.mock import patch

SCRIPT = Path(__file__).with_name('name-check.py')


def check(path, source):
    diff = 'diff --git a/{0} b/{0}\n--- /dev/null\n+++ b/{0}\n@@ -0,0 +1,{1} @@\n'.format(path, len(source.splitlines()))
    diff += ''.join('+' + line + '\n' for line in source.splitlines())
    def git(args, **kw):
        if args[1:3] == ['diff', '--name-status']: result = 'A\t' + path + '\n'
        elif args[1:3] == ['diff', '--name-only']: result = path + '\n'
        elif args[1] == 'show': result = source
        else: result = diff
        return subprocess.CompletedProcess(args, 0, stdout=result)
    out = io.StringIO()
    with patch.object(sys, 'argv', [str(SCRIPT), 'base', 'head']), patch('subprocess.run', side_effect=git), contextlib.redirect_stdout(out):
        runpy.run_path(str(SCRIPT), run_name='__main__')
    return out.getvalue().strip()


class RuntimeAuthCodes(unittest.TestCase):
    def test_permission_denied_and_unauthenticated_are_never_constructed(self):
        for path in ['crates/rdm-core/src/handler.rs', 'crates/rdm-test/src/handler.rs', 'crates/rdm-core/tests/handler.rs', 'tests/handler.rs']:
            for code in ['PermissionDenied', 'Unauthenticated']:
                with self.subTest(path=path, code=code):
                    out = check(path, 'fn handle() -> Code { Code::' + code + ' }\n')
                    self.assertIn(path + ':1:', out)
                    self.assertIn(code, out)

    def test_code_definitions_classifiers_and_test_assertions_are_exempt_by_kind(self):
        source = '''enum Code { PermissionDenied = 7, Unauthenticated = 16 }
impl Code { const ALL: [Self; 2] = [Self::PermissionDenied, Self::Unauthenticated]; }
fn classify_reply(r: &Reply) -> Code { match r { Reply::Reserved => Code::PermissionDenied } }
#[test]
fn reserved_bytes() { let expected = Code::Unauthenticated; assert_eq!(decode(&[16]), expected); }
fn read(code: Code) -> bool { matches!(code, Code::PermissionDenied | Code::Unauthenticated) }
'''
        for path in ['crates/rdm-core/src/codec.rs', 'crates/rdm-test/src/codec.rs', 'tests/codec.rs']:
            self.assertEqual(check(path, source), '')

    def test_a_match_arm_handler_body_is_construction_and_a_pattern_is_not(self):
        self.assertIn('PermissionDenied', check('crates/rdm-core/src/handler.rs', 'fn handle(x: bool) -> Code { match x { true => Code::PermissionDenied, false => Code::Ok } }\n'))
        self.assertEqual(check('crates/rdm-core/src/read.rs', 'fn read(x: Code) { match x { Code::PermissionDenied => (), _ => () } }\n'), '')

    def test_nested_struct_patterns_read_values_but_guards_can_construct_them(self):
        source = 'fn inspect(r: Reply) { match r { Reply::Refused { why: Reason::SenderNotSubject { .. } } => (), _ => () } }'
        self.assertEqual(check('crates/rdm-core/tests/codec.rs', source), '')
        source = 'fn handle(x: bool) { match x { true if { accept(Code::PermissionDenied) } => (), _ => () } }'
        self.assertIn('PermissionDenied', check('crates/rdm-core/tests/handler.rs', source))

    def test_a_handler_inside_a_test_module_is_still_a_handler(self):
        source = '#[cfg(test)]\nmod tests {\n fn handle() -> Code { Code::Unauthenticated }\n}\n'
        self.assertIn(':3:', check('crates/rdm-core/tests/handler.rs', source))

    def test_aliases_and_numeric_factories_cannot_construct_runtime_auth_codes(self):
        for source in [
            'use Code::PermissionDenied as Denied; fn handle() -> Code { Denied }',
            'use Code::{PermissionDenied as Denied, Unauthenticated as Absent}; fn handle() -> Code { Absent }',
            'fn handle() -> Code { Code::from_number(7).unwrap() }',
            'fn handle() -> Code { Code::from_number(16).unwrap() }',
            'fn handle(n: u8) -> Code { Code::from_number(n).unwrap() }',
            'fn handle() -> Code { Code::ALL[7] }',
            'fn handle() -> ReplyKind { ReplyKind::Unauthorized }',
        ]:
            with self.subTest(source=source):
                self.assertNotEqual(check('crates/rdm-core/tests/handler.rs', source), '')
        self.assertEqual(check('crates/rdm-core/src/handler.rs', 'fn handle() -> Code { Code::from_number(0).unwrap() }'), '')

    def test_literals_comments_and_patterns_carry_no_runtime_auth_construction(self):
        source = r'''fn observe(code: Code) {
    let body = r###"Code::PermissionDenied and Code::Unauthenticated"###;
    /* outer /* inner */ Code::PermissionDenied */
    let brace = '{';
    match code { Code::PermissionDenied | Code::Unauthenticated => (), _ => () }
}'''
        self.assertEqual(check('crates/rdm-core/src/handler.rs', source), '')

    def test_a_reply_code_view_is_a_classifier_and_its_nested_handler_is_not(self):
        source = 'impl ForwardReply { fn code(&self) -> Code { match self { Self::Reserved => Code::PermissionDenied } } }'
        self.assertEqual(check('crates/rdm-core/src/codec.rs', source), '')
        source = '#[test] fn fixture() { fn handle() -> Code { Code::PermissionDenied } }'
        self.assertNotEqual(check('tests/handler.rs', source), '')

    def test_r24_constructors_are_also_exempt_by_kind_and_never_by_directory(self):
        for path in ['crates/rdm-core/src/handler.rs', 'crates/rdm-test/src/handler.rs', 'crates/rdm-core/tests/handler.rs', 'tests/handler.rs']:
            for source in ['fn handle() -> Reply { P::unauthorized("denied".into()) }', 'fn handle() -> Reply { StatusReply::Unauthorized { reason: "denied".into() } }', 'fn handle() -> NotAuthority { NotAuthority::SenderNotSubject { sender: "s" } }']:
                with self.subTest(path=path, source=source): self.assertNotEqual(check(path, source), '')
        source = 'impl NodeProtocol for P { fn unauthorized(reason: String) -> Reply { Reply::Unauthorized { reason } } }'
        self.assertEqual(check('crates/rdm-core/src/codec.rs', source), '')
        source = 'fn read(why: &NotAuthority) { match why { NotAuthority::SenderNotSubject { .. } => (), _ => () } }'
        self.assertEqual(check('crates/rdm-core/src/read.rs', source), '')

    def test_let_patterns_read_a_code_and_do_not_construct_one(self):
        self.assertEqual(check('crates/rdm-core/src/handler.rs', 'fn inspect(code: Code) { if let Code::PermissionDenied = code {} }'), '')
        self.assertNotEqual(check('crates/rdm-core/src/handler.rs', 'fn handle() { let code = Code::PermissionDenied; emit(code); }'), '')

    def test_the_real_git_diff_refuses_a_planted_handler_in_every_fixture_location(self):
        import tempfile
        def git_at(root, *args):
            process = subprocess.Popen(['git', '-C', str(root), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            out, err = process.communicate()
            self.assertEqual(process.returncode, 0, err)
            return out.strip()
        paths = ['crates/rdm-core/src/handler.rs', 'crates/rdm-test/src/handler.rs', 'crates/rdm-core/tests/handler.rs', 'tests/handler.rs']
        with tempfile.TemporaryDirectory(prefix='rdm-code-check-') as tmp:
            root = Path(tmp)
            git_at(root, 'init', '-q')
            git_at(root, '-c', 'user.name=RDM fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'fixture base', '--allow-empty')
            base = git_at(root, 'rev-parse', 'HEAD')
            for path in paths:
                dest = root / path
                dest.parent.mkdir(parents=True, exist_ok=True)
                dest.write_text('fn handle() -> Code { Code::PermissionDenied }\nfn handle_other() -> Code { Code::Unauthenticated }\n')
            git_at(root, 'add', '.')
            git_at(root, '-c', 'user.name=RDM fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-qm', 'planted handlers')
            head = git_at(root, 'rev-parse', 'HEAD')
            out = subprocess.run([sys.executable, str(SCRIPT.resolve()), base, head], cwd=root, capture_output=True, text=True, check=True).stdout
            for path in paths:
                self.assertIn(path + ':1: runtime construction of Code::PermissionDenied', out)
                self.assertIn(path + ':2: runtime construction of Code::Unauthenticated', out)


if __name__ == '__main__': unittest.main()
