#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""InStock 基础质量 linter（纯 stdlib）。

检查：bare except、tab/空格混用缩进、print 用于日志、job/web 缺 __main__ 入口。
agent-actionable 输出 file:line + WHAT + HOW to fix。错误退出码 1，警告 0，
--strict 时警告亦退出 1。

用法: python scripts/lint-quality.py [path] [--strict]   # 默认 path=instock/
"""

import ast
import os
import sys

# entry 层包：模块应有 if __name__ == "__main__": 入口。
ENTRY_PACKAGES = {'job', 'web'}

# 扫描时跳过的目录名。
EXCLUDE_DIRS = {'__pycache__', 'static', 'templates', '.git', '.idea', 'venv', 'env'}


def iter_py_files(root):
    base = os.path.dirname(root.rstrip('/')) or '.'
    ignore_patterns = []
    for gi in (os.path.join(root, '.gitignore'),
               os.path.join(base, '.gitignore')):
        if os.path.isfile(gi):
            with open(gi, encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#'):
                        ignore_patterns.append(line)

    for dirpath, dirnames, filenames in os.walk(root):
        dirnames[:] = [d for d in dirnames if d not in EXCLUDE_DIRS]
        for name in filenames:
            if not name.endswith('.py'):
                continue
            full = os.path.join(dirpath, name)
            rel = os.path.relpath(full, root).replace(os.sep, '/')
            if any(p in rel for p in ignore_patterns):
                continue
            yield full, rel


def package_of_file(rel_path):
    parts = rel_path.replace(os.sep, '/').split('/')
    if len(parts) < 2 or parts[0] != 'instock':
        return None
    if parts[1] in ENTRY_PACKAGES:
        return parts[1]
    return None


def has_main_guard(tree):
    """判断模块是否包含 if __name__ == "__main__": 入口。"""
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        test = node.test
        # __name__ == "__main__"
        if isinstance(test, ast.Compare) and len(test.ops) == 1 and \
                isinstance(test.ops[0], ast.Eq):
            left = test.left
            right = test.comparators[0] if test.comparators else None
            if isinstance(left, ast.Name) and left.id == '__name__' and \
                    isinstance(right, ast.Constant) and right.value == '__main__':
                return True
    return False


def find_bare_excepts(tree):
    """返回所有 bare except 的行号（ExceptHandler.type is None）。"""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.ExceptHandler) and node.type is None:
            out.append(node.lineno)
    return out


def find_prints(tree):
    """返回所有 print(...) 调用的行号。"""
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and isinstance(node.func, ast.Name) and \
                node.func.id == 'print':
            out.append(node.lineno)
    return out


def find_mixed_indent_lines(src):
    """返回行内同时含 tab 与空格的缩进行号（1-based）。"""
    bad = []
    for i, line in enumerate(src.splitlines(), 1):
        if not line:
            continue
        leading = line[:len(line) - len(line.lstrip(' \t'))]
        if '\t' in leading and ' ' in leading:
            bad.append(i)
    return bad


def lint(root):
    errors = []   # (severity 'E'/'W', rel, lineno, what, why, fix)
    file_count = 0
    for full, rel in iter_py_files(root):
        file_count += 1
        try:
            with open(full, encoding='utf-8') as f:
                src = f.read()
        except OSError as e:
            errors.append(('E', rel, 0, '无法读取文件',
                           str(e), '修复文件权限/编码。'))
            continue

        try:
            tree = ast.parse(src, filename=full)
        except SyntaxError as e:
            errors.append(('E', rel, e.lineno or 0, '语法错误',
                           str(e), '修复语法后重跑。'))
            continue

        # bare except（error）
        for lineno in find_bare_excepts(tree):
            errors.append(('E', rel, lineno, 'bare except',
                           'except: 会吞掉 KeyboardInterrupt/SystemExit 等所有异常。',
                           '改为 except Exception: 或更具体的异常类型。'))

        # tab/空格混用缩进（error）
        for lineno in find_mixed_indent_lines(src):
            errors.append(('E', rel, lineno, 'tab/空格混用缩进',
                           '同一行的缩进同时使用了 tab 与空格，Python 解释器可能报 TabError。',
                           '统一使用 4 个空格缩进。'))

        # print 用于日志（warning）
        for lineno in find_prints(tree):
            errors.append(('W', rel, lineno, 'print() 用于日志',
                           'print 输出无法被运维捕获/分级，且不带时间戳。',
                           '用 logging.info/debug/warning 替代。'))

        # job/web 模块缺 __main__ 入口（soft warning）
        pkg = package_of_file(rel)
        if pkg is not None and os.path.basename(rel) != '__init__.py':
            if not has_main_guard(tree):
                errors.append(('W', rel, 0,
                               'entry 模块缺少 __main__ 入口',
                               '{} 层模块作为入口应可直接 python 执行'.format(pkg),
                               '补充 if __name__ == "__main__": 调用主逻辑。'))

    return errors, file_count


def main(argv):
    args = [a for a in argv[1:] if not a.startswith('-')]
    strict = '--strict' in argv[1:]
    root = args[0] if args else 'instock'
    if not os.path.isdir(root):
        print('错误: 路径不存在: {}'.format(root), file=sys.stderr)
        return 2

    errors, file_count = lint(root)
    if not errors:
        print('✓ 质量检查通过（{} 个文件检查）'.format(file_count))
        return 0

    for sev, rel, lineno, what, why, fix in errors:
        mark = '✗' if sev == 'E' else '⚠'
        loc = '{}:{}'.format(rel, lineno) if lineno else rel
        print('{} [{}] {}  {}'.format(mark, sev, loc, what))
        print('  {}'.format(why))
        print('  Fix: {}'.format(fix))

    e = sum(1 for v in errors if v[0] == 'E')
    w = sum(1 for v in errors if v[0] == 'W')
    print('', file=sys.stderr)
    print('✓ 质量检查: {} error, {} warning（{} 个文件检查）'.format(e, w, file_count),
          file=sys.stderr)
    if e > 0:
        return 1
    return 1 if strict and w > 0 else 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
