#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""InStock 依赖层级 linter。

用 ast 静态解析 instock/ 下所有 .py 文件，检查 instock 内部 import 是否
符合架构层级规则。发现违规输出 agent-actionable 报告，退出码 1；否则 0。

用法: python scripts/lint-deps.py [path]   # 默认 path=instock/
"""

import ast
import os
import sys

# 架构层级映射（权威，来自知识图谱）。
# Layer 0 foundation / Layer 1 core / Layer 2 domain / Layer 3 entry。
LAYER_PACKAGES = {
    'lib': 0,
    'core': 1,
    'trade': 2,
    'job': 3,
    'web': 3,
}

# 每个 importer 顶层包允许 import 的 instock 内部包集合。
# 同包内 import（intra-package）始终允许；跨层只允许向下。
# lib 不得 import 任何更高层 instock 包；core 可引 lib/core；trade 可引
# lib/core/trade；job/web（entry）可引 lib/core/trade，entry 层之间不得互引。
ALLOWED_IMPORTS = {
    'lib': {'lib'},
    'core': {'lib', 'core'},
    'trade': {'lib', 'core', 'trade'},
    'job': {'lib', 'core', 'trade', 'job'},
    'web': {'lib', 'core', 'trade', 'web'},
}

# 各 importer 包违规时的规则说明。
FORBIDDEN_REASON = {
    'lib': '必须零内部依赖',
    'core': '不得 import trade/job/web',
    'trade': '不得 import job/web',
    'job': '不得 import web（entry 层之间亦不得互引）',
    'web': '不得 import job（entry 层之间亦不得互引）',
}

# 既有已知违规：键为 (文件相对路径, 被引模块)。
# harness 阶段不修，仅标注。详见 docs/ARCHITECTURE.md。
KNOWN_VIOLATIONS = {
    ('instock/lib/trade_time.py', 'instock.core.singleton_trade_date'):
        '已知既有违规，harness 阶段不修，见 docs/ARCHITECTURE.md。',
}

# 已知违规对应的修复建议（覆盖通用生成逻辑）。
KNOWN_FIX_OPTIONS = {
    ('instock/lib/trade_time.py', 'instock.core.singleton_trade_date'):
        '把 stock_trade_date 挪到 lib，或把 trade_time 挪到 core。',
}

# 扫描时跳过的目录名。
EXCLUDE_DIRS = {'__pycache__', 'static', 'templates', '.git', '.idea', 'venv', 'env'}


def iter_py_files(root):
    """遍历 root 下所有 .py 文件，跳过 EXCLUDE_DIRS 与 .gitignore 命中的文件。

    返回的 rel 路径保留 instock/ 前缀（相对 root 的父目录），以便
    package_of_file 与 KNOWN_VIOLATIONS 的键统一匹配。
    """
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
            rel = os.path.relpath(full, base).replace(os.sep, '/')
            if any(p in rel for p in ignore_patterns):
                continue
            yield full, rel


def package_of_file(rel_path):
    """由文件相对路径推断其所属 instock 顶层包。

    例：instock/lib/trade_time.py → 'lib'；instock/core/indicator/foo.py → 'core'。
    instock/__init__.py 与 instock/bin/* 等不在 LAYER_PACKAGES 中，返回 None。
    """
    parts = rel_path.replace(os.sep, '/').split('/')
    if len(parts) < 2 or parts[0] != 'instock':
        return None
    if parts[1] not in LAYER_PACKAGES:
        return None
    return parts[1]


def target_package_of(module):
    """从 import 模块名取 instock 下第一个子包名。

    'instock.core.singleton_trade_date' → 'core'；'instock' → None。
    """
    if not module or not module.startswith('instock'):
        return None
    tail = module[len('instock'):].lstrip('.')
    if not tail:
        return None
    return tail.split('.')[0]


def extract_instock_imports(tree):
    """从 ast 树提取所有 instock 内部 import，返回 [(lineno, module, symbol)]。

    symbol 仅用于修复建议文本：ImportFrom 取首个 imported name；Import 取模块末段。
    """
    out = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if alias.name.startswith('instock'):
                    symbol = alias.name.split('.')[-1]
                    out.append((node.lineno, alias.name, symbol))
        elif isinstance(node, ast.ImportFrom):
            mod = node.module or ''
            if mod.startswith('instock') and (node.level is None or node.level == 0):
                symbol = node.names[0].name if node.names else mod.split('.')[-1]
                out.append((node.lineno, mod, symbol))
    return out


def build_fix_options(importer_pkg, importer_module, symbol, target_pkg):
    """为未知违规生成修复建议。"""
    if importer_pkg == 'lib':
        return '把 {} 挪到 lib，或把 {} 挪到 {}。'.format(symbol, importer_module, target_pkg)
    return ('把 {} 挪到 {} 或更低层，'
            '或把 {} 挪到 {}。').format(symbol, importer_pkg, importer_module, target_pkg)


def lint(root):
    violations = []
    file_count = 0
    for full, rel in iter_py_files(root):
        file_count += 1
        importer_pkg = package_of_file(rel)
        if importer_pkg is None:
            continue
        try:
            with open(full, encoding='utf-8') as f:
                src = f.read()
        except OSError as e:
            violations.append(('?', rel, 0, '?', '无法读取文件: {}'.format(e)))
            continue
        try:
            tree = ast.parse(src, filename=full)
        except SyntaxError as e:
            violations.append(('E', rel, e.lineno or 0, '?',
                               '语法错误: {}'.format(e)))
            continue

        importer_layer = LAYER_PACKAGES[importer_pkg]
        allowed = ALLOWED_IMPORTS[importer_pkg]
        importer_module = os.path.splitext(os.path.basename(rel))[0]

        for lineno, module, symbol in extract_instock_imports(tree):
            target_pkg = target_package_of(module)
            if target_pkg is None:
                continue
            if target_pkg in allowed:
                continue
            key = (rel.replace(os.sep, '/'), module)
            known_extra = KNOWN_VIOLATIONS.get(key)
            fix = (KNOWN_FIX_OPTIONS.get(key)
                   if key in KNOWN_FIX_OPTIONS
                   else build_fix_options(importer_pkg, importer_module,
                                         symbol, target_pkg))
            target_layer = LAYER_PACKAGES.get(target_pkg, '?')
            violations.append((importer_pkg, rel, lineno, module,
                               target_pkg, target_layer, fix, known_extra))

    return violations, file_count


def main(argv):
    root = argv[1] if len(argv) > 1 else 'instock'
    if not os.path.isdir(root):
        print('错误: 路径不存在: {}'.format(root), file=sys.stderr)
        return 2

    violations, file_count = lint(root)

    if not violations:
        print('✓ 依赖层级检查通过（{} 个文件检查）'.format(file_count))
        return 0

    # ponytail: 已知违规降级为 warning（不致命），只在出现未知违规时 exit 1。
    # 这样 CI 不会从第一天起就红，但能捕获新增层级回归。
    errors = [v for v in violations if len(v) != 8 or v[7] is None]
    warnings = [v for v in violations if len(v) == 8 and v[7] is not None]

    for v in violations:
        if len(v) == 5:  # 读取/语法错误
            _, rel, lineno, _, msg = v
            print('✗ {}:{}  {}'.format(rel, lineno, msg))
            print('  Fix: 修复语法/编码后重跑。')
            continue
        importer_pkg, rel, lineno, module, target_pkg, target_layer, fix, known_extra = v
        importer_layer = LAYER_PACKAGES[importer_pkg]
        marker = '⚠' if known_extra else '✗'
        print('{} {}:{}'.format(marker, rel, lineno))
        print('  imports {} (layer {} → layer {})'.format(
            module, importer_layer, target_layer))
        print('  {} (layer {}) {}。'.format(
            importer_pkg, importer_layer, FORBIDDEN_REASON[importer_pkg]))
        if known_extra:
            print('  {}'.format(known_extra))
        print('  Fix options: {}'.format(fix))

    if errors:
        print('', file=sys.stderr)
        print('✗ 依赖层级检查未通过：{} 处未知违规、{} 处已知违规（{} 个文件检查）'.format(
            len(errors), len(warnings), file_count), file=sys.stderr)
        return 1
    print('⚠ 依赖层级检查通过（含 {} 处已知违规，见 docs/ARCHITECTURE.md；{} 个文件检查）'.format(
        len(warnings), file_count))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))
