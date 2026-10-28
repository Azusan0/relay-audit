"""批量录制基准指纹。

用法:
    python record_all.py [endpoints.json]

配置文件格式（建议存成 endpoints.json，已在 .gitignore 里排除，避免密钥入库）:
{
  "endpoints": [
    {"vendor": "deepseek", "base": "https://api.deepseek.com/v1",
     "key": "sk-xxx", "flavor": "openai",
     "models": ["deepseek-chat", "deepseek-reasoner"]},
    {"vendor": "anthropic", "base": "https://api.anthropic.com",
     "key": "sk-ant-xxx", "flavor": "anthropic",
     "models": ["claude-sonnet-5", "claude-opus-5"]}
  ]
}

每个模型录一次签名，存到 baselines/<vendor>-<model>.json。
录完可以用 python -m json.tool 看，或直接在 UI 的「基准指纹库」里查看。
"""
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import engine  # noqa: E402


def main():
    cfg_path = sys.argv[1] if len(sys.argv) > 1 else os.path.join(
        os.path.dirname(os.path.abspath(__file__)), "endpoints.json")
    if not os.path.isfile(cfg_path):
        print(f"找不到配置文件: {cfg_path}")
        print("请创建 endpoints.json，格式见脚本头部注释。")
        return 1
    with open(cfg_path, encoding="utf-8") as f:
        cfg = json.load(f)

    ok, fail, skip = [], [], []
    for ep in cfg.get("endpoints", []):
        vendor = ep.get("vendor", "unknown")
        base, key, flavor = ep.get("base", ""), ep.get("key", ""), ep.get("flavor", "openai")
        models = ep.get("models") or []
        if not base or not key:
            skip.append(f"{vendor}: 缺 base 或 key")
            print(f"[跳过] {vendor}: 缺 base/key")
            continue
        for model in models:
            name = f"{vendor}-{model}"
            print(f"[录制] {name}  ({base})", end=" ... ", flush=True)
            try:
                path, sig = engine.record_baseline(name, base, key, flavor, model, samples=3)
                ok.append((name, sig.get("bench"), sig.get("served"), path))
                print(f"OK  bench={sig.get('bench')} served={sig.get('served')}")
            except Exception as e:  # noqa: BLE001
                fail.append((name, repr(e)))
                print(f"失败: {e!r}")

    print("\n===== 汇总 =====")
    print(f"成功 {len(ok)} 个 · 失败 {len(fail)} 个 · 跳过 {len(skip)} 组")
    for name, bench, served, path in ok:
        print(f"  [OK] {name:<34} bench={bench:<6} served={served}")
    for name, err in fail:
        print(f"  [X]  {name:<34} {err[:80]}")
    print(f"\n基准目录: {engine.BASELINE_DIR}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
