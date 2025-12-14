#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
更新 `src/main/resources/data.json` 中每个菜谱的 `images` 字段为最后一步的图片路径，
并为每个 step 添加 `imageUrl` 字段。

Usage:
    python ./scripts/update_data_images.py

备份会保存为 `src/main/resources/data.json.bak`。
"""
import json
import shutil
from pathlib import Path


def pad(n: int) -> str:
    return f"{n:02d}"


def sanitize_name(name: str) -> str:
    # 简单替换路径分隔符，保留中文与空格等
    return name.replace("/", "_").replace("\\\\", "_")


def main():
    repo_root = Path(__file__).resolve().parents[1]
    data_path = repo_root / "src" / "main" / "resources" / "data.json"
    if not data_path.exists():
        print(f"找不到文件: {data_path}")
        return

    backup_path = data_path.with_suffix(data_path.suffix + ".bak")
    shutil.copyfile(data_path, backup_path)
    print(f"已备份 {data_path} -> {backup_path}")

    with data_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    # 支持顶层为 list 或 dict（包含列表）的情况
    if isinstance(data, dict) and data.get("dishName") is None and "recipes" in data:
        items = data.get("recipes", [])
    else:
        items = data

    for item in items:
        dish_name = item.get("dishName", "unknown")
        safe = sanitize_name(dish_name)

        steps = item.get("steps", []) or []
        # 为每一步添加 imageUrl
        last_step_num = None
        for s in steps:
            # 尝试读取 stepNumber，否则使用索引+1
            sn = s.get("stepNumber")
            if sn is None:
                # fallback: use index order
                sn = steps.index(s) + 1
            s["imageUrl"] = f"/generated_images/{safe}/step_{pad(int(sn))}.png"
            last_step_num = int(sn)

        if last_step_num is None:
            # 没有 steps 时，默认 step 01
            last_step_num = 1

        # 将 images 字段设为最后一步对应的图片（字符串）
        item["images"] = f"/generated_images/{safe}/step_{pad(int(last_step_num))}.png"

    # 如果原数据为 dict 且我们替换了 `recipes`，则需要保留结构
    if isinstance(data, dict) and data.get("dishName") is None and "recipes" in data:
        data["recipes"] = items

    with data_path.open("w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"已更新文件: {data_path}")


if __name__ == "__main__":
    main()
