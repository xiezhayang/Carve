#!/usr/bin/env python3
"""
查看 node2_load1m.pt 的基本信息，并用本地 CSV 做简单推理测试。
用法: 在项目根目录执行  python3 check_pt.py
"""
import os
import sys

# 保证能 import 到 alerter
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def show_pt_info(model_path: str):
    """打印 .pt 文件的基本情况"""
    print("before show_pt_info")
    import torch
    print("after import torch")
    print("=" * 50)
    print("1. 模型文件基本信息")
    print("=" * 50)
    if not os.path.isfile(model_path):
        print(f"文件不存在: {model_path}")
        return None
    size_mb = os.path.getsize(model_path) / (1024 * 1024)
    print(f"路径: {model_path}")
    print(f"大小: {size_mb:.4f} MB")
    print()
    ckpt = torch.load(model_path, map_location="cpu", weights_only=True)
    print(" checkpoint 键:", list(ckpt.keys()))
    print(" seq_len:", ckpt.get("seq_len"))
    print(" input_size:", ckpt.get("input_size"))
    print(" scaler_min (前 3 个):", (ckpt.get("scaler_min") or [])[:3])
    print(" scaler_scale (前 3 个):", (ckpt.get("scaler_scale") or [])[:3])
    state = ckpt.get("state_dict") or {}
    print(" state_dict 参数:")
    for k, v in state.items():
        print(f"   {k}: shape={tuple(v.shape)}, dtype={v.dtype}")
    print()
    return ckpt


def run_simple_test(model_path: str, csv_path: str = "node2_cpu_1m.csv"):
    """用 CSV 跑一段简单推理，输出误差统计"""
    import numpy as np
    import pandas as pd
    from alerter.inference import load_checkpoint, StreamInference

    print("=" * 50)
    print("2. 简单推理测试")
    print("=" * 50)
    if not os.path.isfile(csv_path):
        print(f"CSV 不存在: {csv_path}，跳过推理测试")
        return
    model, scaler_min, scaler_scale, seq_len = load_checkpoint(model_path)
    inf = StreamInference(model, scaler_min, scaler_scale, seq_len)
    df = pd.read_csv(csv_path, comment="#")
    df = df.sort_values("ts").reset_index(drop=True)
    ts = df["ts"].values
    values = df["value"].astype(float).ffill().values
    errors = []
    for i in range(len(ts)):
        inf.push(int(ts[i]), float(values[i]))
        err, _ = inf.step()
        if err is not None:
            errors.append(err)
    if not errors:
        print("数据点不足 seq_len+1，无法计算误差")
        return
    errors = np.array(errors)
    print(f"CSV: {csv_path}, 有效预测步数: {len(errors)}")
    print(f"平均绝对误差: {errors.mean():.6f}")
    print(f"中位数误差:   {np.median(errors):.6f}")
    print(f"最小/最大:   {errors.min():.6f} / {errors.max():.6f}")
    print("前 5 个误差:", [round(e, 6) for e in errors[:5]])
    print("后 5 个误差:", [round(e, 6) for e in errors[-5:]])
    print()


def main():
    print("before main")
    model_path = "node2_load1m.pt"
    print("before show_pt_info")
    show_pt_info(model_path)
    print("after show_pt_info")
    #run_simple_test(model_path, csv_path="node2_cpu_1m.csv")


if __name__ == "__main__":
    main()
