import h5py
import numpy as np
import pandas as pd

# 替换为你的 .h5 文件路径
# H5_FILE_PATH = "AmazingData_cache\\basedata\\hist_stock_status\\000004.SZ.h5"
H5_FILE_PATH = "AmazingData_cache\\basedata\\backward_factor\\backward_factor.h5"
# 1. 打开 h5 文件（推荐用 with 语句，自动关闭文件）
with h5py.File(H5_FILE_PATH, "r") as f:
    print("=== .h5 文件整体结构 ===")
    # 打印文件内的顶级组/数据集名称（类似文件夹/文件）
    print("顶级内容列表：", list(f.keys()))
    
    # 2. 遍历所有内容（包括嵌套的组/数据集）
    print("\n=== 遍历所有内容（含嵌套）===")
    def print_h5_structure(name, obj):
        """递归打印 h5 文件结构"""
        # 判断是 组（Group，类似文件夹）还是 数据集（Dataset，类似文件）
        if isinstance(obj, h5py.Group):
            print(f"[组] {name}")
        elif isinstance(obj, h5py.Dataset):
            print(f"[数据集] {name} | 形状：{obj.shape} | 数据类型：{obj.dtype}")
    
    f.visititems(print_h5_structure)
    
    # 3. 读取具体数据集的内容（示例：假设存在名为 'data' 的数据集）
    if "data" in f:
        print("\n=== 读取 'data' 数据集内容 ===")
        dataset = f["data"]
        # 查看前10行数据（避免大文件加载全部内容）
        print("前10行数据：")
        print(dataset[:10] if dataset.shape[0] > 10 else dataset[:])
        
        # 查看数据集的属性（如描述、创建时间等）
        print("\n数据集属性：")
        for k, v in dataset.attrs.items():
            print(f"  {k}: {v}")
    
    # 4. 读取嵌套组中的数据集（示例：group1/sub_data）
    if "group1" in f and "sub_data" in f["group1"]:
        print("\n=== 读取嵌套数据集 group1/sub_data ===")
        sub_dataset = f["group1/sub_data"]
        print("数据形状：", sub_dataset.shape)
        print("数据前5个值：", sub_dataset[:5])


df = pd.read_hdf(H5_FILE_PATH, key="backward_factor")

# 第三步：查看读取结果（和普通DataFrame操作完全一致）
print("=== 读取的完整数据（前5行）===")
print(df.head())  # 查看前5行，快速预览

print("\n=== 数据基本信息（行列数、列名、数据类型）===")
df.info()  # 新手必看，确认数据是否完整

print("\n=== 数据形状（行, 列）===")
print(df.shape)  # 对应你之前看到的2911行左右

print("\n=== 所有列名 ===")
print(df.columns.tolist())  # 查看所有字段名