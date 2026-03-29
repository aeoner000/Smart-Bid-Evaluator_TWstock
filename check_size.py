import os
import pkg_resources

def get_size(path):
    total_size = 0
    try:
        for dirpath, dirnames, filenames in os.walk(path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                if not os.path.islink(fp):
                    total_size += os.path.getsize(fp)
    except Exception:
        pass
    return total_size

pkgs = []
for d in pkg_resources.working_set:
    # 嘗試抓取套件的實際安裝目錄
    path = os.path.join(d.location, d.key.replace('-', '_'))
    if os.path.exists(path):
        size = get_size(path)
        if size > 0:
            pkgs.append((d.project_name, size))

# 加上一些沒被抓到但常見的目錄名稱
additional_names = [('numpy', 'numpy'), ('pandas', 'pandas'), ('scipy', 'scipy'), ('matplotlib', 'matplotlib')]
# ... (略過複雜邏輯，直接排序輸出)

for name, size in sorted(pkgs, key=lambda x: x[1], reverse=True):
    print(f"{name:<25}: {size / 1024 / 1024:>7.2f} MB")
