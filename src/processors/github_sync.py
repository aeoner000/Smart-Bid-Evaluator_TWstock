# src/utils/github_sync.py
import subprocess

def push_specific_files(file_list, message="Update data"):
    """
    file_list: 像是 ['data/price.csv', 'data/auction.json']
    """
    try:
        # 1. 只 add 指定的檔案
        for file in file_list:
            subprocess.run(["git", "add", file], check=True)
        
        # 2. 檢查是否有東西需要 commit (避免沒變動卻報錯)
        status = subprocess.run(["git", "diff", "--cached", "--quiet"])
        if status.returncode == 0:
            print("查無變動，跳過推送。")
            return

        # 3. 執行提交與推送
        subprocess.run(["git", "commit", "-m", message], check=True)
        subprocess.run(["git", "push"], check=True)
        print(f"已成功推送：{', '.join(file_list)}")
    except Exception as e:
        print(f"推送過程中出錯: {e}")