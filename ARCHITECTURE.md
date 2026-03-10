# 架構與數據流說明

本文件概述系統各層級、資料流以及後續擴充點。

## 層級劃分

```
應用層  (main.py)            - 參數解析、爬蟲序列執行
  │
爬蟲層  (src/crawlers)       - 六種爬蟲對應六個表
  │
資料庫層  (src/database)      - IPO_DAO 提供 CRUD 與增量
  │
SQLite 存儲  (data/database)  - bid_info, fin_stmts, ...
  │
處理層  (src/processors)     - data_clean 等進行合併 / 特徵
  │
模型層  (src/models)          - preprocess 與 stacking 模組
```

## 初始化流程

1. 讀取 `config.yaml` → `src/utils/config_loader.py` 封裝。
2. 讀取 `.env` (FinMind 令牌)。
3. 建立 SQLite 連線 (context manager)。

## 爬蟲詳述

- **AuctionCrawler**：首要資料來源，可全量或增量抓取。
- 其餘爬蟲（Financial、Market、Price、Revenue、Target）均以 `bid_info` 作為 key 進行 diff，比對新增或更新筆數。
- `src/utils/finmind_manager.py` 負責 Token 循環與額度耗盡等待。

## 增量機制

資料存取層提供 `IPO_DAO.diff_index(raw_table, target_table, key_cols)`，回傳需要更新的 key 列表。
爬蟲接著對應欄位處理後寫回資料庫。

## 儲存與類型

資料寫入前進行欄位轉換 (`date_columns`, `numeric_columns`, `int_columns`)，保證 pandas/SQLite 型別一致。

## 優化建議（未實作）

- 使用 WAL 模式與批量插入設定 (`config.yaml` 範例)。
- 增加單元測試與 CI。
- 建立 API 文件與版本管理流程。

---

**文檔版本**: 1.0.1
**最後更新**: 2026年3月
