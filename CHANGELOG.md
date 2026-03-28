# Changelog

## 1.0.1 - 2026-03-28

### バグ修正
- `_get_interval_sec()` が自身を再帰呼び出しして `RecursionError` が発生していた問題を修正
- `initGui()` でウィジェット生成前にエイリアスを設定していたため `AttributeError` になっていた問題を修正（エイリアス設定を `show_dock()` 内に移動）
- `_on_project_read()` が二重定義されており、正しい方が上書きされていた問題を修正
- `_do_export()` 内の診断コードで `self.csv_path`（未定義）を参照していた問題を修正（`self.latest_csv` に修正）

---

## 1.0.0 - 2026-01-28

### 改善
- CSVフォルダ復元を安定化（内部パスの再初期化）
- レイヤ選択の復元を強化（id → source → name の順でフォールバック）

---

## 0.9.0

### 新機能
- 初回安定版リリース
- Excel / CSV 自動同期
- OBJECTID Join 安定化
- プロジェクト設定の保存・復元（フォールバック対応）
- QGIS 3.28 ～ 3.40 LTR 対応
