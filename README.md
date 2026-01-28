# JointoQGIS

JointoQGIS は、Excel / CSV ファイルを QGIS レイヤに自動で Join し、
更新を監視・同期する QGIS プラグインです。

## 主な機能
- Excel → CSV 自動生成（ロック解除待ち対応）
- CSV 更新監視（30秒 / 60秒）
- OBJECTID による安全な Join（fid誤結合防止）
- プロジェクト単位の設定保存・復元（フォールバック対応）

## 対応環境
- QGIS 3.28 LTR ～ 3.40 LTR
- Windows

## 基本的な使い方
1. プラグインを起動
2. Excel ファイルと CSV 出力フォルダを指定
3. Join 対象レイヤとキー（OBJECTID）を選択
4. 必要に応じて「設定を保存して次回復元」をON
5. Excel 更新で自動同期

## Version
- 1.0.0 : 初回安定版リリース
