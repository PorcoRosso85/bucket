# Error taxonomy (object-store + distributed core)

> Scope note
>
> 「全てのエラーを網羅」は、採用する故障モデル(=何を起こり得るとみなすか)を定義しない限り無限集合になります。
>
> このドキュメントでは **このリポジトリの実装/テストが扱う故障モデル** を「網羅」とみなします。
>
> - 対象: stdlib-onlyの S3(サブセット) + レプリケーションコア (固定メンバー)
> - 非対象(今は未実装/仕様外): メンバーシップ変更、複数AZ跨ぎの配置戦略、byzantine(悪意)耐性、実OSのfsync/renameの完全モデル、TLS/SigV4

## 1. APIレベル(S3互換サブセット)

- `InvalidBucketName` (400)
- `InvalidRequest` (400) (例: 不正な%エンコードなど)
- `NoSuchBucket` (404)
- `NoSuchKey` (404)
- `BucketAlreadyOwnedByYou`/`BucketAlreadyExists` (409)
- `BucketNotEmpty` (409)
- `InternalError` (500)

### 1.1 HTTPレベル(プロトコル/入力)

- `BadRequest` (400)
  - 破損したHTTP、未対応の転送方式などでリクエストを解釈できない
- `PayloadTooLarge` (413)
  - PutObjectなどで **最大ボディサイズ(16MiB)** を超える

## 2. クラスタ/レプリケーション層

- `NoQuorum` (503相当): 多数派に到達できずコミットできない/すべきでない
- `NotLeader` (503相当): 書き込みはリーダに集約される (※ 本リポジトリのHTTP層はまだ固定リーダの簡易実装)
- `StaleTerm`/`StaleLeader` (内部状態): 古いterm/viewのメッセージは無視される
- `NeedsResync`/`StaleReplica` (内部状態): ディスク/ネットワークの破損検出によりリシンクが必要

## 3. 故障モデル(シミュレーションで注入)

### 3.1 Network faults

- Drop (送信が落ちる)
- Delay (遅延)
- Dup (重複)
- Misdirect (誤配送)
- Corrupt (ビット反転)
- Partition / Heal (リンク遮断/復旧)

### 3.2 Process faults

- Crash (ノードが落ちる: **リーダも対象**)
- Recover (復帰)
- Pause / Resume (停止/再開)

### 3.3 Disk faults (簡易モデル + fsync境界)

- write_lost (追記が消える)
- write_corrupt (追記内容が破損)
- write_torn (追記が途中までしか書けない)
- write_misdirect (誤った内容/位置に書く: 結果として破損検出→resync)
- scrub_corrupt (後から保存済みデータが破損)

加えて **fsync境界** として、

- `unstable_log` (未fsync) は crash で失われうる
- `durable_log` / `snapshot` は crash 後も残る

をモデル化しています。

## 4. リポジトリ内の保証範囲

- **Safety(安全性)**
  - `commit_index <= durable_last_index`
  - `applied_index <= commit_index`
  - `commit_index >= snapshot_index`
  - (粗いチェック) 稼働中に leader は高々1つ

- **Durability(耐久性; モデル内)**
  - コミットは「リーダがfsync済み + 多数派ack」を条件にする
  - よって **コミット済みエントリはcrash後も失われない** (fsync境界モデルの範囲内)

- **Convergence(収束)**
  - 故障注入を停止しresyncを許可すると、稼働中レプリカの状態が同一に収束する
