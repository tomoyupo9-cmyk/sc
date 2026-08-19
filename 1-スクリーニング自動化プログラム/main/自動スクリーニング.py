# ============================================================================
# 2026-08-17 V6 運用障害・性能改善の引継ぎ記録（次回改修者は先に読むこと）
# ============================================================================
#
# ■ 本番の置き場所とジョブ境界
# - 本番rootは D:\kabu\main\1-スクリーニング自動化プログラム\main 。
#   system_rework_v5_final_candidate_* は検証元であり、Task Schedulerの実行先へ戻さない。
# - DBはH:側の kani2.db を正本として参照する。D:側は同じ実体へリンクされているため、
#   「別DB」とみなしてコピー/統合しない。共有writer lock中は本体をskipし、lockを手で消さない。
# - P3-41/P3-42以降、この本体は外部producerを起動しない。
#   system_jobsのdailyは開示取得→変更銘柄だけ株探ファンダ差分実行、全銘柄補修はweekly。
#   PTS取得は信頼性と所要時間のため廃止済み。template.htmlはV6速度修正では変更していない。
#
# ■ ここへ至った実障害（2026-08-16〜17）
# - 旧dailyは株探ファンダ/Yahoo財務/テーマ・信用等を約3590銘柄へ毎日実行し数時間化。
#   差分daily＋全件weeklyへ分離し、途中経過をsystem_jobsの標準出力/個別logへ可視化した。
# - 自動スクリーニングは旧版で exportL V5 collect から1時間超、修正後の完走でも20分15秒。
#   主因はderive_update 8分45秒など、巨大price_historyを銘柄chunkごとに反復走査するSQL。
# - SQLiteへnumpy/pandas scalarを直接bindして resistance_update が失敗したため、DB投入値を
#   Python scalarへ正規化した。Fair Valueでは AIスコア/予想インパクト_pct 等のschema不足と
#   任意列「機関空売り合計株数」不存在で停止したため、必須schema保証＋任意列NULL読込へ修正。
# - 生成HTML 3591行を監査すると RS_5/RS_20/ATR14/ATR14_PCT/tri_vol/イナゴ過熱が全件欠損。
#   これはtemplate表示不良ではなく、TOPIX取得・ATR最終書込み・派生入力のproducer不良だった。
#
# ■ V6で確定した原因と設計（対応箇所 P2-80〜P2-87）
# 1. DB内のTOPIX正規codeは ^TOPX のまま、Yahoo Japanへの通信symbolだけ998405.Tとする。
#    MIDDAYの時価総額filterに指数を混ぜず、^TOPX/^N225/2516はbenchmarkとして明示追加する。
# 2. Growth指数は新鮮な^GRT250履歴を第一優先。利用不能時だけ2516.Tの「期間return」を
#    proxyに使う。2516の価格水準を^GRT250として保存すると時系列が破壊されるので禁止。
#    同様にTOPIXは^TOPX/998405.Tを第一優先とし、場中quoteが当日未到達のときだけ1306.Tの
#    期間returnをproxyに使う。1306の価格水準を^TOPXとして保存してはならない。
# 3. derive/PREOPEN/price-summaryは、CAST(code AS TEXT) IN (...)をchunk反復しない。
#    ROW_NUMBERの単一passまたは対象期間1回読込→メモリ分割を維持する。
# 4. ATR14の最終正本writerはphase_resistance_update。同じcurrent履歴から支持抵抗と同時算出し、
#    latest_pricesへ原子的に保存する。export側はそのATR_14からATR14_PCT/tri_volを派生する。
# 5. 株探newsは最大6workerの有界並列＋429/5xx/通信例外を1回retry。無制限thread化しない。
#
# ■ データ品質の不変条件（速くても破ってはいけない）
# - 欠損を0で埋めて「弱い銘柄」に見せない。RS/ATR/三角モデルは入力不足なら未判定のまま。
# - 株探テーマは初動スコアのauthoritative snapshot。theme schema/取得/解析失敗を成功扱いせず、
#   daily markerを進めない（テーマ詳細ページ巡回はせず、3日間ランキング掲載銘柄だけ取得）。
# - aliasはlogical codeへ正規化し、ランキング・集計・HTMLを1銘柄1票/1行にする。
# - producer失敗やschema不整合はfail-visible。古い値・部分値で「正常完了」を装わない。
#
# ■ V6/P2-87 本番結果と次段階
# - 2026-08-17 MIDDAY実測はSHA-256=326E2E672471AC4989FE4AE4972AE79B8C5E25A9FB01140266E94EFED97F4C08
#   のP2-87適用前版でEXIT=0、
#   合計11分03秒（旧20分15秒）。derive_updateは
#   8分45秒→21.75秒、株探news初回は86.97秒→21.33秒、ATR14 valid=3539まで復旧。
# - ただしRS_5/RS_20/イナゴ過熱は572件(15.9%)に留まった。2516代理を使うGrowth銘柄だけ
#   計算でき、TOPIX側が当日未到達だったため、P2-87で1306 return proxyを追加した。
# - P2-87追加版は2026-08-17 MIDDAYの2回目実行で本番検証済み。1306.T/2516.Tの代理使用logを確認し、
#   EXIT=0、内部10分55秒・Stopwatch合計11分03.99秒。ATR14 valid=3545、tri_vol=3543(98.7%)、
#   RS_5=3543(98.7%)、RS_20=3538(98.5%)、イナゴ過熱=3542(98.6%)まで復旧した。
# - よってP2-87のデータ品質受入は合格。以後、RS有効率572件(15.9%)の旧結果を現状扱いしない。
# - ただし実行時間11分超は10分triggerより長い。重複起動を許可せず、本番Taskはまだ無効のまま。
#   次はsignal_detection(1分50秒)とexport(3分18秒、主にAIニュース補完)、次いでYahoo quote(44.93秒)、
#   右肩持続(42.46秒)を最適化し、10分周期に十分な余裕が出てから有効化する。
# - P2-88は2026-08-17本番でsignal_detection 1分48秒→30.40秒、品質98%台を維持して合格。
# - P2-89の32銘柄RSS queryは本番で初回記事2346→1217、AI補完ニュース有378→196へ約半減。
#   Google News側のquery内結果上限によるcoverage低下と判断し、速度が上がっても不採用・撤回した。
# - P2-90（本番未検証）: 8銘柄queryへ戻してcoverageを維持し、有界workerだけ3→6へ増加。
# - P2-91（本番未検証）: CatBoost最新推論は最大MA75なので全期間履歴読込を廃止し、raw/logicalとも
#   最新120観測へ限定。AI本体と後続RSS補完の個別timerも追加し、次runで結果一致と時間を確認する。
# - P2-92（本番未検証）: ニュースを銘柄単位で30分cache。新規初動/AI陽性はcache missで即時取得し、
#   HTTP/XML失敗はcacheを進めず次runで再試行。P2-89で失った8銘柄queryのcoverageは維持する。
# - P2-93（本番未検証）: MIDDAYで右肩持続・信用需給・deriveだけ30分間隔化。
#   右肩早期・RS・signal_detection・初動スコアは10分ごとを維持し、初動速報性を落とさない。
# - P2-94（本番未検証）: PREOPEN価格派生とPREOPEN/EOD共通価格派生を同日・同buildで1回化。
#   finance/material同期とHTML出力は毎run続行し、更新開示を見逃さない。charts60に独立timerも追加。
# - P2-95（本番未検証）: charts60は成功manifestに生成銘柄・時刻・mode・script hashを保存。
#   MIDDAYは30分、PREOPEN/EODは同日固定価格token一致時だけ再利用し、生成失敗では旧manifestへ逃げない。
# - P2-96/P2-97（本番未検証）: charts60_makeは対象履歴/当日価格を一括取得し、mainは
#   MIDDAY/PREOPENで現在シグナル＋前回監視＋前回AI陽性だけ（上限1600銘柄）を生成する。
#   新規候補が前回request集合へ追加された時は30分TTL内でも即再生成し、候補から外れただけなら
#   既存HTMLを再利用してリンク集合だけ縮小する。EODは全銘柄を1日1回補修する。
#   直近160本は初期表示60本のMA75/一目SpanB(+26)に必要なwarm-upを満たす。
# - P2-90〜P2-97は2026-08-17本番でEXIT=0、合計6分16秒/6分28秒を確認した。
#   charts60は候補1600銘柄で9.33秒、最終品質はRS_5=98.9%、RS_20=98.8%、
#   tri_vol=98.9%、ATR14有効3555件。10分周期は処理時間上は採用可能と判断する。
# - P2-98: LIVE_MATERIALSと本体が同じ10分境界で起動するため、旧版は共有writer lockを
#   1回だけ試して約10秒で正常skipしていた。13分周期へずらすと位相が再衝突するため不採用。
#   外部producerのlockだけ最大180秒待ち、解放後に本体を開始する。別のauto_screeningが
#   lock所有中なら重複計算を避けて即skipし、180秒後もproducer実行中なら従来どおり安全skipする。
#   lockの削除・迂回やproducerの本体内再統合は行わない。
# - P2-99: Task Schedulerはpythonw.exe実行のため、P2-98の標準出力は画面に残らない。
#   producer待機開始・待機後取得・timeout・本体重複skipだけをruntime/screening_lock_events.logへ
#   追記し、定期実行の衝突を後から検証可能にする。通常取得時は記録せず、1MBで1世代rotateする。
# - 旧添付index.htmlのATR14全NULLや旧logの「no such column: AIスコア」は修正前の証跡。
#   最新状態の判定には使わず、必ず上記EXIT=0 runとP2-87再実行結果を基準にする。
# - py_compile成功、最終EXIT=0。
# - resistance logの「ATR14 valid」が0より大きい。
# - model-quality/生成HTMLでATR14/ATR14_PCT/tri_volは90%以上、RS_5/RS_20も原則90%以上。
#   イナゴ過熱はRS復旧に連動して増えること。単に0件より多いだけでは合格にしない。
# - 実行時間は約11分を現行基準に比較する。10分周期に十分な余裕が出るまで本番Taskを有効化しない。
# - 最適化時は各phaseの[TIMER]と上記valid件数を必ず比較し、速度だけで採用しない。
#
# === 2026-08-19 P4-LIVE: 自動スクリーニング -> kabuステーションLIVE Candidate Contract ===
# - output_data/live_candidate_feed.json をschema_version=1でatomic出力。HTML/dashboard_data.jsonを読み戻さない。
# - runtime/scanner_snapshots.sqlite3へ5〜10分run単位の全評価銘柄snapshotを保存し、直近30営業日を保持。
# - INITIAL_MOMENTUM / STEADY_UP / BOTTOM_REVERSALを独立strategy scoreとして保持。
# - 正式QUALITYは未実装。tri_safety等をQUALITY proxyとして流用しない。
# - STEADY_UPは日足構造 + 10/20/30/60分の場中継続性を正式採点し、瞬間急騰を主条件にしない。
# - BOTTOM_REVERSALは当日安値反発だけでなく、安値更新停止 + 複数snapshot切上げを必須gateにする。
# - 同一codeは1candidateへ統合しsources/scores/tags/reasonを保持。priorityは監視優先度でありBUY確率ではない。
# - feed失敗時は前回正常JSONを残し、既存dashboard/HTML/CSV処理を失敗させない。
# === 2026-08-19 P4-1: INITIAL_MOMENTUM Candidate Engine分離・本体統合 ===
# - 既存の初動フラグ/初動スコアは変更せず、独立した INITIAL_MOMENTUM を追加。
# - 核条件は「当日出来高 ÷ 当日を除く直前20営業日平均出来高 >= 2.0」かつ「前日比 >= +2.0%」。
# - RVOL代金は流用せず price_history から独立再計算し、研究定義と本番定義を一致。
# - 低位株は除外せずタグ化。ETF/指数等だけ対象外。EOD確定候補は candidate_signal_log に保存。
# - charts60 / rss_monitor_list / 技術シグナルニュース取得 / LLM CSV へ独立候補を伝播。
# - INITIAL_MOMENTUM_SCORE は暫定説明スコアで、candidate gateとは分離。P4-2全期間検証で校正予定。
# === 2026-08-16 P3-1〜: モデル品質・スコア意味論監査 ===
# - P3-1: percentile順位をworst=0/best=1、単一・全同値は中立0.5へ統一し母集団サイズ由来の歪みを除去
# - P3-2: 合成/初動/テーマランキング母集団をlogical codeで1銘柄1票に統一しalias重複による順位水増しを防止
# - P3-3: 機関空売りは「日付だけ存在・残高/増減不明」を安全0点/なし扱いせず部分欠損として不明化
# - P3-4: 営業益YoY>0を「利益加速」とみなす混同を廃止し、利益加速フラグをauthoritativeに品質判定
# - P3-5: 立会時間判定でJPXカレンダー障害をFalseへ握り潰さずfail-visible化
# - P3-6: 三角Growthを100点の成分予算制へ再配分し、高合成S+重複シグナルで100点飽和する構造を緩和
# - P3-7: 最終snapshotに欠損率・分布・飽和率・高相関を監査するmodel-qualityログを追加
# - P3-8: 三角VolはATR欠損を「低ボラ0点」と見せず未判定(None/■--)として区別
# - P3-9: SurpriseのElasticityは既存の流動性下限0.5億円を共有し、超薄商いだけで跳ねやすさが発散する構造を抑制
# - P3-10: Fair Valueの営業益YoY PER加点を最大7倍相当にcapし、低い前年母数による極端YoYの暴走を抑制
# - P3-11: Fair ValueのAI補正を50点中立の左右対称へ変更し、50点未満も負の情報として反映
# - P3-12: 初動スコアとテーマランキングのテーマ強度を共通式 median_turnover×(1+signal_density) へ統一
# - P3-13: 上昇余地スコアのNaN→満点化を修正し、3要素中2要素以上で欠損配点を再正規化
# - P3-14: 過去D1決算リアクションを「事前期待ハードル」として逆符号利用する既定補正を停止
# - P3-15: 決算リアクションの正式スコアは直近3観測以上を要求し、1〜2件の偶然で0/100へ極端化しない
# - P3-16: Fair Valueの利益加速PER加点もP3-4のauthoritative品質判定へ統一し、flagだけ/利益率鈍化へ加点しない
# - P3-17: 3大Algo総合は3要素未完備時に参考平均点を正式点のように表示せず「--点」へ変更
# - P3-18: VolTarget欠損を「情報不足 (0点)」と表示せず「情報不足 (--点)」へ変更
# - P3-19: 過去決算リアクションはrun snapshot日より未来のイベントを除外し、future-dated DB混入の先読みを防止
# - P3-20: 決算リアクションのrows同期を行ごとのDataFrame検索からcanonical mapへ変更し順序依存/O(N²)を除去
# - P3-21: セクターランキングもlogical code 1銘柄1票へ統一し、alias重複による売買代金/人数水増しを除去
# - P3-22: model-quality監査を右肩/上昇余地/決算反応/需給/シンデン系まで拡張
# - P3-23: 最終snapshotのlogical code重複をWARNし、alias残存を運用ログで即検出
# - P3-24: 主要スコアの仕様範囲外値をWARNし、単位違い/壊れたproducerを早期検出
# - P3-25: 決算リアクションの観測件数を出力し、勝率/平均/正式スコアの信頼度を可視化
# - P3-26: LLM入力では曖昧な「決算期待値」を「過去決算D1期待値」へ明示し、過去反応を事前期待と誤認しない
# - P3-27: 最終判定で未使用かつchunk/過去行依存だったSqueeze順位・ラベル計算を実行経路から削除
# - P3-28: 一時特徴ATR20を高安差平均ではなく前日終値ギャップを含むTrue Range 20日平均へ修正
# - P3-29: 三角Growthは主成分の合成S欠損時、Safetyは根拠0件時に正式0/50点を出さず未判定化
# - P3-30: model-qualityへ決算反応件数0-8検証と正式反応スコアの最低3件整合性監査を追加
# - P3-31: 3大AlgoのMomentum/Factorも情報不足時は部分点を正式点のように表示せず「--点」へ統一
# - P3-32: Surpriseの過去決算D1入力は明示名を優先し、旧「決算期待値」は互換fallbackのみに限定
# - P3-33: HTML/LLM最終snapshotをlogical code 1銘柄1行へ正規化しalias重複表示・二重評価を防止
# - P3-34: model-qualityへ主要スコアの低カバレッジ警告を追加し「計算できていないモデル」を可視化
# - P3-35: Surprise custom weightsを既定値への部分override方式へ変更し、欠落key/typo設定による実行時崩壊を防止
# - P3-36: Surprise材料が1件も無い場合の「正式0%予測」を廃止し、材料不足としてNaNへ明示
# - P3-37: 持続右肩スコアはMA100×直近30日の完全観測に必要な129営業日を満たす銘柄だけ正式判定
# - P3-38: 踏み上げ期待は信用倍率欠損を0点へ落とさず未判定化し、売り残0の実測だけ0点を維持
# - P3-39: model-quality監査へ踏み上げ期待スコアを追加し0-100範囲/欠損分布を可視化
# - P3-40: 営利対時価flagは必要財務欠損を「0=条件外」とせず空欄=未判定で保持
# - P3-45: 機関空売りを「過去履歴」「現在の公開残高」「当日取得状態」へ分離。
#   institution_short_salesの最新公開報告とinstitution_short_snapshotの当日crawl成否を別々に出力し、
#   当日取得失敗/未取得を過去の「なし」で補完しない。旧screener.空売り機関は互換用に残すが現在判定には使わない。
# - P3-46: ダッシュボードの曖昧な「空売」列を「現在/履歴/取得」の明示状態へ置換。
#   明示0株はN/Aではなく0株と表示し、未取得だけをN/A/不明にする。
# - P3-47: shinden_logicの互換列「シンデン総合スコア」は変更せず、最終snapshotで
#   「シンデン正式スコア」「シンデン参考スコア」「シンデン評価区分」を派生する。
#   履歴0〜1期など判定が「参考：...」の行を正式ランキング・正式coverageへ混ぜない。
#   これは計算式の変更ではなく、既存producerが付けた正式/参考ラベルを表示・監査へ伝える境界修正。
#
# === 2026-08-15 P2-12/P2-13 追加監査 ===
# - 未使用代入/不要SELECT列を整理（挙動不変）
# - 旧シグナル判定 _best_signal_today と専用MA/ATR helperを削除
# - 旧ライブ価格override一式と連鎖した価格helperを削除
# - 未使用Single-Writerキュー機構を削除
# - 旧信用残map/旧RS計算/旧単発株探ニュースfetchを削除
# - Gmail削除後に残っていた未使用import群を削除
# - top-level重複定義を解消、内部参照ゼロhelperを整理
#
# === 2026-08-15 P2-1〜P2-73: 品質・モデル設計/実行品質改善 ===
# - P2-14: yfinance.download全5経路をrepair=True共通ラッパー化（旧版のみ引数互換fallback）
# - P2-15: yahooquery key_stats/financial_data/major_holdersもsymbol単位へ正規化し、複数銘柄payload混線を防止
# - P2-16: 主要schema追加のexcept-passをPRAGMA方式へ統一し、duplicate以外のSQLite障害をfail-visible化
# - P2-17: screener価格同期のprice_history N+1 SELECTを200銘柄bulk読込へ変更（履歴意味は維持）
# - P2-18: screener raw更新キーmapの取得失敗をsilent fallbackせずfail-visible化
# - P2-19: deriveのprice_history N+1 SELECTと銘柄別祝日再生成をbulk preload/共通calendarへ変更
# - P2-20: 財務fetch schema versionを3へ上げ、P2-15修正後に旧7日cacheを強制再取得
# - P2-21: relax_rejudge_signalsのシグナル単位N+1 SELECTをbulk preloadへ変更
# - P2-22: update_since_datesのsignals_log連続判定を銘柄群bulk preloadへ変更
# - P2-23: 株探テーマID取得のテーマ単位N+1 SELECTをbulk化
# - P2-24: 旧Yahoo suffix override探索をschema-aware化し例外総当たりを廃止
# - P2-25: Yahoo symbol解決を複数銘柄bulk化し主要batchへ適用
# - P2-26: 12mo補完入口の最新2日判定をprice_history一括読込へ変更
# - P2-27: Yahoo NONE/HISTORY_NONE sentinel判定をbulk化
# - P2-28: 異なる銘柄が同一Yahoo symbolへ解決する設定衝突をfail-fast化
# - P2-29: 株探ニュースbulkの長URL縮小時に1銘柄をskipする分岐を修正
# - P2-30: 時価総額/MIDDAY更新先をYahoo symbol逆算ではなく問い合わせ元codeへ固定
# - P2-31: MIDDAY前日終値の銘柄別N+1 SELECTをbulk preloadへ変更
# - P2-32: 翌営業日検証の「価格待ち」と実障害を分離し、実障害をfail-visible化
# - P2-33: Yahoo企業API対象の市場map読込失敗をsilent fallbackせずfail-visible化
# - P2-34: bootstrap有効履歴件数の銘柄別N+1 COUNTをlogical alias bulk集計へ変更
# - P2-35: alias分散履歴がlogical 60日以上ある既存銘柄のbootstrap推定漏れを修正
# - P2-36: marketCapのyfinance fallbackもYahoo symbolをbulk解決しDB N+1を回避
# - P2-37: sentinel override読込障害をoverride無し扱いせずfail-visible化
# - P2-38: bulk Yahoo resolverのlegacy schema読込障害をsilent無視せずfail-visible化
# - P2-39: yfinance MultiIndexのTicker階層を自動検出しPrice/Ticker逆順にも対応
# - P2-40: dashboard Yahoo URLも明示overrideを市場推定より優先しsymbol整合性を統一
# - P2-41: _prepare_rowsのYahoo URL解決を全行scalar DB探索から1回のbulk解決へ変更
# - P2-42: 配当1年合計をcutoff以上かつtoday以下へ限定し未来日混入を防止
# - P2-43: 財務batch内helperに完全shadowされる旧トップレベル財務helper3本を削除
# - P2-44: Yahoo override schema ensureの無条件commitを廃止し呼出元transaction原子性を保護
# - P2-45: 翌日検証CSVを手動comma joinからcsv.writerへ変更し理由内comma等を安全にescape
# - P2-46: 主要Yahoo batchは全対象symbolを先に一括解決しchunk境界を跨ぐoverride衝突も検知
# - P2-47: yfinance MultiIndex stackをfuture_stack優先＋旧Pandas互換fallback化
# - P2-48: Yahoo企業API市場判定を対象銘柄限定bulk＋canonical alias優先へ決定化
# - P2-49: 財務payload解析例外時に途中までの値で更新日を進めず銘柄単位でatomicに再取得対象化
# - P2-50: 財務batchのfresh取得専用loopに残っていた到達不能な旧raw-cache解析分岐を削除
# - P2-51: 財務部に残っていた未使用YQダミーblock/後勝ちしない重複YQ_MAX_WORKERS定義を削除
# - P2-52: yahooqueryを任意import化しmarketCap fallbackを実到達可能に、必須phaseは明示エラー化
# - P2-53: jpholidayも任意import化し非祝日系処理の起動を妨げず、祝日計算時だけ明示エラー化
# - P2-54: 財務legacy-cleanupの無条件commitをSAVEPOINT化し呼出元transaction原子性を保護
# - P2-55: yfinanceも任意import化し、未導入時は取得phaseで明示エラー（module import検査を可能化）
# - P2-56: dashboard templateのimport時即openを廃止しHTML生成時lazy-loadへ変更
# - P2-57: 設定に見えるが実装参照ゼロだった旧定数/regex/cacheを削除し誤設定リスクを除去
# - P2-58: Yahoo bulk resolverをsymbol→codeだけでなくcode→symbolも1対1検証し二重更新を防止
# - P2-59: legacy Yahoo suffix overrideも7203.0等aliasをcanonical優先で解決しbulk/scalarを一致
# - P2-60: 財務cache/更新先raw keyを同一canonical優先snapshotから決めalias混線を防止
# - P2-61: 財務executemanyの実更新行数を検証し0件/重複複数行UPDATEをrollback
# - P2-62: 時価総額executemanyも期待件数と実更新件数を照合しsilent no-opを防止
# - P2-63: 残存unused local/QUIET/Gmail現行説明など実装と不一致の休眠記述を整理
# - P2-64: latest_prices/抵抗線/テーマschema helperの無条件commitをSAVEPOINT化
# - P2-65: V5支持抵抗schema helperもSAVEPOINT化し未close cursor/無条件commitを解消
# - P2-66: exec_manyの隠れcommitを廃止し、空売りsnapshot SAVEPOINTを途中破壊する実行時バグを修正
# - P2-67: テーマランキング内部DB障害を正常な空ランキングへ変換せず、呼出側だけでoptional fallback化
# - P2-68: 財務loggerの壊れた引用注釈とscore→alpha説明不一致を修正し、任意index失敗をWARN可視化
# - P2-69: 財務コメント同期DMLをBEGIN/conn.rollbackではなくSAVEPOINT化し、呼出元transactionを保護
# - P2-70: JPXカレンダー依存/設定障害を「休場日」に変換せずfail-visible化し平日のEOD誤分類を防止
# - P2-71: 翌日検証でsignal日付parse不正だけskipし、営業日カレンダー障害は握り潰さずdaily成功誤認を防止
# - P2-72: finance_notes→screener同期もexecutemany実更新行数を検証し重複raw keyの多重UPDATEをrollback
# - P2-73: legacy Yahoo overrideの実SELECT障害もoverride無し扱いせずbulk resolverでfail-visible化
# - P2-1: 三角Safetyの欠損時基準点を60→50へ中立化
# - P2-2: Fair Valueの予想EPSをfinance_notes.forecast_eps正本に限定（legacy fallbackは明示opt-in）
# - P2-3: yfinance日足fallbackでrepair=Trueを試行し、非対応版のみ互換fallback
# - P2-4: 19時財務batchの未使用flags_rows/専用判定を削除
# - P2-5: pl_quarter.updated_atが存在する将来schemaではupdated_at優先で重複正本を選択
# - P2-6: Fair Value内の未使用/常時中立特徴量を除去し、モデル説明と実装を一致
# - P2-7: yfinanceのMultiIndex列をOHLCV列へ安全に正規化
# - P2-8: 営業利益更新の未使用read-okフラグを削除
# - P2-9: Fair Valueの列追加をPRAGMA確認方式へ変更し、DBエラー握り潰しを廃止
# - P2-10: strict forecast EPSの有効件数をログ表示して全欠損を可視化
# - P2-11: forecast EPSログをstale/legacy fallback込みの「実際に計算可能な件数」へ厳密化
# === 2026-08-13 v14: 次期急回復型（6340型）Python連携 ===
# - shinden_logic_v24_next_turn.py を自動スクリーニングから安全に実行
# - 次期転換8列をDB→JSON/HTML/LLM CSVへ一貫して伝播
# - backfill実行中はシンデン更新を自動SKIP
# - template_next_turn_python_backed.html が同階層にあれば優先使用（無ければtemplate.html）
# === 2026-08-13 v13: 季節進捗 1年度fallback ===
# - 過去同Qが1年度しかない銘柄も参考値として表示
# - 2年以上は従来どおり正式平均、1年参考/2年以上をログで監査
# === 2026-08-13 v12: 季節進捗 quarterly_actual_history 実DB準拠 ===
# - pl_quarter依存を廃止
# - quarterly_actual_history 28k件から3ヵ月営業利益を年度内累積
# - tdnet_xbrl_metrics.actual_opを通期実績の第一ソースに採用
# - 現在年度除外・直近最大5年度・2年度以上で過去平均を算出
# === 2026-08-13 v11: 季節進捗の全空欄修正 ===
# - 全NULL化→途中returnで全空欄になる事故を修正
# - pl_quarter完了年度4Q合計を通期実績fallbackに追加
# === 2026-08-13 v10: 決算跨ぎS/AワンクリックUI対応 ===
# - dashboard用に決算発表予定日・シンデン主要列の存在を保証
# - template側の「今日の決算跨ぎ S/A」フィルターを安全に利用可能
# === 2026-08-12 v9: 過去決算反応の復元・診断強化 ===
# - Gmailアプリパスワードは環境変数 GMAIL_APP_PASSWORD から取得（ハードコード廃止）
# - finance_notes.past_earnings_dates が空のとき明示WARN
# - コード型を4桁文字列に統一して結合漏れを防止
# - 既存の反応列があってもmerge suffixで消えないよう再計算値を優先
# - UI表記に合わせ直近8回で勝率/期待値/履歴を計算
# === 2026-08-12 v8: 季節進捗を実績から再構成 ===
# - pl_quarter.進捗率という不存在列への依存を廃止
# - 3ヵ月営業利益累積 / 完了年度actual_op で過去同Q進捗を再構成
# - current_quarter不要、過去2期以上のみ採用
# === 2026-08-12 v7: TTM pandas互換修正 + seasonality schema-safe ===
# - groupby.applyを廃止し直近4行→groupby.aggへ変更
# - DeprecationWarningとrename失敗を解消
# - v6のcurrent_quarter非依存季節調整を維持
# === 2026-08-12 v6: 季節調整 schema-safe 修正 ===
# - finance_notes.current_quarter 依存を廃止
# - pl_quarter 最新行から現在Qを判定
# - 現在期自身を過去同Q平均から除外
# - 旧失敗マーカー回避のため daily phase を v2 化
# === 2026-08-12 v5: CatBoost学習/推論19特徴量完全一致 ===
# - body/RSI/stop_hunt/POC を学習側と統一
# - perfect_order/trend_strong/market3特徴量のダミー0を廃止
# - model metadata の validation閾値/target_pct を使用
# - AIスコアの強制モメンタム上書きを廃止
# - 特徴量不足を0埋めせず判定対象外
# === 2026-08-16 P3-42: EOD確定処理もexternal live-materialsへ分離 ===
# === 2026-08-16 P3-41: 外部producer/Task Scheduler新構成へ統合 ===
# - fetch_all / 株探ファンダ / 空売り / シンデン / テーマWeb取得を本体から起動しない
# - 空売りなしはinstitution_short_snapshotを正本化
# - system_jobs共有writer lockとモデルgeneration整合を追加
# === 2026-08-12 v4: シンデン日次バッチ分離 + backfill時fetch_all自動SKIP ===
# - v25 worker heartbeat優先
# - v24以前のrunning CLAIMも互換検出
# - stale状態は6時間で無効
#
# === 2026-08-12 再点検版 ===
# - shinden_logic v2(history guard)対応
# - 株探ファンダ日次呼び出しの誤引数修正
# - _prepare_rows二重実行を除去
# - LLM用CSVへシンデン主要列を追加
#
# --- 標準ライブラリ ---
import csv
import io
import decimal
import html
import json
import logging
import math
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time
import urllib.parse
import warnings
import webbrowser
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta, timezone, time as dt_time
from email.utils import parsedate_to_datetime as _pdt
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional, Union
from urllib.parse import quote
from zoneinfo import ZoneInfo

# --- 拡張ライブラリ（フォールバック処理） ---
try:
    import orjson
except ImportError:
    import json as orjson  # orjsonがない環境では標準のjsonで代用

# --- サードパーティライブラリ ---
import bs4 # BeautifulSoup
import joblib
# P2-53: 祝日判定を使わない処理までjpholiday未導入で起動不能にしない。
try:
    import jpholiday
except ImportError:
    jpholiday = None
import numpy as np
import pandas as pd
# P1-363: Stooqは最終フォールバック。未導入でも本体を起動可能にする。
try:
    import pandas_datareader.data as _pdr
except Exception:
    _pdr = None
import requests
# P1-376: workdaysには後段の内蔵shimがあるため、起動時必須依存にしない。
try:
    import workdays
except Exception:
    workdays = None
# P2-55: yfinance未導入でも静的/非価格処理のmodule import自体は可能にする。
try:
    import yfinance
except ImportError:
    yfinance = None
from jinja2 import Environment, FileSystemLoader, select_autoescape
from markupsafe import Markup, escape
# P1-361: 通知は補助機能。plyer未導入で全スクリーナーを起動不能にしない。
try:
    from plyer import notification
except Exception:
    notification = None
# P1-362: BERTは設定で無効化可能な任意機能。transformersを起動時必須にしない。
try:
    from transformers import AutoModelForSequenceClassification, AutoTokenizer, pipeline
except Exception:
    AutoModelForSequenceClassification = None
    AutoTokenizer = None
    pipeline = None
# P2-52: yahooquery未導入でもモジュール全体は起動可能にする。
# marketCapはyfinance fallback可能、MIDDAY/財務batchは実行時に明示エラーにする。
try:
    from yahooquery import Ticker
except ImportError:
    Ticker = None

# --- 内部用エイリアス・パッチ（必要最小限） ---
# パッチ処理や特定のライブラリ構成用に使用されているもの
import logging as __lg_patch_v8
import warnings as __wn_patch_v8
import os as __os_patch_v8
import sys as __sys_patch_v8


# === シンデン型スコア ===
# v4: 自動スクリーニングではシンデン再計算を行わない。
# shinden_logic.py を日次バッチ/手動で別実行し、
# screener に保存済みのシンデン列をHTML/CSVへ表示するだけ。
# === /シンデン型スコア ===

# ==============================================================================
# 【全体設定・定数群】（パラメータの調整やパスの変更はここで行います）
# ==============================================================================

# --- [1] ファイル・ディレクトリ パス設定 ---
_SCRIPT_DIR         = Path(__file__).resolve().parent
DB_PATH             = os.environ.get("KABU_DB_PATH", r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\db\kani2.db")
OUTPUT_DIR          = os.environ.get("KABU_OUTPUT_DIR", r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data")
CSV_INPUT_PATH      = os.environ.get("KABU_CSV_INPUT_PATH", r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\screener_result.csv")
MASTER_CODES_PATH   = os.environ.get("KABU_CODES_PATH", r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\input_data\株コード番号.txt")
EXTRA_CLOSED_PATH   = os.environ.get("KABU_EXTRA_CLOSED_PATH", r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\market_closed_extra.txt")
MARKER_FILE         = Path(OUTPUT_DIR) / "last_funda.txt"
MODEL_PATH          = os.environ.get("KABU_MODEL_PATH", str(_SCRIPT_DIR / "model" / "stock_predictor_lv3.pkl"))
SYSTEM_JOB_STATE_DB = Path(os.environ.get("KABU_JOB_STATE_DB", str(_SCRIPT_DIR / "runtime" / "system_jobs_state.db")))
SYSTEM_WRITER_LOCK  = Path(os.environ.get("KABU_JOB_LOCK", str(_SCRIPT_DIR / "runtime" / "db_writer.lock")))
SCREEN_RUNTIME_DIR  = Path(os.environ.get("KABU_SCREEN_RUNTIME_DIR", str(_SCRIPT_DIR / "runtime")))
KABUNEWS_CACHE_PATH = Path(os.environ.get("KABU_NEWS_CACHE_PATH", str(SCREEN_RUNTIME_DIR / "kabutan_news_cache.json")))
CHARTS60_MANIFEST_PATH = Path(os.environ.get("KABU_CHARTS60_MANIFEST", str(SCREEN_RUNTIME_DIR / "charts60_snapshot.json")))
SCREEN_LOCK_EVENT_LOG = Path(os.environ.get("KABU_SCREEN_LOCK_EVENT_LOG", str(SCREEN_RUNTIME_DIR / "screening_lock_events.log")))

# P4-2 housekeeping: generated artifact retention.
# live/current files are never deleted; only timestamped history, stale chart artifacts,
# and old runtime logs are pruned. Environment variables allow safe operational tuning.
DASHBOARD_HISTORY_KEEP = max(1, int(os.environ.get("KABU_DASHBOARD_HISTORY_KEEP", "3")))
CHARTS60_RETENTION_DAYS = max(1, int(os.environ.get("KABU_CHARTS60_RETENTION_DAYS", "7")))
RUNTIME_LOG_RETENTION_DAYS = max(1, int(os.environ.get("KABU_RUNTIME_LOG_RETENTION_DAYS", "30")))
# P4-LIVE Candidate Contract / intraday snapshot settings.
LIVE_CANDIDATE_SCHEMA_VERSION = 1
LIVE_CANDIDATE_MAX = max(1, int(os.environ.get("KABU_LIVE_CANDIDATE_MAX", "300")))
LIVE_CANDIDATE_VALID_SECONDS = max(60, int(os.environ.get("KABU_LIVE_CANDIDATE_VALID_SECONDS", "900")))
LIVE_SNAPSHOT_KEEP_TRADE_DAYS = max(1, int(os.environ.get("KABU_LIVE_SNAPSHOT_KEEP_TRADE_DAYS", "30")))
LIVE_STEADY_MIN_SCORE = float(os.environ.get("KABU_STEADY_UP_MIN_SCORE", "65"))
LIVE_STEADY_MIN_TURNOVER_OKU = float(os.environ.get("KABU_STEADY_UP_MIN_TURNOVER_OKU", "0.5"))
LIVE_STEADY_MAX_DAY_RETURN_PCT = float(os.environ.get("KABU_STEADY_UP_MAX_DAY_RETURN_PCT", "8.0"))
LIVE_BOTTOM_MIN_SCORE = float(os.environ.get("KABU_BOTTOM_REVERSAL_MIN_SCORE", "60"))
LIVE_BOTTOM_MIN_REBOUND_PCT = float(os.environ.get("KABU_BOTTOM_REVERSAL_MIN_REBOUND_PCT", "1.5"))
LIVE_LOW_LIQUIDITY_OKU = float(os.environ.get("KABU_LIVE_LOW_LIQUIDITY_OKU", "0.3"))
LIVE_LOW_LIQUIDITY_PENALTY = float(os.environ.get("KABU_LIVE_LOW_LIQUIDITY_PENALTY", "15"))
LIVE_LOW_PRICE_MAX = float(os.environ.get("KABU_LIVE_LOW_PRICE_MAX", "300"))
LIVE_SNAPSHOT_DB = Path(os.environ.get("KABU_LIVE_SNAPSHOT_DB", str(SCREEN_RUNTIME_DIR / "scanner_snapshots.sqlite3")))
LIVE_CANDIDATE_FEED_PATH = Path(os.environ.get("KABU_LIVE_CANDIDATE_FEED", str(Path(OUTPUT_DIR) / "live_candidate_feed.json")))

EXTERNAL_JOBS_REQUIRED = os.environ.get("KABU_EXTERNAL_JOBS_REQUIRED", "1").strip().lower() not in {"0","false","no","off"}
try:
    # P2-98: 6分半前後の本体に最大3分のproducer待ちを加えても、通常は10分枠内に収まる。
    # 運用上の緊急調整は環境変数で短縮/無効化できるが、lock自体は迂回しない。
    SHARED_WRITER_WAIT_SECONDS = max(0.0, float(os.environ.get("KABU_SCREEN_LOCK_WAIT_SECONDS", "180")))
except (TypeError, ValueError):
    SHARED_WRITER_WAIT_SECONDS = 180.0

# --- [2] 実行モード・オプション設定 ---
RUN_SESSION         = "EOD"   # "EOD"(普通はこちら) または "MIDDAY"(全部やりたいときこちら)
#RUN_SESSION         = "MIDDAY"   # "EOD"(普通はこちら) または "MIDDAY"(全部やりたいときこちら)
AUTO_MODE           = True    # 自動判定フラグ 時間帯によって自動でMIDDAY(False)とEOD(True)を強制する
#AUTO_MODE           = False    # 自動判定フラグ 時間帯によって自動でMIDDAY(False)とEOD(True)を強制する
USE_CSV             = True    # CSV取り込みフラグ
TEST_MODE           = False   # テストモードフラグ（件数制限）
TEST_LIMIT          = 50      # テスト時の最大件数
PRICE_GUARD_ENABLED = True   # 休日価格補完フラグ
MIDDAY_FILTER_BY_FLAGS = False # MIDDAY対象を絞るか
CHARTS60_MIDDAY_REFRESH_MINUTES = 30
CHARTS60_FOCUS_MAX_CODES = 1600
CHARTS60_FALLBACK_CODES = 300
CHARTS60_HISTORY_BARS = 160

# --- [3] トレンド・シグナル判定 パラメータ ---
LOOKBACK            = 90
SIGNAL_LOOKBACK_DAYS= 300
MIN_DAYS            = 60
RIBBON_KEEP_DAYS    = 30
HL_WIN              = 5
THRESH_SCORE        = 70      # 合格スコアのしきい値
SLOPE_MIN_ANN       = 0.08    # 年率傾きの下限
R2_MIN              = 0.30    # R2下限
MDD_MAX             = 0.30    # 最大ドローダウン上限
WEEK_UP_MIN         = 0.55    # 週間上昇の下限

# --- [4] 早期トリガー・ブレイク判定 パラメータ ---
HH_N                = 60      # ブレイク判定の過去高値期間
POCKET_WIN          = 10      # ポケットピボットの参照日数
REB_WIN             = 10      # 20MA割れ→奪回を探すウィンドウ
RECLAIM_WIN         = 10      # 200MA上抜けの探索ウィンドウ
SCORE_TH            = 70      # 早期フラグのスコア閾値
PIVOT_EPS           = 0.002   # ブレイク余白(+0.2%)
VOL_BOOST           = 1.5     # ブレイク時の出来高ブースト(×20日平均)
EXT_20_MAX          = 0.05    # 20MAからの乖離上限(=+5%)
EXT_50_MAX          = 0.10    # 50MAからの乖離上限(=+10%)

# --- [5] 再評価 (Rejudge) パラメータ ---
REJUDGE_LOOKAHEAD_DAYS  = 5
REJUDGE_REQ_HIGH_PCT    = 5.0
REJUDGE_MAX_ADVERSE_PCT = 7.0

# --- [6] 支持・抵抗線 パラメータ ---
RES_LOOKBACK_DAYS   = 90
RES_TOUCH_BAND_PCT  = 0.015   # ±1.5%の幅でゾーンを形成
RES_MIN_TOUCHES     = 2       # 最低2回のタッチでゾーン候補
RES_ZONE_MERGE_PCT  = 0.02    # 2%以内のシードはマージ

# --- [7] API取得・その他システム設定 ---
EPS                 = 0.0
JST                 = timezone(timedelta(hours=9))
YQ_BATCH_MID        = 400
YQ_SLEEP_MID        = 0.10

_KABUNEWS_CONF = {
    "rss_base": "https://news.google.com/rss/search",
    "lang": "ja",
    "gl": "JP",
    "ceid": "JP:ja",
    "max_items_per_symbol": 3,
    "enable_bert": False,
    "bert_model": "koheiduck/bert-japanese-finetuned-sentiment",
    "http_timeout": 8,
    # P2-90: P2-89の32銘柄queryはGoogle News側の検索結果上限により記事coverageが
    # 約半減したため不採用。8銘柄queryへ戻し、取得粒度を保ったまま有界6workerで処理する。
    "max_workers": 6,
    "max_pairs_per_query": 8,
    "max_url_length": 1800,
    # P2-92: 10分runで同じ候補を連続照会しない。新規候補はcache missのため即時取得、
    # 既存候補だけ30分間再利用する。HTTP/parse失敗はcacheを進めず次runで再試行する。
    "cache_ttl_minutes": 30,
    "cache_schema_version": 1,
    "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "pos_keywords": [
        "上方修正", "増額修正", "業績予想の上方修正", "最高益", "過去最高益", 
        "上場来高値", "配当増額", "増配", "大幅増益", "好決算", 
        "連結営業利益の増加", "株式分割", "自社株買い", "中期経営計画", "大型受注"
    ],
}

# ==============================================================================
# 【外部スクリプト・プロセス実行ユーティリティ群】
# ==============================================================================

# P3-41: 外部producer起動ユーティリティは廃止。
# fetch_all / 株探ファンダ / 空売り / シンデンは system_jobs.py / Task Scheduler が担当。

_DETACHED_PROCESS = 0x00000008
_CREATE_NEW_PROCESS_GROUP = 0x00000200

def _charts60_number(value, default=float("-inf")) -> float:
    try:
        out = float(str(value).replace(",", ""))
        return out if math.isfinite(out) else default
    except Exception:
        return default


def _charts60_focus_codes(conn: sqlite3.Connection) -> set[str]:
    """P2-97: 10分チャートの対象を売買判断へ必要な候補へ限定する。

    current signalを正本に、直前monitor listとDBへ同期済みの前回AI陽性を補助集合にする。
    AIは今回export内で後から計算されるため、現在runの新規初動/早期を優先し、AIだけ1run遅れを許容する。
    """
    columns = {str(r[1]) for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    wanted = [
        "コード", "初動フラグ", "INITIAL_MOMENTUM", "底打ちフラグ", "右肩上がりフラグ", "右肩早期フラグ",
        "右肩早期種別", "AIスコア", "初動スコア", "INITIAL_MOMENTUM_SCORE", "右肩早期スコア",
        "右肩上がりスコア", "合成スコア", "売買代金億",
    ]
    exprs = [f'"{c}"' if c in columns else f'NULL AS "{c}"' for c in wanted]
    rows = conn.execute(f"SELECT {','.join(exprs)} FROM screener").fetchall()
    idx = {name: pos for pos, name in enumerate(wanted)}

    previous = set()
    monitor_path = Path(OUTPUT_DIR) / "rss_monitor_list.json"
    try:
        raw = json.loads(monitor_path.read_text(encoding="utf-8"))
        previous = {
            canonical_code_for_db(code)
            for code in (raw.get("target_codes") or [])
            if re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", canonical_code_for_db(code))
        }
    except Exception:
        previous = set()

    try:
        ai_threshold_pct = float(_ai_load_model_metadata().get("decision_threshold", 0.175)) * 100.0
    except Exception:
        ai_threshold_pct = 17.5

    ranked = {}
    fallback = {}
    for row in rows:
        code = canonical_code_for_db(row[idx["コード"]])
        if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", str(code or "")):
            continue
        initial = "候補" in str(row[idx["初動フラグ"]] or "")
        initial_momentum = _charts60_number(row[idx["INITIAL_MOMENTUM"]], 0.0) >= 1.0
        bottom = "候補" in str(row[idx["底打ちフラグ"]] or "")
        right = "候補" in str(row[idx["右肩上がりフラグ"]] or "")
        early = (
            "候補" in str(row[idx["右肩早期フラグ"]] or "")
            or bool(str(row[idx["右肩早期種別"]] or "").strip())
        )
        ai_score = _charts60_number(row[idx["AIスコア"]])
        ai_positive = ai_score >= ai_threshold_pct
        was_monitored = code in previous
        score = max(
            _charts60_number(row[idx["初動スコア"]]),
            _charts60_number(row[idx["INITIAL_MOMENTUM_SCORE"]]),
            _charts60_number(row[idx["右肩早期スコア"]]),
            _charts60_number(row[idx["右肩上がりスコア"]]),
            _charts60_number(row[idx["合成スコア"]]),
        )
        turnover = _charts60_number(row[idx["売買代金億"]], 0.0)
        priority = (
            int(initial_momentum), int(initial or bottom), int(early), int(was_monitored), int(ai_positive), int(right),
            score, ai_score, turnover, code,
        )
        if code not in fallback or priority > fallback[code]:
            fallback[code] = priority
        if initial_momentum or initial or bottom or right or early or ai_positive or was_monitored:
            if code not in ranked or priority > ranked[code]:
                ranked[code] = priority

    if ranked:
        selected = sorted(ranked, key=lambda c: ranked[c], reverse=True)[:CHARTS60_FOCUS_MAX_CODES]
        clipped = max(0, len(ranked) - len(selected))
        print(
            f"[charts60] focus current/previous/AI={len(ranked)} selected={len(selected)} "
            f"clipped={clipped} previous={len(previous)} ai_threshold={ai_threshold_pct:.1f}"
        )
        return set(selected)

    # 初回やschema移行直後でも空集合から全件生成へ誤fallbackしない。流動性上位だけで復旧する。
    selected = sorted(fallback, key=lambda c: fallback[c], reverse=True)[:CHARTS60_FALLBACK_CODES]
    print(f"[charts60][WARN] no focus signal; liquidity fallback={len(selected)}")
    return set(selected)


def _run_charts60(py_path: str, requested_codes=None):
    """個別チャート画像（charts60）を生成し、今回実際に書き直したコード集合を返す。"""
    py = Path(py_path)
    if not py.exists():
        raise FileNotFoundError(f"charts60_make.py が見つかりません: {py}")

    requested = None if requested_codes is None else {
        canonical_code_for_db(code)
        for code in requested_codes
        if canonical_code_for_db(code)
    }

    # P1-619: charts60_make は履歴本数不足を正常skipする。プロセス全体の成功だけでは、
    # 今回skipされた銘柄の前回HTML/前回chart_flagsまでcurrent扱いできてしまう。
    # 実行前後のファイルfingerprintを比較し、「今回本当に生成したコード」を別途確定する。
    _chart_dir = Path(OUTPUT_DIR) / "charts60"
    _before = {}
    if _chart_dir.exists():
        _before_files = (
            list(_chart_dir.glob("*.html")) if requested is None
            else [_chart_dir / f"{code}.html" for code in requested]
        )
        for _f in _before_files:
            if not _f.is_file():
                continue
            try:
                _st = _f.stat()
                _before[_f.name] = (int(_st.st_mtime_ns), int(_st.st_size))
            except Exception:
                # 事前fingerprint不能なファイルは、後段でcurrentと証明できないよう既存扱いにする。
                _before[_f.name] = None

    # そのまま実行（DBパスはスクリプト内に埋め込み済み）
    # メインと同じ Python 実行ファイルを使う（pythonw.exe で起動していればそれを継承）
    _cmd = [
        sys.executable, str(py), "--db", str(DB_PATH),
        "--out", str(Path(OUTPUT_DIR) / "charts60"),
        "--history-bars", str(CHARTS60_HISTORY_BARS),
    ]
    _codes_file = None
    if requested is not None:
        SCREEN_RUNTIME_DIR.mkdir(parents=True, exist_ok=True)
        _tmp = tempfile.NamedTemporaryFile(
            mode="w", encoding="utf-8", suffix=".codes.txt", prefix="charts60_",
            dir=str(SCREEN_RUNTIME_DIR), delete=False,
        )
        try:
            _tmp.write("\n".join(sorted(requested)))
            _tmp.write("\n")
            _codes_file = Path(_tmp.name)
        finally:
            _tmp.close()
        _cmd.extend(["--codes", "@" + str(_codes_file)])

    _cp = None
    try:
        _cp = subprocess.Popen(
            _cmd, creationflags=_DETACHED_PROCESS | _CREATE_NEW_PROCESS_GROUP, close_fds=True,
        )
        _shared_writer_lock_set_child_pid(_cp.pid)
        try:
            _rc = int(_cp.wait(timeout=1800))
        except subprocess.TimeoutExpired:
            try:
                _cp.terminate(); _cp.wait(timeout=10)
            except Exception:
                try: _cp.kill(); _cp.wait(timeout=10)
                except Exception: pass
            raise RuntimeError("charts60_make timeout >1800s")
        if _rc != 0:
            raise RuntimeError(f"charts60_make failed rc={_rc}")
    finally:
        _shared_writer_lock_set_child_pid(None)
        if _codes_file is not None:
            try: _codes_file.unlink()
            except FileNotFoundError: pass
            except Exception as e: print(f"[charts60][WARN] temp codes cleanup failed: {e}")

    _generated_codes = set()
    if _chart_dir.exists():
        _after_files = (
            list(_chart_dir.glob("*.html")) if requested is None
            else [_chart_dir / f"{code}.html" for code in requested]
        )
        for _f in _after_files:
            if not _f.is_file():
                continue
            try:
                _st = _f.stat()
                _after_fp = (int(_st.st_mtime_ns), int(_st.st_size))
                if _f.name not in _before or _before.get(_f.name) != _after_fp:
                    _c = canonical_code_for_db(_f.stem)
                    if _c:
                        _generated_codes.add(_c)
            except Exception:
                continue
    return _generated_codes


def _charts60_script_fingerprint(py_path: str) -> str:
    """charts generatorの差し替え後に旧manifestを再利用しない。"""
    import hashlib
    py = Path(py_path)
    with py.open("rb") as fh:
        return hashlib.sha256(fh.read()).hexdigest()


def _charts60_fixed_price_token(conn: sqlite3.Connection, run_mode: str) -> str | None:
    """P2-95: PREOPEN/EODの確定価格訂正を検知する軽量token。

    MIDDAYは価格が毎回変わるため30分TTLで鮮度を管理し、このtokenは使わない。
    """
    mode = str(run_mode or "").upper()
    if mode == "MIDDAY":
        return None
    asof = _expected_snapshot_date_for_run(mode)
    start = asof.isoformat()
    end = (asof + timedelta(days=1)).isoformat()
    row = conn.execute(
        """
        SELECT COUNT(*), COALESCE(MAX(rowid),0),
               ROUND(COALESCE(SUM(COALESCE(始値,0)),0),4),
               ROUND(COALESCE(SUM(COALESCE(高値,0)),0),4),
               ROUND(COALESCE(SUM(COALESCE(安値,0)),0),4),
               ROUND(COALESCE(SUM(COALESCE(終値,0)),0),4),
               COALESCE(SUM(COALESCE(出来高,0)),0)
        FROM price_history
        WHERE 日付 >= ? AND 日付 < ?
        """,
        (start, end),
    ).fetchone()
    values = [start] + [None if v is None else str(v) for v in (row or [])]
    return "|".join(values)


def _charts60_reusable_manifest(
    conn: sqlite3.Connection, py_path: str, run_mode: str, requested_codes=None
):
    """freshな前回成功snapshotだけ (current codes, generated_at, age)で返す。

    P2-97: focus集合からの削除だけなら既存fileを再利用して返却集合を縮める。
    新しいcandidateが1銘柄でも追加された時はTTL内でも生成し、その銘柄を取りこぼさない。
    """
    try:
        raw = json.loads(CHARTS60_MANIFEST_PATH.read_text(encoding="utf-8"))
        if not isinstance(raw, dict) or int(raw.get("schema", -1)) != 2:
            return None
        mode = str(run_mode or "").upper()
        if str(raw.get("day") or "") != _today_jst():
            return None
        if str(raw.get("build") or "") != _daily_build_token():
            return None
        if str(raw.get("mode") or "").upper() != mode:
            return None
        if str(raw.get("script_sha256") or "") != _charts60_script_fingerprint(py_path):
            return None
        generated_at = float(raw.get("generated_at"))
        age = time.time() - generated_at
        if age < -300.0:
            return None
        ttl_seconds = CHARTS60_MIDDAY_REFRESH_MINUTES * 60 if mode == "MIDDAY" else 16 * 3600
        if age > ttl_seconds:
            return None
        if mode != "MIDDAY":
            current_price_token = _charts60_fixed_price_token(conn, mode)
            if str(raw.get("price_token") or "") != str(current_price_token or ""):
                return None
        generated_codes = {
            canonical_code_for_db(code)
            for code in (raw.get("codes") or [])
            if canonical_code_for_db(code)
        }
        if not generated_codes:
            return None
        requested = None if requested_codes is None else {
            canonical_code_for_db(code)
            for code in requested_codes
            if canonical_code_for_db(code)
        }
        old_scope_kind = str(raw.get("scope_kind") or "")
        old_requested = {
            canonical_code_for_db(code)
            for code in (raw.get("requested_codes") or [])
            if canonical_code_for_db(code)
        }
        if requested is None:
            if old_scope_kind != "all":
                return None
            current_codes = generated_codes
        else:
            if old_scope_kind != "focus" or (requested - old_requested):
                return None
            current_codes = generated_codes & requested
            if requested and not current_codes:
                return None
        chart_dir = Path(OUTPUT_DIR) / "charts60"
        # manifestにあるのに実ファイルが消えた場合は全件再生成側へ倒す。
        if any(not (chart_dir / f"{code}.html").is_file() for code in current_codes):
            return None
        return current_codes, str(raw.get("generated_at_jst") or ""), age
    except Exception:
        return None


def _run_or_reuse_charts60(conn: sqlite3.Connection, py_path: str, run_mode: str):
    """P2-95/P2-97: 場中候補をrefreshし、EODは全銘柄を補修する。"""
    mode = str(run_mode or "").upper()
    requested = None if mode == "EOD" else _charts60_focus_codes(conn)
    reusable = _charts60_reusable_manifest(conn, py_path, run_mode, requested)
    if reusable is not None:
        codes, generated_at_jst, age = reusable
        globals()["_CHARTS60_SNAPSHOT_AT"] = generated_at_jst
        print(
            f"[charts60] reuse successful snapshot mode={str(run_mode).upper()} "
            f"age={max(0.0, age):.0f}s codes={len(codes)} at={generated_at_jst or '-'}"
        )
        return codes

    scope_log = "all" if requested is None else f"focus:{len(requested)}"
    print(f"[charts60] generate scope={scope_log} history_bars={CHARTS60_HISTORY_BARS}")
    codes = _timed("charts60_generate", _run_charts60, py_path, requested)
    codes = {canonical_code_for_db(code) for code in (codes or set()) if canonical_code_for_db(code)}
    if not codes:
        # 正常終了でも0件は再利用manifestにしない。次runで再生成を試みる。
        print("[charts60][WARN] generated code set is empty; reusable manifest not advanced")
        return set()

    now_epoch = time.time()
    generated_at_jst = _now_jst().isoformat(timespec="seconds")
    payload = {
        "schema": 2,
        "day": _today_jst(),
        "build": _daily_build_token(),
        "mode": str(run_mode or "").upper(),
        "generated_at": now_epoch,
        "generated_at_jst": generated_at_jst,
        "script_sha256": _charts60_script_fingerprint(py_path),
        "price_token": _charts60_fixed_price_token(conn, run_mode),
        "scope_kind": "all" if requested is None else "focus",
        "requested_codes": [] if requested is None else sorted(requested),
        "codes": sorted(codes),
    }
    CHARTS60_MANIFEST_PATH.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text_file(
        CHARTS60_MANIFEST_PATH,
        json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
    )
    globals()["_CHARTS60_SNAPSHOT_AT"] = generated_at_jst
    return codes


def sync_to_github_pages(repo_root: str, target_file: str):
    """
    Windowsのcmd.exe（shell=True）の介在を100%遮断し、
    カレントディレクトリ誤認バグを構造上完全に根絶した決定版。
    万が一.gitフォルダが消失・破損していた場合の自動修復機能付き。
    """
    try:
        # Windowsのネイティブな絶対パス文字列に完全変換
        repo_path = os.path.abspath(repo_root)
        src = Path(target_file)
        dest = Path(repo_path) / "index.html"

        if not src.exists():
            print(f"[git][ERROR] Source HTML file not found: {target_file}")
            return False

        # リポジトリの親フォルダが存在しない場合は自動作成
        os.makedirs(repo_path, exist_ok=True)

        # --- 🔥【重要】万が一.gitフォルダが壊れて消失していた場合の自動初期化 ---
        dot_git_path = os.path.join(repo_path, ".git")
        if not os.path.exists(dot_git_path):
            print(f"[git][WARN] .git directory not found in {repo_path}. Initializing repository...")
            _res_init = subprocess.run(
                ["git", "init"], 
                cwd=repo_path, 
                shell=False, 
                capture_output=True, 
                text=True,
                creationflags=subprocess.CREATE_NO_WINDOW,
                timeout=120
            )
            # P1-328: git init失敗後にadd/commitへ進んで成功風ログを出さない。
            if _res_init.returncode != 0:
                print(f"[git][ERROR] 'git init' failed.\nReason: {_res_init.stderr.strip()}")
                return False

        # --- Git index.lock ---
        lock_file = Path(repo_path) / ".git" / "index.lock"
        if lock_file.exists():
            # P1-327: 新鮮なindex.lockは別gitプロセスが使用中かもしれない。
            # 無条件削除せず同期を中止し、十分古い残骸だけを除去する。
            try:
                _lock_age = max(0.0, time.time() - lock_file.stat().st_mtime)
            except Exception:
                _lock_age = 0.0
            if _lock_age < 600:
                print(f"[git][WARN] fresh index.lock detected ({_lock_age:.0f}s old); sync skipped")
                return False
            try:
                lock_file.unlink()
                print(f"[git] Removed stale index.lock ({_lock_age:.0f}s old).")
            except Exception as e:
                print(f"[git][WARN] stale index.lock could not be removed: {e}")
                return False

        # 成果物のコピー（D:作業場 → C:リポジトリ）
        # P1-369: index.htmlを直接truncate copyせず、同一dirのtempへ完成後os.replaceする。
        _git_tmp = Path(repo_path) / f".index.html.tmp.{os.getpid()}.{time.time_ns()}"
        try:
            shutil.copy2(src, _git_tmp)
            os.replace(_git_tmp, dest)
        finally:
            try:
                if _git_tmp.exists():
                    _git_tmp.unlink()
            except Exception:
                pass
        print(f"[git] Copied target atomically: {src.name} → {dest}")

        # ステルスフラグ
        CREATE_NO_WINDOW = subprocess.CREATE_NO_WINDOW

        # 確実なる処置：shell=False に固定し、コマンドをリスト形式で渡す。
        # cwdに「Windows絶対パス文字列」を直接指定することで、Gitは100%迷子にならずにその場所で起動する。
        
        # --- 3. git add 実行 ---
        res_add = subprocess.run(
            ["git", "add", "index.html"], 
            cwd=repo_path, 
            shell=False,
            capture_output=True,
            text=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=120
        )
        if res_add.returncode != 0:
            print(f"[git][ERROR] 'git add' failed.\nReason: {res_add.stderr.strip()}")
            return False

        # --- 4. index.html のステージ済み差分だけを確認 ---
        # P1-356: git status全体を見ると、repo内の無関係な未追跡/未stage変更だけで
        # 「差分あり」と誤認し、index.htmlに差分が無いのにcommitして失敗する。
        status = subprocess.run(
            ["git", "diff", "--cached", "--quiet", "--", "index.html"],
            cwd=repo_path, shell=False, capture_output=True, text=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=120
        )
        if status.returncode == 0:
            # P1-383: 前回commit成功/push失敗の未送信commitが残っている可能性がある。
            # ローカルHTMLに差分が無くてもpushを再試行し、公開済みを確認してから成功扱いする。
            print("[git] No new index.html changes. Verifying/pushing any pending commits...")
            try:
                _push_retry = subprocess.run(
                    # P1-487: commitは現在checkout中のbranchへ作られる。local branchがmain以外でも
                    # 今回のHEADをremote mainへ送るよう明示する。
                    ["git", "push", "origin", "HEAD:main"],
                    cwd=repo_path, shell=False, capture_output=True, text=True,
                    creationflags=CREATE_NO_WINDOW, timeout=120
                )
            except subprocess.TimeoutExpired:
                print("[git][ERROR] retry push timed out after 120s")
                return False
            if _push_retry.returncode != 0:
                print(f"[git][ERROR] retry push failed.\nReason: {_push_retry.stderr.strip()}")
                return False
            print("[git] No new HTML diff; pending commits (if any) are pushed.")
            return True
        if status.returncode != 1:
            print(f"[git][ERROR] staged diff check failed.\nReason: {status.stderr.strip()}")
            return False

        # --- 5. git commit 実行 ---
        # P1-205: Git履歴の表示時刻もJSTへ統一。
        msg = f"Update dashboard: {_now_jst():%Y-%m-%d %H:%M:%S}"
        # P1-360: repoに既存のstage済み変更があってもdashboard commitへ巻き込まない。
        # --only + pathspec で index.html だけをcommit対象に限定する。
        res_commit = subprocess.run(
            ["git", "commit", "-m", msg, "--only", "--", "index.html"],
            cwd=repo_path, 
            shell=False,
            capture_output=True,
            text=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=120
        )
        if res_commit.returncode != 0:
            print(f"[git][ERROR] 'git commit' failed.\nReason: {res_commit.stderr.strip()}")
            return False
        print(f"[git] Committed changes: {msg}")

        # --- 6. git push 実行 ---
        print("[git] Pushing to GitHub repository...")
        res_push = subprocess.run(
            ["git", "push", "origin", "HEAD:main"], 
            cwd=repo_path, 
            shell=False,
            capture_output=True,
            text=True,
            creationflags=CREATE_NO_WINDOW,
            timeout=120
        )
        if res_push.returncode != 0:
            print(f"[git][ERROR] 'git push' failed.\nReason: {res_push.stderr.strip()}")
            return False

        print(f"[git] ✅ GitHub Pages successfully updated at {_now_jst():%H:%M:%S}")
        return True

    except Exception as e:
        print(f"[git][FATAL] Unexpected infrastructure error: {e}")
        return False
# ==============================================================================





# --- [ENCODING GUARD | Windows cp932 safe] ---
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(encoding='utf-8', errors='backslashreplace')
        sys.stderr.reconfigure(encoding='utf-8', errors='backslashreplace')
except Exception:
    pass
# --- [END ENCODING GUARD] ---
# === [MERGE-LIGHT-EOD] ANCHOR ===
# -*- coding: utf-8 -*-
"""
自動スクリーニング_完全統合版 + 右肩上がり（Template版/両立フィルタ/Gmail/オフラインHTML/祝日対応/MIDDAY自動）

修正点（この版）
- HTML出力フェーズの JSON 生成で、DataFrame 内の bytes / NaN / pandas.Timestamp / NumPy スカラーを
  安全に変換できるように修正（TypeError: bytes is not JSON serializable 対策）

機能ダイジェスト
- EOD/MIDDAY 自動判定（JST 11:30–12:30 は MIDDAY スナップショット、それ以外は EOD）
- 祝日/土日スキップ（jpholiday + 追加休場日ファイル対応）
- yahooquery で quotes / history を一括取得（初回は 12mo、通常は 10d）
- 初動/底打ち/上昇余地スコア/右肩上がりスコア の判定とログ（signals_log）
- 前営業日の翌日検証（判定とCSV出力）
- オフライン1ファイルHTMLダッシュボード（候補一覧/検証/全カラム/price_history/signals_log）

前提: Python 3.11 / pip install yahooquery pandas jpholiday
"""
# --- Standard library

# === [QUIET MODE PATCH v8] ================================================
# Verbosity switch (default: QUIET). Set PRICEGUARD_VERBOSE=1 or pass --verbose to enable noisy logs.


# CLI flag check (very lightweight and safe)
__argv_v8 = list(__sys_patch_v8.argv)
__CLI_VERBOSE = ("--verbose" in __argv_v8) or ("-v" in __argv_v8)
__CLI_QUIET   = ("--quiet" in __argv_v8) or ("-q" in __argv_v8)
if __CLI_VERBOSE and __CLI_QUIET:
    __CLI_QUIET = False  # verbose wins

# Env check
__ENV_VERBOSE = str(__os_patch_v8.environ.get("PRICEGUARD_VERBOSE", "0")).lower() not in ("", "0", "false", "no")

VERBOSE = bool(__CLI_VERBOSE or __ENV_VERBOSE)
if __CLI_QUIET:
    VERBOSE = False

# PDF/Parser noisy libs → silence (ERROR only)
for __name in ("pdfminer", "pdfminer.layout", "pdfminer.pdfinterp", "pdfplumber", "fitz", "pymupdf"):
    try:
        __lg_patch_v8.getLogger(__name).setLevel(__lg_patch_v8.ERROR)
    except Exception:
        pass

# Optionally drop super-noisy warnings
try:
    __wn_patch_v8.filterwarnings("ignore", message=r".*invalid float.*")
    __wn_patch_v8.filterwarnings("ignore", message=r".*non-stroke color.*")
except Exception:
    pass

# === JSON sanitize helpers (NaN/Infinity -> None) ===

# === _str_series: 安全な文字列Series化（未定義なら必ず定義） ===
# === injected helpers: _str_series / _norm_code_str / _norm_code_series ===
# どこかで未定義でも必ずここで供給されるようにする




# ---- DB global guards (fallback) ----
_DB_LOCK = threading.RLock()
_DB_SINGLETON = None
# -------------------------------------

# ==== safe compare helpers (pandas object列対策) ====











def _sanitize_numbers(x):
    if isinstance(x, float):
        return None if not math.isfinite(x) else x
    if isinstance(x, dict):
        return {k: _sanitize_numbers(v) for k, v in x.items()}
    if isinstance(x, (list, tuple)):
        return [_sanitize_numbers(v) for v in x]
    return x

# ---- robust JSON dumper: accepts json.dumps kwargs and sanitizes NaN/Inf ----


# === injected: safe link completion ===
def _normalize_yahoo_suffix_token(value) -> str | None:
    """Yahoo市場表記を .T/.N/.S/.F のみに正規化。未知値は採用しない。"""
    if value is None:
        return None
    sfx = str(value).strip().upper()
    if not sfx:
        return None
    # full Yahoo symbol override (1234.N / 285A.F) もsuffixだけ抽出。
    m = re.search(r"\.([TNSF])$", sfx)
    if m:
        return "." + m.group(1)
    aliases = {
        "T": ".T", "TSE": ".T", "TOKYO": ".T", "JPX": ".T",
        "PRIME": ".T", "STANDARD": ".T", "GROWTH": ".T",
        "MOTHERS": ".T", "JASDAQ": ".T",
        "N": ".N", "NSE": ".N", "NAGOYA": ".N",
        "S": ".S", "SSE": ".S", "SAPPORO": ".S",
        "F": ".F", "FSE": ".F", "FUKUOKA": ".F",
    }
    return aliases.get(sfx)


def _get_yahoo_suffix_from_overrides(code4: str) -> str | None:
    """
    override系テーブルから Yahooサフィックスを探して返す。
    返り値例: ".T", ".N", ".S", ".F"（先頭ドット付きで返す）
    見つからなければ None。
    P1-510: main yahoo_symbol_override とHTMLリンクの解決を揃え、未知suffixは無視する。
    """
    code4 = _normalize_jp_security_code(code4)
    if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", code4 or ""):
        return None

    try:
        conn = _get_db_conn()
    except Exception:
        conn = None
    if conn is None:
        return None

    # main overrideを最優先。NONE/HISTORY_NONEは状態sentinelなのでsuffixにはしない。
    try:
        _main_ov = _read_yahoo_override(conn, code4)
    except Exception:
        _main_ov = None
    if str(_main_ov or "").strip().upper() not in {"", "NONE", "HISTORY_NONE"}:
        _main_sfx = _normalize_yahoo_suffix_token(_main_ov)
        if _main_sfx:
            return _main_sfx

    table_candidates = [
        "symbol_overrides", "overrides_symbol", "overrides", "yahoo_overrides",
        "symbol_meta", "symbols_override",
    ]
    code_cols = ["code", "symbol", "ticker", "stock_code", "銘柄コード"]
    suffix_cols = ["yahoo_suffix", "yahoo_market_suffix", "yahoo_market", "yahoo_sfx"]

    # P2-24: 旧実装は6×5×4候補をSELECTして「no such table/column」を例外で判定していた。
    # sqlite_master/PRAGMAで実在schemaを先に絞り、存在するtable/columnだけ照会する。
    try:
        _existing_tables = {
            str(r[0]) for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall() if r and r[0]
        }
    except Exception:
        return None

    for t in table_candidates:
        if t not in _existing_tables:
            continue
        try:
            _cols = {str(r[1]) for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()}
        except Exception:
            continue
        _actual_code_cols = [c for c in code_cols if c in _cols]
        _actual_suffix_cols = [c for c in suffix_cols if c in _cols]
        if not _actual_code_cols or not _actual_suffix_cols:
            continue
        # 旧候補順序（table→code列→suffix列）の優先順位は維持する。
        # P2-59: REAL/TEXT混在で7203が7203.0として残る旧表もcanonical aliasとして読む。
        _variants = code_query_variants(code4)
        _ph = ",".join("?" for _ in _variants)
        for c_code in _actual_code_cols:
            for c_sfx in _actual_suffix_cols:
                try:
                    _rows = conn.execute(
                        f'SELECT CAST("{c_code}" AS TEXT), "{c_sfx}" FROM "{t}" '
                        f'WHERE CAST("{c_code}" AS TEXT) IN ({_ph})',
                        tuple(_variants),
                    ).fetchall()
                except Exception:
                    continue
                _by_raw = {str(_r[0]): _r[1] for _r in _rows}
                for _v in _variants:
                    _value = _by_raw.get(str(_v))
                    if _value:
                        norm = _normalize_yahoo_suffix_token(_value)
                        if norm:
                            return norm
    return None


def _market_to_yahoo_suffix(mkt: str | None) -> str:
    """
    市場→Yahooサフィックス推定。override が無い時の補助。既知以外は ".T"。
    """
    if not mkt:
        return ".T"
    s = str(mkt).upper()
    tokyo_keys = ("東", "TSE", "TOKYO", "JPX", "PRIME", "STANDARD", "GROWTH", "MOTHERS", "JASDAQ")
    if any(k in s for k in tokyo_keys):
        return ".T"
    if any(k in s for k in ("名", "NSE", "NAGOYA")):
        return ".N"
    if any(k in s for k in ("札", "SSE", "SAPPORO")):
        return ".S"
    if any(k in s for k in ("福", "FSE", "FUKUOKA")):
        return ".F"
    return ".T"


def _resolve_yahoo_suffix(code4: str, market: str | None) -> str:
    """
    1) overrideテーブル優先
    2) 市場からの推定
    3) どちらも無ければ ".T"
    """
    sfx = None
    try:
        sfx = _get_yahoo_suffix_from_overrides(code4)
    except Exception:
        sfx = None
    if sfx:
        return sfx
    return _market_to_yahoo_suffix(market)


def _ensure_links(d: dict) -> dict:
    """
    各種リンク（Yahoo/Kabutan/TradingView/自前charts/X）を欠損時だけ補完する。
    既存値は尊重し、欠損のみにセット。Yahooは override サフィックスを最優先。
    """
    if not isinstance(d, dict):
        return d

    # コード抽出
    code = (
        d.get("code") or d.get("コード") or d.get("symbol") or d.get("ticker") or
        d.get("銘柄コード") or d.get("stock_code")
    )
    if not code:
        return d

    # P1-146: 285A等を落とさず、7203.0も7203へ正規化。
    c4 = _normalize_jp_security_code(code)
    if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", c4 or ""):
        return d

    # 市場情報
    market = (
        d.get("market") or d.get("市場") or d.get("exchange") or
        d.get("市場コード") or d.get("market_raw") or d.get("section")
    )

    # P2-41: yahoo_urlが既にbulk解決済みなら、各行でoverride DBを再探索しない。
    _yahoo_default = None
    if not d.get("yahoo_url"):
        suffix = _resolve_yahoo_suffix(c4, market)
        _yahoo_default = f"https://finance.yahoo.co.jp/quote/{c4}{suffix}"

    # ===== 補完（既存値が無い場合のみ）=====
    # P1-147: キーが存在しても None/空文字なら「欠損」なので補完する。
    defaults = {
        "yahoo_url":       _yahoo_default,
        "kabutan_url":     f"https://kabutan.jp/stock/?code={c4}",
        "tradingview_url": f"https://jp.tradingview.com/symbols/TSE-{c4}/",
        "charts60_url":    f"./charts60/{c4}.html",
        "x_url":           f"https://x.com/search?q=%23{c4}%20OR%20{c4}%20株&f=live",
    }
    for _k, _v in defaults.items():
        if not d.get(_k) and _v is not None:
            d[_k] = _v
    return d

# === end injected ===






# === fast json / sanitize helpers (injected) ===
try:
    _HASorjson = True
except Exception:
    _HASorjson = False

def _json_sanitize(obj):
    """Normalize JSON-unsafe values (NaN/Inf -> None), convert sets, ensure str keys."""
    if obj is None:
        return None
    t = type(obj)
    if t in (int, str, bool):
        return obj
    if t is float:
        if math.isnan(obj) or math.isinf(obj):
            return None
        return obj
    if isinstance(obj, dict):
        out = {}
        for k, v in obj.items():
            ks = str(k)
            out[ks] = _json_sanitize(v)
        return out
    if isinstance(obj, (list, tuple)):
        return [_json_sanitize(x) for x in obj]
    if isinstance(obj, set):
        return [_json_sanitize(x) for x in obj]
    try:
        return str(obj)
    except Exception:
        return None


# === safe JSON dumper (accepts default kwarg) ===
def dumps_json_clean(obj,
                     ensure_ascii: bool = False,
                     separators=None,
                     default=None,
                     indent=None):
    """Safe JSON dump that accepts default=, normalizes numpy/Decimal/datetime/set/Path.
    Uses orjson when available, otherwise falls back to stdlib json.
    """


    def _coerce(x):
        if default is not None:
            try:
                return default(x)
            except Exception:
                pass
        if np is not None:
            if isinstance(x, (np.generic,)):
                return x.item()
            if isinstance(x, (np.ndarray,)):
                return x.tolist()
        if isinstance(x, (set, tuple)):
            return list(x)
        # P1-322: moduleは from pathlib import Path のみ。pathlib.Path参照はNameErrorになる。
        if isinstance(x, Path):
            return str(x)
        if isinstance(x, (datetime, date, dt_time)):
            try:
                return x.isoformat()
            except Exception:
                return str(x)
        if isinstance(x, decimal.Decimal):
            try:
                f = float(x)
                if math.isfinite(f):
                    return f
            except Exception:
                pass
            return str(x)
        try:
            return str(x)
        except Exception:
            return repr(x)

    # P1-318: stdlib json fallback は NaN/Infinity を既定でそのまま出力する。
    # ブラウザ JSON.parse と dashboard_data.json の双方を壊さないよう、
    # native/numpy/Decimal の非有限値を dump 前に再帰的に None へ正規化する。
    def _finite_clean(x):
        if isinstance(x, float):
            return x if math.isfinite(x) else None
        if isinstance(x, decimal.Decimal):
            try:
                f = float(x)
                return f if math.isfinite(f) else None
            except Exception:
                return str(x)
        if np is not None:
            if isinstance(x, np.generic):
                return _finite_clean(x.item())
            if isinstance(x, np.ndarray):
                return _finite_clean(x.tolist())
        if isinstance(x, dict):
            return {k: _finite_clean(v) for k, v in x.items()}
        if isinstance(x, (list, tuple, set)):
            return [_finite_clean(v) for v in x]
        return x

    _obj_clean = _finite_clean(obj)

    try:
        opt = 0
        try:
            opt |= orjson.OPT_SERIALIZE_NUMPY
        except Exception:
            pass
        bs = orjson.dumps(_obj_clean, default=_coerce, option=opt)
        s = bs.decode("utf-8")
        if ensure_ascii:
            s = s.encode("unicode_escape").decode("ascii")
        if indent or separators is not None:
            s = json.dumps(json.loads(s), ensure_ascii=ensure_ascii, indent=indent, separators=separators)
        return s
    except Exception:
        return json.dumps(
            _obj_clean,
            ensure_ascii=ensure_ascii,
            separators=separators,
            indent=indent,
            default=_coerce,
            allow_nan=False
        )


def _prune_dashboard_history(keep: int = DASHBOARD_HISTORY_KEEP) -> int:
    """Keep only newest timestamped dashboard_data_*.json snapshots.

    dashboard_data.json (live/current) is intentionally outside the glob pattern
    and is therefore never touched.
    """
    try:
        root = Path(OUTPUT_DIR)
        files = sorted(
            (p for p in root.glob("dashboard_data_*.json") if p.is_file()),
            key=lambda p: (p.stat().st_mtime_ns, p.name),
            reverse=True,
        )
        removed = 0
        for p in files[max(1, int(keep)):]:
            try:
                p.unlink()
                removed += 1
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"[HOUSEKEEPING][WARN] history prune failed: {p.name}: {e}")
        if removed:
            print(f"[HOUSEKEEPING] dashboard history pruned={removed} keep={max(1, int(keep))}")
        return removed
    except Exception as e:
        print(f"[HOUSEKEEPING][WARN] dashboard history scan failed: {e}")
        return 0


def _prune_charts60_stale(current_codes=None, retention_days: int = CHARTS60_RETENTION_DAYS) -> int:
    """Delete stale charts60 artifacts while protecting current-run chart codes."""
    chart_dir = Path(OUTPUT_DIR) / "charts60"
    if not chart_dir.is_dir():
        return 0
    protected = {
        canonical_code_for_db(c)
        for c in (current_codes or set())
        if canonical_code_for_db(c)
    }
    cutoff = time.time() - max(1, int(retention_days)) * 86400
    removed = 0
    try:
        for p in chart_dir.iterdir():
            if not p.is_file():
                continue
            # Only generated chart artifacts. Leave manifests/locks/unknown files alone.
            if p.suffix.lower() not in {".html", ".png"}:
                continue
            code = canonical_code_for_db(p.stem)
            if code and code in protected:
                continue
            try:
                if p.stat().st_mtime >= cutoff:
                    continue
                p.unlink()
                removed += 1
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"[HOUSEKEEPING][WARN] charts60 prune failed: {p.name}: {e}")
        if removed:
            print(
                f"[HOUSEKEEPING] charts60 stale artifacts pruned={removed} "
                f"retention_days={max(1, int(retention_days))} protected={len(protected)}"
            )
        return removed
    except Exception as e:
        print(f"[HOUSEKEEPING][WARN] charts60 scan failed: {e}")
        return 0


def _prune_runtime_logs(retention_days: int = RUNTIME_LOG_RETENTION_DAYS) -> int:
    """Delete only regular files older than retention from main/runtime_logs."""
    log_dir = _SCRIPT_DIR / "runtime_logs"
    if not log_dir.is_dir():
        return 0
    cutoff = time.time() - max(1, int(retention_days)) * 86400
    removed = 0
    try:
        for p in log_dir.iterdir():
            if not p.is_file():
                continue
            try:
                if p.stat().st_mtime >= cutoff:
                    continue
                p.unlink()
                removed += 1
            except FileNotFoundError:
                pass
            except Exception as e:
                print(f"[HOUSEKEEPING][WARN] runtime log prune failed: {p.name}: {e}")
        if removed:
            print(
                f"[HOUSEKEEPING] runtime logs pruned={removed} "
                f"retention_days={max(1, int(retention_days))}"
            )
        return removed
    except Exception as e:
        print(f"[HOUSEKEEPING][WARN] runtime log scan failed: {e}")
        return 0


def _housekeeping_generated_artifacts(current_chart_codes=None) -> bool:
    """Best-effort housekeeping. Never fail the screening run because cleanup failed."""
    _prune_charts60_stale(current_chart_codes, CHARTS60_RETENTION_DAYS)
    _prune_runtime_logs(RUNTIME_LOG_RETENTION_DAYS)
    return True


def _atomic_write_text_file(path: str | os.PathLike, text: str, encoding: str = "utf-8") -> None:
    """P1-321: 既存成果物を途中書込みで壊さない同一ディレクトリatomic replace。"""
    dst = Path(path)
    dst.parent.mkdir(parents=True, exist_ok=True)
    tmp_name = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", encoding=encoding, dir=str(dst.parent),
            prefix=dst.name + ".tmp.", delete=False
        ) as tf:
            tmp_name = tf.name
            tf.write(text)
            tf.flush()
            os.fsync(tf.fileno())
        os.replace(tmp_name, dst)
        tmp_name = None
    finally:
        if tmp_name:
            try:
                os.unlink(tmp_name)
            except Exception:
                pass



# ==============================================================================
# P4-LIVE Candidate Contract / scanner snapshot engine
# ==============================================================================
def _live_num(value):
    """JSON/score用のfinite float変換。NaN/InfはNone。"""
    try:
        if value is None or value == "":
            return None
        if isinstance(value, str):
            value = value.replace(",", "").replace("％", "").replace("%", "").strip()
            if not value:
                return None
        v = float(value)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _live_get(row, *names):
    for name in names:
        try:
            if isinstance(row, dict):
                v = row.get(name)
            else:
                v = row[name] if name in row else None
        except Exception:
            v = None
        if v is not None and v != "":
            try:
                if pd.isna(v):
                    continue
            except Exception:
                pass
            return v
    return None


def _live_flag(row, *names) -> bool:
    v = _live_get(row, *names)
    if v is None:
        return False
    if isinstance(v, (bool, np.bool_)):
        return bool(v)
    n = _live_num(v)
    if n is not None and n != 0:
        return True
    s = str(v).strip().lower()
    return s in {"true", "yes", "on", "候補", "該当", "1"} or "候補" in s


def _live_stock_code(code, market="", name="") -> str:
    c = canonical_code_for_db(code)
    if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", str(c or "")):
        return ""
    if _p4_im_is_non_stock(c, market, name):
        return ""
    return str(c)


def _live_daily_structure_map(conn: sqlite3.Connection, asof_date: str) -> dict[str, dict]:
    """日足構造だけからSTEADY_UP用特徴量を計算。既存右肩判定は変更しない。"""
    start = (pd.Timestamp(asof_date) - pd.Timedelta(days=240)).strftime("%Y-%m-%d")
    ph = pd.read_sql_query(
        """
        SELECT rowid AS _rowid, コード, 日付, 始値, 高値, 安値, 終値, 出来高
          FROM price_history
         WHERE date(日付) >= date(?) AND date(日付) <= date(?)
         ORDER BY 日付, rowid
        """,
        conn, params=[start, asof_date],
    )
    if ph.empty:
        return {}
    # Candidate Exportは既存price_historyの確定日足だけを読む。
    # ここでJPX祝日ライブラリへ新たな依存を増やさず、logical code×日付の最終rowだけ採用する。
    ph["コード"] = ph["コード"].map(canonical_code_for_db)
    ph["日付"] = pd.to_datetime(ph["日付"], errors="coerce")
    ph = ph.dropna(subset=["コード", "日付"]).sort_values(["日付", "_rowid"], kind="stable")
    ph = ph.drop_duplicates(["コード", "日付"], keep="last")
    for c in ("始値", "高値", "安値", "終値", "出来高"):
        ph[c] = pd.to_numeric(ph[c], errors="coerce")
    ph["日付"] = pd.to_datetime(ph["日付"], errors="coerce")
    ph = ph.dropna(subset=["コード", "日付", "終値"])
    ph = ph[np.isfinite(ph["終値"]) & (ph["終値"] > 0)].copy()
    out = {}
    for code, g in ph.groupby("コード", sort=False):
        g = g.sort_values(["日付", "_rowid"], kind="stable").drop_duplicates("日付", keep="last")
        if len(g) < 30:
            continue
        close = g["終値"].astype(float)
        high = g["高値"].astype(float)
        low = g["安値"].astype(float)
        ma5s = close.rolling(5, min_periods=5).mean()
        ma25s = close.rolling(25, min_periods=25).mean()
        ma75s = close.rolling(75, min_periods=75).mean()
        cur = float(close.iloc[-1])
        ma5 = _live_num(ma5s.iloc[-1]); ma25 = _live_num(ma25s.iloc[-1]); ma75 = _live_num(ma75s.iloc[-1])
        ma25_slope = None
        if len(ma25s.dropna()) >= 21 and ma25 not in (None, 0):
            old = _live_num(ma25s.iloc[-21])
            if old not in (None, 0): ma25_slope = (ma25 / old - 1.0) * 100.0
        ma75_slope = None
        if len(ma75s.dropna()) >= 21 and ma75 not in (None, 0):
            old = _live_num(ma75s.iloc[-21])
            if old not in (None, 0): ma75_slope = (ma75 / old - 1.0) * 100.0
        ma5_slope = None
        if len(ma5s.dropna()) >= 6 and ma5 not in (None, 0):
            old = _live_num(ma5s.iloc[-6])
            if old not in (None, 0): ma5_slope = (ma5 / old - 1.0) * 100.0
        recent20 = g.tail(20).copy()
        ma25_recent = ma25s.loc[recent20.index]
        valid_stay = ma25_recent.notna() & recent20["終値"].notna()
        stay25 = float((recent20.loc[valid_stay, "終値"] > ma25_recent.loc[valid_stay]).mean()) if valid_stay.any() else None
        lows = low.tail(25)
        low_rise = (lows > lows.shift(5)).dropna()
        low_rise_ratio = float(low_rise.mean()) if len(low_rise) else None
        ret = close.pct_change()
        up_days5 = int((ret.tail(5) > 0).sum())
        prev20max = high.shift(1).rolling(20, min_periods=10).max()
        high_updates5 = int((high.tail(5) > prev20max.tail(5)).fillna(False).sum())
        c120 = close.tail(120)
        dd = c120 / c120.cummax() - 1.0
        max_dd120 = float(dd.min() * 100.0) if len(dd) else None
        ret20 = (cur / float(close.iloc[-21]) - 1.0) * 100.0 if len(close) >= 21 and close.iloc[-21] > 0 else None
        day_ret = (cur / float(close.iloc[-2]) - 1.0) * 100.0 if len(close) >= 2 and close.iloc[-2] > 0 else None
        high90 = _live_num(high.tail(90).max())
        near90 = bool(high90 and cur >= high90 * 0.97)
        out[str(code)] = {
            "ma5": ma5, "ma25": ma25, "ma75": ma75,
            "ma25_slope20_pct": ma25_slope, "ma75_slope20_pct": ma75_slope,
            "ma5_slope5_pct": ma5_slope, "ma25_stay_ratio": stay25,
            "low_rise_ratio": low_rise_ratio, "up_days5": up_days5,
            "high_updates5": high_updates5, "max_dd120_pct": max_dd120,
            "ret20_pct": ret20, "day_ret_pct": day_ret, "near_90d_high": near90,
        }
    return out


def _live_steady_score(features: dict, intraday: dict, current_price, turnover_oku, day_return_pct) -> tuple[float, bool, list[str]]:
    """日足の上昇構造 + 場中のじわ上げ継続性を0-100で採点。単発急騰は高得点にしない。"""
    f = features or {}; x = intraday or {}
    p = _live_num(current_price); turn = _live_num(turnover_oku) or 0.0
    dr = _live_num(day_return_pct)
    ma5 = _live_num(f.get("ma5")); ma25 = _live_num(f.get("ma25")); ma75 = _live_num(f.get("ma75"))
    s25 = _live_num(f.get("ma25_slope20_pct")); s75 = _live_num(f.get("ma75_slope20_pct")); s5 = _live_num(f.get("ma5_slope5_pct"))
    stay = _live_num(f.get("ma25_stay_ratio")); lowrise = _live_num(f.get("low_rise_ratio"))
    up5 = int(f.get("up_days5") or 0); hup = int(f.get("high_updates5") or 0)
    ret20d = _live_num(f.get("ret20_pct"))
    r10 = _live_num(x.get("ret_10m")); r20 = _live_num(x.get("ret_20m")); r30 = _live_num(x.get("ret_30m")); r60 = _live_num(x.get("ret_60m"))
    upr = _live_num(x.get("up_snapshot_ratio")); pull = _live_num(x.get("max_pullback_pct"))
    score = 0.0; reasons=[]
    # 日足構造: 最大65点
    if ma5 is not None and ma25 is not None and ma5 > ma25: score += 10; reasons.append("MA5>MA25")
    if ma25 is not None and ma75 is not None and ma25 > ma75: score += 8; reasons.append("MA25>MA75")
    if s25 is not None and s25 > 0: score += 12; reasons.append("MA25上向き")
    if s75 is not None and s75 > 0: score += 4; reasons.append("MA75上向き")
    if stay is not None and stay >= 0.70: score += 10; reasons.append(f"MA25上{stay:.0%}")
    if lowrise is not None and lowrise >= 0.60: score += 7; reasons.append("安値切上げ")
    if up5 >= 3: score += 5; reasons.append(f"5日上昇{up5}日")
    if s5 is not None and s5 > 0: score += 5; reasons.append("MA5上向き")
    if hup > 0: score += 2; reasons.append("高値更新")
    if (dr is None or dr <= LIVE_STEADY_MAX_DAY_RETURN_PCT) and (ret20d is None or ret20d <= 30.0): score += 2; reasons.append("非過熱")
    # 場中継続性: 最大35点。10/20/30/60分、上昇snapshot比率、最大押しを正式採点。
    positive_horizons = 0
    for label, val, pts in (("10分",r10,4),("20分",r20,5),("30分",r30,6),("60分",r60,7)):
        if val is not None and val > 0:
            score += pts; positive_horizons += 1; reasons.append(f"{label}+{val:.2f}%")
    if upr is not None:
        if upr >= 0.80: score += 8; reasons.append(f"snapshot上昇{upr:.0%}")
        elif upr >= 0.60: score += 5; reasons.append(f"snapshot上昇{upr:.0%}")
    if pull is not None:
        if pull >= -0.35: score += 5; reasons.append("場中押し極浅")
        elif pull >= -0.80: score += 3; reasons.append("場中押し浅い")
    # 場中履歴が十分ある場合は、単発上昇ではなく複数時間軸の継続性を要求。履歴不足時は日足gateのみで暫定判定。
    intraday_available = sum(v is not None for v in (r10,r20,r30,r60)) >= 2
    intraday_ok = (positive_horizons >= 2 and (upr is None or upr >= 0.55) and (pull is None or pull >= -1.5)) if intraday_available else True
    gate = bool(
        p is not None and ma25 is not None and p > ma25
        and s25 is not None and s25 > 0
        and turn >= LIVE_STEADY_MIN_TURNOVER_OKU
        and (dr is None or dr <= LIVE_STEADY_MAX_DAY_RETURN_PCT)
        and intraday_ok
        and score >= LIVE_STEADY_MIN_SCORE
    )
    return round(min(100.0, score), 1), gate, reasons


def _live_snapshot_conn() -> sqlite3.Connection:
    LIVE_SNAPSHOT_DB.parent.mkdir(parents=True, exist_ok=True)
    c = sqlite3.connect(str(LIVE_SNAPSHOT_DB), timeout=30.0)
    c.execute("PRAGMA journal_mode=WAL")
    c.execute("PRAGMA synchronous=NORMAL")
    c.execute("PRAGMA busy_timeout=30000")
    c.execute("""
        CREATE TABLE IF NOT EXISTS intraday_scanner_snapshot (
            captured_at TEXT NOT NULL,
            trade_date TEXT NOT NULL,
            code TEXT NOT NULL,
            current_price REAL,
            previous_close REAL,
            day_open REAL,
            day_high REAL,
            day_low REAL,
            volume REAL,
            turnover_oku REAL,
            rvol_turnover REAL,
            ma5 REAL,
            ma25 REAL,
            ma75 REAL,
            support_today REAL,
            resistance_today REAL,
            PRIMARY KEY (captured_at, code)
        )
    """)
    c.execute("CREATE INDEX IF NOT EXISTS idx_intraday_snapshot_code_time ON intraday_scanner_snapshot(code, captured_at)")
    c.execute("CREATE INDEX IF NOT EXISTS idx_intraday_snapshot_trade_date ON intraday_scanner_snapshot(trade_date, code)")
    return c


def _live_prune_snapshot_trade_days(c: sqlite3.Connection) -> int:
    dates=[r[0] for r in c.execute("SELECT DISTINCT trade_date FROM intraday_scanner_snapshot ORDER BY trade_date DESC").fetchall()]
    if len(dates) <= LIVE_SNAPSHOT_KEEP_TRADE_DAYS:
        return 0
    keep=set(dates[:LIVE_SNAPSHOT_KEEP_TRADE_DAYS])
    placeholders=','.join('?' for _ in keep)
    before=c.total_changes
    c.execute(f"DELETE FROM intraday_scanner_snapshot WHERE trade_date NOT IN ({placeholders})", tuple(sorted(keep)))
    return c.total_changes-before


def _live_capture_snapshots(rows: list[dict], daily_map: dict[str, dict]) -> tuple[str, str, int]:
    now = _now_jst()
    captured = now.isoformat(timespec="microseconds")
    trade_date = now.date().isoformat()
    vals=[]
    for r in rows:
        code=_live_stock_code(_live_get(r,"コード","code"), _live_get(r,"市場","market"), _live_get(r,"銘柄名","name"))
        if not code: continue
        current_price = _live_num(_live_get(r,"現在値_raw","現在値","current_price"))
        # snapshotは「各runで価格取得できた全市場銘柄」が対象。候補判定前に保存し、価格欠損行は保存しない。
        if current_price is None or current_price <= 0:
            continue
        d=daily_map.get(code,{})
        vals.append((
            captured, trade_date, code,
            current_price,
            _live_num(_live_get(r,"前日終値","previous_close")),
            _live_num(_live_get(r,"始値","day_open")),
            _live_num(_live_get(r,"高値","day_high")),
            _live_num(_live_get(r,"安値","day_low")),
            _live_num(_live_get(r,"出来高","volume")),
            _live_num(_live_get(r,"売買代金億","売買代金(億)","turnover_oku")),
            _live_num(_live_get(r,"RVOL代金","rvol_turnover")),
            _live_num(d.get("ma5")), _live_num(d.get("ma25")), _live_num(d.get("ma75")),
            _live_num(_live_get(r,"支持線今日","support_today")),
            _live_num(_live_get(r,"抵抗線今日","resistance_today")),
        ))
    c=_live_snapshot_conn()
    try:
        with c:
            c.executemany("""
                INSERT OR REPLACE INTO intraday_scanner_snapshot(
                    captured_at,trade_date,code,current_price,previous_close,day_open,day_high,day_low,
                    volume,turnover_oku,rvol_turnover,ma5,ma25,ma75,support_today,resistance_today
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, vals)
            pruned=_live_prune_snapshot_trade_days(c)
        if pruned:
            print(f"[live-feed] snapshot prune rows={pruned} keep_trade_days={LIVE_SNAPSHOT_KEEP_TRADE_DAYS}")
    finally:
        c.close()
    return captured, trade_date, len(vals)


def _live_intraday_feature_map(trade_date: str, current_codes: set[str]) -> dict[str, dict]:
    if not current_codes:
        return {}
    c=_live_snapshot_conn()
    try:
        df=pd.read_sql_query(
            "SELECT * FROM intraday_scanner_snapshot WHERE trade_date=? ORDER BY captured_at, code",
            c, params=[trade_date],
        )
    finally:
        c.close()
    if df.empty: return {}
    df=df[df["code"].astype(str).isin(current_codes)].copy()
    df["_dt"]=pd.to_datetime(df["captured_at"], errors="coerce", utc=True)
    for col in ("current_price","day_low","day_high","support_today"):
        df[col]=pd.to_numeric(df[col], errors="coerce")
    out={}
    horizons=(5,10,20,30,60)
    for code,g in df.groupby("code", sort=False):
        g=g.dropna(subset=["_dt","current_price"]).sort_values("_dt", kind="stable")
        if g.empty: continue
        cur=g.iloc[-1]; curp=_live_num(cur["current_price"]); nowdt=cur["_dt"]
        feat={}
        for mins in horizons:
            target=nowdt-pd.Timedelta(minutes=mins)
            prev=g[g["_dt"]<=target]
            p0=_live_num(prev.iloc[-1]["current_price"]) if not prev.empty else None
            feat[f"ret_{mins}m"] = round((curp/p0-1.0)*100.0,4) if curp is not None and p0 not in (None,0) else None
        low=_live_num(cur["day_low"])
        feat["low_rebound_pct"] = round((curp/low-1.0)*100.0,4) if curp is not None and low not in (None,0) else None
        win=g[g["_dt"]>=nowdt-pd.Timedelta(minutes=60)].copy()
        prices=win["current_price"].astype(float)
        diffs=prices.diff().dropna()
        feat["up_snapshot_ratio"] = round(float((diffs>0).mean()),4) if len(diffs) else None
        if len(prices):
            draw=(prices/prices.cummax()-1.0)*100.0
            feat["max_pullback_pct"] = round(float(draw.min()),4)
        else: feat["max_pullback_pct"]=None
        lows=win["day_low"].dropna()
        last_low_change_dt=None
        if len(lows):
            last_val=None
            for idx,val in lows.items():
                if last_val is None or float(val) < float(last_val)-1e-12:
                    last_low_change_dt=win.loc[idx,"_dt"]
                    last_val=float(val)
        _mins_since_low = round(float((nowdt-last_low_change_dt).total_seconds()/60.0),2) if last_low_change_dt is not None else None
        feat["low_seen_at"] = last_low_change_dt.isoformat() if last_low_change_dt is not None else None
        feat["minutes_since_low"] = _mins_since_low
        feat["no_new_low_minutes"] = _mins_since_low
        # backward-compatible alias
        feat["minutes_since_low_update"] = _mins_since_low
        feat["low_update_stopped"] = bool(_mins_since_low is not None and _mins_since_low >= 20.0)
        out[str(code)]=feat
    return out


def _live_bottom_score(intraday: dict, current_price, support_today=None) -> tuple[float,bool,list[str]]:
    x=intraday or {}; score=0.0; reasons=[]
    rebound=_live_num(x.get("low_rebound_pct")); r10=_live_num(x.get("ret_10m")); r20=_live_num(x.get("ret_20m")); r30=_live_num(x.get("ret_30m"))
    upr=_live_num(x.get("up_snapshot_ratio")); pull=_live_num(x.get("max_pullback_pct")); stopped=bool(x.get("low_update_stopped"))
    if rebound is not None and rebound >= LIVE_BOTTOM_MIN_REBOUND_PCT: score+=25; reasons.append(f"安値+{rebound:.1f}%")
    if stopped: score+=20; reasons.append("安値更新停止")
    if r10 is not None and r10 > 0: score+=10; reasons.append("10分切上げ")
    if r20 is not None and r20 > 0: score+=10; reasons.append("20分切上げ")
    if r30 is not None and r30 > 0: score+=15; reasons.append("30分切上げ")
    if upr is not None and upr >= 0.60: score+=10; reasons.append("snapshot安定上昇")
    if pull is not None and pull >= -1.0: score+=5; reasons.append("押し浅い")
    p=_live_num(current_price); sup=_live_num(support_today)
    if p is not None and sup is not None and p >= sup: score+=5; reasons.append("支持線回復")
    gate=bool(score>=LIVE_BOTTOM_MIN_SCORE and rebound is not None and rebound>=LIVE_BOTTOM_MIN_REBOUND_PCT and stopped and r20 is not None and r20>0 and (upr is None or upr>=0.5))
    return round(min(100.0,score),1), gate, reasons


def _live_candidate_payload(conn: sqlite3.Connection, rows: list[dict]) -> dict:
    generated=_now_jst().isoformat(timespec="seconds")
    snapshot=str(_auto_run_mode() or RUN_SESSION or "UNKNOWN").upper()
    asof=_expected_snapshot_date_for_run(snapshot).isoformat()
    daily=_live_daily_structure_map(conn, asof)
    captured, trade_date, snapshot_rows=_live_capture_snapshots(rows, daily)
    codes={_live_stock_code(_live_get(r,"コード"),_live_get(r,"市場"),_live_get(r,"銘柄名")) for r in rows}
    codes.discard("")
    intraday_map=_live_intraday_feature_map(trade_date,codes)
    candidates=[]; counts={k:0 for k in ("INITIAL_MOMENTUM","STEADY_UP","BOTTOM_REVERSAL","FUNDAMENTAL_EARLY","THEME_DISCOVERY","PRE_EARNINGS","POST_EARNINGS")}
    for r in rows:
        code=_live_stock_code(_live_get(r,"コード"),_live_get(r,"市場"),_live_get(r,"銘柄名"))
        if not code: continue
        name=str(_live_get(r,"銘柄名") or ""); market=str(_live_get(r,"市場") or "")
        current=_live_num(_live_get(r,"現在値_raw","現在値")); prev=_live_num(_live_get(r,"前日終値"))
        dr=_live_num(_live_get(r,"前日終値比率_raw","前日終値比率"))
        turn=_live_num(_live_get(r,"売買代金億","売買代金(億)")); rvol=_live_num(_live_get(r,"RVOL代金")); vol=_live_num(_live_get(r,"出来高"))
        d=daily.get(code,{})
        intr=intraday_map.get(code,{})
        steady_score, steady_on, steady_reason=_live_steady_score(d,intr,current,turn,dr)
        bottom_score,bottom_on,bottom_reason=_live_bottom_score(intr,current,_live_get(r,"支持線今日"))
        im_on=_live_flag(r,"INITIAL_MOMENTUM")
        im_score=_live_num(_live_get(r,"INITIAL_MOMENTUM_SCORE")) or 0.0
        # 正式QUALITYは未実装。tri_safety等を意味の異なるproxyとしてsource化しない。
        quality_score=None
        market_score=_live_num(_live_get(r,"合成スコア","AIスコア")) or 0.0
        active=[]; scores={
            "market_score":round(market_score,1),
            "initial_momentum":round(im_score,1),
            "steady_up":steady_score,
            "bottom_reversal":bottom_score,
            "quality":quality_score,
            "fundamental_early":0.0,
            "theme_discovery":0.0,
            "pre_earnings":0.0,
            "post_earnings":0.0,
        }
        reasons=[]
        if im_on: active.append("INITIAL_MOMENTUM"); reasons.append(str(_live_get(r,"INITIAL_MOMENTUM_REASON") or "初動モメンタム"))
        if steady_on: active.append("STEADY_UP"); reasons.extend(steady_reason[:4])
        if bottom_on: active.append("BOTTOM_REVERSAL"); reasons.extend(bottom_reason[:4])
        if not active: continue
        for s in active: counts[s]+=1
        strategy_scores=[scores[{"INITIAL_MOMENTUM":"initial_momentum","STEADY_UP":"steady_up","BOTTOM_REVERSAL":"bottom_reversal"}[s]] for s in active]
        base=max(strategy_scores) if strategy_scores else 0.0
        priority=base+min(10.0,max(0,len(active)-1)*3.0)
        if turn is not None and turn < LIVE_LOW_LIQUIDITY_OKU: priority-=LIVE_LOW_LIQUIDITY_PENALTY
        priority=round(max(0.0,min(100.0,priority)),1)
        tags=[]
        if current is not None and current <= LIVE_LOW_PRICE_MAX: tags.append("LOW_PRICE_SPECIAL")
        if rvol is not None and rvol >= 2.0: tags.append("HIGH_RVOL")
        ma25=_live_num(d.get("ma25")); ma75=_live_num(d.get("ma75"))
        if current is not None and ma25 is not None and current>ma25: tags.append("ABOVE_MA25")
        if current is not None and ma75 is not None and current>ma75: tags.append("ABOVE_MA75")
        if d.get("near_90d_high"): tags.append("NEAR_90D_HIGH")
        candidate={
            "code":str(code), "name":name, "market":market,
            "sources":active, "priority":priority,
            "current_price":current, "previous_close":prev, "day_return_pct":dr,
            "day_open":_live_num(_live_get(r,"始値")), "day_high":_live_num(_live_get(r,"高値")), "day_low":_live_num(_live_get(r,"安値")),
            "volume":vol, "turnover_oku":turn, "rvol_turnover":rvol,
            "ma5":_live_num(d.get("ma5")), "ma25":ma25, "ma75":ma75,
            "support_today":_live_num(_live_get(r,"支持線今日")), "resistance_today":_live_num(_live_get(r,"抵抗線今日")),
            "scores":scores,
            "intraday":{
                "ret_5m":_live_num(intr.get("ret_5m")), "ret_10m":_live_num(intr.get("ret_10m")),
                "ret_20m":_live_num(intr.get("ret_20m")), "ret_30m":_live_num(intr.get("ret_30m")), "ret_60m":_live_num(intr.get("ret_60m")),
                "low_rebound_pct":_live_num(intr.get("low_rebound_pct")), "up_snapshot_ratio":_live_num(intr.get("up_snapshot_ratio")),
                "max_pullback_pct":_live_num(intr.get("max_pullback_pct")),
                "low_seen_at":intr.get("low_seen_at"),
                "minutes_since_low":_live_num(intr.get("minutes_since_low")),
                "no_new_low_minutes":_live_num(intr.get("no_new_low_minutes")),
            },
            "flags":{
                "above_ma25":bool(current is not None and ma25 is not None and current>ma25),
                "above_ma75":bool(current is not None and ma75 is not None and current>ma75),
                "near_90d_high":bool(d.get("near_90d_high")),
                "low_price_special":bool(current is not None and current<=LIVE_LOW_PRICE_MAX),
            },
            "tags":tags,
            "reason":" / ".join(dict.fromkeys(x for x in reasons if x))[:400],
        }
        candidates.append(candidate)
    candidates.sort(key=lambda x:(x["priority"],x["scores"].get("market_score",0),x.get("turnover_oku") or 0,x["code"]), reverse=True)
    candidates=candidates[:LIVE_CANDIDATE_MAX]
    return {
        "schema_version":LIVE_CANDIDATE_SCHEMA_VERSION,
        "generated_at":generated,
        "snapshot":snapshot,
        "source":"auto_screening",
        "valid_for_seconds":LIVE_CANDIDATE_VALID_SECONDS,
        "candidate_count":len(candidates),
        "candidates":candidates,
        "stats":{
            "universe_count":len(codes), "snapshot_rows_written":snapshot_rows,
            "initial_momentum_count":counts["INITIAL_MOMENTUM"], "steady_up_count":counts["STEADY_UP"],
            "bottom_reversal_count":counts["BOTTOM_REVERSAL"],
            "exported_count":len(candidates), "max_candidates":LIVE_CANDIDATE_MAX,
            "snapshot_db":str(LIVE_SNAPSHOT_DB), "captured_at":captured,
        },
    }


def export_live_candidate_feed(conn: sqlite3.Connection, rows_or_df) -> bool:
    """LIVE AUTO向け正式Candidate Contractをatomic更新。失敗時は前回正常JSONを残す。"""
    try:
        if isinstance(rows_or_df,pd.DataFrame): rows=rows_or_df.to_dict(orient="records")
        else: rows=list(rows_or_df or [])
        payload=_live_candidate_payload(conn,rows)
        clean=dumps_json_clean(payload, ensure_ascii=False)
        # allow_nan=False相当のdumps_json_cleanを経たうえで、再parseしてschema/count整合も確認。
        check=json.loads(clean)
        if check.get("schema_version") != LIVE_CANDIDATE_SCHEMA_VERSION or not isinstance(check.get("candidates"),list):
            raise RuntimeError("live candidate payload schema validation failed")
        if int(check.get("candidate_count",-1)) != len(check["candidates"]):
            raise RuntimeError("live candidate candidate_count mismatch")
        if any(not isinstance(x.get("code"),str) for x in check["candidates"]):
            raise RuntimeError("live candidate code must be string")
        _atomic_write_text_file(LIVE_CANDIDATE_FEED_PATH, clean)
        st=check.get("stats",{})
        print(
            "[live-feed] generated: "
            f"universe={st.get('universe_count',0)} candidates={check.get('candidate_count',0)} "
            f"INITIAL_MOMENTUM={st.get('initial_momentum_count',0)} "
            f"STEADY_UP={st.get('steady_up_count',0)} BOTTOM_REVERSAL={st.get('bottom_reversal_count',0)} "
            f"snapshot_rows_written={st.get('snapshot_rows_written',0)} path={LIVE_CANDIDATE_FEED_PATH}"
        )
        return True
    except Exception as e:
        # 旧正常feedを消さない。generated_atでLIVE側がstale判定できる。
        logging.error("[live-feed][ERROR] candidate feed generation failed; previous normal file preserved", exc_info=True)
        print(f"[live-feed][WARN] generation failed; previous file preserved: {e}")
        return False


def _open_db(path: str) -> sqlite3.Connection:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(
        path,
        timeout=30.0,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        check_same_thread=False,   # 必要に応じて True へ
        isolation_level=None       # autocommit（必要なら変更）
    )
    # PRAGMA 初期化
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.execute("PRAGMA busy_timeout=60000;")
    conn.row_factory = sqlite3.Row

    return conn

def _get_db_conn() -> sqlite3.Connection:
    """必要になった時だけ開いて使い回す（再帰なし・スレッド安全）"""
    global _DB_SINGLETON
    with _DB_LOCK:
        if _DB_SINGLETON is not None:
            # コネクション健全性チェック
            try:
                _DB_SINGLETON.execute("SELECT 1;")
                return _DB_SINGLETON
            except Exception:
                try:
                    _DB_SINGLETON.close()
                except Exception:
                    pass
                _DB_SINGLETON = None
        # 新規オープン
        _DB_SINGLETON = _open_db(DB_PATH)
        return _DB_SINGLETON

def _close_db_conn_safely():
    """任意：終了処理などで明示クローズしたい場合のみ使用"""
    global _DB_SINGLETON
    with _DB_LOCK:
        if _DB_SINGLETON is not None:
            try:
                _DB_SINGLETON.close()
            finally:
                _DB_SINGLETON = None



# price_history読取は logical-code + distinct-market-date を基本規約とする。
# === Canonicalize code for DB keys (再発防止の要) ===
_TOPIX_ALIASES = {'^TOPIX', 'TOPIX', '998405.T', '^TOPX'}
# Yahoo JapanのTOPIX正規シンボルは 998405.T。DB内の論理コードは
# ^TOPX に統一するが、Yahooへ ^TOPX をそのまま送るとquoteが取れず、
# RS_5/RS_20が全銘柄NULLになる。問合せシンボルだけ明示変換する。
_YAHOO_INDEX_SYMBOLS = {
    '^TOPX': '998405.T',
    '^TOPIX': '998405.T',
    'TOPIX': '998405.T',
    '998405.T': '998405.T',
}

def canonical_code_for_db(code: str) -> str:
    """DB JOIN/UPDATE 用の証券コードを決定的に正規化する。

    P1-163: 7203.0 / 285a / 285A.T のような同一銘柄の別表記を
    7203 / 285A に統一し、TOPIX別名は ^TOPX に統一する。
    """
    if code is None:
        return ""
    s = str(code).strip()
    if not s:
        return ""
    u = s.upper()
    if u in {a.upper() for a in _TOPIX_ALIASES}:
        return '^TOPX'
    if u in {'NAN', 'NONE'}:
        return ""
    # P1-329: 東証だけでなく名証/札証/福証のYahoo suffixもDBキーから外す。
    for suf in ('.T', '.N', '.S', '.F', '-T', '-N', '-S', '-F', '.JP', '-JP'):
        if u.endswith(suf):
            u = u[:-len(suf)]
            break
    if re.fullmatch(r"[0-9]+\.0+", u):
        u = u.split('.', 1)[0]
    if u.isdigit():
        return u.zfill(4)
    return u

def code_query_variants(code: object) -> list[str]:
    """P1-256: canonical/legacy DB表記を同一銘柄として検索する候補列。"""
    c = canonical_code_for_db(code)
    if not c:
        return []
    vals = [c]
    if re.fullmatch(r"\d{4}", c):
        vals.append(c + ".0")
    elif re.fullmatch(r"\d{3}[A-Z]", c):
        vals.append(c.lower())

    # P1-333: P1-329以前のDBにYahoo市場suffix付きキーが残っていても検索できるようにする。
    # 証券コード自体は同一なので、T/N/S/Fのcross-list表記はlogical aliasとして扱う。
    if re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", c):
        vals.extend(c + suf for suf in (".T", ".N", ".S", ".F"))

    raw = "" if code is None else str(code).strip()
    if raw and raw not in vals:
        vals.append(raw)
    return list(dict.fromkeys(vals))

def expand_code_query_variants(codes) -> list[str]:
    """P1-276: 複数銘柄のcanonical/legacy検索候補を重複なく展開する。"""
    out = []
    for code in (codes or []):
        out.extend(code_query_variants(code))
    return list(dict.fromkeys(v for v in out if v))


def _latest_finance_notes_by_canonical(df: pd.DataFrame, key_col: str = "_key") -> pd.DataFrame:
    """P1-602: finance_notesのalias重複から実際に最終更新された正本行を選ぶ。

    finance_notesは現行writerではコードPRIMARY KEYのcurrent snapshotだが、legacy DBには
    7203 / 7203.0 等のalias行が共存し得る。ON CONFLICT UPDATEではrowidが変わらないため、
    rowid最大=最新更新とは限らない。updated_atを主、canonical表記・rowidをtie-breakにする。
    """
    if df is None or getattr(df, "empty", True):
        return df.copy() if hasattr(df, "copy") else pd.DataFrame()
    if "コード" not in df.columns:
        return df.copy()
    x = df.copy()
    # P1-607: key_col="コード" の呼出しではcanonical keyを書き戻す前にraw表記を保存する。
    # 先に上書きすると7203.0も7203になり、canonical表記tie-breakが全行1へ潰れる。
    _raw = x["コード"].astype(str).str.strip().str.upper()
    x[key_col] = x["コード"].map(canonical_code_for_db)
    x = x[x[key_col].astype(str).str.len() > 0].copy()
    _raw = _raw.loc[x.index]
    if x.empty:
        return x
    if "updated_at" in x.columns:
        # P1-650: legacy naive ISO と現行offset付きISOが混在しても比較可能なJST-naiveへ統一。
        x["_fn_updated_sort"] = x["updated_at"].map(_p1_608_jst_naive_ts)
    else:
        x["_fn_updated_sort"] = pd.NaT
    x["_fn_canon_match"] = (_raw == x[key_col].astype(str).str.upper()).astype(int)
    if "_rowid" not in x.columns:
        x["_rowid"] = range(1, len(x) + 1)
    x = (x.sort_values([key_col, "_fn_updated_sort", "_fn_canon_match", "_rowid"],
                       kind="stable", na_position="first")
           .drop_duplicates(key_col, keep="last"))
    return x.drop(columns=["_fn_updated_sort", "_fn_canon_match"], errors="ignore")


def _p1_608_jst_naive_ts(value):
    """P1-608: DB内の発表/更新時刻をJST壁時計のnaive Timestampへ揃える。"""
    try:
        if value is None or (isinstance(value, float) and np.isnan(value)):
            return pd.NaT
        ts = pd.Timestamp(value)
        if pd.isna(ts):
            return pd.NaT
        if ts.tzinfo is not None:
            ts = ts.tz_convert("Asia/Tokyo").tz_localize(None)
        return ts
    except Exception:
        return pd.NaT


def _finance_codes_stale_after_latest_earnings(conn: sqlite3.Connection) -> set[str]:
    """P1-608: finance_notes更新後に新しい決算イベントが来た銘柄を返す。

    fetch_allは各runでearnings_eventsを更新する一方、株探ファンダ全件更新は日次1回。
    朝のfinance_notes更新後に昼/引け後の決算・業績予想イベントが追加された場合、
    旧score/進捗/forecastをcurrent入力として再利用しないための鮮度ゲート。
    """
    try:
        _tables = {r[0] for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()}
        if "earnings_events" not in _tables or "finance_notes" not in _tables:
            return set()

        _ecols = {r[1] for r in conn.execute("PRAGMA table_info(earnings_events)").fetchall()}
        _fcols = {r[1] for r in conn.execute("PRAGMA table_info(finance_notes)").fetchall()}
        if "コード" not in _ecols or not ({"発表日時", "提出時刻"} & _ecols) or "コード" not in _fcols:
            return set()

        # P1-612: 実績決算は「finance_notesを今更新した」だけでfreshとしない。
        # TDnet直後は株探financeページの反映が遅れ、旧四半期を再取得してupdated_atだけ
        # 新しくなることがある。タイトルも保持し、実績決算かどうかを後段で判定する。
        _title_expr = 'タイトル' if 'タイトル' in _ecols else "'' AS タイトル"
        # P1-621: 同じearnings_eventsの正式読込は COALESCE(発表日時,提出時刻) なのに、
        # freshness gateだけ提出時刻固定だと旧DB/旧producerの発表日時-only行を見逃す。
        # 空文字もNULL相当に扱い、発表日時を優先して無ければ提出時刻へfallbackする。
        if "発表日時" in _ecols and "提出時刻" in _ecols:
            _event_ts_expr = "COALESCE(NULLIF(TRIM(発表日時),''), NULLIF(TRIM(提出時刻),''))"
        elif "発表日時" in _ecols:
            _event_ts_expr = "NULLIF(TRIM(発表日時),'')"
        else:
            _event_ts_expr = "NULLIF(TRIM(提出時刻),'')"
        _ev = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, {_event_ts_expr} AS _event_time, {_title_expr} FROM earnings_events "
            f"WHERE {_event_ts_expr} IS NOT NULL", conn
        )
        if _ev.empty:
            return set()
        _ev["_key"] = _ev["コード"].map(canonical_code_for_db)
        _ev["_event_ts"] = _ev["_event_time"].map(_p1_608_jst_naive_ts)
        _ev = _ev[
            _ev["_key"].astype(str).str.len().gt(0) & _ev["_event_ts"].notna()
        ].copy()
        if _ev.empty:
            return set()
        # P1-614: 「最新イベント」と「最新の実績決算イベント」は別々に保持する。
        # 同日、決算短信の直後に通期業績予想修正などが出ると、最新1件だけを見る方式では
        # 後者が前者を隠し、四半期実績のsemantic freshness gateを回避できる。
        # P1-615: 「決算」という部分一致だけでは「決算期変更」等まで実績決算扱いする。
        # semantic gateはTDnetの実績本体を強く示す「決算短信」に限定する。
        _ev["_is_actual_result"] = _ev["タイトル"].map(
            lambda _x: "決算短信" in str(_x or "")
        )
        _ev_sorted = _ev.sort_values(["_key", "_event_ts", "_rowid"], kind="stable")
        _ev_latest = _ev_sorted.drop_duplicates("_key", keep="last")
        _event_map = {r["_key"]: r["_event_ts"] for _, r in _ev_latest.iterrows()}
        _ev_actual = _ev_sorted[_ev_sorted["_is_actual_result"]].drop_duplicates("_key", keep="last")
        _actual_event_map = {r["_key"]: r["_event_ts"] for _, r in _ev_actual.iterrows()}

        # current producer（株探ファンダv14+）が保存する実績四半期履歴。
        # テーブルが存在する環境だけsemantic freshness gateを有効化し、旧DB互換は維持する。
        _qh_available = False
        _actual_announce = {}
        if "quarterly_actual_history" in _tables:
            _qcols = {r[1] for r in conn.execute("PRAGMA table_info(quarterly_actual_history)").fetchall()}
            if {"コード", "announcement_date"}.issubset(_qcols):
                _qh_available = True
                # P1-629: P1-625の季節進捗本体と同じく、値/発表日の有無より先に
                # logical Qのauthoritative rowを確定する。旧aliasの誤った新しい発表日で
                # finance freshnessを「追いついた」と誤認しない。
                _qh_fk_expr = "fiscal_key" if "fiscal_key" in _qcols else "NULL AS fiscal_key"
                _qh_qn_expr = "quarter_no" if "quarter_no" in _qcols else "NULL AS quarter_no"
                _qh_up_expr = "updated_at" if "updated_at" in _qcols else "NULL AS updated_at"
                _qh = pd.read_sql_query(
                    f"SELECT rowid AS _rowid, コード, announcement_date, "
                    f"{_qh_fk_expr}, {_qh_qn_expr}, {_qh_up_expr} "
                    "FROM quarterly_actual_history", conn
                )
                if not _qh.empty:
                    _qh["_raw_code"] = _qh["コード"].astype(str).str.strip()
                    _qh["_key"] = _qh["コード"].map(canonical_code_for_db)
                    _qh["_announce_ts"] = _qh["announcement_date"].map(_p1_608_jst_naive_ts)
                    # P1-651: P1-650のauthoritative時刻統一をfinance freshness gateにも適用。
                    # quarterly_actual_history.updated_atのaware/naive混在で正しいsnapshotをFATALにしない。
                    _qh["_updated_ts"] = _qh.get("updated_at").map(_p1_608_jst_naive_ts)
                    _qh["_canon_match"] = (
                        _qh["_raw_code"].str.upper() == _qh["_key"].astype(str).str.upper()
                    ).astype(int)
                    _qh["quarter_no"] = pd.to_numeric(_qh.get("quarter_no"), errors="coerce")
                    _qh["fiscal_key"] = _qh.get("fiscal_key").fillna("").astype(str).str.strip()
                    _qh = _qh[_qh["_key"].astype(str).str.len().gt(0)].copy()
                    if not _qh.empty:
                        _has_logical_q = (
                            _qh["fiscal_key"].ne("") & _qh["quarter_no"].notna()
                        )
                        _qh_q = _qh[_has_logical_q].copy()
                        _qh_other = _qh[~_has_logical_q].copy()
                        if not _qh_q.empty:
                            _qh_q = (
                                _qh_q.sort_values(
                                    ["_key", "fiscal_key", "quarter_no", "_updated_ts",
                                     "_canon_match", "_announce_ts", "_rowid"],
                                    kind="stable", na_position="first"
                                )
                                .drop_duplicates(["_key", "fiscal_key", "quarter_no"], keep="last")
                            )
                        # 旧schema等でQキーが無い行は従来互換として残すが、future発表日は下で除外。
                        _qh = pd.concat([_qh_q, _qh_other], ignore_index=True)
                        _today_fin_fresh = pd.Timestamp(date.fromisoformat(_today_jst()))
                        _qh = _qh[
                            _qh["_announce_ts"].notna()
                            & (_qh["_announce_ts"].dt.normalize() <= _today_fin_fresh)
                        ].copy()
                        if not _qh.empty:
                            _qh = (
                                _qh.sort_values(
                                    ["_key", "_announce_ts", "_updated_ts", "_canon_match", "_rowid"],
                                    kind="stable", na_position="first"
                                )
                                .drop_duplicates("_key", keep="last")
                            )
                            _actual_announce = {
                                r["_key"]: r["_announce_ts"] for _, r in _qh.iterrows()
                            }

        _updated_expr = "updated_at" if "updated_at" in _fcols else "NULL AS updated_at"
        _fn = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, {_updated_expr} FROM finance_notes", conn
        )
        _fin_updated = {}
        if not _fn.empty:
            _fn = _latest_finance_notes_by_canonical(_fn, "_key")
            for _, _r in _fn.iterrows():
                _fin_updated[_r["_key"]] = _p1_608_jst_naive_ts(_r.get("updated_at"))

        _stale = set()
        for _key, _event_ts in _event_map.items():
            _fin_ts = _fin_updated.get(_key, pd.NaT)
            if pd.isna(_fin_ts) or _event_ts > _fin_ts:
                _stale.add(_key)
                continue

            # P1-612/P1-614: 最新イベントが予想修正でも、それより前に同日/直近の実績決算が
            # あれば、その最新実績決算に対するquarterly history到達も独立して必須化する。
            if _qh_available and _key in _actual_event_map:
                _actual_event_ts = _actual_event_map[_key]
                _ann_ts = _actual_announce.get(_key, pd.NaT)
                if pd.isna(_ann_ts) or pd.Timestamp(_ann_ts).date() < pd.Timestamp(_actual_event_ts).date():
                    _stale.add(_key)
        return _stale
    except Exception as _e:
        # P1-613: 鮮度判定不能を「stale 0件」として続行すると、そのrunだけ旧財務を
        # currentとして再公開できる。必要テーブル不存在は上で互換分岐済みなので、
        # ここへ来るDB読込/parse障害はfail-closedで上位へ返す。
        print(f"[finance-fresh][ERROR] latest earnings freshness check failed: {_e}", flush=True)
        raise RuntimeError("finance freshness check failed; refusing to treat cached finance as current") from _e


def _dedupe_price_history_df(df: pd.DataFrame) -> pd.DataFrame:
    """P1-276: price_history の論理同一銘柄・同一日重複を決定的に1行へ畳む。

    過去DBに 7203 / 7203.0、285A / 285a が同日に共存していても、
    MA・RS・ATR・前日終値などで1日を2本として数えない。
    候補が複数なら OHLCV の非NULL数が多い行を優先し、同点ならcanonical表記、
    さらに同点ならrowidの大きい行を採用する。
    P1-454: legacy土日/祝日足も同時に除外する。
    """
    if df is None or getattr(df, 'empty', True):
        return df.copy() if hasattr(df, 'copy') else pd.DataFrame()
    if 'コード' not in df.columns or '日付' not in df.columns:
        return df.copy()
    x = df.copy()
    x['_code_key'] = x['コード'].map(canonical_code_for_db)
    x['_date_key'] = pd.to_datetime(x['日付'], errors='coerce').dt.normalize()
    x = x[x['_code_key'].astype(str).str.len().gt(0) & x['_date_key'].notna()].copy()
    # P1-454: P1-158以前に残った土日/祝日足も論理履歴から除外する。
    # 単なるfuture cutoffだけでは、月曜朝に「金曜実足 + 土曜legacy足」の土曜が最新になり得る。
    if not x.empty:
        _extra_ph_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
        _unique_ph_dates = pd.Series(x['_date_key'].dt.date.unique()).dropna().tolist()
        _valid_ph_dates = {
            _d for _d in _unique_ph_dates
            if not is_jp_market_holiday(_d, _extra_ph_closed)
        }
        x = x[x['_date_key'].dt.date.isin(_valid_ph_dates)].copy()
    if x.empty:
        return x.drop(columns=[c for c in ('_code_key','_date_key') if c in x.columns], errors='ignore')
    rawu = x['コード'].astype(str).str.strip().str.upper()
    x['_canon_match'] = rawu.eq(x['_code_key'].astype(str).str.upper()).astype(int)
    qcols = [c for c in ('始値','高値','安値','終値','出来高') if c in x.columns]
    x['_quality'] = x[qcols].notna().sum(axis=1) if qcols else 0
    if '_rowid' in x.columns:
        x['_rid_sort'] = pd.to_numeric(x['_rowid'], errors='coerce').fillna(-1)
    else:
        x['_rid_sort'] = range(len(x))
    x = x.sort_values(
        ['_code_key','_date_key','_quality','_canon_match','_rid_sort'],
        ascending=[True,True,True,True,True],
        kind='mergesort'
    ).drop_duplicates(subset=['_code_key','_date_key'], keep='last')
    x['コード'] = x['_code_key']
    # 日付は元の表示形式を保ちつつ、並びは論理日付で決定する。
    x = x.sort_values(['_code_key','_date_key'], kind='mergesort')
    return x.drop(columns=['_code_key','_date_key','_canon_match','_quality','_rid_sort'], errors='ignore').reset_index(drop=True)


def _delete_legacy_price_alias_rows(conn: sqlite3.Connection, code, date_value) -> None:
    """P1-290: canonical行を書き込む直前に同日legacy aliasだけ削除する。"""
    c = canonical_code_for_db(code)
    if not c:
        return
    legacy = [v for v in code_query_variants(code) if str(v) != str(c)]
    if not legacy:
        return
    ph = ','.join('?' * len(legacy))
    conn.execute(
        f"DELETE FROM price_history WHERE CAST(コード AS TEXT) IN ({ph}) AND 日付=?",
        tuple(legacy) + (str(date_value)[:10],)
    )


def _filter_price_history_write_rows(rows, *, date_index: int = 1):
    """P1-462: price_history書込み直前にJPX営業日だけを許可する。

    過去バグ由来の休日/未来足は読み取り側でも除外しているが、今後DBへ
    新規混入させないため全writer共通の入口でも検証する。
    """
    rows = list(rows or [])
    if not rows:
        return []
    cutoff = _expected_jpx_asof_date()
    extra = _load_extra_closed(EXTRA_CLOSED_PATH)
    valid_cache = {}
    out = []
    dropped = 0
    for row in rows:
        try:
            ds = str(row[date_index])[:10]
            d = date.fromisoformat(ds)
        except Exception:
            dropped += 1
            continue
        ok = valid_cache.get(d)
        if ok is None:
            ok = (d <= cutoff) and (not is_jp_market_holiday(d, extra))
            valid_cache[d] = ok
        if ok:
            out.append(row)
        else:
            dropped += 1
    if dropped:
        print(f"[price_history][GUARD] rejected non-JPX/future rows={dropped}")
    return out


def _atomic_write_price_history_rows(conn: sqlite3.Connection, rows, *, close_only: bool = False, intraday_merge: bool = False) -> int:
    """P1-352〜355: alias削除と履歴upsertを同一SAVEPOINTで行う。
    P1-528: intraday_merge=Trueでは同日高安/累積出来高を後続の部分応答で巻き戻さない。
    """
    rows = _filter_price_history_write_rows(rows, date_index=1)
    if not rows:
        return 0
    sp = f"ph_rows_{time.time_ns()}"
    cur = conn.cursor()
    try:
        conn.execute(f"SAVEPOINT {sp}")
        if close_only:
            norm_rows = []
            for code, d, close in rows:
                code = canonical_code_for_db(code)
                if not code:
                    continue
                _delete_legacy_price_alias_rows(conn, code, d)
                norm_rows.append((code, str(d)[:10], close))
            cur.executemany(
                """INSERT INTO price_history(コード,日付,終値) VALUES(?,?,?)
                   ON CONFLICT(コード,日付) DO UPDATE SET
                     終値=COALESCE(excluded.終値, 終値)""",
                norm_rows
            )
        else:
            norm_rows = []
            for code, d, o, h, l, c, v in rows:
                code = canonical_code_for_db(code)
                if not code:
                    continue
                _delete_legacy_price_alias_rows(conn, code, d)
                norm_rows.append((code, str(d)[:10], o, h, l, c, v))
            if intraday_merge:
                cur.executemany(
                    """INSERT INTO price_history(コード,日付,始値,高値,安値,終値,出来高)
                       VALUES(?,?,?,?,?,?,?)
                       ON CONFLICT(コード,日付) DO UPDATE SET
                         始値=COALESCE(始値, excluded.始値),
                         高値=CASE
                           WHEN 高値 IS NULL THEN excluded.高値
                           WHEN excluded.高値 IS NULL THEN 高値
                           ELSE MAX(高値, excluded.高値) END,
                         安値=CASE
                           WHEN 安値 IS NULL THEN excluded.安値
                           WHEN excluded.安値 IS NULL THEN 安値
                           ELSE MIN(安値, excluded.安値) END,
                         終値=COALESCE(excluded.終値, 終値),
                         出来高=CASE
                           WHEN 出来高 IS NULL THEN excluded.出来高
                           WHEN excluded.出来高 IS NULL THEN 出来高
                           ELSE MAX(出来高, excluded.出来高) END""",
                    norm_rows
                )
            else:
                cur.executemany(
                    """INSERT INTO price_history(コード,日付,始値,高値,安値,終値,出来高)
                       VALUES(?,?,?,?,?,?,?)
                       ON CONFLICT(コード,日付) DO UPDATE SET
                         始値=COALESCE(excluded.始値, 始値),
                         高値=COALESCE(excluded.高値, 高値),
                         安値=COALESCE(excluded.安値, 安値),
                         終値=COALESCE(excluded.終値, 終値),
                         出来高=COALESCE(excluded.出来高, 出来高)""",
                    norm_rows
                )
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        return len(norm_rows)
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _market_for_code_variants(conn: sqlite3.Connection, code) -> str | None:
    """P1-486: legacy aliasを含むscreenerから市場を決定的に選ぶ。"""
    if conn is None:
        return None
    variants = code_query_variants(code)
    if not variants:
        return None
    ph = ",".join("?" * len(variants))
    rows = conn.execute(
        f"SELECT CAST(コード AS TEXT), 市場 FROM screener WHERE CAST(コード AS TEXT) IN ({ph})",
        tuple(variants),
    ).fetchall()
    if not rows:
        return None
    by_code = {}
    for raw_code, market in rows:
        key = str(raw_code)
        # 同じraw keyが複数残っても、空市場より非空市場を優先。
        if key not in by_code or (not by_code[key] and market):
            by_code[key] = market
    for v in variants:
        market = by_code.get(str(v))
        if market is not None and str(market).strip():
            return market
    return None


def _yfinance_download_repaired(*args, **kwargs):
    """P2-14/P2-55: repair=Trueを既定化。未導入/旧版を明示的に扱う。"""
    if yfinance is None:
        raise RuntimeError("yfinance is required for Yahoo price-history fallback; install yfinance before this phase")
    if "repair" not in kwargs:
        kwargs["repair"] = True
    try:
        return yfinance.download(*args, **kwargs)
    except TypeError as e:
        # 古いyfinanceだけrepair引数が未対応。別のTypeErrorまで握り潰さない。
        if "repair" not in str(e).lower():
            raise
        legacy_kwargs = dict(kwargs)
        legacy_kwargs.pop("repair", None)
        return yfinance.download(*args, **legacy_kwargs)


def resolve_yahoo_symbol(code: str,
                         conn: Optional[sqlite3.Connection] = None,
                         try_online: bool = False) -> str:
    """
    screenerの 'コード' を Yahoo Finance 問い合わせ用シンボルに解決する。

    優先順位:
      1) DB上書きテーブル yahoo_symbol_override に一致があればそれを最優先で採用
      2) 規則で解決
         - '^'で始まる指数 or 既に'.T'付き   → そのまま
         - 4桁の純数字（例: 7203）          → 'XXXX.T'
         - 3桁数字+英字/4桁英数字ミックス   → 候補 ['CODE.T','CODE'] を順に採用
           （try_online=Trueならyfinanceで実在チェック、Falseなら'.T'優先）
         - それ以外                           → そのまま

    注意:
      - price_history へ保存する「コード」は問い合わせシンボルではなく
        *元の code（screenerのコード文字列）* を使ってください（JOINのキー保持）。
    """
    raw = "" if code is None else str(code).strip()
    if not raw:
        return raw
    RAWU = raw.upper()

    # 指数・Yahoo明示サフィックスは問い合わせシンボルなので破壊しない。
    if RAWU in _YAHOO_INDEX_SYMBOLS:
        return _YAHOO_INDEX_SYMBOLS[RAWU]
    if RAWU.startswith('^') or re.search(r"\.(?:T|N|S|F)$", RAWU):
        return RAWU

    # P1-164: bare code はDB共通規則へ。7203.0→7203、285a→285A。
    s = canonical_code_for_db(raw)
    if not s:
        return s
    S = s.upper()

    # 1) 明示オーバーライド（最優先）
    if conn is not None:
        _ov = _read_yahoo_override(conn, s)
        # P1-506: NONE/HISTORY_NONEは状態フラグでありYahoo symbolではない。
        # full symbol overrideだけをそのまま採用し、sentinelは通常の市場解決へフォールスルーする。
        if _ov and str(_ov).strip().upper() not in {"NONE", "HISTORY_NONE"}:
            return str(_ov).strip().upper()
            
    # 2) 規則解決
    # 2-2) 4桁の純数字
    # 2-2) 4桁の純数字
    if len(S) == 4 and S.isdigit():
        # screener から市場情報を拾う（あれば）
        market = None
        if conn is not None:
            try:
                # P1-486: alias複数行が残っていても無順序LIMIT 1で市場を選ばない。
                market = _market_for_code_variants(conn, s)
            except Exception:
                market = None

        # 市場＋override を使ってサフィックス決定
        # （他の処理と同じく _resolve_yahoo_suffix / _market_to_yahoo_suffix を使う）
        suffix = ".T"
        try:
            # _resolve_yahoo_suffix は内部で override テーブルも見てくれる
            suffix = _resolve_yahoo_suffix(s, market)
        except Exception:
            try:
                suffix = _market_to_yahoo_suffix(market)
            except Exception:
                suffix = ".T"

        symbol_default = s + suffix   # 例: 1449 + ".S" → "1449.S"

        if try_online:
            try:
                # 優先: 市場ベースの symbol_default → フォールバック: ".T" / 生
                for cand in (symbol_default, s + ".T", s):
                    _df = _yfinance_download_repaired(
                        cand,
                        period="5d",
                        interval="1d",
                        progress=False,
                        threads=False,
                    )
                    if _df is not None and not _df.empty:
                        return cand
            except Exception:
                pass

        # オフライン or 取れなかった場合は市場ベースのシンボルを返す
        return symbol_default


    # 2-3) 3桁数字+英字 or 4桁英数字ミックス
    pat_3d1a    = re.compile(r"^\d{3}[A-Z]$")   # 例: 130A
    pat_4_alnum = re.compile(r"^[A-Z0-9]{4}$")  # 例: 1A3B, A130, 13AB など

    if pat_3d1a.match(S) or (len(S) == 4 and pat_4_alnum.match(S) and not S.isdigit()):
        # P1-331: 英数字コードも4桁数字と同じく市場列からYahoo suffixを解決する。
        # 旧版は285A等だけ常に.T固定で、名証/札証/福証の英数字銘柄を誤照会していた。
        market = None
        if conn is not None:
            try:
                market = _market_for_code_variants(conn, s)
            except Exception:
                market = None

        try:
            suffix = _resolve_yahoo_suffix(s, market)
        except Exception:
            try:
                suffix = _market_to_yahoo_suffix(market)
            except Exception:
                suffix = ".T"
        symbol_default = S + suffix

        if try_online:
            try:
                candidates = [symbol_default]
                if symbol_default != S + ".T":
                    candidates.append(S + ".T")
                candidates.append(S)
                for cand in candidates:
                    df = _yfinance_download_repaired(cand, period="5d", interval="1d", progress=False, threads=False)
                    if df is not None and not df.empty:
                        return cand
            except Exception:
                pass
        return symbol_default

    # 2-4) それ以外 → canonical form
    return S

def resolve_yahoo_symbols_bulk(codes, conn: Optional[sqlite3.Connection]) -> list[str]:
    """P2-25: 複数銘柄のYahoo symbolをDB一括読込で解決する。

    scalar resolve_yahoo_symbol() と同じ優先順位を保ちつつ、main override / 市場 /
    legacy suffix overrideを銘柄ごとにSELECTしない。try_online相当のprobeは行わない。
    """
    raws = ["" if c is None else str(c).strip() for c in (codes or [])]
    if not raws:
        return []
    if conn is None:
        return [resolve_yahoo_symbol(c, None, False) for c in raws]

    canonical = []
    for raw in raws:
        u = raw.upper()
        if u in {'^TOPIX', 'TOPIX', '998405.T', '^TOPX'}:
            canonical.append('^TOPX')
        elif u.startswith('^') or re.search(r"\.(?:T|N|S|F)$", u):
            canonical.append(u)
        else:
            canonical.append(canonical_code_for_db(raw))

    lookup_codes = list(dict.fromkeys(
        c for raw, c in zip(raws, canonical)
        if c and not str(c).startswith('^') and not re.search(r"\.(?:T|N|S|F)$", raw.upper())
    ))
    all_variants = expand_code_query_variants(lookup_codes)

    # main yahoo_symbol_override
    _ensure_override_table(conn)
    ov_by_raw = {}
    market_by_raw = {}
    for _i in range(0, len(all_variants), 500):
        _part = all_variants[_i:_i + 500]
        if not _part:
            continue
        _ph = ",".join("?" * len(_part))
        for _rc, _sym in conn.execute(
            f"SELECT CAST(コード AS TEXT), 問い合わせシンボル FROM yahoo_symbol_override "
            f"WHERE CAST(コード AS TEXT) IN ({_ph})", tuple(_part)
        ).fetchall():
            ov_by_raw[str(_rc)] = _sym
        for _rc, _mkt in conn.execute(
            f"SELECT CAST(コード AS TEXT), 市場 FROM screener "
            f"WHERE CAST(コード AS TEXT) IN ({_ph})", tuple(_part)
        ).fetchall():
            _key = str(_rc)
            if _key not in market_by_raw or (not market_by_raw[_key] and _mkt):
                market_by_raw[_key] = _mkt

    # legacy suffix tables: P2-24/P2-59 schema-aware + alias-aware探索。
    legacy_suffix = {}  # logical code -> normalized suffix
    table_candidates = [
        "symbol_overrides", "overrides_symbol", "overrides", "yahoo_overrides",
        "symbol_meta", "symbols_override",
    ]
    code_cols = ["code", "symbol", "ticker", "stock_code", "銘柄コード"]
    suffix_cols = ["yahoo_suffix", "yahoo_market_suffix", "yahoo_market", "yahoo_sfx"]
    try:
        existing_tables = {
            str(r[0]) for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall() if r and r[0]
        }
    except Exception as _e:
        # P2-38: schema読込失敗を「legacy override table無し」と誤認しない。
        raise RuntimeError(f"Yahoo legacy-override schema lookup failed: {_e}") from _e
    legacy_targets = [c for c in lookup_codes if re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", c or "")]
    for t in table_candidates:
        if t not in existing_tables:
            continue
        try:
            cols = {str(r[1]) for r in conn.execute(f'PRAGMA table_info("{t}")').fetchall()}
        except Exception as _e:
            raise RuntimeError(f"Yahoo legacy-override schema read failed for {t}: {_e}") from _e
        actual_code_cols = [c for c in code_cols if c in cols]
        actual_suffix_cols = [c for c in suffix_cols if c in cols]
        for c_code in actual_code_cols:
            for c_sfx in actual_suffix_cols:
                unresolved = [c for c in legacy_targets if c not in legacy_suffix]
                if not unresolved:
                    break
                # P2-59: logical codeごとにcanonical→legacy alias順で選ぶ。
                _vars_by_logical = {c: code_query_variants(c) for c in unresolved}
                _all_legacy_vars = list(dict.fromkeys(
                    v for c in unresolved for v in _vars_by_logical[c] if v
                ))
                _rows_by_raw = {}
                for _i in range(0, len(_all_legacy_vars), 500):
                    _part = _all_legacy_vars[_i:_i + 500]
                    if not _part:
                        continue
                    _ph = ",".join("?" * len(_part))
                    try:
                        _rows = conn.execute(
                            f'SELECT CAST("{c_code}" AS TEXT), "{c_sfx}" FROM "{t}" '
                            f'WHERE CAST("{c_code}" AS TEXT) IN ({_ph})', tuple(_part)
                        ).fetchall()
                    except Exception as _e:
                        # P2-73: 実在schemaへのSELECT失敗は「override無し」ではない。
                        # fallback symbolで別市場を照会するより、このbatchを明示失敗させる。
                        raise RuntimeError(
                            f"Yahoo legacy-override read failed: table={t} code_col={c_code} suffix_col={c_sfx}: {_e}"
                        ) from _e
                    for _rc, _sfx in _rows:
                        _rows_by_raw[str(_rc)] = _sfx
                for _logical in unresolved:
                    for _v in _vars_by_logical[_logical]:
                        if str(_v) not in _rows_by_raw:
                            continue
                        _norm = _normalize_yahoo_suffix_token(_rows_by_raw[str(_v)])
                        if _norm:
                            legacy_suffix[_logical] = _norm
                            break

    out = []
    for raw, c in zip(raws, canonical):
        if not raw:
            out.append(raw)
            continue
        u = raw.upper()
        if u in _YAHOO_INDEX_SYMBOLS:
            out.append(_YAHOO_INDEX_SYMBOLS[u])
            continue
        if u.startswith('^') or re.search(r"\.(?:T|N|S|F)$", u):
            out.append(u)
            continue
        if not c:
            out.append(c)
            continue
        variants = code_query_variants(c)
        ov = None
        market = None
        for v in variants:
            if ov is None and str(v) in ov_by_raw:
                ov = ov_by_raw[str(v)]
            if market is None:
                m = market_by_raw.get(str(v))
                if m is not None and str(m).strip():
                    market = m
            if ov is not None and market is not None:
                break
        if ov and str(ov).strip().upper() not in {"NONE", "HISTORY_NONE"}:
            out.append(str(ov).strip().upper())
            continue
        C = str(c).upper()
        if (len(C) == 4 and C.isdigit()) or re.fullmatch(r"\d{3}[A-Z]", C) or (
            len(C) == 4 and re.fullmatch(r"[A-Z0-9]{4}", C) and not C.isdigit()
        ):
            suffix = legacy_suffix.get(C) or _market_to_yahoo_suffix(market)
            out.append(C + suffix)
        else:
            out.append(C)

    # P2-28: 異なるlogical codeが同じYahoo symbolへ解決されたら、bulk応答を別銘柄へ
    # 誤配布し得る。alias同一銘柄は許可し、別銘柄collisionだけfail-fastする。
    _owners = {}
    _symbol_by_code = {}
    for _raw, _resolved in zip(raws, out):
        _ck = canonical_code_for_db(_raw)
        _sym = str(_resolved or "").strip().upper()
        if not _ck or not _sym:
            continue
        _prev = _owners.get(_sym)
        if _prev is not None and _prev != _ck:
            raise RuntimeError(
                f"Yahoo symbol collision: {_sym} resolved for both {_prev} and {_ck}"
            )
        _prev_sym = _symbol_by_code.get(_ck)
        if _prev_sym is not None and _prev_sym != _sym:
            # P2-58: 逆方向も1対1。7203.T/7203.N等が同じlogical codeへ混在すると、
            # 後段で同じDB行へ別市場データを二重反映し得るためfail-fastする。
            raise RuntimeError(
                f"Yahoo code collision: {_ck} resolved to both {_prev_sym} and {_sym}"
            )
        _owners[_sym] = _ck
        _symbol_by_code[_ck] = _sym
    return out


def _ensure_latest_prices_code_col(conn):
    cur = conn.cursor()
    _sp = "p2_64_latest_prices_code_col"
    try:
        cur.execute(f"SAVEPOINT {_sp}")
        cols = [r[1] for r in cur.execute("PRAGMA table_info(latest_prices)")]
        if "コード" not in cols:
            cur.execute("ALTER TABLE latest_prices ADD COLUMN コード TEXT")
        cols = [r[1] for r in cur.execute("PRAGMA table_info(latest_prices)")]
        if "code" in cols:
            cur.execute("UPDATE latest_prices SET コード = CAST(code AS TEXT) WHERE コード IS NULL OR コード = ''")
        cur.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception as e:
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            cur.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        print("[ERROR] _ensure_latest_prices_code_col failed:", e)
        raise RuntimeError("latest_prices code-column ensure failed") from e
    finally:
        cur.close()
            
def _ensure_latest_prices_index_rows(conn):
    """screenerの論理銘柄でlatest_prices未登録行を原子的に補完する。"""
    cur = conn.cursor()
    try:
        # P1-429/P1-580: placeholderもrun modeのsnapshot日。
        # PREOPENに当日の日付だけ先行させず、前営業日の確定snapshotへ合わせる。
        today_jst = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        _have = {
            canonical_code_for_db(r[0])
            for r in cur.execute("SELECT コード FROM latest_prices").fetchall()
            if canonical_code_for_db(r[0])
        }
        _to_insert = []
        for _raw_code, _upd in cur.execute("SELECT コード, 更新日 FROM screener").fetchall():
            _key = canonical_code_for_db(_raw_code)
            if not _key or _key in _have:
                continue
            _to_insert.append((_raw_code, _upd or today_jst))
            _have.add(_key)
        if not _to_insert:
            return

        # P1-425: autocommit接続で補完途中だけ入るのを防ぐ。
        _sp = f"sp_latest_index_rows_{time.time_ns()}"
        conn.execute(f"SAVEPOINT {_sp}")
        try:
            cur.executemany("INSERT INTO latest_prices(コード, 日付) VALUES(?,?)", _to_insert)
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
            raise
    except Exception as e:
        print("[ERROR] _ensure_latest_prices_index_rows failed:", e)
        raise RuntimeError("latest_prices row ensure failed") from e
    finally:
        cur.close()

def setup_database_indexes(conn: sqlite3.Connection) -> None:
    """テーブル構築後、または初期化フェーズで明示的に呼び出すインデックス作成関数"""
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_ph_code_date ON price_history(コード, 日付);")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_signals_code_date ON signals_log(コード, 日時);")
    except sqlite3.OperationalError as e:
        print(f"[DB Setup] インデックス作成をスキップしました: {e}")

def ensure_runlog_schema(conn):
    """日次実行ログテーブルの作成と、システム全体のインデックス最適化を担う"""
    conn.execute("""
    CREATE TABLE IF NOT EXISTS run_log (
      phase       TEXT NOT NULL,
      run_date    TEXT NOT NULL,   -- 'YYYY-MM-DD' JST
      status      TEXT,            -- 'ok' | 'error' | 'running'
      started_at  TEXT,
      finished_at TEXT,
      info_json   TEXT,
      build_token TEXT,
      PRIMARY KEY (phase, run_date)
    );
    """)
    # P1-489: 旧DBのrun_logにもbuild識別子列を後方互換で追加する。
    try:
        _runlog_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(run_log)").fetchall()}
        if "build_token" not in _runlog_cols:
            conn.execute("ALTER TABLE run_log ADD COLUMN build_token TEXT")
    except Exception as _e:
        raise RuntimeError(f"run_log build_token schema migration failed: {_e}") from _e
    conn.commit()
    # テーブル作成後に安全にインデックスを構築
    setup_database_indexes(conn)

try:
    __JST = ZoneInfo("Asia/Tokyo")
except Exception:
    __JST = None

def _now_jst():
    # P1-121: 時刻ゲートもJST固定。ZoneInfoが使えなくてもUTC+9へフォールバックする。
    return datetime.now(__JST) if __JST else datetime.now(timezone(timedelta(hours=9)))

def _today_jst():
    return _now_jst().strftime("%Y-%m-%d")

__DAILY_BUILD_TOKEN = None

def _daily_build_token() -> str:
    """P1-488: 同日中のスクリプト差替えを日次markerが隠さないためのbuild識別子。"""
    global __DAILY_BUILD_TOKEN
    if __DAILY_BUILD_TOKEN:
        return __DAILY_BUILD_TOKEN
    try:
        import hashlib
        with open(os.path.abspath(__file__), "rb") as _f:
            __DAILY_BUILD_TOKEN = hashlib.sha256(_f.read()).hexdigest()[:16]
    except Exception:
        try:
            _st = os.stat(os.path.abspath(__file__))
            __DAILY_BUILD_TOKEN = f"{int(_st.st_size):x}-{int(getattr(_st, 'st_mtime_ns', int(_st.st_mtime*1e9))):x}"
        except Exception:
            # buildを識別できない時に古いmarkerを信用するより、毎回再実行側へ倒す。
            __DAILY_BUILD_TOKEN = f"unknown-{os.getpid()}"
    return __DAILY_BUILD_TOKEN

# P1-499: build tokenは最初のdaily marker参照時ではなく、module読込時点で固定する。
# 実行中に.py自体が差し替えられても「旧コード実行中なのに新buildのmarker」を残さない。
__DAILY_BUILD_TOKEN = _daily_build_token()

def _daily_marker_payload() -> str:
    return f"{_today_jst()}|{_daily_build_token()}"




def _daily_marker_matches_today(name: str) -> bool:
    """日次markerがJST当日成功済みかを副作用なく確認する。"""
    marker_path = os.path.join(OUTPUT_DIR, f"last_{name}.txt")
    try:
        if not os.path.exists(marker_path):
            return False
        with open(marker_path, "r", encoding="utf-8") as f:
            # P1-488: 旧date-only markerや別buildのmarkerは成功済みとみなさない。
            return f.read().strip() == _daily_marker_payload()
    except Exception:
        return False




def _boot_token() -> str:
    """再起動を跨いだ残骸lockを即判定するためのboot識別子。"""
    try:
        if os.name == "nt":
            import ctypes
            uptime_ms = int(ctypes.windll.kernel32.GetTickCount64())
            boot_epoch_min = int((time.time() - uptime_ms / 1000.0) // 60)
            return f"win:{boot_epoch_min}"
        _bp = Path("/proc/sys/kernel/random/boot_id")
        if _bp.exists():
            return "linux:" + _bp.read_text(encoding="utf-8").strip()
    except Exception:
        pass
    return "unknown"


def _pid_is_alive(pid) -> bool:
    try:
        pid = int(pid)
    except Exception:
        return False
    if pid <= 0:
        return False
    if pid == os.getpid():
        return True
    try:
        if os.name == "nt":
            import ctypes
            PROCESS_QUERY_LIMITED_INFORMATION = 0x1000
            h = ctypes.windll.kernel32.OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, False, pid)
            if h:
                ctypes.windll.kernel32.CloseHandle(h)
                return True
            return False
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except Exception:
        return False


def _text_lock_owner_is_stale(lock_path: str) -> bool:
    try:
        txt = Path(lock_path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    _pm = re.search(r"\bpid=(\d+)\b", txt)
    _bm = re.search(r"\bboot=([^\s]+)", txt)
    if _bm:
        old_boot = _bm.group(1)
        cur_boot = _boot_token()
        if old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
            return True
    return bool(_pm and not _pid_is_alive(_pm.group(1)))


def _json_lock_owner_is_stale(lock_path: Path) -> bool:
    try:
        data = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    old_boot = str(data.get("boot") or "")
    cur_boot = _boot_token()
    if old_boot and old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
        return True
    pids = [data.get("pid"), data.get("child_pid")]
    known = [pid for pid in pids if pid is not None]
    return bool(known) and not any(_pid_is_alive(pid) for pid in known)


def _text_lock_owner_is_alive(lock_path: str) -> bool:
    try:
        txt = Path(lock_path).read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    _pm = re.search(r"\bpid=(\d+)\b", txt)
    _bm = re.search(r"\bboot=([^\s]+)", txt)
    if _bm:
        old_boot = _bm.group(1)
        cur_boot = _boot_token()
        if old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
            return False
    return bool(_pm and _pid_is_alive(_pm.group(1)))


def _json_lock_owner_is_alive(lock_path: Path) -> bool:
    try:
        data = json.loads(lock_path.read_text(encoding="utf-8"))
    except Exception:
        return False
    old_boot = str(data.get("boot") or "")
    cur_boot = _boot_token()
    if old_boot and old_boot != "unknown" and cur_boot != "unknown" and old_boot != cur_boot:
        return False
    pids = [data.get("pid"), data.get("child_pid")]
    return any(pid is not None and _pid_is_alive(pid) for pid in pids)


def _claim_daily_phase_lock(name: str, stale_seconds: int = 12 * 3600):
    """P1-325: 複数プロセス同時起動でも日次フェーズを1本だけにする原子的lock claim。"""
    marker_path = os.path.join(OUTPUT_DIR, f"last_{name}.txt")
    lock_path = marker_path + ".lock"
    for _ in range(2):
        try:
            fd = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                payload = f"pid={os.getpid()} boot={_boot_token()} started={_now_jst():%Y-%m-%d %H:%M:%S}\n".encode("utf-8")
                os.write(fd, payload)
            finally:
                os.close(fd)
            return lock_path
        except FileExistsError:
            # 完了markerが既に立ったなら競合相手が正常完了済み。
            if _daily_marker_matches_today(name):
                return None
            try:
                _lock_mtime = os.path.getmtime(lock_path)
                age = max(0.0, time.time() - _lock_mtime)
                _lock_day = datetime.fromtimestamp(_lock_mtime, tz=_now_jst().tzinfo).date()
                _today_day = date.fromisoformat(_today_jst())
            except Exception:
                age = 0.0
                _lock_day = _today_day = None
            # P1-330: 前日以前のcrash残骸が翌朝まで12hロックを維持しない。
            # ただし日跨ぎ直後に前日ジョブがまだ生きている可能性を考え、2時間の猶予は残す。
            _cross_day_stale = (
                _lock_day is not None and _today_day is not None
                and _lock_day != _today_day and age > 2 * 3600
            )
            _dead_owner = _text_lock_owner_is_stale(lock_path)
            _live_owner = _text_lock_owner_is_alive(lock_path)
            if _dead_owner or (not _live_owner and (age > float(stale_seconds) or _cross_day_stale)):
                try:
                    os.unlink(lock_path)
                    print(f"[daily-lock] stale lock removed: {name} age={age:.0f}s dead_owner={_dead_owner}")
                    continue
                except Exception:
                    pass
            return None
    return None


def _release_daily_phase_lock(lock_path):
    if not lock_path:
        return
    try:
        os.unlink(lock_path)
    except FileNotFoundError:
        pass
    except Exception as e:
        print(f"[daily-lock][WARN] release failed: {lock_path}: {e}")


def _timed_daily_once(name, func, *args, **kwargs):
    """1日1回だけ実行する関数（タイマー付き・複数プロセス安全）。"""
    marker_path = os.path.join(OUTPUT_DIR, f"last_{name}.txt")
    today_str = _today_jst()
    marker_payload = _daily_marker_payload()

    if _daily_marker_matches_today(name):
        print(f"[SKIP daily] {name} (already ok for {today_str}, build={_daily_build_token()})")
        return

    # P1-325: marker確認→実行の隙間を原子的lockで塞ぐ。
    lock_path = _claim_daily_phase_lock(name)
    if lock_path is None:
        if _daily_marker_matches_today(name):
            print(f"[SKIP daily] {name} (completed by another process for {today_str})")
            return
        # P1-389: 別プロセスが重要フェーズを更新中なのに、この実行だけ先へ進んで
        # 途中状態/旧状態のHTMLを公開しない。
        raise RuntimeError(f"daily phase is currently running in another process: {name}")

    try:
        # claim待ちの間に別プロセスが完了していた場合の二重チェック。
        if _daily_marker_matches_today(name):
            print(f"[SKIP daily] {name} (already ok after lock claim)")
            return

        res = _timed(name, func, *args, **kwargs)
        # P1-502: 日次関数が明示的にFalseを返した場合は成功markerを立てない。
        # 外部/任意phaseが「失敗だが例外は投げない」を返しても、旧実装は当日完了扱いしていた。
        if res is False:
            raise RuntimeError(f"daily phase returned False: {name}")

        # P1-326: 完了markerも直接truncateせずatomic replace。
        try:
            _atomic_write_text_file(marker_path, marker_payload)
        except Exception as e:
            # P1-350: phase本体だけ成功して完了markerの永続化に失敗した状態を
            # 「日次処理成功」として返さない。次回の重複実行リスクも上位へ明示する。
            print(f"[MARKER ERROR] {name} のマーカー保存失敗: {e}")
            raise RuntimeError(f"daily marker persistence failed: {name}") from e
        return res
    finally:
        _release_daily_phase_lock(lock_path)


def _interval_marker_path(name: str) -> Path:
    safe = re.sub(r"[^0-9A-Za-z_.-]+", "_", str(name or "phase")).strip("._") or "phase"
    return SCREEN_RUNTIME_DIR / f"interval_{safe}.json"


def _interval_marker_is_fresh(name: str, interval_seconds: float) -> tuple[bool, float]:
    """P2-93: 同日・同buildで成功済みの中頻度phaseかを判定する。"""
    path = _interval_marker_path(name)
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(raw, dict):
            return False, float("inf")
        if str(raw.get("day") or "") != _today_jst():
            return False, float("inf")
        if str(raw.get("build") or "") != _daily_build_token():
            return False, float("inf")
        completed_at = float(raw.get("completed_at"))
        age = time.time() - completed_at
        if age < -300.0:
            return False, age
        return age < max(0.0, float(interval_seconds)), age
    except Exception:
        return False, float("inf")


def _timed_interval_once(name, interval_seconds, func, *args, **kwargs) -> bool:
    """P2-93: 中頻度phaseを指定間隔に1回だけ実行。失敗時はmarkerを進めない。"""
    fresh, age = _interval_marker_is_fresh(name, interval_seconds)
    if fresh:
        print(
            f"[SKIP interval] {name} "
            f"age={max(0.0, age):.0f}s < {float(interval_seconds):.0f}s"
        )
        return False
    result = _timed(name, func, *args, **kwargs)
    if result is False:
        raise RuntimeError(f"interval phase returned False: {name}")
    payload = {
        "day": _today_jst(),
        "build": _daily_build_token(),
        "completed_at": time.time(),
    }
    path = _interval_marker_path(name)
    path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_text_file(path, json.dumps(payload, ensure_ascii=False, separators=(",", ":")))
    return True

# ===== /日次実行ログユーティリティ =====

# === V5_ResSup: 抵抗/支持 計算＋HTML出力（静的UI・自然統合） ===============


_V5_N_DAYS = 90
_V5_TOUCH_PCT = 0.03
_V5_TOUCH_MIN = 3
_V5_ROUND_STEPS = [1,5,10,50,100,500,1000,5000,10000]
_V5_SWING_LOOKBACK = 60
_V5_HISTORY_TABLE = "price_history"
_V5_LATEST_TABLE  = "latest_prices"
_V5_COLS = {
    "Res_HH":"REAL","Res_Zone":"REAL","Res_Zone_Touches":"INTEGER","Res_Zone_Last":"TEXT",
    "Res_Round":"REAL","Res_Round_Step":"INTEGER","Res_Round_Near":"INTEGER",
    "Res_Line_Today":"REAL","Res_Line_R2":"REAL","Res_Nearest":"REAL",
    "Sup_LL":"REAL","Sup_Zone":"REAL","Sup_Zone_Touches":"INTEGER","Sup_Zone_Last":"TEXT",
    "Sup_Round":"REAL","Sup_Round_Step":"INTEGER","Sup_Round_Near":"INTEGER",
    "Sup_Line_Today":"REAL","Sup_Line_R2":"REAL","Sup_Nearest":"REAL",
}

def _v5_q(conn, sql, params=()):
    return list(conn.execute(sql, params))

def _v5_ensure_cols(conn, latest):
    cur = conn.cursor()
    _sp = "p2_65_v5_ensure_cols"
    try:
        cur.execute(f"SAVEPOINT {_sp}")
        existing = {r[1] for r in cur.execute(f"PRAGMA table_info({latest})")}
        for k, decl in _V5_COLS.items():
            if k not in existing:
                cur.execute(f"ALTER TABLE {latest} ADD COLUMN {k} {decl}")
        cur.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            cur.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        raise
    finally:
        cur.close()

def _v5_unify_code(conn, latest):
    cols = [r[1] for r in _v5_q(conn, f"PRAGMA table_info({latest})")]
    if "コード" in cols:
        return "コード"
    if "code" in cols:
        cur = conn.cursor()
        _sp = "p2_65_v5_unify_code"
        try:
            cur.execute(f"SAVEPOINT {_sp}")
            # P2-16/P2-65: 不存在確認済み。schema migrationは外側transactionをcommitしない。
            cur.execute(f"ALTER TABLE {latest} ADD COLUMN コード TEXT")
            cur.execute(f"UPDATE {latest} SET コード = CAST(code AS TEXT) WHERE コード IS NULL")
            cur.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                cur.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
            raise
        finally:
            cur.close()
        return "コード"
    return cols[0] if cols else "コード"

# --- 安全変換ユーティリティ（None/空/NaN→default） ---
def _v5_num(x, default=None):
    try:
        if x is None:
            return default
        v = float(x)
        if v != v:  # NaN
            return default
        return v
    except Exception:
        return default

def _v5_round_levels(p: float):
    pval = _v5_num(p, None)
    if pval is None:
        return [(0, None, None)]
    out = []
    for step in _V5_ROUND_STEPS:
        if step <= 0:
            continue
        nearest = round(pval / step) * step
        diff = abs(float(nearest) - pval)
        out.append((step, float(nearest), diff))
    out.sort(key=lambda x: (-x[0], x[2]))
    return out

def _v5_hist_zone(
    values,
    band_ratio,
    touch_min,
    ref_price=None
):
    vals = []

    for v in values:
        vv = _v5_num(v, None)
        if vv is not None:
            vals.append(vv)

    if not vals:
        return None

    vals.sort()

    best_center = None
    best_cnt = 0
    best_score = -1

    for i, v in enumerate(vals):

        hi = v + max(v * band_ratio, 30)

        j = i

        while (
            j + 1 < len(vals)
            and vals[j + 1] <= hi
        ):
            j += 1

        cnt = j - i + 1

        if cnt < touch_min:
            continue

        center = sum(vals[i:j+1]) / cnt

        # ----------------------------
        # 距離補正
        # ----------------------------

        if ref_price is not None:

            distance = abs(center - ref_price) / ref_price
            # 現在値から遠い帯を強く減点
            score = cnt / (1 + distance * 20)

        else:

            score = cnt

        if score > best_score:

            best_score = score
            best_cnt = cnt
            best_center = center

    if best_center is None:
        return None

    return (best_center, best_cnt)

def _v5_linreg_today(xs, ys):
    xs2, ys2 = [], []
    for x, y in zip(xs, ys):
        yy = _v5_num(y, None)
        if yy is not None:
            xs2.append(float(x))
            ys2.append(yy)
    n = len(xs2)
    if n < 5:
        return (None, None)
    mx = sum(xs2) / n
    my = sum(ys2) / n
    sxx = sum((x - mx) ** 2 for x in xs2)
    if sxx == 0:
        return (None, None)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs2, ys2))
    a = sxy / sxx
    b = my - a * mx
    yhat = [a * x + b for x in xs2]
    sst = sum((y - my) ** 2 for y in ys2)
    ssr = sum((yh - my) ** 2 for yh in yhat)
    r2 = (ssr / sst) if sst else None
    y_today = a * (xs2[-1] + 1) + b
    return (float(y_today), (float(r2) if r2 is not None else None))

def _v5_fetch_ohlcv(conn, code, cutoff_date=None):
    # P1-279: legacy/canonicalを横断し、同一日alias重複を1本へ畳んで返す。
    # P1-532: P1-158以前のlegacy未来日（平日）をV5支持抵抗の「最新足」にしない。
    # P1-575: V5もrun mode基準のsnapshot日を使う。PREOPENは前営業日、MIDDAY/EODは当日。
    # _dedupe_price_history_df() は休日行を落とすが、未来の平日は別途as-of cutoffが必要。
    _vars = code_query_variants(code)
    if not _vars:
        return []
    _ph = ",".join("?" * len(_vars))
    if cutoff_date is None:
        cutoff_date = _expected_snapshot_date_for_run(_auto_run_mode())
    _cutoff = pd.to_datetime(cutoff_date).date().isoformat()
    df = pd.read_sql_query(f"""
        SELECT rowid AS _rowid, コード, 日付, 始値, 高値, 安値, 終値
        FROM {_V5_HISTORY_TABLE}
        WHERE CAST(コード AS TEXT) IN ({_ph})
          AND date(日付) <= date(?)
        ORDER BY 日付 ASC, rowid ASC
    """, conn, params=_vars + [_cutoff])
    df = _dedupe_price_history_df(df)
    # P1-676: 技術計算の観測日は「有効な正の終値」がある行だけ。
    # 日付placeholder(終値NULL/0/非有限)を1営業日として数えてV5支持抵抗を再生成しない。
    if not df.empty:
        df["終値"] = pd.to_numeric(df["終値"], errors="coerce")
        df = df[df["終値"].notna() & np.isfinite(df["終値"]) & (df["終値"] > 0)].copy()
    if df.empty:
        return []
    return list(df[["日付","始値","高値","安値","終値"]].itertuples(index=False, name=None))
    
def _v5_calc(rows, code=None):
    keys = list(_V5_COLS.keys())

    if not rows:
        return {k: None for k in keys}

    rows = rows[-_V5_N_DAYS:]

    dates = [r[0] for r in rows]
    highs = [_v5_num(r[2], None) for r in rows]
    lows  = [_v5_num(r[3], None) for r in rows]

    close_today = _v5_num(rows[-1][4], None)

    res_hh = max([v for v in highs if v is not None], default=None)
    sup_ll = min([v for v in lows if v is not None], default=None)

    # --------------------------------------------------
    # 支持帯・抵抗帯は現在値基準で探索
    # --------------------------------------------------

    if close_today is not None:

        highs_above = [
            h for h in highs
            if h is not None and h > close_today
        ]

        lows_below = [
            l for l in lows
            if l is not None and l < close_today
        ]

    else:
        highs_above = []
        lows_below = []

    zr = _v5_hist_zone(
        highs_above,
        _V5_TOUCH_PCT,
        _V5_TOUCH_MIN,
        close_today
    )

    zs = _v5_hist_zone(
        lows_below,
        _V5_TOUCH_PCT,
        _V5_TOUCH_MIN,
        close_today
    )

    res_zone, res_touch = (
        (zr[0], zr[1]) if zr else (None, None)
    )

    sup_zone, sup_touch = (
        (zs[0], zs[1]) if zs else (None, None)
    )

    # --------------------------------------------------
    # 最終接触日
    # --------------------------------------------------

    res_last = None

    if res_zone is not None:

        for i in range(len(rows) - 1, -1, -1):

            hi = highs[i]

            if hi is None:
                continue

            if abs(hi - res_zone) <= res_zone * _V5_TOUCH_PCT:
                res_last = dates[i]
                break

    sup_last = None

    if sup_zone is not None:

        for i in range(len(rows) - 1, -1, -1):

            lo = lows[i]

            if lo is None:
                continue

            if abs(lo - sup_zone) <= sup_zone * _V5_TOUCH_PCT:
                sup_last = dates[i]
                break

    # --------------------------------------------------
    # キリ番
    # --------------------------------------------------

    rl = _v5_round_levels(close_today)

    step, nearest, _ = (
        rl[0]
        if rl and rl[0][1] is not None
        else (None, None, None)
    )

    res_round_step = step
    sup_round_step = step

    res_round = nearest
    sup_round = nearest

    if close_today is None or nearest is None:
        near_flag = None
    else:
        near_flag = (
            1
            if abs(close_today - nearest)
            <= close_today * _V5_TOUCH_PCT
            else 0
        )

    # --------------------------------------------------
    # 回帰線
    # --------------------------------------------------

    sw = rows[-_V5_SWING_LOOKBACK:]

    xs = list(range(len(sw)))

    rh = [_v5_num(r[2], None) for r in sw]
    rl_ = [_v5_num(r[3], None) for r in sw]

    res_line_today, res_r2 = _v5_linreg_today(xs, rh)
    sup_line_today, sup_r2 = _v5_linreg_today(xs, rl_)

    # --------------------------------------------------
    # 20日・60日高安
    # --------------------------------------------------

    high20 = max(
        [v for v in highs[-20:] if v is not None],
        default=None
    )

    high60 = max(
        [v for v in highs[-60:] if v is not None],
        default=None
    )

    low20 = min(
        [v for v in lows[-20:] if v is not None],
        default=None
    )

    low60 = min(
        [v for v in lows[-60:] if v is not None],
        default=None
    )

    # --------------------------------------------------
    # 最寄り支持・抵抗
    # --------------------------------------------------

    if close_today is None:

        res_near = None
        sup_near = None

    else:

        up_candidates = [

            res_zone,
            res_hh,
            high20,
            high60,
            res_line_today,

        ]

        up_candidates = [

            v
            for v in up_candidates
            if v is not None and v > close_today

        ]

        dn_candidates = [

            sup_zone,
            sup_ll,
            low20,
            low60,
            sup_line_today,

        ]

        dn_candidates = [

            v
            for v in dn_candidates
            if v is not None and v < close_today

        ]

        res_near = (
            min(
                up_candidates,
                key=lambda x: abs(x - close_today)
            )
            if up_candidates
            else None
        )

        sup_near = (
            min(
                dn_candidates,
                key=lambda x: abs(x - close_today)
            )
            if dn_candidates
            else None
        )

    if str(code) == "5074":

        print("=" * 80)
        print("DEBUG 5074")

        print("close_today =", close_today)

        print("highs_above =", highs_above[-20:])
        print("lows_below  =", lows_below[-20:])

        print("zr =", zr)
        print("zs =", zs)

        print("up_candidates =", up_candidates)
        print("dn_candidates =", dn_candidates)

        print("res_zone =", res_zone)
        print("sup_zone =", sup_zone)

        print("res_near =", res_near)
        print("sup_near =", sup_near)

        print("=" * 80)
        
    return {

        "Res_HH": res_hh,

        "Res_Zone": res_zone,
        "Res_Zone_Touches": res_touch,
        "Res_Zone_Last": res_last,

        "Res_Round": res_round,
        "Res_Round_Step": res_round_step,
        "Res_Round_Near": near_flag,

        "Res_Line_Today": res_line_today,
        "Res_Line_R2": res_r2,

        "Res_Nearest": res_near,

        "Sup_LL": sup_ll,

        "Sup_Zone": sup_zone,
        "Sup_Zone_Touches": sup_touch,
        "Sup_Zone_Last": sup_last,

        "Sup_Round": sup_round,
        "Sup_Round_Step": sup_round_step,
        "Sup_Round_Near": near_flag,

        "Sup_Line_Today": sup_line_today,
        "Sup_Line_R2": sup_r2,

        "Sup_Nearest": sup_near,
    }
def _v5_update_latest(conn, latest):
    _v5_ensure_cols(conn, latest)
    code_col = _v5_unify_code(conn, latest)
    codes = [r[0] for r in _v5_q(conn, f"SELECT {code_col} FROM {latest}")]
    if not codes:
        return 0

    # P1-533: 計算途中の例外でlatest_pricesを半分だけ新V5値へ更新しない。
    # まず全銘柄をメモリ上で計算し、全件成功後に1 SAVEPOINTで反映する。
    # P1-575: export直前のV5再計算が、phase_resistance_updateで消したstale支持抵抗を
    # 復活させないよう、個別の最終価格日がrun mode基準日と一致する銘柄だけ計算する。
    # 不一致/履歴なしは全V5列をNULLにし、成功した古い計算結果をcurrent-runへ混入させない。
    _v5_expected_date = _expected_snapshot_date_for_run(_auto_run_mode())
    _v5_expected = _v5_expected_date.isoformat()
    params_rows = []
    for code in codes:
        _rows = _v5_fetch_ohlcv(conn, code, cutoff_date=_v5_expected_date)
        _last_date = str(_rows[-1][0])[:10] if _rows else None
        if _rows and _last_date == _v5_expected:
            vals = _v5_calc(_rows, code)
        else:
            vals = {k: None for k in _V5_COLS.keys()}
        params_rows.append([vals[k] for k in _V5_COLS.keys()] + [code])

    set_clause = ",".join([f"{k}=?" for k in _V5_COLS.keys()])
    sql = f"UPDATE {latest} SET {set_clause} WHERE {code_col}=?"
    sp = "sp_v5_update_latest"
    try:
        conn.execute(f"SAVEPOINT {sp}")
        conn.executemany(sql, params_rows)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        raise
    return len(params_rows)

# [REMOVED by integrator: legacy V5 HTML block]
# [REMOVED by integrator: legacy V5 HTML block]
# === /V5_ResSup =============================================================

# === 価格計算の安全ガード（内蔵版 | 完全版） ===
JST = ZoneInfo("Asia/Tokyo")

def _is_jp_business_day(d):
    """JPX営業日。判定不能を実在する「休場日」と混同しない。"""
    try:
        extra = _load_extra_closed(EXTRA_CLOSED_PATH)
        return not is_jp_market_holiday(d, extra)
    except RuntimeError:
        # P2-70: jpholiday未導入/追加休場日ファイル破損などは「休場」ではなく判定不能。
        # Falseへ落とすと_auto_run_modeが平日をEOD扱いし、誤った確定処理へ進み得るため上位へ返す。
        raise
    except Exception as e:
        # 未分類のカレンダー障害も同じくfail-visible。営業日/休場日のどちらにも推測しない。
        raise RuntimeError(f"JPX business-day lookup failed for {d}: {e}") from e

def _ensure_override_table(conn):
    """P1-505/P2-44: Yahoo override schemaを保証する。呼出元transactionは勝手にcommitしない。"""
    cur = conn.cursor()
    try:
        cur.execute("""CREATE TABLE IF NOT EXISTS yahoo_symbol_override (
            コード TEXT PRIMARY KEY,
            問い合わせシンボル TEXT NOT NULL,
            updated_at TEXT
        )""")
        cols = {str(r[1]) for r in cur.execute("PRAGMA table_info(yahoo_symbol_override)").fetchall()}
        if "updated_at" not in cols:
            cur.execute("ALTER TABLE yahoo_symbol_override ADD COLUMN updated_at TEXT")
        # P2-44: このhelperはresolver/filterの深い場所からも呼ばれる。
        # unconditional conn.commit() は呼出元の未完了DMLまで確定させるため禁止。
    finally:
        cur.close()


def _read_yahoo_override_record(conn, code_str: str):
    """canonical→legacy alias順でoverrideを決定的に読む。返り値=(symbol, updated_at)。"""
    try:
        _ensure_override_table(conn)
        variants = code_query_variants(code_str)
        if not variants:
            return None, None
        ph = ",".join("?" * len(variants))
        rows = conn.execute(
            f"SELECT CAST(コード AS TEXT), 問い合わせシンボル, updated_at "
            f"FROM yahoo_symbol_override WHERE CAST(コード AS TEXT) IN ({ph})",
            tuple(variants),
        ).fetchall()
        mp = {str(r[0]): (r[1], r[2]) for r in rows}
        for v in variants:
            rec = mp.get(str(v))
            if rec:
                sym = str(rec[0]).strip() if rec[0] is not None else ""
                return (sym or None), rec[1]
    except Exception:
        return None, None
    return None, None


def _read_yahoo_override(conn, code_str: str):
    sym, _ = _read_yahoo_override_record(conn, code_str)
    return sym








def _vectorize_minimum_fields(df):
    if df is None or getattr(df, "empty", True):
        return df

    # コードの正規化
    if "コード" in df.columns:
        try:
            # P1-151: Seriesでも共通正規化を使い、7203.0→7203 / 285A→285A。
            c4 = pd.Series(df["コード"], copy=False).map(_normalize_jp_security_code)
            df["c4"] = c4
        except Exception:
            pass
    else:
        return df

    # Yahoo URL（市場優先、無ければ推定）
    try:
        market_col = df["市場"] if "市場" in df.columns else None
        if market_col is not None:
            df["yahoo_url"] = df.apply(lambda r: _yahoo_quote_url(str(r.get("c4") or ""), str(r.get("市場") or ""), None), axis=1)
        else:
            df["yahoo_url"] = df["c4"].map(lambda c: _yahoo_quote_url(str(c or ""), None, None))
    except Exception:
        pass

    # X URL
    if "銘柄名" in df.columns:
        try:
            df["x_url"] = "https://x.com/search?q=" + df["銘柄名"].astype(str).map(lambda s: quote(s))
        except Exception:
            pass

    # チャートURL
    try:
        df["chart_url"] = "./charts60/" + df["c4"] + ".html"
        # P1-619: 今回charts60で実際に再生成できた銘柄だけリンクをcurrentとして公開する。
        _chart_codes = globals().get("_CHARTS60_CURRENT_RUN_CODES")
        if isinstance(_chart_codes, set):
            df.loc[~df["c4"].isin(_chart_codes), "chart_url"] = None
        elif globals().get("_CHARTS60_CURRENT_RUN_OK") is False:
            df["chart_url"] = None
    except Exception:
        pass

    return df





# === /価格計算の安全ガード ===
# === ライブ価格取得（場中のみ自動） ===
# === /ライブ価格取得 ===

# ==== 非同期実行ユーティリティ（投げっぱなし起動） ====



# --- PATCH: live quote (price/volume/dayHigh/dayLow/marketCap) fetch ---
# --- /PATCH live quote fetch ---

# --- PATCH: RVOL denominator loader (20-day average turnover in Oku) ---
# --- /PATCH RVOL denominator ---

# --- PATCH: apply live overrides (incl. RVOL turnover) ---
# --- /PATCH apply live overrides ---



# ==== /非同期実行ユーティリティ ====

# ========= bulk utils (v8 additions) =========

# ========= end bulk utils =========

def add_price_features(df):
    """
    価格系列の典型指標をベクトル化で一括付与するテンプレ関数
    """
    if df.empty:
        return df
    # 防御: 必須列のみで進める
    need = ["コード","日付","終値"]
    for c in need:
        if c not in df.columns:
            return df

    df = df.sort_values(["コード","日付"]).copy()
    grp = df.groupby("コード", sort=False)

    def _rolling_mean(series, window, min_periods):
        return (
            series.groupby(df["コード"], sort=False)
            .rolling(window, min_periods=min_periods).mean()
            .reset_index(level=0, drop=True).reindex(df.index)
        )

    # 移動平均
    if "終値" in df.columns:
        df["終値_ma5"]  = _rolling_mean(df["終値"], 5, 1)
        df["終値_ma13"] = _rolling_mean(df["終値"], 13, 1)
        df["終値_ma20"] = _rolling_mean(df["終値"], 20, 1)
        df["終値_ma26"] = _rolling_mean(df["終値"], 26, 1)

    # 出来高平均とRVOL
    if "出来高" in df.columns:
        df["出来高_ma5"]  = _rolling_mean(df["出来高"], 5, 1)
        df["出来高_ma20"] = _rolling_mean(df["出来高"], 20, 1)
        with pd.option_context('mode.use_inf_as_na', True):
            df["RVOL20"] = (df["出来高"] / df["出来高_ma20"]).replace([np.inf, -np.inf], np.nan)

    # P3-28: ATR20は高安差だけでなく前日終値とのギャップを含むTrue Rangeを使う。
    if "高値" in df.columns and "安値" in df.columns:
        _high = pd.to_numeric(df["高値"], errors="coerce")
        _low = pd.to_numeric(df["安値"], errors="coerce")
        _prev_close = grp["終値"].shift(1)
        _tr = pd.concat([
            (_high - _low).abs(),
            (_high - _prev_close).abs(),
            (_low - _prev_close).abs(),
        ], axis=1).max(axis=1, skipna=True)
        df["ATR20"] = _rolling_mean(_tr, 20, 20)

    # 変化率（参考: 当日/前日-1）
    try:
        df["終値_pct1"] = grp["終値"].pct_change(1, fill_method=None) * 100.0
    except Exception:
        pass

    # --- legacy column aliases for backward compatibility ---
    for _new, _legacy in [("終値_ma5","MA5"), ("終値_ma13","MA13"),
                         ("終値_ma20","MA20"), ("終値_ma26","MA26")]:
        if _new in df.columns and _legacy not in df.columns:
            try:
                df[_legacy] = df[_new]
            except Exception:
                pass
    return df
# ========= end price series features =========

def calc_rsi(series, period=14):
    """CatBoost学習側と完全に同じRSI定義。"""
    delta = series.diff()
    gain = delta.clip(lower=0).rolling(period, min_periods=period).mean()
    loss = (-delta.clip(upper=0)).rolling(period, min_periods=period).mean()

    rs = gain / loss.replace(0, np.nan)
    rsi = 100 - (100 / (1 + rs))

    only_gain = (loss == 0) & (gain > 0)
    flat = (loss == 0) & (gain == 0)
    rsi = rsi.mask(only_gain, 100.0)
    rsi = rsi.mask(flat, 50.0)
    return rsi


def _ai_safe_div(num, den):
    """CatBoost学習側と同じ0除算処理。"""
    if isinstance(den, pd.Series):
        den = den.replace(0, np.nan)
    return num / den


def _ai_true_rolling_poc_close(df: pd.DataFrame, window: int = 20) -> pd.Series:
    """
    各銘柄について「直近window営業日で出来高最大だった日の終値」を毎日再計算。
    学習側の定義と一致させる。
    """
    out = pd.Series(np.nan, index=df.index, dtype=float)

    for _, idx in df.groupby("コード", sort=False).groups.items():
        positions = np.asarray(list(idx), dtype=int)
        vol = df.loc[positions, "volume"].to_numpy(dtype=float)
        close = df.loc[positions, "close"].to_numpy(dtype=float)
        n = len(positions)

        if n < window:
            continue

        try:
            from numpy.lib.stride_tricks import sliding_window_view
            windows = sliding_window_view(vol, window_shape=window)
            argmax = np.argmax(windows, axis=1)
            starts = np.arange(len(windows))
            poc_values = close[starts + argmax]
            out.loc[positions[window - 1:]] = poc_values
        except Exception:
            for i in range(window - 1, n):
                s = i - window + 1
                j = s + int(np.argmax(vol[s:i + 1]))
                out.loc[positions[i]] = close[j]

    return out


def _ai_format_target_threshold(current_price, target_pct):
    """
    AI分類モデルの教師ラベル閾値（target_pct）を、人間向けに表示する。

    注意:
    - これはAIが回帰予測した銘柄別の目標株価ではない。
    - target_pct は metadata.json に保存された分類ラベルの到達基準。
      例: 1.10 なら「現在値から+10%へ到達するか」を判定するための基準価格。
    """
    try:
        p = float(current_price)
        tp = float(target_pct)
    except Exception:
        return "-", None
    if not (p > 0 and tp > 1.0):
        return "-", None

    tgt = int(round(p * tp))
    current_int = int(round(p))
    diff_val = tgt - current_int
    pct = (tp - 1.0) * 100.0
    return f"+{pct:.1f}%到達基準 {current_int}→{tgt} (+{diff_val}円)", diff_val


def _ai_load_model_metadata():
    """
    新学習コードが保存する metadata.json を読む。
    無い場合は従来互換の既定値。
    """
    meta = {
        "decision_threshold": 0.55,
        "strong_threshold": None,   # 学習側で検証済みの強気閾値がある場合だけ使用
        "attention_band": 0.10,    # 表示用: decision_threshold 未満の境界帯。正式な陽性閾値ではない
        "target_pct": 1.10,
        "min_price": 300.0,
        "label_mode": "touch",
    }

    try:
        meta_path = Path(MODEL_PATH).with_suffix(".metadata.json")
        if meta_path.exists():
            with open(meta_path, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                meta.update(loaded)
    except Exception as e:
        print(f"[AI][WARN] model metadata 読込失敗。既定値を使用: {e}")

    try:
        th = float(meta.get("decision_threshold", 0.55))
        meta["decision_threshold"] = th if 0.0 < th < 1.0 else 0.55
    except Exception:
        meta["decision_threshold"] = 0.55

    try:
        tp = float(meta.get("target_pct", 1.10))
        meta["target_pct"] = tp if tp > 1.0 else 1.10
    except Exception:
        meta["target_pct"] = 1.10

    try:
        mp = float(meta.get("min_price", 300.0))
        meta["min_price"] = max(0.0, mp)
    except Exception:
        meta["min_price"] = 300.0

    # P1-7: strong_threshold は metadata に明示された場合だけ正式閾値として使う。
    # 従来の decision_threshold+0.10 は validation 由来ではないため廃止。
    try:
        st_raw = meta.get("strong_threshold", None)
        st = None if st_raw in (None, "", "None") else float(st_raw)
        if st is not None and not (meta["decision_threshold"] < st < 1.0):
            st = None
        meta["strong_threshold"] = st
    except Exception:
        meta["strong_threshold"] = None

    try:
        band = float(meta.get("attention_band", 0.10))
        meta["attention_band"] = min(0.30, max(0.0, band))
    except Exception:
        meta["attention_band"] = 0.10

    return meta


def _ai_probability_label(prob, meta):
    """
    P1-7: predict_proba を metadata の正式な decision_threshold に合わせて表示分類する。

    - decision_threshold 以上: モデルの正式な陽性判定。
    - strong_threshold: metadata に検証済み値が明示されている場合だけ「★超強気」に使用。
    - attention_band: decision_threshold 未満の表示用境界帯で、正式な陽性判定ではない。
    """
    try:
        p = float(prob)
        th = float(meta.get("decision_threshold", 0.55))
    except Exception:
        return "-"

    strong = meta.get("strong_threshold", None)
    if strong is not None:
        try:
            strong = float(strong)
        except Exception:
            strong = None

    if strong is not None and th < strong < 1.0 and p >= strong:
        return "★超強気"
    if p >= th:
        return "★到達有望"

    try:
        band = float(meta.get("attention_band", 0.10))
    except Exception:
        band = 0.10
    near_th = round(max(0.0, th - max(0.0, band)), 12)
    if p + 1e-12 >= near_th:
        return "△閾値近辺"
    return "◯閾値未満"


def _ai_market_daily_features(
    conn: sqlite3.Connection,
    latest_target_date: pd.Timestamp,
    lookback_calendar_days: int = 45,
) -> pd.DataFrame:
    """
    学習側の market_return / market_sentiment をDB全銘柄から再現する。
    """
    if latest_target_date is None or pd.isna(latest_target_date):
        return pd.DataFrame(columns=["date", "market_return", "market_sentiment"])

    end_date = pd.Timestamp(latest_target_date).normalize()
    start_date = end_date - pd.Timedelta(days=lookback_calendar_days)

    sql = """
        SELECT コード, 日付, 終値, 出来高
        FROM price_history
        WHERE 日付 >= ? AND 日付 <= ?
        ORDER BY コード, 日付
    """
    mdf = pd.read_sql_query(
        sql,
        conn,
        params=(
            start_date.strftime("%Y-%m-%d"),
            end_date.strftime("%Y-%m-%d"),
        ),
    )

    if mdf.empty:
        return pd.DataFrame(columns=["date", "market_return", "market_sentiment"])

    # P1-283: alias別銘柄として市場平均へ二重加重しない。
    mdf = _dedupe_price_history_df(mdf)
    mdf["date"] = pd.to_datetime(mdf["日付"], errors="coerce")
    mdf["close"] = pd.to_numeric(mdf["終値"], errors="coerce")
    mdf["volume"] = pd.to_numeric(mdf["出来高"], errors="coerce")

    mdf = mdf.dropna(subset=["コード", "date", "close", "volume"])
    mdf = mdf[(mdf["close"] > 0) & (mdf["volume"] >= 0)].copy()
    if mdf.empty:
        return pd.DataFrame(columns=["date", "market_return", "market_sentiment"])

    mdf = mdf.sort_values(["コード", "date"]).reset_index(drop=True)
    # P1-100: 銘柄自身の前観測が「市場全体の直前取引日」と一致する時だけ1日リターンとする。
    # 日付穴がある銘柄の数日分リターンを return_1d としてAIへ混ぜない。
    _market_dates = sorted(pd.Series(mdf["date"].dropna().unique()).tolist())
    _prev_market = {_market_dates[i]: _market_dates[i-1] for i in range(1, len(_market_dates))}
    _prev_obs_date = mdf.groupby("コード", sort=False)["date"].shift(1)
    _raw_return_1d = (
        mdf.groupby("コード", sort=False)["close"]
        .pct_change(1, fill_method=None)
    )
    _expected_prev = mdf["date"].map(_prev_market)
    mdf["return_1d"] = _raw_return_1d.where(_prev_obs_date.eq(_expected_prev))

    market_return = (
        mdf.groupby("date")["return_1d"]
        .mean()
        .rename("market_return")
    )
    # P1-99: return_1dを計算できない銘柄（新規上場/履歴不足）を
    # 「上がっていない銘柄」としてセンチメント分母へ入れない。
    valid_ret = mdf[mdf["return_1d"].notna()].copy()
    up_counts = (
        valid_ret[valid_ret["return_1d"] > 0]
        .groupby("date")["コード"]
        .count()
    )
    total_counts = valid_ret.groupby("date")["コード"].count()
    up_counts = up_counts.reindex(total_counts.index, fill_value=0)
    market_sentiment = (up_counts / total_counts).rename("market_sentiment")

    return pd.concat([market_return, market_sentiment], axis=1).reset_index()


def add_ai_analysis(conn, rows):
    """
    CatBoost 19特徴量・学習/推論完全一致版。

    - 学習側と19特徴量の計算式を一致
    - market系3特徴量をDB全銘柄から実計算
    - 特徴量不足銘柄を0埋めしない
    - AIスコアは純粋なpredict_proba
    - metadata.jsonのvalidation閾値/target_pctを利用
    """
    # P1-229: モデル欠損・ロード失敗・途中例外でも、前回AI値を今回値として残さない。
    if rows is None:
        return rows
    for _r in rows:
        _r["AIスコア"] = "-"
        _r["AI判定"] = "-"
        _r["AI目標値"] = "-"
        _r["AI目標値_raw"] = -999999

    if not os.path.exists(MODEL_PATH):
        print(f"[AI] モデルファイルが見つかりません: {MODEL_PATH}")
        return rows

    # P1-209: AI履歴検索の入口をcanonical codeへ。7203.0/285aで履歴ゼロ扱いしない。
    target_codes = [
        canonical_code_for_db(r["コード"])
        for r in rows
        if r.get("コード") is not None and r.get("現在値")
    ]
    target_codes = list(dict.fromkeys(c for c in target_codes if c))
    if not target_codes:
        return rows

    print(f"[AI] {len(target_codes)}銘柄の分析を開始（CatBoost 19特徴量・学習定義一致版）...")

    try:
        _model_p = Path(MODEL_PATH)
        _publishing_lock = _model_p.with_suffix(".publishing.lock")
        if _publishing_lock.exists():
            print(f"[AI] モデルpublish中のため今回のAI推論を見送ります: {_publishing_lock}")
            return
        model = joblib.load(MODEL_PATH)
        meta = _ai_load_model_metadata()
        _meta_gen = str(meta.get("generation_id") or "").strip()
        _model_gen = str(getattr(model, "_kabu_generation_id", "") or "").strip()
        if (_meta_gen or _model_gen) and (not _meta_gen or not _model_gen or _meta_gen != _model_gen):
            raise RuntimeError(f"AI model generation mismatch: model={_model_gen or 'missing'} meta={_meta_gen or 'missing'}")

        decision_prob = float(meta["decision_threshold"])
        strong_prob = meta.get("strong_threshold", None)
        attention_band = float(meta.get("attention_band", 0.10))
        near_prob = max(0.0, decision_prob - attention_band)
        target_pct = float(meta["target_pct"])
        min_price = float(meta["min_price"])

        _strong_log = f"{float(strong_prob):.3f}" if strong_prob is not None else "metadataなし"
        print(
            f"[AI] decision_threshold={decision_prob:.3f} "
            f"strong_threshold={_strong_log} "
            f"near_display_from={near_prob:.3f} "
            f"target=+{(target_pct - 1.0) * 100:.1f}% "
            f"min_price={min_price:.0f}"
        )

        # 1. 対象銘柄の価格履歴
        # P1-580: PREOPEN AIも当日legacy足を先読みせず、前営業日の確定snapshotだけを使う。
        _ai_cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        # P2-91: 最新AI推論の最大窓はMA75（他は20日以下）。旧実装はほぼ全銘柄を
        # CAST INで指定しながら全期間履歴をPythonへ転送し、毎run同じ過去特徴量まで再計算した。
        # raw codeごとの最新120観測だけをDB側で取り、alias統合後もlogical codeごとに
        # 最新120観測へ再限定する。75日特徴量の結果は全期間版と同一で、転送/計算量だけ減らす。
        _ai_history_rows = 120
        hist_q = """
            WITH ranked AS (
                SELECT rowid AS _rowid, コード, 日付, 始値, 高値, 安値, 終値, 出来高,
                       ROW_NUMBER() OVER (
                           PARTITION BY コード
                           ORDER BY date(日付) DESC, rowid DESC
                       ) AS _rn
                FROM price_history
                WHERE date(日付)<=date(?)
            )
            SELECT _rowid, コード, 日付, 始値, 高値, 安値, 終値, 出来高
            FROM ranked
            WHERE _rn<=?
            ORDER BY 日付, _rowid
        """
        # P1-416: legacy future/weekend行をAI最新観測/市場特徴量の基準日にしない。
        df = pd.read_sql_query(hist_q, conn, params=[_ai_cutoff, _ai_history_rows])
        # P1-282: canonical検索だけでlegacy-only履歴を落とさず、split系列も1本へ結合。
        df = _dedupe_price_history_df(df)
        _ai_target_set = set(target_codes)
        if not df.empty:
            df = df[df["コード"].isin(_ai_target_set)].copy()
            df = (
                df.sort_values(["コード", "日付", "_rowid"], kind="stable")
                  .groupby("コード", sort=False, group_keys=False)
                  .tail(_ai_history_rows)
                  .reset_index(drop=True)
            )
        if df.empty:
            print("[AI][WARN] 対象銘柄の price_history がありません")
            return rows

        df = df.rename(
            columns={
                "始値": "open",
                "高値": "high",
                "安値": "low",
                "終値": "close",
                "出来高": "volume",
            }
        )
        df["date"] = pd.to_datetime(df["日付"], errors="coerce")

        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c], errors="coerce")

        # 学習側と同じ基礎クリーニング
        df = df.dropna(
            subset=["コード", "date", "open", "high", "low", "close", "volume"]
        )
        df = df[
            (df["open"] > 0)
            & (df["high"] > 0)
            & (df["low"] > 0)
            & (df["close"] > 0)
            & (df["volume"] >= 0)
        ].copy()

        if df.empty:
            return rows

        df = df.sort_values(["コード", "date"]).reset_index(drop=True)
        grp = df.groupby("コード", sort=False)

        # 2. 学習側と一致する特徴量
        df["return_1d"] = grp["close"].pct_change(1, fill_method=None)
        df["range"] = _ai_safe_div(df["high"] - df["low"], df["close"])

        # 学習側は /open
        df["body"] = _ai_safe_div(df["close"] - df["open"], df["open"])

        df["upper_shadow"] = _ai_safe_div(
            df["high"] - df[["close", "open"]].max(axis=1),
            df["close"],
        )

        for p in [5, 25, 75]:
            ma = grp["close"].transform(
                lambda x, w=p: x.rolling(w, min_periods=w).mean()
            )
            df[f"ma_{p}"] = ma
            df[f"kairi_{p}"] = _ai_safe_div(df["close"] - ma, ma)

        df["rsi_14"] = grp["close"].transform(calc_rsi)

        ma20 = grp["close"].transform(
            lambda x: x.rolling(20, min_periods=20).mean()
        )
        std20 = grp["close"].transform(
            lambda x: x.rolling(20, min_periods=20).std()
        )
        df["bb_pos"] = _ai_safe_div(df["close"] - ma20, 2 * std20)

        vol_ma5 = grp["volume"].transform(
            lambda x: x.rolling(5, min_periods=5).mean()
        )
        df["vol_ratio"] = _ai_safe_div(df["volume"], vol_ma5)

        # 旧版ではダミー0だった2特徴量
        df["perfect_order"] = (
            (df["ma_5"] > df["ma_25"])
            & (df["ma_25"] > df["ma_75"])
        ).astype(int)
        df["trend_strong"] = (
            (df["close"] > df["ma_5"])
            & (df["close"] > df["ma_25"])
        ).astype(int)

        # stop_hunt: 銘柄別rolling + 下ヒゲ条件
        df["min_low_5d"] = grp["low"].transform(
            lambda x: x.shift(1).rolling(5, min_periods=5).min()
        )
        df["stop_hunt_reversal"] = (
            (df["low"] < df["min_low_5d"])
            & (df["close"] > df["open"])
            & ((df["open"] - df["low"]) > (df["close"] - df["open"]))
        ).astype(int)

        # 真の20日POC
        df["poc_20d_close"] = _ai_true_rolling_poc_close(df, window=20)
        df["dist_from_poc"] = _ai_safe_div(
            df["close"] - df["poc_20d_close"],
            df["poc_20d_close"],
        )

        # 流動性
        df["turnover"] = df["close"] * df["volume"]
        df["turnover_20d_avg"] = grp["turnover"].transform(
            lambda x: x.rolling(20, min_periods=20).mean()
        )
        conditions = [
            df["turnover_20d_avg"] < 100_000_000,
            (df["turnover_20d_avg"] >= 100_000_000)
            & (df["turnover_20d_avg"] < 3_000_000_000),
            df["turnover_20d_avg"] >= 3_000_000_000,
        ]
        df["liquidity_class"] = np.select(
            conditions, [0, 1, 2], default=1
        ).astype(int)

        # 3. 市場特徴量をDB全銘柄から計算
        latest_target_date = df["date"].max()
        market_daily = _ai_market_daily_features(
            conn,
            latest_target_date=latest_target_date,
            lookback_calendar_days=45,
        )

        if not market_daily.empty:
            df = df.merge(market_daily, on="date", how="left", sort=False)
        else:
            df["market_return"] = np.nan
            df["market_sentiment"] = np.nan

        df["relative_strength"] = df["return_1d"] - df["market_return"]

        df.replace([np.inf, -np.inf], np.nan, inplace=True)
        df = df.sort_values(["コード", "date"]).reset_index(drop=True)

        # 4. 最新行のみ推論
        features = [
            "return_1d", "range", "body", "upper_shadow",
            "kairi_5", "kairi_25", "kairi_75",
            "rsi_14", "bb_pos", "vol_ratio",
            "perfect_order", "trend_strong",
            "market_return", "market_sentiment", "relative_strength",
            "stop_hunt_reversal", "dist_from_poc",
            "turnover_20d_avg", "liquidity_class",
        ]

        df["コード"] = df["コード"].map(canonical_code_for_db)
        df_latest = (
            df.groupby("コード", sort=False)
            .tail(1)
            .set_index("コード")
        )

        # 学習価格帯と一致
        valid_mask = df_latest["close"] >= min_price
        # P1-230: 市場最新日より古い銘柄を「今日のAI判定」として採用しない。
        valid_mask &= df_latest["date"].dt.normalize().eq(pd.Timestamp(latest_target_date).normalize())

        # 学習側は特徴量欠損行をdropしているため、推論側も0埋めしない
        valid_mask &= df_latest[features].notna().all(axis=1)

        df_valid = df_latest.loc[valid_mask].copy()
        excluded_count = int((~valid_mask).sum())

        if excluded_count:
            print(
                f"[AI] 特徴量不足/学習価格帯外のため {excluded_count}銘柄を判定対象外"
            )

        scores = {}
        if not df_valid.empty:
            X = df_valid[features].copy()
            X["liquidity_class"] = X["liquidity_class"].astype(int)
            probs = model.predict_proba(X)[:, 1]
            scores = dict(zip(df_valid.index.astype(str), probs))

        # 5. 結果反映
        for r in rows:
            code = canonical_code_for_db(r.get("コード"))

            if code not in scores:
                r["AIスコア"] = "-"
                r["AI判定"] = "-"
                r["AI目標値"] = "-"
                r["AI目標値_raw"] = -999999
                continue

            prob = float(scores[code])

            # 純粋なモデル確率。旧版の+5/+10%強制底上げは廃止。
            score_val = round(prob * 100, 1)
            r["AIスコア"] = score_val

            # P1-7: 正式な陽性判定は metadata の decision_threshold に一致させる。
            # 「超強気」は strong_threshold が metadata に存在する時だけ使用する。
            r["AI判定"] = _ai_probability_label(prob, meta)

            # P1-6: target_pct は「AIが予測した目標株価」ではなく、
            # 学習時の分類ラベル（到達判定）の価格閾値。誤解を避けて到達基準として表示する。
            try:
                p_val = float(str(r.get("現在値", 0)).replace(",", ""))
            except Exception:
                p_val = 0.0

            target_display, _ = _ai_format_target_threshold(p_val, target_pct)
            if target_display == "-":
                r["AI目標値"] = "-"
                r["AI目標値_raw"] = -999999
                continue

            r["AI目標値"] = target_display
            # 後方互換のキー名は維持。ただし固定target_pctの円差は銘柄価格順になるだけなので、
            # ダッシュボードのソート値はAI到達確率（AIスコア）に変更する。
            r["AI目標値_raw"] = score_val

    except Exception:
        logging.error("[AI] 分析エラー", exc_info=True)

    return rows

# ==========================================
# ★追加: DBからTOB情報を取得する関数
# ==========================================
def load_tob_titles_map(days=180, conn: sqlite3.Connection | None = None):
    """
    DBの tob_events テーブルから直近days日間の情報を取得し、
    銘柄コードごとのリストにして返す。

    P1-591: dashboard export中は呼出元と同じconnectionを使う。
    別connectionでDB_PATHを開き直すと、current-runの未commit snapshotや
    テスト/代替DB接続を無視して別DBのTOB情報を表示し得る。
    """
    out = {}
    _own_conn = False
    _conn = conn

    if _conn is None:
        db_path = DB_PATH if 'DB_PATH' in globals() else "kani2.db"
        if not Path(db_path).exists():
            return out
        _conn = sqlite3.connect(db_path, timeout=30.0)
        _own_conn = True

    try:
        cur = _conn.cursor()

        # テーブル存在確認
        cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='tob_events'")
        if not cur.fetchone():
            return out

        # 提出時刻の新しい順に取得
        # P1-124: SQLiteのUTC current dateではなくJST日付から履歴cutoffを固定する。
        cutoff_date = (date.fromisoformat(_today_jst()) - timedelta(days=int(days))).isoformat()
        sql = """
            SELECT rowid, コード, タイトル, 提出時刻
            FROM tob_events
            WHERE date(提出時刻) >= date(?) AND date(提出時刻) <= date(?)
            ORDER BY datetime(提出時刻) DESC, rowid DESC
        """
        # P1-493: 実提出イベントなので未来日を採用しない。legacy alias/重複行も
        # canonicalコード+タイトル+時刻で1件に畳み、TOB警告を二重表示しない。
        rows = cur.execute(sql, (cutoff_date, _today_jst())).fetchall()
        _seen_tob = set()
        
        for r in rows:
            # コードの正規化
            raw_code = str(r[1] or "")
            # P1-168: 英字コード/floatコードをTOBマップでも共通正規化。
            code4 = canonical_code_for_db(raw_code)

            title = html.escape(r[2] or "")
            
            # 日付整形 (例: 2026-01-23 -> 01/23)
            ts_str = (r[3] or "").replace("/", "-")
            _tob_key = (code4, title, ts_str[:19])
            if not code4 or _tob_key in _seen_tob:
                continue
            _seen_tob.add(_tob_key)
            if len(ts_str) >= 10:
                date_short = ts_str[5:10].replace("-", "/") 
            else:
                date_short = "--/--"

            # リスト表示用HTML
            display_str = f"<span style='color:#94a3b8; font-family:monospace;'>({date_short})</span> {title}"
            
            if code4 not in out:
                out[code4] = []
            out[code4].append(display_str)
            
    except Exception as e:
        # P1-493: tob_eventsが存在するのに読めない場合、重要イベント警告を
        # 「0件」として黙って消さない。呼出側へ失敗を伝播する。
        raise RuntimeError(f"TOB map build failed: {e}") from e
    finally:
        try:
            cur.close()
        except Exception:
            pass
        if _own_conn:
            try:
                _conn.close()
            except Exception:
                pass

    return out


# ==============================================================
# ★ 修正：信用需給の立体評価（浮動株比率・増減率・需給負荷スコア）の実装
# ==============================================================
def phase_update_margin_metrics(conn: sqlite3.Connection):
    """
    信用残データ、時系列変化（5日/20日/決算前）、浮動株比率、
    信用需給負荷スコア等を一括計算してscreenerテーブルを更新する。
    """
    cur = conn.cursor()
    
    # 新規指標用のカラムを追加
    columns_to_ensure = [
        ("信用倍率", "REAL"), ("売り残", "REAL"), ("買い残", "REAL"), 
        ("需給OH", "REAL"), ("需給安全フラグ", "INTEGER"), ("踏み上げ期待スコア", "REAL"),
        ("信用買い残_浮動株比率", "REAL"), ("信用買い残増減率_5d", "REAL"),
        ("信用買い残増減率_20d", "REAL"), ("決算前20日買い残増加率", "REAL"),
        ("信用需給負荷スコア", "REAL"), ("浮動株数", "REAL")
    ]
    # P2-16: duplicate columnを例外で判定せず、schema確認後に不足列だけ追加する。
    _margin_schema_cols = {r[1] for r in cur.execute("PRAGMA table_info(screener)").fetchall()}
    for col, decl in columns_to_ensure:
        if col not in _margin_schema_cols:
            cur.execute(f'ALTER TABLE screener ADD COLUMN "{col}" {decl}')
            _margin_schema_cols.add(col)

    # 1. 信用残データの時系列を取得（最新、5期前、20期前）
    try:
        sql_margin_hist = """
            SELECT rowid AS _rowid, コード, 基準日, 売り残, 買い残, 倍率
            FROM stock_credit_margin
        """
        margin_hist_df = pd.read_sql_query(sql_margin_hist, conn)
        # P1-463: 7203 / 7203.0 等をcanonical化しただけでは、同一基準日の
        # alias2行が時系列に残る。欠損の少ない行→canonical表記→新rowidを優先して
        # logical code × 基準日を1観測へ畳んでから5d/20dを計算する。
        margin_hist_df['_raw_code'] = margin_hist_df['コード'].astype(str)
        margin_hist_df['コード'] = margin_hist_df['コード'].map(canonical_code_for_db)
        margin_hist_df['基準日'] = pd.to_datetime(margin_hist_df['基準日'], errors='coerce')
        # P1-476: 信用残の基準日は実績日。壊れた未来日/解析不能日を最新観測へ昇格させない。
        _margin_today = pd.Timestamp(date.fromisoformat(_today_jst()))
        margin_hist_df = margin_hist_df[
            margin_hist_df['基準日'].notna() & (margin_hist_df['基準日'].dt.normalize() <= _margin_today)
        ].copy()
        for _mc in ('売り残','買い残','倍率'):
            margin_hist_df[_mc] = pd.to_numeric(margin_hist_df[_mc], errors='coerce')
        margin_hist_df['_quality'] = margin_hist_df[['売り残','買い残','倍率']].notna().sum(axis=1)
        margin_hist_df['_canon_match'] = (margin_hist_df['_raw_code'].str.strip() == margin_hist_df['コード']).astype(int)
        margin_hist_df = (margin_hist_df[margin_hist_df['コード'].astype(str).str.len() > 0]
                          .sort_values(['コード','基準日','_quality','_canon_match','_rowid'], kind='stable')
                          .drop_duplicates(['コード','基準日'], keep='last')
                          .sort_values(['コード','基準日'], ascending=[True,False], kind='stable')
                          .reset_index(drop=True))
        # P1-618: 信用残は週次snapshot。銘柄ごとの「最後にある行」を無期限にcurrent扱いすると、
        # 最新週から欠落した銘柄だけ数週〜数か月前の買い残/売り残を今日の需給・Fair Valueへ混ぜる。
        # 壁時計から7日等を決め打ちせず、DB内で確認できる最新の有効基準日をauthoritative snapshot日とする。
        _margin_snapshot_date = margin_hist_df['基準日'].max() if not margin_hist_df.empty else pd.NaT
    except Exception as e:
        # P1-420: 信用残ソース読込失敗を前回値維持の「正常終了」にしない。
        print(f"[margin][ERROR] stock_credit_margin の読み込みに失敗しました: {e}")
        raise RuntimeError("stock_credit_margin read failed") from e

    # P1-3: 「決算前20日買い残増加率」を本当に決算発表日基準で計算するため、
    # screener の決算発表予定日をコード別に取得する。
    earnings_date_map = {}
    try:
        sc_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
        if "決算発表予定日" in sc_cols:
            edf = pd.read_sql_query('SELECT rowid AS _rowid, コード, 決算発表予定日 FROM screener', conn)
            edf['_raw_code'] = edf['コード'].astype(str)
            edf['コード'] = edf['コード'].map(_normalize_jp_security_code)

            def _parse_earnings_date(v):
                if v is None or (isinstance(v, float) and pd.isna(v)):
                    return pd.NaT
                s = re.sub(r"<[^>]+>", "", str(v)).strip()
                m = re.search(r"(20\d{2})[/-](\d{1,2})[/-](\d{1,2})", s)
                if not m:
                    return pd.NaT
                try:
                    return pd.Timestamp(int(m.group(1)), int(m.group(2)), int(m.group(3)))
                except Exception:
                    return pd.NaT

            edf['決算発表予定日_dt'] = edf['決算発表予定日'].map(_parse_earnings_date)
            # P1-494: legacy screener aliasが同一銘柄で複数残っていても、無順序dict(zip)で
            # 決算予定日を選ばない。有効日→canonical表記→新rowidを優先する。
            edf['_has_date'] = edf['決算発表予定日_dt'].notna().astype(int)
            edf['_canon_match'] = (edf['_raw_code'].str.strip().str.upper() == edf['コード']).astype(int)
            edf = (edf[edf['コード'].astype(str).str.len() > 0]
                   .sort_values(['コード','_has_date','_canon_match','_rowid'], kind='stable')
                   .drop_duplicates('コード', keep='last'))
            earnings_date_map = dict(zip(edf['コード'], edf['決算発表予定日_dt']))
    except Exception as e:
        # 決算前20日買い残増加率だけ欠けたsnapshotを同じmargin更新として公開しない。
        print(f"[margin][ERROR] 決算発表予定日の読み込みに失敗しました: {e}")
        raise RuntimeError("margin earnings-date map build failed") from e

    def _market_business_day_n_before(ts, n=20):
        """日本市場の営業日を n 日戻す。追加休場日も既存設定を利用する。"""
        d = pd.Timestamp(ts).date()
        extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
        cnt = 0
        while cnt < int(n):
            d -= timedelta(days=1)
            try:
                closed = is_jp_market_holiday(d, extra_closed)
            except Exception as e:
                # P1-423: カレンダー障害をweekend-onlyへ格下げして信用残基準日をずらさない。
                raise RuntimeError(f"margin business-day calendar failed for {d}: {e}") from e
            if not closed:
                cnt += 1
        return pd.Timestamp(d)

    # 時系列データの変化率計算
    margin_summary = []
    for code, group in margin_hist_df.groupby('コード'):
        group = group.sort_values('基準日', ascending=False, na_position='last').reset_index(drop=True)
        latest = group.iloc[0]
        latest_date = latest['基準日']
        # P1-618: current信用残は最新の市場snapshotに存在する銘柄だけ。
        # 古い履歴は5d/20d参照用に保持するが、最新週欠落銘柄の旧残高をcurrentへ昇格させない。
        _margin_current = bool(
            pd.notna(_margin_snapshot_date) and pd.notna(latest_date)
            and pd.Timestamp(latest_date).normalize() == pd.Timestamp(_margin_snapshot_date).normalize()
        )
        # P1-41: 最新行の残高欠損を0株へ変換しない。
        buy_latest = float(latest['買い残']) if _margin_current and pd.notna(latest['買い残']) else None
        sell_latest = float(latest['売り残']) if _margin_current and pd.notna(latest['売り残']) else None
        ratio_latest = float(latest['倍率']) if _margin_current and pd.notna(latest['倍率']) else None

        # P1-2: 「n行前」ではなく基準日から日付で選ぶ。
        # stock_credit_margin は週末残高なので、7日前≒5取引日、28日前≒20取引日。
        def _buy_at_or_before(target_date):
            if pd.isna(target_date):
                return None
            cand = group[(group['基準日'].notna()) & (group['基準日'] <= target_date)]
            if cand.empty:
                return None
            v = cand.iloc[0]['買い残']
            return float(v) if pd.notna(v) else None

        if pd.notna(latest_date):
            buy_5d = _buy_at_or_before(latest_date - pd.Timedelta(days=7))
            buy_20d = _buy_at_or_before(latest_date - pd.Timedelta(days=28))
        else:
            # 基準日が壊れている旧データだけは従来の観測順へフォールバック。
            buy_5d = float(group.iloc[1]['買い残']) if len(group) > 1 and pd.notna(group.iloc[1]['買い残']) else None
            buy_20d = float(group.iloc[4]['買い残']) if len(group) > 4 and pd.notna(group.iloc[4]['買い残']) else None
        
        # 増減率 (%)
        diff_5d = ((buy_latest - buy_5d) / buy_5d * 100.0) if buy_latest is not None and buy_5d and buy_5d > 0 else None
        diff_20d = ((buy_latest - buy_20d) / buy_20d * 100.0) if buy_latest is not None and buy_20d and buy_20d > 0 else None

        # P1-3: 決算発表日を終点にした「決算前20営業日」の買い残増加率。
        # 終点は決算日より前の最後の週次信用残（決算後データを混ぜない）。
        # 始点は決算日の20市場営業日前以下で最も新しい週次信用残。
        # まだ20営業日前の窓に入っていない場合は 0 ではなく None とする。
        diff_pre_earnings_20d = None
        event_date = earnings_date_map.get(code)
        if _margin_current and event_date is not None and not pd.isna(event_date):
            event_ts = pd.Timestamp(event_date).normalize()
            pre_event = group[(group['基準日'].notna()) & (group['基準日'] < event_ts)]
            if not pre_event.empty:
                anchor_row = pre_event.iloc[0]
                anchor_date = anchor_row['基準日']
                buy_anchor = float(anchor_row['買い残']) if pd.notna(anchor_row['買い残']) else None
                start_target = _market_business_day_n_before(event_ts, 20)

                # 将来決算の場合、まだ20営業日前に到達していなければ未計算。
                latest_observed_date = latest_date if pd.notna(latest_date) else anchor_date
                if pd.notna(latest_observed_date) and latest_observed_date >= start_target:
                    buy_start = _buy_at_or_before(start_target)
                    if buy_anchor is not None and buy_start is not None and buy_start > 0:
                        diff_pre_earnings_20d = ((buy_anchor - buy_start) / buy_start) * 100.0

        margin_summary.append({
            'コード': code,
            '売り残': sell_latest,
            '買い残': buy_latest,
            '倍率': ratio_latest,
            '信用買い残増減率_5d': diff_5d,
            '信用買い残増減率_20d': diff_20d,
            '決算前20日買い残増加率': diff_pre_earnings_20d
        })
    
    # P1-217: stock_credit_margin が存在するが0行でも、mergeキーを持つ空DFにする。
    # 旧版は DataFrame([]) に「コード」列が無く pd.merge(..., on='コード') が KeyError。
    margin_df = pd.DataFrame(
        margin_summary,
        columns=[
            'コード', '売り残', '買い残', '倍率',
            '信用買い残増減率_5d', '信用買い残増減率_20d', '決算前20日買い残増加率'
        ],
    )

    # 2. screenerから浮動株数、現在値、売買代金を取得
    sc_df = pd.read_sql_query("SELECT コード, 現在値, 売買代金20日平均億, 浮動株数 FROM screener", conn)
    # P1-243: JOINキーはcanonical化するが、UPDATE先のrawコードは保持。
    sc_df['_screener_code_raw'] = sc_df['コード']
    sc_df['コード'] = sc_df['コード'].map(_normalize_jp_security_code)
    
    df = pd.merge(sc_df, margin_df, on='コード', how='left')
    
    # 3. 指標の個別計算
    bb_raw = pd.to_numeric(df['買い残'], errors='coerce')
    bs_raw = pd.to_numeric(df['売り残'], errors='coerce')
    price = pd.to_numeric(df['現在値'], errors='coerce').fillna(0)
    turn20 = pd.to_numeric(df['売買代金20日平均億'], errors='coerce').fillna(0)
    float_shares = pd.to_numeric(df['浮動株数'], errors='coerce')
    
    # 20日平均出来高（株数）
    v20 = np.where(price > 0, (turn20 * 1e8) / price, np.nan)
    v20 = np.where(v20 == 0, np.nan, v20)
    
    # ① 需給OH (Days to Cover)
    df['需給OH'] = np.where(bb_raw.notna() & np.isfinite(v20), bb_raw / v20, np.nan)
    reported_ratio = pd.to_numeric(df['倍率'], errors='coerce')
    calc_ratio = pd.Series(np.nan, index=df.index, dtype=float)
    both_known = bb_raw.notna() & bs_raw.notna()
    calc_ratio.loc[both_known & (bs_raw > 0)] = bb_raw.loc[both_known & (bs_raw > 0)] / bs_raw.loc[both_known & (bs_raw > 0)]
    calc_ratio.loc[both_known & (bs_raw == 0) & (bb_raw > 0)] = 999.9
    df['信用倍率_calc'] = reported_ratio.combine_first(calc_ratio)

    # ② 信用買い残 ÷ 浮動株数 (%)
    df['信用買い残_浮動株比率'] = np.where(
        bb_raw.notna() & (float_shares.notna()) & (float_shares > 0),
        (bb_raw / float_shares) * 100.0, np.nan
    )

    # ③ 信用需給負荷スコア
    # 通常: 浮動株比率40% + 出来高回転日数30% + 20日買い残急増率30%
    # P1-4: 浮動株数が無い時に需給OHを二重計上せず、残る独立因子を再配分する。
    # P1-17: 20日買い残増減率の欠損を 0%（増加なし＝安全）として扱わない。
    #         各因子の「欠損」と「実測0点」を分離し、利用可能な独立因子が2つ以上ある時だけ
    #         既知因子の配点を100点スケールへ比例再配分する。1因子以下なら未計算(None)。
    float_ratio_num = pd.to_numeric(df['信用買い残_浮動株比率'], errors='coerce')
    oh_num = pd.to_numeric(df['需給OH'], errors='coerce')
    inc20_num = pd.to_numeric(df['信用買い残増減率_20d'], errors='coerce')

    has_float_ratio = float_ratio_num.notna()
    has_oh = oh_num.notna()
    has_inc20 = inc20_num.notna()

    score_float = pd.Series(np.clip(float_ratio_num.fillna(0.0) / 10.0 * 40.0, 0, 40), index=df.index)
    score_oh = pd.Series(np.clip(oh_num.fillna(0.0) / 4.0 * 30.0, 0, 30), index=df.index)
    score_inc = pd.Series(np.clip(inc20_num.fillna(0.0) / 50.0 * 30.0, 0, 30), index=df.index)

    # 欠損因子は「0点」ではなく配点母数から除外する。
    score_float = score_float.where(has_float_ratio, 0.0)
    score_oh = score_oh.where(has_oh, 0.0)
    score_inc = score_inc.where(has_inc20, 0.0)

    available_weight = (
        has_float_ratio.astype(float) * 40.0
        + has_oh.astype(float) * 30.0
        + has_inc20.astype(float) * 30.0
    )
    available_count = (
        has_float_ratio.astype(int)
        + has_oh.astype(int)
        + has_inc20.astype(int)
    )
    known_score = score_float + score_oh + score_inc

    normalized_score = pd.Series(np.nan, index=df.index, dtype=float)
    enough_info = (available_count >= 2) & (available_weight > 0)
    normalized_score.loc[enough_info] = np.clip(
        known_score.loc[enough_info] * (100.0 / available_weight.loc[enough_info]),
        0, 100
    )
    df['信用需給負荷スコア'] = normalized_score.round(1)

    # 需給安全フラグ・踏み上げ期待スコア
    cond_ratio = df['信用倍率_calc'].between(1.0, 3.0)
    cond_overhang = df['需給OH'] <= 3.0
    # P1-553: 信用倍率/需給OHのどちらかが欠損なら「危険=0」と確定しない。
    # 需給安全フラグは 1=安全 / 0=条件外 / NULL=判定不能 を明示的に分離する。
    _safety_known = df['信用倍率_calc'].notna() & pd.to_numeric(df['需給OH'], errors='coerce').notna()
    df['需給安全フラグ'] = pd.Series(np.nan, index=df.index, dtype=float)
    df.loc[_safety_known, '需給安全フラグ'] = (cond_ratio & cond_overhang).loc[_safety_known].astype(int)

    # P1-24: 踏み上げ期待を売り残の絶対株数(1万/5万株)で判定しない。
    # 同じ5万株でも小型薄商いと大型高流動性では意味が全く異なるため、
    # 「平常出来高の何日分」または「浮動株の何%」で量を0-1へ正規化する。
    sell_days = pd.Series(np.where(bs_raw.notna() & (v20 > 0), bs_raw / v20, np.nan), index=df.index, dtype=float)
    sell_float_pct = pd.Series(
        np.where(bs_raw.notna() & (float_shares.notna()) & (float_shares > 0), bs_raw / float_shares * 100.0, np.nan),
        index=df.index, dtype=float
    )
    qty_by_days = (sell_days / 1.0).clip(0.0, 1.0)      # 平常出来高1日分で量100%
    qty_by_float = (sell_float_pct / 0.50).clip(0.0, 1.0)  # 浮動株0.5%で量100%
    squeeze_qty = pd.concat([qty_by_days, qty_by_float], axis=1).max(axis=1, skipna=True)
    squeeze_qty = squeeze_qty.where(qty_by_days.notna() | qty_by_float.notna(), np.nan)

    _margin_ratio_for_squeeze = pd.to_numeric(df['信用倍率_calc'], errors='coerce')
    _ratio_known_for_squeeze = _margin_ratio_for_squeeze.notna() & np.isfinite(_margin_ratio_for_squeeze) & (_margin_ratio_for_squeeze >= 0)
    ratio_component = pd.Series(
        np.select(
            [_margin_ratio_for_squeeze <= 1.0, _margin_ratio_for_squeeze <= 2.0, _margin_ratio_for_squeeze <= 3.0],
            [100.0, 75.0, 40.0],
            default=0.0,
        ),
        index=df.index, dtype=float,
    ).where(_ratio_known_for_squeeze, np.nan)
    # P3-38: 信用倍率不明は「倍率>3で踏み上げ余地0」と同義ではない。
    squeeze_score = ratio_component * squeeze_qty
    # 売り残0株が実測なら0点。売り残>0なのに比較分母が無い場合は未計算。
    squeeze_score = squeeze_score.where(bs_raw.notna(), np.nan)
    squeeze_score = squeeze_score.where(~((bs_raw > 0) & squeeze_qty.isna()), np.nan)
    squeeze_score = squeeze_score.where(~(bs_raw.notna() & (bs_raw <= 0)), 0.0)
    df['踏み上げ期待スコア'] = squeeze_score.round(1)
    
    # DBへの UPDATE 実行
    updates = []
    for _, r in df.iterrows():
        updates.append((
            float(r['倍率']) if pd.notna(r['倍率']) else None,
            float(r['売り残']) if pd.notna(r['売り残']) else None,
            float(r['買い残']) if pd.notna(r['買い残']) else None,
            float(r['需給OH']) if pd.notna(r['需給OH']) else None,
            int(r['需給安全フラグ']) if pd.notna(r['需給安全フラグ']) else None,
            # P1-45: 未計算の踏み上げ期待を0点としてDBへ戻さない。
            float(r['踏み上げ期待スコア']) if pd.notna(r['踏み上げ期待スコア']) else None,
            float(r['信用買い残_浮動株比率']) if pd.notna(r['信用買い残_浮動株比率']) else None,
            float(r['信用買い残増減率_5d']) if pd.notna(r['信用買い残増減率_5d']) else None,
            float(r['信用買い残増減率_20d']) if pd.notna(r['信用買い残増減率_20d']) else None,
            float(r['決算前20日買い残増加率']) if pd.notna(r['決算前20日買い残増加率']) else None,
            float(r['信用需給負荷スコア']) if pd.notna(r['信用需給負荷スコア']) else None,
            str(r['_screener_code_raw'])
        ))
        
    if updates:
        _sp = f"sp_margin_metrics_{time.time_ns()}"
        conn.execute(f"SAVEPOINT {_sp}")
        try:
            cur.executemany("""
                UPDATE screener
                SET 信用倍率=?, 売り残=?, 買い残=?, 需給OH=?, 需給安全フラグ=?, 踏み上げ期待スコア=?,
                    信用買い残_浮動株比率=?, 信用買い残増減率_5d=?, 信用買い残増減率_20d=?,
                    決算前20日買い残増加率=?, 信用需給負荷スコア=?
                WHERE コード=?
            """, updates)
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
            raise
        # P1-426: 信用指標の多行書戻しを部分更新にしない。
    cur.close()
    print(f"[margin] 信用需給多角指標（浮動株比率・時系列増減・負荷スコア）を更新しました: {len(updates)} 銘柄")
    
# ==== [Short-term Trading Enhancements] Derived Metrics (schema assumed) ====

def _apply_shortterm_metrics(conn: sqlite3.Connection):

    cur = conn.cursor()
    try:
        hist_tbl = "price_history"
        cur.execute("SELECT rowid, コード, 日付, 終値, signal_date, シグナル更新日 FROM latest_prices")
        rows = cur.fetchall()
        col_names = [d[0] for d in cur.description]

        def idx(name, default=-1):
            try: return col_names.index(name)
            except ValueError: return default

        i_code = idx("コード")
        i_date = idx("日付")
        i_close = idx("終値") if idx("終値") != -1 else idx("現在値")
        i_sig = idx("signal_date")
        if i_sig == -1: i_sig = idx("シグナル更新日")
        
        print("[derive-update] ATR等の計算を一括処理中...")

        # --- BULK FETCH (直近120日分を一括で取得) ---
        # P1-580: 短期指標も市場内MAX日ではなくrun mode基準日へ固定。
        # PREOPENにlegacy当日足が一部だけあっても、その1銘柄だけ当日ATRへ進めない。
        _asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        q_hist = f"""
            SELECT rowid AS _rowid, コード, 日付, 高値, 安値, 終値
            FROM {hist_tbl}
            WHERE date(日付) >= date(?, '-120 days') AND date(日付) <= date(?)
            ORDER BY コード, 日付, rowid
        """
        # P1-406: ATR/最高値もlegacy未来日を系列末尾にしない。
        df_all = pd.read_sql_query(q_hist, conn, params=[_asof, _asof], parse_dates=["日付"])
        if df_all.empty:
            return
            
        for c in ("高値", "安値", "終値"):
            df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
        # P1-280: aliasを分けたままATRを計算すると系列が途中で分断される。
        df_all = _dedupe_price_history_df(df_all)
        # P1-676: ATRも有効な正の終値を持つ観測行だけで計算する。
        # NULL/0/非有限終値のplaceholderがprev_close/TRや観測数を歪めないよう除外。
        df_all = df_all[
            df_all["終値"].notna()
            & np.isfinite(df_all["終値"])
            & (df_all["終値"] > 0)
        ].copy()
        if df_all.empty:
            return
        # ATR14の一括ベクトル計算
        grp = df_all.groupby("コード", sort=False)
        df_all["prev_close"] = grp["終値"].shift(1)
        
        tr1 = (df_all["高値"] - df_all["安値"]).abs()
        tr2 = (df_all["高値"] - df_all["prev_close"]).abs()
        tr3 = (df_all["安値"] - df_all["prev_close"]).abs()
        df_all["TR"] = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        df_all["ATR14"] = grp["TR"].transform(lambda x: x.ewm(span=14, adjust=False, min_periods=1).mean())

        updates = []
        # P1-258: latest_prices/historyのlegacy表記差を吸収してATR等を結合。
        df_all["_code_key"] = df_all["コード"].map(canonical_code_for_db)
        df_dict = {code: g for code, g in df_all.groupby("_code_key", sort=False) if code}

        for r in rows:
            code = r[i_code]
            ldate = r[i_date]
            close_v = r[i_close]
            sig_val = r[i_sig] if i_sig != -1 else None

            hdf = df_dict.get(canonical_code_for_db(code))
            if hdf is None or hdf.empty:
                updates.append((None, None, None, r[0]))
                continue
            # P1-546: 全体as-ofはfreshでも個別銘柄だけ最終足が古い場合、
            # stale系列からATR/SignalHighをcurrent metricsとして再計算しない。
            try:
                _hdf_last = pd.to_datetime(hdf["日付"], errors="coerce").dropna().max().date()
                _asof_date = date.fromisoformat(str(_asof)[:10])
            except Exception:
                _hdf_last = None
                _asof_date = None
            if _hdf_last is None or _asof_date is None or _hdf_last != _asof_date:
                updates.append((None, None, None, r[0]))
                continue
            
            atr14 = float(hdf["ATR14"].iloc[-1]) if not pd.isna(hdf["ATR14"].iloc[-1]) else None
            
            signal_date = pd.to_datetime(str(sig_val), errors="coerce") if sig_val is not None else pd.NaT

            if pd.isna(signal_date):
                h2 = hdf.tail(60)
            else:
                h2 = hdf[hdf["日付"] >= signal_date]

            if h2.empty:
                max_high = np.nan
                idx_max_dt = pd.to_datetime(ldate, errors="coerce")
            else:
                hi_num = h2["高値"]
                if hi_num.dropna().empty:
                    max_high = np.nan
                    idx_max_dt = pd.to_datetime(ldate, errors="coerce")
                else:
                    max_high = float(hi_num.max())
                    idx_max_row = hi_num.idxmax()
                    idx_max_dt = pd.to_datetime(h2.loc[idx_max_row, "日付"], errors="coerce")

            if close_v is not None:
                try: cur_close = float(close_v)
                except Exception: cur_close = np.nan
            else:
                if not h2.empty and h2["終値"].dropna().size > 0:
                    cur_close = float(h2["終値"].dropna().iloc[-1])
                elif not hdf.empty and hdf["終値"].dropna().size > 0:
                    cur_close = float(hdf["終値"].dropna().iloc[-1])
                else:
                    cur_close = np.nan

            if not np.isfinite(cur_close) or not np.isfinite(max_high) or max_high == 0.0:
                rate_since = None
            else:
                rate_since = (cur_close / max_high - 1.0) * 100.0

            base_date = pd.to_datetime(ldate, errors="coerce")
            if pd.isna(base_date) or pd.isna(idx_max_dt):
                days_since = None
            else:
                try: days_since = int((base_date - idx_max_dt).days)
                except Exception: days_since = None

            updates.append((atr14, rate_since, days_since, r[0]))

        # P1-449: latest_prices短期指標を全行同一snapshotで反映。
        cur.execute("SAVEPOINT p1_449_shortterm_metrics")
        try:
            cur.executemany(
                "UPDATE latest_prices "
                "SET ATR_14 = ?, Rate_Since_Signal_High = ?, Days_Since_Signal_High = ? "
                "WHERE rowid = ?",
                updates
            )
            cur.execute("RELEASE SAVEPOINT p1_449_shortterm_metrics")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_449_shortterm_metrics")
                cur.execute("RELEASE SAVEPOINT p1_449_shortterm_metrics")
            except Exception:
                pass
            raise
        print(f"[derive-update] ATR_14/Rate/Days updated: {len(updates)} rows")

    finally:
        try: cur.close()
        except Exception: pass

# ==== 最新行同期（price_history -> latest_prices） ====
def _cleanup_latest_prices_logical_duplicates(conn: sqlite3.Connection) -> int:
    """P1-295: latest_prices に既存のlogical duplicateがあれば情報を極力保って1行へ統合。"""
    _sp = f"sp_latest_alias_{time.time_ns()}"
    _sp_open = False
    try:
        conn.execute(f"SAVEPOINT {_sp}")
        _sp_open = True
        cols=[r[1] for r in conn.execute("PRAGMA table_info(latest_prices)").fetchall()]
        if 'コード' not in cols:
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
            return 0
        rows=conn.execute("SELECT rowid, * FROM latest_prices ORDER BY rowid").fetchall()
        if not rows:
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
            return 0
        names=['rowid']+cols
        groups={}
        for row in rows:
            rec=dict(zip(names,row))
            key=canonical_code_for_db(rec.get('コード'))
            if key:
                groups.setdefault(key,[]).append(rec)
        # screener raw表記があればsurvivor選択に優先利用。
        sraw={}
        try:
            for (rc,) in conn.execute("SELECT コード FROM screener").fetchall():
                k=canonical_code_for_db(rc)
                if k and k not in sraw: sraw[k]=str(rc)
        except Exception:
            pass
        merged_count=0
        data_cols=[c for c in cols if c!='コード']
        _lp_extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
        def nonempty(v):
            return v is not None and (not isinstance(v,str) or v.strip()!='')
        def _valid_lp_date(v):
            if v is None or str(v).strip()=="":
                return 0
            try:
                _d = date.fromisoformat(str(v)[:10])
                return 1 if not is_jp_market_holiday(_d, _lp_extra_closed) else 0
            except Exception:
                return 0
        for key,recs in groups.items():
            if len(recs)<=1:
                continue
            raw_pref=sraw.get(key)
            def rank(r):
                datev=str(r.get('日付') or '')
                valid_date=_valid_lp_date(r.get('日付'))
                complete=sum(nonempty(r.get(c)) for c in data_cols)
                rawmatch=1 if raw_pref is not None and str(r.get('コード'))==raw_pref else 0
                canonmatch=1 if str(r.get('コード')).strip().upper()==key.upper() else 0
                # P1-456: legacy土日行より、表記がaliasでも実営業日rowを優先。
                return (valid_date,rawmatch,datev,complete,canonmatch,int(r['rowid']))
            survivor=max(recs,key=rank)
            others=[r for r in recs if r['rowid']!=survivor['rowid']]
            # survivorの欠損だけ、より新しく情報量の多いduplicateから補完する。
            donors=sorted(others,key=rank,reverse=True)
            merged={c:survivor.get(c) for c in data_cols}
            for c in data_cols:
                if nonempty(merged.get(c)):
                    continue
                for d in donors:
                    if nonempty(d.get(c)):
                        merged[c]=d.get(c); break
            if data_cols:
                set_sql=','.join(f'"{c}"=?' for c in data_cols)
                conn.execute(f'UPDATE latest_prices SET {set_sql} WHERE rowid=?',
                             tuple(merged.get(c) for c in data_cols)+(survivor['rowid'],))
            delete_ids=[r['rowid'] for r in others]
            if delete_ids:
                ph=','.join('?'*len(delete_ids))
                conn.execute(f'DELETE FROM latest_prices WHERE rowid IN ({ph})',delete_ids)
                merged_count+=len(delete_ids)
        conn.execute(f"RELEASE SAVEPOINT {_sp}")
        return merged_count
    except Exception as e:
        if _sp_open:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
        print('[latest_prices][ERROR] logical duplicate cleanup failed:',e)
        raise RuntimeError("latest_prices logical duplicate cleanup failed") from e


def phase_sync_latest_prices(conn: sqlite3.Connection):
    """
    Sync the latest logical row per canonical code from price_history into latest_prices.
    Only updates 日付 and 終値; other columns remain as-is.
    """
    cur = conn.cursor()
    try:
        cur.execute("PRAGMA foreign_keys = ON")
        _merged_lp = _cleanup_latest_prices_logical_duplicates(conn)
        if _merged_lp:
            print(f"[sync-latest] P1-295 legacy duplicate rows merged={_merged_lp}")
        # P1-273: price_history=7203 / latest_prices=7203.0 の場合に
        # canonical行を別途INSERTして同一銘柄2行にしない。既存raw行を優先更新する。
        # P1-580: latest_prices同期もPREOPENは前営業日まで。legacy当日行を昇格させない。
        _cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        # P1-404: legacy未来/休日行をlatest_pricesへ昇格させない。
        # 最新1論理足しか使わないのに全期間履歴をDataFrameへ読む旧処理を廃止。
        # raw aliasごとの直近8行だけSQL側で絞り、休日/alias重複は従来どおり
        # _dedupe_price_history_df() でlogical codeへ統合する。
        _ph = pd.read_sql_query(
            """
            WITH ranked AS (
                SELECT rowid AS _rowid, コード, 日付, 終値,
                       ROW_NUMBER() OVER (
                           PARTITION BY CAST(コード AS TEXT)
                           ORDER BY date(日付) DESC, rowid DESC
                       ) AS _rn
                FROM price_history
                WHERE date(日付) <= date(?)
            )
            SELECT _rowid, コード, 日付, 終値
            FROM ranked
            WHERE _rn <= 8
            """,
            conn, params=[_cutoff])
        if _ph.empty:
            # P1-497: screenerがあるのに有効価格履歴0件でreturnすると、旧latest_pricesを
            # そのまま今回snapshotとして公開し得る。空ユニバース時だけ正常skip。
            _sc_n = int(conn.execute("SELECT COUNT(*) FROM screener").fetchone()[0] or 0)
            if _sc_n > 0:
                raise RuntimeError("price_history has no rows for latest_prices sync; refusing stale latest_prices")
            return
        # P1-308: 同日aliasでは非NULL/ canonicalを優先した1本を選んでから最新日を決める。
        _ph = _dedupe_price_history_df(_ph)
        if _ph.empty:
            _sc_n = int(conn.execute("SELECT COUNT(*) FROM screener").fetchone()[0] or 0)
            if _sc_n > 0:
                raise RuntimeError("price_history has no valid JPX business-day rows; refusing stale latest_prices")
            return
        _ph["_key"] = _ph["コード"].map(canonical_code_for_db)
        _ph["_date_sort"] = pd.to_datetime(_ph["日付"], errors="coerce")
        _ph = (_ph[_ph["_key"] != ""]
               .sort_values(["_key", "_date_sort"], kind="stable")
               .drop_duplicates("_key", keep="last"))
        _lp_rows = cur.execute("SELECT rowid, コード FROM latest_prices ORDER BY rowid").fetchall()
        _lp_raw = {}
        for _rid, _rc in _lp_rows:
            _rk = canonical_code_for_db(_rc)
            if _rk and _rk not in _lp_raw:
                _lp_raw[_rk] = _rc
        _updates = []
        _inserts = []
        for _, _r in _ph.iterrows():
            _key = _r["_key"]
            _raw = _lp_raw.get(_key)
            if _raw is not None:
                _updates.append((_r["日付"], _r["終値"], _raw))
            else:
                _inserts.append((_key, _r["日付"], _r["終値"]))
                _lp_raw[_key] = _key
        _sp = f"sp_sync_latest_{time.time_ns()}"
        conn.execute(f"SAVEPOINT {_sp}")
        try:
            if _updates:
                cur.executemany("UPDATE latest_prices SET 日付=?, 終値=? WHERE コード=?", _updates)
            if _inserts:
                cur.executemany("INSERT INTO latest_prices(コード,日付,終値) VALUES(?,?,?)", _inserts)
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
            raise
        # P1-418: autocommit接続でもlatest syncの複数行更新を部分適用にしない。
        print(f"[sync-latest] latest_prices synced logical={len(_updates)+len(_inserts)}")
    except Exception as e:
        conn.rollback()
        print("[sync-latest][ERROR]", e)
        raise RuntimeError("latest_prices synchronization failed") from e
    finally:
        try: cur.close()
        except Exception: pass

def phase_shortterm_enhancements(conn: sqlite3.Connection):
    """
    既存フローの任意の場所（派生値更新の直後など）で呼び出してください。
    - ATR(14) と since-signal を計算して latest_prices に反映（スキーマは既に整備済み前提）
    """
    _apply_shortterm_metrics(conn)
# ==== Resistance lines (水平/斜め) : price_history → latest_prices ====


def _res__pivot_highs_lows(df: pd.DataFrame, is_support: bool = False, order: int = 3):
    """谷（安値）または山（高値）のピボット点を抽出"""
    if df.empty: 
        return []
    col = "安値" if is_support else "高値"
    vals = df[col].to_numpy()
    n = len(df)
    mask = np.zeros(n, dtype=bool)
    for i in range(order, n - order):
        left = vals[i - order:i]
        right = vals[i + 1:i + 1 + order]
        if is_support:
            mask[i] = (vals[i] <= left.min()) and (vals[i] <= right.min())
        else:
            mask[i] = (vals[i] >= left.max()) and (vals[i] >= right.max())
    return df.loc[mask, col].tolist()

def _res__build_touch_zones(df: pd.DataFrame, close_today: float, is_support: bool = False):
    """過去の価格帯からタッチ数が多いゾーンを抽出（現在値距離ペナルティ考慮）"""
    if df.empty: return []
    
    piv = _res__pivot_highs_lows(df, is_support, order=3)
    heads = df["安値"].nsmallest(10).tolist() if is_support else df["高値"].nlargest(10).tolist()
    
    # 💡 追加：直近のローソク足も強制的にシード(基準)に加え、急騰後の浅い押し目を取りこぼさないようにする
    recent = df["安値"].tail(20).tolist() if is_support else df["高値"].tail(20).tolist()
    seeds = sorted(list(set(piv + heads + recent)))
    
    merged_seeds = []
    if seeds:
        cur_group = [seeds[0]]
        for x in seeds[1:]:
            if abs(x - np.mean(cur_group)) / max(1, np.mean(cur_group)) <= RES_ZONE_MERGE_PCT:
                cur_group.append(x)
            else:
                merged_seeds.append(float(np.mean(cur_group)))
                cur_group = [x]
        merged_seeds.append(float(np.mean(cur_group)))

    out = []
    for center in merged_seeds:
        lo, hi = center * (1 - RES_TOUCH_BAND_PCT), center * (1 + RES_TOUCH_BAND_PCT)
        touch_mask = (df["高値"] >= lo) & (df["安値"] <= hi)
        touches = int(touch_mask.sum())
        
        if touches >= RES_MIN_TOUCHES:
            # 現在値からの距離ペナルティ
            distance = abs(center - close_today) / close_today
            score = touches / (1 + distance * 10)  # 遠いほどスコア激減
            
            last_idx = np.where(touch_mask.values)[0][-1]
            out.append({
                "price": float(center),
                "touches": touches,
                "score": score,
                "last_date": pd.to_datetime(df.iloc[last_idx]["日付"]).date()
            })
            
    # 支持・抵抗でフィルタ（現在値より上か下か）
    if is_support:
        out = [z for z in out if z["price"] <= close_today]
    else:
        out = [z for z in out if z["price"] > close_today]

    # スコア（タッチ数と近さのバランス）で降順ソート
    out.sort(key=lambda x: -x["score"])
    return out

def derive_support_resistance_full(df: pd.DataFrame):
    if df.empty or len(df) < 5:
        return {}
    
    # P1-62: 25日/75日線は必要本数が揃った時だけ計算する。
    # 5日履歴を「75日線」と呼んで支持抵抗候補へ入れない。
    df = df.copy()
    df["MA25"] = df["終値"].rolling(25, min_periods=25).mean()
    df["MA75"] = df["終値"].rolling(75, min_periods=75).mean()

    current_price = float(df["終値"].iloc[-1])
    res_hh = float(df["高値"].max())
    sup_ll = float(df["安値"].min())
    
    high20 = float(df["高値"].tail(20).max())
    low20  = float(df["安値"].tail(20).min())
    high60 = float(df["高値"].tail(60).max())
    low60  = float(df["安値"].tail(60).min())
    ma25_v = df["MA25"].iloc[-1]
    ma75_v = df["MA75"].iloc[-1]
    ma25 = float(ma25_v) if pd.notna(ma25_v) else None
    ma75 = float(ma75_v) if pd.notna(ma75_v) else None
    
    # ゾーン計算（スコア順で一番上がベスト）
    res_zones = _res__build_touch_zones(df, current_price, is_support=False)
    sup_zones = _res__build_touch_zones(df, current_price, is_support=True)
    best_res = res_zones[0] if res_zones else None
    best_sup = sup_zones[0] if sup_zones else None

    # 適切な節目計算
    if current_price < 500: step = 10
    elif current_price < 2000: step = 50
    elif current_price < 5000: step = 100
    elif current_price < 10000: step = 500
    else: step = 1000
    res_round = math.ceil(current_price / step) * step
    sup_round = math.floor(current_price / step) * step
    if res_round == current_price: res_round += step
    if sup_round == current_price: sup_round -= step

    # トレンドライン抵抗
    xs = np.arange(len(df), dtype=float)
    ys_high = df["高値"].to_numpy(dtype=float)
    slope_h, intercept_h = np.polyfit(xs, ys_high, 1)
    res_line_today = float(slope_h * len(xs) + intercept_h)
    
    ys_low = df["安値"].to_numpy(dtype=float)
    slope_l, intercept_l = np.polyfit(xs, ys_low, 1)
    sup_line_today = float(slope_l * len(xs) + intercept_l)

    # 💡 Tom提案のコアロジック２：多彩な候補から「最も近いもの」を選ぶ
    res_candidates = [
        best_res["price"] if best_res else None,
        res_hh, high20, high60,
        ma25 if ma25 is not None and ma25 > current_price else None,
        ma75 if ma75 is not None and ma75 > current_price else None,
        res_line_today, res_round
    ]
    res_candidates = [x for x in res_candidates if x is not None and x > current_price]
    res_near = min(res_candidates, key=lambda x: abs(x - current_price)) if res_candidates else None

    sup_candidates = [
        best_sup["price"] if best_sup else None,
        sup_ll, low20, low60,
        ma25 if ma25 is not None and ma25 < current_price else None,
        ma75 if ma75 is not None and ma75 < current_price else None,
        sup_line_today, sup_round
    ]
    sup_candidates = [x for x in sup_candidates if x is not None and x < current_price]
    sup_near = min(sup_candidates, key=lambda x: abs(x - current_price)) if sup_candidates else None

    return {
        "Res_HH": res_hh,
        "Res_Zone": best_res["price"] if best_res else None,
        "Res_Zone_Touches": best_res["touches"] if best_res else None,
        "Res_Zone_Last": str(best_res["last_date"]) if best_res else None,
        "Res_Round": res_round,
        "Res_Line_Today": res_line_today,
        "Res_Nearest": res_near,
        
        "Sup_LL": sup_ll,
        "Sup_Zone": best_sup["price"] if best_sup else None,
        "Sup_Zone_Touches": best_sup["touches"] if best_sup else None,
        "Sup_Zone_Last": str(best_sup["last_date"]) if best_sup else None,
        "Sup_Round": sup_round,
        "Sup_Nearest": sup_near,
    }

def _ensure_resistance_columns(conn):
    cur = conn.cursor()
    _sp = "p2_64_resistance_cols"
    try:
        cur.execute(f"SAVEPOINT {_sp}")
        cols = [r[1] for r in cur.execute("PRAGMA table_info(latest_prices)")]
        needs = [
            "Res_HH", "Res_Zone", "Res_Zone_Touches", "Res_Zone_Last",
            "Res_Round", "Res_Round_Step", "Res_Round_Near",
            "Res_Line_Today", "Res_Line_R2", "Res_Nearest", "S_High_Status",
            "Sup_LL", "Sup_Zone", "Sup_Zone_Touches", "Sup_Zone_Last",
            "Sup_Round", "Sup_Round_Step", "Sup_Round_Near",
            "Sup_Line_Today", "Sup_Line_R2", "Sup_Nearest"
        ]
        for c in needs:
            if c not in cols:
                cur.execute(f"ALTER TABLE latest_prices ADD COLUMN {c} TEXT")
        if "ATR_14" not in cols:
            cur.execute("ALTER TABLE latest_prices ADD COLUMN ATR_14 REAL")
        cur.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            cur.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        raise
    finally:
        cur.close()

def phase_resistance_update(conn):
    _ensure_resistance_columns(conn)
    _ensure_latest_prices_code_col(conn)
    _ensure_latest_prices_index_rows(conn)

    cur = conn.cursor()
    try:
        codes = [r[0] for r in cur.execute("SELECT DISTINCT コード FROM latest_prices")]
        if not codes:
            return

        # P1-259: 計算はcanonical、UPDATEはlatest_pricesに実在するrawキーへ戻す。
        _lp_raw = {}
        for _rc in codes:
            _rk = canonical_code_for_db(_rc)
            if _rk and _rk not in _lp_raw:
                _lp_raw[_rk] = _rc

        print(f"[resistance] {len(codes)}銘柄のサポレジを一括計算中...")
        lookback_days = RES_LOOKBACK_DAYS + 10

        # P1-405: legacy future/weekend行を市場基準日にしない。
        # P1-576: 市場内の「最大日」ではなくrun modeのauthoritative snapshot日を使う。
        # PREOPENは前営業日、MIDDAY/EODは当日。PREOPENにlegacy当日行が1本だけ残っていても
        # 全銘柄の前営業日サポレジ/S高を誤って消さない。
        market_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        start_date = (
            date.fromisoformat(str(market_asof)[:10]) - timedelta(days=lookback_days)
        ).isoformat()

        # P1-421: DBを書き換える前に今回値をすべてメモリ上で完成させる。
        # 計算途中の例外/returnで前回値を全NULL化しない。
        df_all = pd.read_sql_query("""
            SELECT rowid AS _rowid, コード, 日付, 始値, 高値, 安値, 終値
            FROM price_history
            WHERE date(日付) >= date(?) AND date(日付) <= date(?)
            ORDER BY 日付, rowid ASC
        """, conn, params=[start_date, market_asof], parse_dates=["日付"])

        if not df_all.empty:
            for c in ("始値", "高値", "安値", "終値"):
                df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
            df_all = _dedupe_price_history_df(df_all)
            df_all = df_all.dropna(subset=["高値", "安値", "終値"])
            # P1-676: 支持抵抗も破損した0/非有限終値をcurrent観測日にしない。
            df_all = df_all[np.isfinite(df_all["終値"]) & (df_all["終値"] > 0)].copy()

        def _v5_sqlite_value(value):
            """numpy/pandas scalarをsqlite3が直接bindできる標準型へ変換する。"""
            if value is None:
                return None
            if isinstance(value, (pd.Timestamp, datetime, date)):
                return str(value)[:19]
            if isinstance(value, np.generic):
                value = value.item()
            if isinstance(value, float) and not math.isfinite(value):
                return None
            if isinstance(value, bool):
                return int(value)
            return value

        updates = []
        if not df_all.empty:
            for code, df in df_all.groupby("コード", sort=False):
                if not code or code not in _lp_raw:
                    continue
                code_asof = str(pd.to_datetime(df["日付"], errors="coerce").max().date())
                if code_asof != str(market_asof)[:10]:
                    continue
                df = df.tail(RES_LOOKBACK_DAYS)
                if df.empty:
                    continue

                sh_val = ""
                try:
                    sh_val = analyze_stop_high_status(df)
                except Exception:
                    pass
                # HTML exportで同じ銘柄を個別SQL付きで再計算しない。
                # ここで一括取得済みdfから、最終表示と同じV5計算を完成させる。
                _v5_rows = list(
                    df[["日付", "始値", "高値", "安値", "終値"]]
                    .itertuples(index=False, name=None)
                )
                res_data = _v5_calc(_v5_rows, code)
                if not res_data:
                    continue
                # P2-84: 支持抵抗と同じcurrent履歴からATR14も最終確定する。
                # shorttermフェーズの中間値に依存せず、公開直前のauthoritative
                # snapshotでATR/tri_volが全件欠損になる経路を塞ぐ。
                _atr_high = pd.to_numeric(df["高値"], errors="coerce")
                _atr_low = pd.to_numeric(df["安値"], errors="coerce")
                _atr_close = pd.to_numeric(df["終値"], errors="coerce")
                _atr_prev = _atr_close.shift(1)
                _atr_tr = pd.concat(
                    [
                        (_atr_high - _atr_low).abs(),
                        (_atr_high - _atr_prev).abs(),
                        (_atr_low - _atr_prev).abs(),
                    ],
                    axis=1,
                ).max(axis=1)
                _atr_series = _atr_tr.ewm(span=14, adjust=False, min_periods=1).mean()
                _atr14 = (
                    float(_atr_series.iloc[-1])
                    if len(_atr_series) and pd.notna(_atr_series.iloc[-1])
                    else None
                )
                updates.append(
                    tuple(_v5_sqlite_value(res_data.get(_c)) for _c in _V5_COLS)
                    + (_v5_sqlite_value(_atr14), sh_val, _lp_raw.get(code, code))
                )

        # P1-421: clear + 今回値writeを同一SAVEPOINTで一括適用。
        _res_sp = f"sp_resistance_{time.time_ns()}"
        conn.execute(f"SAVEPOINT {_res_sp}")
        try:
            _v5_clear = ", ".join(f'"{_c}"=NULL' for _c in _V5_COLS)
            cur.execute(f'UPDATE latest_prices SET {_v5_clear}, ATR_14=NULL, S_High_Status=NULL')
            if updates:
                _v5_set = ", ".join(f'"{_c}"=?' for _c in _V5_COLS)
                cur.executemany(
                    f'UPDATE latest_prices SET {_v5_set}, ATR_14=?, S_High_Status=? WHERE コード=?',
                    updates,
                )
            conn.execute(f"RELEASE SAVEPOINT {_res_sp}")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_res_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_res_sp}")
            except Exception:
                pass
            raise

        _atr_valid = sum(1 for _u in updates if _u[-3] is not None)
        print(f"[resistance] updated {len(updates)} rows / ATR14 valid={_atr_valid}")
    finally:
        cur.close()


# ==== /Resistance lines ====

# --- DASH settings ---
# dash template str

# === [charts60 flags integration] ============================================
def _load_flags_map(conn):
    """
    chart_flags テーブル（PK: コード）を読み込み、{コード4桁: 行dict} を返す。
    期待スキーマ:
      コード, 銘柄名, GCフラグ, 三役好転フラグ,
      ボリバンm2, ボリバンm1, ボリバン0, ボリバンp1, ボリバンp2,
      5日線上, 25日線上, 75日線上, 作成日時
    """
    try:
        df = pd.read_sql_query("SELECT * FROM chart_flags", conn)
    except Exception as e:
        print("[flags][WARN] chart_flags 読み込み失敗:", e)
        return {}
    def c4(x):
        # P1-160: chart_flagsも共通コード正規化。
        return _normalize_jp_security_code(x)
    # P1-666: legacy alias（7203 / 7203.0 等）がchart_flagsに共存しても、
    # SELECT無順序のdict上書きで古い行をcurrent表示へ戻さない。
    # 作成日時を主、canonical raw表記をtie-breakとしてlogical codeごとに正本1行へ集約する。
    if df.empty:
        return {}
    df = df.copy()
    df["_raw_code"] = df["コード"].astype(str)
    df["_code_key"] = df["コード"].map(c4)
    df["_row_order"] = np.arange(len(df), dtype=int)
    if "作成日時" in df.columns:
        df["_created_ord"] = df["作成日時"].map(_p1_608_jst_naive_ts)
    else:
        df["_created_ord"] = pd.NaT
    df["_canon_match"] = (
        df["_raw_code"].str.strip().str.upper() == df["_code_key"].astype(str).str.upper()
    ).astype(int)
    df = (df[df["_code_key"].astype(str).str.len() > 0]
          .sort_values(["_code_key", "_created_ord", "_canon_match", "_row_order"],
                       kind="stable", na_position="first")
          .drop_duplicates("_code_key", keep="last"))
    mp = {str(r["_code_key"]): dict(r) for _, r in df.iterrows()}
    print(f"[flags] loaded {len(mp)} logical rows from chart_flags")
    return mp

def _pick_ma_label(row):
    # 優先度 5 > 25 > 75
    try:
        if int(row.get("5日線上") or 0) == 1: return "5"
        if int(row.get("25日線上") or 0) == 1: return "25"
        if int(row.get("75日線上") or 0) == 1: return "75"
    except Exception:
        pass
    return ""

def _pick_bbands(row):
    # -2/-1/0/+1/+2 のいずれか、複数立ってる場合は強い側を優先
    keys = [("ボリバンm2","-2"), ("ボリバンm1","-1"), ("ボリバン0","0"),
            ("ボリバンp1","+1"), ("ボリバンp2","+2")]
    # 優先順位: +2 > +1 > 0 > -1 > -2
    order = {"-2":0,"-1":1,"0":2,"+1":3,"+2":4}
    chosen, best = "", -1
    for k, lab in keys:
        try:
            if int(row.get(k) or 0) == 1:
                if order[lab] > best:
                    best = order[lab]; chosen = lab
        except Exception:
            continue
    return chosen

def _mask_stale_pts_for_run(data_rows, run_mode=None):
    """PTSの寿命をrun modeで制御する。

    PREOPEN: 前営業日以降に取得した前夜PTSを許可。
    MIDDAY: 当日取得分のみ許可（昨日夜PTSを場中に出さない）。
    EOD: 当日取得分のみ許可。
    取得時刻不明のlegacy値は安全側でmask。
    """
    mode = str(run_mode or _auto_run_mode()).upper()
    now_d = _now_jst().date()
    prev_biz = prev_business_day_jp(now_d) if mode == "PREOPEN" else now_d
    for row in data_rows or []:
        raw_ts = row.get("PTS取得日時")
        try:
            ts = pd.to_datetime(raw_ts, errors="raise") if raw_ts not in (None, "") else None
            d = ts.date() if ts is not None else None
        except Exception:
            d = None
        valid = False
        if d is not None:
            if mode == "PREOPEN":
                valid = d >= prev_biz and d <= now_d
            else:
                valid = d == now_d
        if not valid:
            row["PTS株価"] = None
            row["PTS時刻"] = None
    return data_rows

# 先頭付近
def enhance_with_chart_flags(
    conn,
    data_rows,
    charts_dir=None,
    dashboard_dir=None,
):
    # SYSTEM-REWORK: chart/outputもKABU_OUTPUT_DIR配下を唯一の正本にする。
    charts_dir = charts_dir or os.path.join(OUTPUT_DIR, "charts60")
    dashboard_dir = dashboard_dir or OUTPUT_DIR
    flags = _load_flags_map(conn)
    # P1-540: P1-531で前段chart_urlを消しても、この後段関数がリンクを再生成していた。
    # 今回charts60子処理が失敗したrunでは、古いHTMLリンクだけでなくchart_flags由来表示もmaskする。
    _charts_current = bool(globals().get("_CHARTS60_CURRENT_RUN_OK", True))
    _charts_current_codes = globals().get("_CHARTS60_CURRENT_RUN_CODES")
    _charts_snapshot_at = str(globals().get("_CHARTS60_SNAPSHOT_AT") or "").strip()

    def code4(row):
        v = row.get("コード") or row.get("code") or ""
        return _normalize_jp_security_code(v)

    base_chart = Path(charts_dir).resolve()
    base_html  = Path(dashboard_dir).resolve()

    for row in data_rows:
        c4 = code4(row)
        _code_current = _charts_current
        if isinstance(_charts_current_codes, set):
            _code_current = _code_current and (c4 in _charts_current_codes)
        if not _code_current:
            row["chart"] = ""
            row["移動平均"] = row["ボリバン"] = row["GC"] = row["三役"] = ""
            continue
        fr = flags.get(c4)

        # === ここが重要：絶対 file:/// をやめて、出力HTMLからの相対パスにする ===
        abs_path = (base_chart / f"{c4}.html").resolve()
        try:
            rel = os.path.relpath(abs_path, start=base_html).replace(os.sep, "/")
        except Exception:
            # 万一relpath失敗時の保険（最後の手段）
            rel = str(abs_path).replace("\\", "/")

        # モーダルJSが拾えるよう data-href を付与（hrefも相対）
        _chart_title = "chart"
        if _charts_snapshot_at:
            _chart_title = f"chart snapshot: {_charts_snapshot_at}"
        row["chart"] = Markup(
            f'<a class="chartlink" data-code="{c4}" data-href="{rel}" href="{rel}" '
            f'title="{html.escape(_chart_title, quote=True)}">chart</a>'
        )

        if not fr:
            row["移動平均"] = row["ボリバン"] = row["GC"] = row["三役"] = ""
            continue

        row["移動平均"] = _pick_ma_label(fr)
        row["ボリバン"] = _pick_bbands(fr)
        try:
            row["GC"] = "○" if int(fr.get("GCフラグ") or 0) == 1 else ""
        except Exception:
            row["GC"] = ""
        try:
            row["三役"] = "○" if int(fr.get("三役好転フラグ") or 0) == 1 else ""
        except Exception:
            row["三役"] = ""
    return data_rows

# === [/charts60 flags integration] ===========================================

# ダッシュボードテンプレート（P3-41: template.htmlを唯一の優先正本）
# P2-56: import時にはファイルI/Oしない。HTML生成時だけlazy-loadする。
_template_base = os.path.dirname(__file__)
_template_candidates = [
    os.path.join(_template_base, "template.html"),
    os.path.join(_template_base, "template_next_turn_python_backed.html"),
]
template_path = None
DASH_TEMPLATE_STR = None

def _load_dashboard_template_str():
    global template_path, DASH_TEMPLATE_STR
    if DASH_TEMPLATE_STR is not None:
        return DASH_TEMPLATE_STR
    _selected = next((p for p in _template_candidates if os.path.exists(p)), None)
    if _selected is None:
        raise FileNotFoundError(
            "dashboard template not found; expected one of: " + ", ".join(_template_candidates)
        )
    with open(_selected, "r", encoding="utf-8") as _f:
        _text = _f.read()
    template_path = _selected
    DASH_TEMPLATE_STR = _text
    print(f"[template] using: {template_path}")
    return DASH_TEMPLATE_STR

# P2-57: 旧テンプレ補助の未使用キーワード/regex定数は削除。

# P1-361: 先頭のoptional import結果をここで補完。
if notification is None:
    class _DummyNoti:
        @staticmethod
        def notify(title="", message="", timeout=3):
            print(f"[NOTIFY] {title} - {message}")
    notification = _DummyNoti()

if workdays is None:
    class _WorkdaysShim:
        @staticmethod
        def networkdays(start: date, end: date, holidays=None):
            if holidays is None: holidays = []
            if start > end: start, end = end, start
            d, cnt = start, 0
            while d <= end:
                if d.weekday() < 5 and d not in holidays:
                    cnt += 1
                d += timedelta(days=1)
            return cnt
        @staticmethod
        def workday(start: date, days: int, holidays=None):
            if holidays is None: holidays = []
            step = 1 if days >= 0 else -1
            d, moved = start, 0
            while moved < abs(days):
                d += timedelta(days=step)
                if d.weekday() < 5 and d not in holidays:
                    moved += 1
            return d
    workdays = _WorkdaysShim()

# -*- coding: utf-8 -*-
"""
自動スクリーニング_完全統合版 + 右肩上がり（Template版/両立フィルタ/Gmail/オフラインHTML/祝日対応/MIDDAY自動）

修正点（この版）
- HTML出力フェーズの JSON 生成で、DataFrame 内の bytes / NaN / pandas.Timestamp / NumPy スカラーを
  安全に変換できるように修正（TypeError: bytes is not JSON serializable 対策）

機能ダイジェスト
- EOD/MIDDAY 自動判定（JST 11:30–12:30 は MIDDAY スナップショット、それ以外は EOD）
- 祝日/土日スキップ（jpholiday + 追加休場日ファイル対応）
- yahooquery で quotes / history を一括取得（初回は 12mo、通常は 10d）
- 初動/底打ち/上昇余地スコア/右肩上がりスコア の判定
- 前営業日の翌日検証（判定とCSV出力）
- オフライン1ファイルHTMLダッシュボード（候補一覧/検証/全カラム/price_history）
- Gmail で index.html を送信（任意、ZIP同梱可）

前提: Python 3.11 / pip install yahooquery pandas jpholiday
"""



# --- EDINET 取得で使う ---

# =====================================

# 速度チューニング
YQ_MAX_WORKERS = 16

# ===== フェイルセーフ =====

warnings.simplefilter(action="ignore", category=FutureWarning)

# ===== ユーティリティ =====
def ffloat(x, default=None):
    try:
        return default if pd.isna(x) else float(x)
    except Exception:
        try:
            return float(str(x))
        except Exception:
            return default

def fint(x, default=None):
    try:
        if pd.isna(x):
            return default
        if isinstance(x, (int,)) and not isinstance(x, bool):
            return int(x)
        if isinstance(x, float):
            return int(x)
        return int(float(str(x)))
    except Exception:
        return default

def today_str():
    # P1-118: 市場日付はOSローカル時刻ではなくJSTで統一。
    return _today_jst()
    
# ================== 表示整形ヘルパ ==================

def _trade_date_from_quote(q, extra_closed_path=EXTRA_CLOSED_PATH):
    """
    APIのquote(dict)から取引日(YYYY-MM-DD, JST)を返す。
    P1-479: regularMarketTimeが無いquoteを「今日」と推定しない。
    priceだけキャッシュされ時刻が欠けたレスポンスを当日のauthoritative履歴へ昇格させるため、
    timestamp不明はNoneとして呼出側で除外する。
    """
    try:
        ts = q.get("regularMarketTime") if isinstance(q, dict) else None
        if ts:
            try:
                ts = int(ts)
            except Exception:
                ts = int(float(ts))
            # P1-140: datetime はクラスとしてimport済み。datetime.timezone は存在せず、
            # regularMarketTime が毎回例外→今日へfallbackしていた。
            t = datetime.fromtimestamp(ts, tz=timezone.utc).astimezone(JST)
            return t.date().isoformat()
    except Exception:
        pass
    # P1-479: 時刻不明を今日扱いしない。
    return None

def _safe_jsonable(val):
    """
    JSONに安全に落とし込むための変換（bytes, NaN, Timestamp 等を処理）
    """

    if val is None:
        return None
    if isinstance(val, (bytes, bytearray)):
        try:
            return val.decode("utf-8", errors="ignore")
        except Exception:
            return str(val)
    # pandas/NumPyの欠損
    if (isinstance(val, float) and (math.isnan(val))) or (hasattr(pd, "isna") and pd.isna(val)):
        return None
    if isinstance(val, (np.floating, np.integer)):
        return val.item()
    # 日付・日時
    if isinstance(val, (pd.Timestamp, datetime, date, dt_time)):
        return str(val)[:19]
    return val

# ===== 祝日判定 =====
def _load_extra_closed(path: str):
    """追加JPX休場日。未設定/不存在は任意だが、存在するファイルの破損はfail-fast。"""
    s = set()
    if not path or not os.path.isfile(path):
        return s
    try:
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                token = line[:10]
                # P1-398: typo/破損した休場日を黙って営業日扱いしない。
                date.fromisoformat(token)
                s.add(token)
    except Exception as e:
        raise RuntimeError(f"extra JPX closed-date file is invalid: {path}: {e}") from e
    return s

def is_jp_market_holiday(d: date, extra_closed: set = None) -> bool:
    if d.weekday() >= 5:
        return True
    # P1-313: JPX固定休場の1月1〜3日をjpholiday依存にしない。
    # jpholiday未導入/例外時でも元日を営業日扱いしない。
    if (d.month == 12 and d.day == 31) or (d.month == 1 and d.day in (1, 2, 3)):
        return True
    if extra_closed and d.strftime("%Y-%m-%d") in extra_closed:
        return True
    if jpholiday is None:
        raise RuntimeError(
            "jpholiday is required for JPX business-day calculation; install jpholiday before this phase"
        )
    try:
        if jpholiday.is_holiday(d):
            return True
    except Exception as e:
        # P1-400/P2-53: 祝日判定障害を「営業日」と決め打ちしない。
        # True固定にするとprev/next_business_dayが無限ループし得るため、上位へ失敗を伝える。
        raise RuntimeError(f"jpholiday lookup failed for {d}: {e}") from e
    return False

def next_business_day_jp(d: date, extra_closed: set = None) -> date:
    cur = d
    while True:
        cur += timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

def prev_business_day_jp(d: date, extra_closed: set = None) -> date:
    cur = d
    while True:
        cur -= timedelta(days=1)
        if not is_jp_market_holiday(cur, extra_closed):
            return cur

def _expected_jpx_asof_date() -> date:
    """P1-401: legacy future/holiday price_historyを計算基準日にしないための上限日。"""
    extra = _load_extra_closed(EXTRA_CLOSED_PATH)
    d = _now_jst().date()
    if is_jp_market_holiday(d, extra):
        d = prev_business_day_jp(d, extra)
    return d

def _expected_snapshot_date_for_run(run_mode: str) -> date:
    """P1-535: セッション別に今回snapshotが到達しているべきJPX営業日を返す。

    PREOPENは当日立会前なので前営業日、MIDDAY/EODは営業日なら当日、
    休場日は直前営業日を要求する。
    """
    extra = _load_extra_closed(EXTRA_CLOSED_PATH)
    d = _now_jst().date()
    if is_jp_market_holiday(d, extra):
        return prev_business_day_jp(d, extra)
    if str(run_mode or '').upper() == 'PREOPEN':
        return prev_business_day_jp(d, extra)
    return d

def _require_current_price_history_snapshot(conn: sqlite3.Connection, run_mode: str) -> str:
    """P1-535: 価格派生計算前に全体price_historyの鮮度を必須検証する。"""
    expected = _expected_snapshot_date_for_run(run_mode).isoformat()
    actual = _latest_valid_history_date(conn)
    if actual != expected:
        raise RuntimeError(
            f"price_history snapshot is stale/incomplete for {run_mode}: expected_asof={expected} actual_asof={actual}"
        )
    return actual

def _latest_valid_history_date(conn: sqlite3.Connection, codes=None):
    """P1-454/P1-592: 今回runのsnapshot上限以下にある最新JPX営業日。

    codesを渡した場合はその論理銘柄集合だけでas-ofを決める。legacy suffix/float aliasも含める。
    P1-592: PREOPENだけは前営業日が正本。営業日だからという理由で壁時計の今日まで許すと、
    legacy/部分的な当日行1本だけで全体as-ofが今日へ進み、鮮度判定を壊し得る。
    """
    cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    params = [cutoff]
    # P1-664: 「最新日」は日付行が存在するだけでは不十分。
    # 当日placeholder/部分取得で終値NULLの行だけあってもsnapshot到達と認定しない。
    where = "date(日付) <= date(?) AND 終値 IS NOT NULL AND CAST(終値 AS REAL) > 0"
    if codes:
        _vars = expand_code_query_variants(codes)
        if not _vars:
            return None
        where += " AND CAST(コード AS TEXT) IN (" + ",".join("?" * len(_vars)) + ")"
        params.extend(_vars)
    rows = conn.execute(
        f"SELECT DISTINCT substr(日付,1,10) AS d FROM price_history WHERE {where} ORDER BY date(日付) DESC",
        params,
    ).fetchall()
    extra = _load_extra_closed(EXTRA_CLOSED_PATH)
    for row in rows:
        if not row or not row[0]:
            continue
        try:
            d = date.fromisoformat(str(row[0])[:10])
        except Exception:
            continue
        if not is_jp_market_holiday(d, extra):
            return d.isoformat()
    return None

# ===== DB =====


# P2-57: 旧コード判定regexは共通canonical化へ統合済みのため削除。

def _cleanup_screener_logical_duplicates(conn: sqlite3.Connection, target_codes=None) -> int:
    """P1-312: screener内の7203/7203.0・285A/285a等を1論理銘柄へ移行する。

    target_codes を渡した場合はCSV今回対象だけを処理。
    P1-634: canonical行が既に存在する場合はそれを正本にし、aliasは正本のNULL/空欄補完だけに使う。
    canonical行が無いlegacy-only群だけ最新rowidを基準にしてコードをcanonicalへ統一する。
    """
    _sp = f"sp_screener_alias_{time.time_ns()}"
    _sp_open = False
    try:
        conn.execute(f"SAVEPOINT {_sp}")
        _sp_open = True
        info = conn.execute("PRAGMA table_info(screener)").fetchall()
        cols = [r[1] for r in info]
        if "コード" not in cols:
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
            return 0
        data_cols = [c for c in cols if c != "コード"]
        qcols = ','.join('"' + c.replace('"','""') + '"' for c in ["コード", *data_cols])
        rows = conn.execute(f"SELECT rowid, {qcols} FROM screener ORDER BY rowid").fetchall()
        wanted = None
        if target_codes is not None:
            wanted = {canonical_code_for_db(c) for c in target_codes if canonical_code_for_db(c)}
        groups = {}
        for row in rows:
            rid, raw = row[0], row[1]
            key = canonical_code_for_db(raw)
            if not key or (wanted is not None and key not in wanted):
                continue
            groups.setdefault(key, []).append((rid, list(row[1:])))

        changed = 0
        def meaningful(v):
            return v is not None and (not isinstance(v, str) or v.strip() != "")

        for key, grp in groups.items():
            # 1行でもlegacy表記ならcanonicalへ移す。
            if len(grp) == 1 and str(grp[0][1][0]).strip() == key:
                continue
            grp = sorted(grp, key=lambda x: x[0])
            # P1-634: UPDATEはrowidを進めないため、rowid最大=最新snapshotではない。
            # 現行コードはscreenerをcanonical rawで更新するので、既存canonical行があれば
            # それをauthoritative survivorにする。legacy aliasは正本欠損の補完にだけ使う。
            _canon_rows = [g for g in grp if str(g[1][0]).strip().upper() == str(key).upper()]
            survivor_rid, survivor_vals = (_canon_rows[-1] if _canon_rows else grp[-1])
            donor_grp = [g for g in grp if g[0] != survivor_rid]
            merged = {"コード": key}
            for j, c in enumerate(data_cols, start=1):
                base = survivor_vals[j] if j < len(survivor_vals) else None
                if meaningful(base):
                    merged[c] = base
                    continue
                fill = None
                for _rid, vals in reversed(donor_grp):
                    v = vals[j] if j < len(vals) else None
                    if meaningful(v):
                        fill = v; break
                merged[c] = fill

            # UNIQUE(コード)衝突を避けるため、基準行以外を先に削除してからcanonicalへ変更。
            other_ids = [rid for rid, _ in grp if rid != survivor_rid]
            if other_ids:
                ph = ','.join('?' * len(other_ids))
                conn.execute(f"DELETE FROM screener WHERE rowid IN ({ph})", other_ids)
                changed += len(other_ids)
            if data_cols:
                set_sql = ','.join('"' + c.replace('"','""') + '"=?' for c in data_cols)
                conn.execute(
                    f'UPDATE screener SET "コード"=?, {set_sql} WHERE rowid=?',
                    (key, *[merged[c] for c in data_cols], survivor_rid),
                )
            else:
                conn.execute('UPDATE screener SET "コード"=? WHERE rowid=?', (key, survivor_rid))
            changed += 1
        conn.execute(f"RELEASE SAVEPOINT {_sp}")
        return changed
    except Exception as e:
        if _sp_open:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
        # P1-349: cleanup失敗を0件成功扱いすると、その直後のcanonical INSERTで
        # legacy行とcanonical行が二重化し得る。必ずCSV取込自体を中止する。
        print(f"[csv-import][ERROR] screener logical duplicate cleanup failed: {e}")
        raise RuntimeError("screener logical duplicate cleanup failed") from e


def phase_csv_import(conn, csv_path=None, overwrite_registered_date=None, **_ignored):
    """
    CSVの「コード, 銘柄名, 市場」だけ取り込み。
    登録日は INSERT 時のみ（JSTの今日）セット。UPDATEでは変更しない。
    P1-312: CSVコードはcanonical化し、既存legacy aliasも取込前に同一行へ移行する。
    """

    path = csv_path or CSV_INPUT_PATH
    if not os.path.isfile(path):
        # P1-314: daily-once 配下で入力欠損を正常returnすると、その日の完了マーカーが立ち
        # 復旧後も再試行されない。必須入力欠損は失敗として上位へ返す。
        raise FileNotFoundError(f"[csv-import] CSVがありません: {path}")

    JST = timezone(timedelta(hours=9))
    today_str = datetime.now(JST).strftime("%Y-%m-%d")

    # ← ここが重要：区切り/エンコーディング自動吸収
    df = pd.read_csv(path, sep=None, engine="python", dtype=str, encoding="utf-8-sig")

    needed = ["コード", "銘柄名", "市場"]
    missing = [c for c in needed if c not in df.columns]
    if missing:
        raise ValueError(f"[csv-import] CSVに必須列がありません: {missing}")

    df = df[needed].copy()
    for c in needed:
        df[c] = df[c].astype(str).map(lambda x: x.strip() if x is not None else x)

    # ★ 追加：コードを ^TOPX 等に統一する
    df["コード"] = df["コード"].map(canonical_code_for_db)
    df = df[df["コード"].astype(str).str.len() > 0].copy()
    # P1-323: 壊れた/空のCSVを「日次取込成功」にしない。復旧後に同日再試行可能にする。
    if df.empty:
        raise RuntimeError(f"[csv-import] 有効なコードが0件です: {path}")

    df["登録日"] = today_str  # INSERT時だけ使われる

    sql = """
    INSERT INTO screener(コード, 銘柄名, 市場, 登録日)
    VALUES (?, ?, ?, ?)
    ON CONFLICT(コード) DO UPDATE SET
      銘柄名 = excluded.銘柄名,
      市場   = excluded.市場
      -- 登録日は更新しない（INSERT時のみ設定）
    """
    rows = list(df.itertuples(index=False, name=None))
    cur = conn.cursor()
    # P1-373: legacy alias移行と今回CSV UPSERTを同一transactionにする。
    # CSV途中不正/DB制約違反でalias移行だけ残る・一部銘柄だけ更新される状態を防ぐ。
    _sp = f"sp_csv_import_{time.time_ns()}"
    conn.execute(f"SAVEPOINT {_sp}")
    try:
        # P1-496: CSV今回対象だけでなくscreener全体のlegacy aliasを統合する。
        # CSVに含まれない既存行でも 7203.0 / 1234.N 等が残ると、percentileやJOINで
        # 同一銘柄を二重観測し得るため、銘柄を削除せず表記/重複だけ全体cleanupする。
        _migrated = _cleanup_screener_logical_duplicates(conn, None)
        if _migrated:
            print(f"[csv-import] legacy screener aliases migrated={_migrated}")
        cur.executemany(sql, rows)
        conn.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        cur.close()
        raise
    cur.close()

    print(f"[csv-import] 取り込み完了: {len(rows)}件（登録日はINSERT時のみ {today_str}）")

def phase_delist_cleanup(conn: sqlite3.Connection,
                         master_csv_path: str = MASTER_CODES_PATH,
                         also_clean_notes: bool = False) -> None:
    """
    マスタCSV(列名: コード)に存在しない銘柄コードを screener から削除する。
    ただし screener_protect に登録されたコードは CSV に無くても削除対象外。
    also_clean_notes=True の場合は finance_notes も同様に削除する。
    """

    if not os.path.isfile(master_csv_path):
        # P1-315: daily-once の完了扱いにせず、復旧後に同日再試行できるよう失敗を返す。
        raise FileNotFoundError(f"[delist] 上場廃止の基準ファイルがありません: {master_csv_path}")

    _re_num4      = re.compile(r"^\s*(\d{4})\s*$")            # 4桁数字（例: 7203）
    _re_num3alpha = re.compile(r"^\s*(\d{3})([A-Za-z])\s*$")  # 3桁+英字（例: 330A）

    def _norm_domestic(code: object) -> str | None:
        """
        国内コードの正規化：
        - 4桁数字       -> 'dddd'
        - 3桁+英字      -> 'dddX'（英字は大文字化）
        - それ以外      -> None（国内コード比較の対象外）
        """
        if code is None:
            return None
        # P1-239: legacy float文字列や小文字英字も共通canonical化してから判定。
        s = canonical_code_for_db(code)
        if not s:
            return None
        m = _re_num4.match(s)
        if m:
            return m.group(1)
        m = _re_num3alpha.match(s)
        if m:
            return f"{m.group(1)}{m.group(2).upper()}"
        return None

    # マスタ側の有効コード集合（国内コードのみ対象）
    master = pd.read_csv(master_csv_path, encoding="utf8", sep=",", engine="python")
    if "コード" not in master.columns:
        raise ValueError(f"[delist] 基準ファイルに列 'コード' がありません: {master_csv_path}")
    valid = {c for c in (_norm_domestic(x) for x in master["コード"]) if c is not None}
    if not valid:
        raise RuntimeError(f"[delist] 基準ファイルの有効コードが0件です: {master_csv_path}")

    cur = conn.cursor()

    # 保護テーブル（大文字化で比較を統一）
    cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='screener_protect'")
    has_protect = cur.fetchone() is not None
    protect_set: set[str] = set()
    if has_protect:
        cur.execute("SELECT コード FROM screener_protect")
        protect_set = {(_norm_domestic(row[0]) or str(row[0]).strip().upper()) for row in cur.fetchall()}

    # DB内コードを走査。protectは無条件除外。
    cur.execute("SELECT コード FROM screener")
    rows = cur.fetchall()
    targets = []
    skipped_by_protect = 0
    for (db_code,) in rows:
        raw = str(db_code).strip()
        raw_u = raw.upper()
        nd = _norm_domestic(raw_u)
        _protect_key = nd or raw_u
        if _protect_key in protect_set:
            skipped_by_protect += 1
            continue

        if nd is None:
            # 国内コード形式でないもの（例: ^N225, 998405.T）は
            # マスタCSVの対象外なので削除判定しない
            continue
        if nd not in valid:
            targets.append((raw,))  # 保存時は原文でOK

    # P1-375: screenerから既に消えていてfinance_notesだけ残ったコードも独立してマスタ照合。
    _note_targets = []
    if also_clean_notes:
        try:
            cur.execute("SELECT コード FROM finance_notes")
            for (_nc,) in cur.fetchall():
                _nd = _norm_domestic(_nc)
                _nkey = _nd or str(_nc).strip().upper()
                if _nkey in protect_set:
                    continue
                if _nd is not None and _nd not in valid:
                    _note_targets.append((_nc,))
        except Exception as e:
            cur.close()
            raise RuntimeError("finance_notes delist scan failed") from e

    if not targets and not _note_targets:
        msg = "上場廃止による削除対象はありません。"
        if skipped_by_protect:
            msg += f"（保護:{skipped_by_protect} 件）"
        print(msg)
        cur.close()
        return

    print(f"上場廃止による削除: screener={len(targets)} 件 / finance_notes={len(_note_targets)} 件" + (f"（保護:{skipped_by_protect} 件）" if skipped_by_protect else ""))
    # P1-370: isolation_level=None(autocommit)でもscreener/finance_notes削除を一体化。
    _sp = f"sp_delist_{time.time_ns()}"
    conn.execute(f"SAVEPOINT {_sp}")
    try:
        cur.executemany("DELETE FROM screener WHERE コード = ?", targets)

        if also_clean_notes and _note_targets:
            try:
                cur.executemany("DELETE FROM finance_notes WHERE コード = ?", _note_targets)
            except Exception as e:
                raise RuntimeError("finance_notes delist cleanup failed") from e

        conn.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        cur.close()
        raise
    cur.close()

# ===== 任意：空売り無し反映 =====
def _invalidate_karauri_nashi_snapshot(conn: sqlite3.Connection) -> int:
    """P1-377: 当日snapshotが使えない時に前回の「なし」を安全側の不明へ戻す。"""
    cur = conn.cursor()
    try:
        cur.execute('UPDATE screener SET 空売り機関=NULL WHERE 空売り機関="なし"')
        return max(0, int(cur.rowcount or 0))
    finally:
        cur.close()

def phase_mark_karauri_nashi(conn: sqlite3.Connection):
    """P3-41: institution_short_snapshot の当日成功行を正本に空売り「なし」を反映。

    crawl_success=1 & has_short=0 の時だけ「なし」。
    取得失敗/未取得は不明(NULL)で、昨日の安全判定を持ち越さない。
    """
    today_s = _today_jst()
    tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    if "institution_short_snapshot" not in tables:
        cleared = _invalidate_karauri_nashi_snapshot(conn)
        print(f"[karauri-flag] snapshot tableなし → 不明化 {cleared}件")
        return False

    rows = conn.execute("""
        SELECT code,crawl_success,has_short
        FROM institution_short_snapshot
        WHERE snapshot_date=?
    """, (today_s,)).fetchall()
    success_codes = {canonical_code_for_db(r[0]) for r in rows if int(r[1] or 0)==1 and canonical_code_for_db(r[0])}
    no_short = {canonical_code_for_db(r[0]) for r in rows if int(r[1] or 0)==1 and r[2] == 0 and canonical_code_for_db(r[0])}

    screener_rows = conn.execute("SELECT コード FROM screener").fetchall()
    logical = {canonical_code_for_db(r[0]) for r in screener_rows if canonical_code_for_db(r[0])}
    # authoritative全件snapshotが揃っていないなら、分かる行だけ使い未取得は不明。
    complete = bool(logical) and logical.issubset(success_codes)

    sp = f"sp_karauri_db_snapshot_{time.time_ns()}"
    conn.execute(f"SAVEPOINT {sp}")
    try:
        conn.execute('UPDATE screener SET 空売り機関=NULL WHERE 空売り機関="なし"')
        if no_short:
            targets=[]
            for (raw,) in screener_rows:
                if canonical_code_for_db(raw) in no_short:
                    targets.append((raw,))
            if targets:
                conn.executemany('UPDATE screener SET 空売り機関="なし" WHERE コード=?', targets)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        raise
    print(f"[karauri-flag] DB snapshot success={len(success_codes)}/{len(logical)} no_short={len(no_short)} complete={complete}")
    return complete


def list_insufficient_codes(conn: sqlite3.Connection, universe_codes=None) -> list[str]:
    """
    直近2営業日の終値が揃っていない（=前日終値比率が計算できない）銘柄を列挙。
    universe_codes を省略すると screener 全件を対象にする。
    """
    if universe_codes is None:
        cur = conn.cursor()
        cur.execute("SELECT コード FROM screener")
        universe_codes = [canonical_code_for_db(r[0]) for r in cur.fetchall() if canonical_code_for_db(r[0])]
        cur.close()

    # P2-26: _latest2_ok()を銘柄ごとに呼ぶN+1を廃止。
    # 旧処理も各銘柄の全履歴を読んでからlogical dateへdedupeしていたため、同じ母集団を
    # 200銘柄ずつbulk読込して判定する。長期売買停止銘柄の旧挙動も変えないため人工的な日付下限は置かない。
    codes = list(dict.fromkeys(canonical_code_for_db(c) for c in (universe_codes or []) if canonical_code_for_db(c)))
    if not codes:
        return []
    cutoff = _expected_jpx_asof_date().isoformat()
    good = set()
    for i in range(0, len(codes), 200):
        part = codes[i:i + 200]
        qvars = expand_code_query_variants(part)
        if not qvars:
            continue
        ph = ",".join("?" * len(qvars))
        df = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, 日付, 終値 FROM price_history "
            f"WHERE CAST(コード AS TEXT) IN ({ph}) AND date(日付) <= date(?) "
            f"ORDER BY 日付 DESC, rowid DESC",
            conn, params=[*qvars, cutoff]
        )
        if df.empty:
            continue
        df = _dedupe_price_history_df(df)
        if df.empty:
            continue
        for code_key, g in df.groupby("コード", sort=False):
            g = g.sort_values("日付", ascending=False).head(2).reset_index(drop=True)
            if len(g) < 2:
                continue
            prev = pd.to_numeric(pd.Series([g.iloc[1]["終値"]]), errors="coerce").iloc[0]
            if pd.notna(prev) and float(prev) != 0.0:
                ck = canonical_code_for_db(code_key)
                if ck:
                    good.add(ck)
    return [c for c in codes if c not in good]

def refresh_full_history_for_insufficient(conn: sqlite3.Connection, universe_codes=None, batch_size: int = 200) -> list[str]:
    """
    「データ不足（直近2日そろわず）」な銘柄だけを抽出し、初回だけ 12ヶ月 を取り直して upsert。
    処理後に _update_screener_from_history で前日終値比率などを再計算する。
    戻り値: 再取得を行った銘柄コードのリスト
    """
    # 1) 対象抽出
    targets = list_insufficient_codes(conn, universe_codes)
    # P1-513: EOD bulkでHISTORY_NONEをskipしても、この直後の12mo補完が再照会しては
    # soft sentinelの意味がない。当日のNONE/HISTORY_NONEを履歴補完でも共通skipする。
    targets = filter_valid_yahoo_codes(conn, targets, for_history=True)
    if not targets:
        print("[full-refresh] 不足銘柄なし（当日history soft-sentinel除外を含む）")
        return []

    print(f"[full-refresh] 12mo 取り直し対象: {len(targets)} 件")
    # P2-46: chunkごとではなく全対象で先に解決し、chunk境界を跨ぐsymbol collisionも検知する。
    _target_symbols = resolve_yahoo_symbols_bulk(targets, conn)
    _target_symbol_map = dict(zip(targets, _target_symbols))
    total_added = 0
    # P1-114: 失敗チャンクを握りつぶすとdaily markerが立って同日再試行されない。
    _failed_chunks = []

    # 2) yfinance で 12mo を取得して price_history に upsert
    for i in range(0, len(targets), batch_size):
        chunk = targets[i:i+batch_size]
        #tickers_map = {c: f"{c}.T" for c in chunk}   # 日本株想定
        # P1-511: ここで銘柄ごとの5d存在確認をすると、直後のbulk downloadと二重照会になる。
        # DB市場/明示overrideだけでsymbolを決め、存在確認はbulk応答そのものに任せる。
        tickers_map = {c: _target_symbol_map[c] for c in chunk}
        try:
            df_wide = _yfinance_download_repaired(
                list(tickers_map.values()),
                period="12mo", interval="1d",
                group_by="ticker", threads=True, auto_adjust=False
            )
        except Exception as e:
            _failed_chunks.append((list(chunk), str(e)))
            print(f"[full-refresh][WARN] download失敗: {e}  chunk先頭={chunk[0] if chunk else ''}")
            continue

        df_add = _to_long_history(df_wide, tickers_map)  # 既存の整形関数を流用:contentReference[oaicite:1]{index=1}
        # P1-690: EOD bulk本体(P1-683/686)と同じ完全性ガードを12mo補完にも適用する。
        # 補完側だけ期待日の終値あり/高安出来高欠損barを保存すると、全体as-of gateは通って
        # 1銘柄だけ混成EOD足をcurrent扱いできるため、不完全な期待日barは保存前に除外し再試行させる。
        _expected_full_date = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        _incomplete_full = []
        if df_add is not None and not df_add.empty and {"コード","日付","始値","高値","安値","終値","出来高"}.issubset(df_add.columns):
            _fw = df_add[["コード","日付","始値","高値","安値","終値","出来高"]].copy()
            _fw["コード"] = _fw["コード"].map(canonical_code_for_db)
            _fw["日付"] = _fw["日付"].astype(str).str[:10]
            _fw = _fw[_fw["日付"] == _expected_full_date].copy()
            if not _fw.empty:
                for _fc in ("始値","高値","安値","終値","出来高"):
                    _fw[_fc] = pd.to_numeric(_fw[_fc], errors="coerce")
                # P1-691: 12mo補完でも指数は出来高NULLを許容し、OHLC完全性だけを要求。
                _is_index_full = _fw["コード"].astype(str).str.startswith("^")
                _bad_full_price = (
                    _fw[["始値","高値","安値","終値"]].isna().any(axis=1)
                    | ~np.isfinite(_fw[["始値","高値","安値","終値"]]).all(axis=1)
                    | (_fw[["始値","高値","安値","終値"]] <= 0).any(axis=1)
                )
                _bad_full_volume = (
                    _fw["出来高"].isna()
                    | ~np.isfinite(_fw["出来高"])
                    | (_fw["出来高"] < 0)
                )
                _bad_full = _bad_full_price | ((~_is_index_full) & _bad_full_volume)
                _incomplete_full = sorted({c for c in _fw.loc[_bad_full, "コード"].dropna() if c})
        if _incomplete_full:
            _failed_chunks.append((list(_incomplete_full), "expected-date OHLCV incomplete"))
            _inc_full_set = {canonical_code_for_db(c) for c in _incomplete_full if canonical_code_for_db(c)}
            _full_code_norm = df_add["コード"].map(canonical_code_for_db)
            _full_date_norm = df_add["日付"].astype(str).str[:10]
            df_add = df_add.loc[~((_full_date_norm == _expected_full_date) & _full_code_norm.isin(_inc_full_set))].copy()
        # P1-117: 空/部分レスポンスも未取得として再試行対象にする。
        # P1-169: 7203.0/285a 表記差だけで部分レスポンス扱いしない。
        _returned = set() if df_add is None or df_add.empty or "コード" not in df_add.columns else {canonical_code_for_db(x) for x in df_add["コード"].dropna()}
        _missing = [c for c in chunk if canonical_code_for_db(c) not in _returned]
        if _missing:
            _failed_chunks.append((list(_missing), "empty/partial yahoo response"))
        added = _upsert_price_history(conn, df_add)      # 既存のupsertを流用:contentReference[oaicite:2]{index=2}
        # P1-681: 12mo取得でコードが1行でも返っただけではbootstrap完了にしない。
        # Yahooの部分/古い応答で過去1〜2行だけ返った場合にmarkerを立てると、以後12mo再取得を永久に飛ばす。
        # このrunの期待JPX日まで有効終値が到達したコードだけを完了扱いにする。
        _expected_bootstrap_date = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        _bootstrap_fresh = set()
        if df_add is not None and not df_add.empty and {"コード", "日付", "終値"}.issubset(df_add.columns):
            _tmp_boot = df_add[["コード", "日付", "終値"]].copy()
            _tmp_boot["コード"] = _tmp_boot["コード"].map(canonical_code_for_db)
            _tmp_boot["日付"] = _tmp_boot["日付"].astype(str).str[:10]
            _tmp_boot["終値"] = pd.to_numeric(_tmp_boot["終値"], errors="coerce")
            _tmp_boot = _tmp_boot[
                (_tmp_boot["日付"] == _expected_bootstrap_date)
                & _tmp_boot["終値"].notna()
                & np.isfinite(_tmp_boot["終値"])
                & (_tmp_boot["終値"] > 0)
            ]
            _bootstrap_fresh = {c for c in _tmp_boot["コード"].dropna() if c}
        if _bootstrap_fresh:
            _mark_price_history_bootstrapped(conn, sorted(_bootstrap_fresh), "full-refresh-12mo")
        total_added += added
        print(f"[full-refresh] {i+len(chunk)}/{len(targets)} (+{added} rows)")

    # 3) screener の 現在値/前日終値比率/出来高 を再計算して反映
    _update_screener_from_history(conn, targets)         # 既存の更新関数を流用:contentReference[oaicite:3]{index=3}
    # P1-642: EOD bulkはこの補完より前に売買代金/RVOL/合成スコアを計算済み。
    # 12mo補完で現在値/出来高が進んだ銘柄だけ価格と派生値の時点が分裂しないよう、
    # 補完後snapshotでもう一度current派生値をauthoritativeに再確定する。
    apply_auto_metrics_eod(conn)
    apply_composite_score(conn)

    print(f"[full-refresh] 追記 {total_added} 行 / 再取得 {len(targets)} 銘柄")
    if _failed_chunks:
        failed_codes = [c for chunk, _ in _failed_chunks for c in chunk]
        raise RuntimeError(
            f"P1-114 full_history_refresh: {len(_failed_chunks)} chunk(s) failed; "
            f"daily marker is intentionally not written. retry codes={failed_codes[:20]}"
        )
    return targets

def _ensure_price_history_bootstrap_state(conn: sqlite3.Connection) -> None:
    """P1-680: 12mo初期履歴を取得済みかをlogical code単位で記録する。"""
    conn.execute("""
        CREATE TABLE IF NOT EXISTS price_history_bootstrap_state (
            コード TEXT PRIMARY KEY,
            completed_at TEXT,
            valid_obs INTEGER,
            source TEXT
        )
    """)


def _valid_price_history_obs_counts(conn: sqlite3.Connection, codes) -> dict[str, int]:
    """P2-34: logical code単位の有効終値日数をalias横断でbulk集計する。"""
    keys = list(dict.fromkeys(
        canonical_code_for_db(c) for c in (codes or []) if canonical_code_for_db(c)
    ))
    counts = {c: 0 for c in keys}
    if not keys:
        return counts

    # code_query_variantsは1銘柄あたり最大数個。SQLiteのbind上限へ余裕を持たせる。
    for i in range(0, len(keys), 40):
        part = keys[i:i + 40]
        mapping = []
        for c in part:
            mapping.extend((str(v), c) for v in code_query_variants(c))
        if not mapping:
            continue
        values_sql = ",".join(["(?,?)"] * len(mapping))
        params = [x for pair in mapping for x in pair]
        rows = conn.execute(f"""
            WITH code_map(raw_code, logical_code) AS (
                VALUES {values_sql}
            ), logical_days AS (
                SELECT m.logical_code AS logical_code,
                       CAST(p.日付 AS TEXT) AS day_key
                  FROM price_history p
                  JOIN code_map m
                    ON CAST(p.コード AS TEXT)=m.raw_code
                 WHERE p.終値 IS NOT NULL
                   AND CAST(p.終値 AS REAL) > 0
                 GROUP BY m.logical_code, CAST(p.日付 AS TEXT)
            )
            SELECT logical_code, COUNT(*)
              FROM logical_days
             GROUP BY logical_code
        """, params).fetchall()
        for logical_code, nobs in rows:
            cc = canonical_code_for_db(logical_code)
            if cc in counts:
                counts[cc] = int(nobs or 0)
    return counts


def _mark_price_history_bootstrapped(conn: sqlite3.Connection, codes, source: str) -> None:
    keys = list(dict.fromkeys(
        canonical_code_for_db(c) for c in (codes or []) if canonical_code_for_db(c)
    ))
    if not keys:
        return
    _ensure_price_history_bootstrap_state(conn)
    now_s = _now_jst().isoformat(timespec="seconds")
    # P2-34: 旧実装の1銘柄1 COUNT(DISTINCT)をlogical alias横断bulkへ変更。
    obs_counts = _valid_price_history_obs_counts(conn, keys)
    rows = [
        (c, now_s, int(obs_counts.get(c, 0)), str(source or "12mo"))
        for c in keys
    ]
    conn.executemany(
        """INSERT INTO price_history_bootstrap_state(コード, completed_at, valid_obs, source)
           VALUES(?,?,?,?)
           ON CONFLICT(コード) DO UPDATE SET
             completed_at=excluded.completed_at,
             valid_obs=excluded.valid_obs,
             source=excluded.source""",
        rows,
    )

def _codes_with_data(conn):
    """P1-680: 12mo初期投入済みだけをEOD 2d更新のexisting銘柄とみなす。

    price_historyに1行でもあればexisting扱いする旧判定では、placeholderや2日だけ先に
    入った新規銘柄が12mo初期投入を永久に飛ばし、MA20/50/200等が不足し続け得た。
    既存DB移行では有効終値60観測以上をbootstrap済みと推定。短い履歴の銘柄は一度だけ
    12mo取得を試み、成功後は若いIPOでもmarkerにより毎日再取得しない。
    """
    _ensure_price_history_bootstrap_state(conn)
    marked = {
        canonical_code_for_db(r[0])
        for r in conn.execute("SELECT コード FROM price_history_bootstrap_state").fetchall()
        if canonical_code_for_db(r[0])
    }

    inferred = set()
    _codes_with_valid_history = set()
    _raw_obs_by_logical = {}
    for raw_code, nobs in conn.execute(
        """SELECT コード, COUNT(DISTINCT 日付)
           FROM price_history
           WHERE 終値 IS NOT NULL AND CAST(終値 AS REAL) > 0
           GROUP BY コード"""
    ).fetchall():
        c = canonical_code_for_db(raw_code)
        if not c:
            continue
        n = int(nobs or 0)
        if n > 0:
            _codes_with_valid_history.add(c)
            _raw_obs_by_logical.setdefault(c, []).append(n)
        if n >= 60:
            inferred.add(c)

    # P2-35: aliasごとは60未満でも、logical codeとして60日以上ある移行DBを見逃さない。
    # 合計すら60未満ならdistinct 60には絶対届かないため、疑わしい複数aliasだけ正確に再集計する。
    _split_candidates = [
        c for c, ns in _raw_obs_by_logical.items()
        if c not in inferred and len(ns) > 1 and sum(ns) >= 60
    ]
    if _split_candidates:
        _logical_counts = _valid_price_history_obs_counts(conn, _split_candidates)
        inferred.update(c for c, n in _logical_counts.items() if int(n or 0) >= 60)

    # P1-682/P1-685: bootstrap markerだけ残り、price_historyの有効終値が0件になったコードを
    # existing扱いしない。さらにmarkerをメモリ上で無視するだけでは、同runの12mo再取得が
    # 部分失敗して1〜2本だけ履歴が戻った次runに古いmarkerが再活性化し、12mo bootstrapを
    # 再び永久skipできる。履歴0件を検知した時点でDB上の古いmarker自体を原子的に削除する。
    _stale_bootstrap_markers = sorted(marked - _codes_with_valid_history)
    if _stale_bootstrap_markers:
        _sp = f"sp_p1_685_bootstrap_marker_gc_{time.time_ns()}"
        try:
            conn.execute(f"SAVEPOINT {_sp}")
            conn.executemany(
                "DELETE FROM price_history_bootstrap_state WHERE コード=?",
                [(c,) for c in _stale_bootstrap_markers],
            )
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_sp}")
            except Exception:
                pass
            raise
        marked.difference_update(_stale_bootstrap_markers)

    marked.intersection_update(_codes_with_valid_history)

    new_inferred = sorted(inferred - marked)
    if new_inferred:
        _mark_price_history_bootstrapped(conn, new_inferred, "inferred>=60")
        marked.update(new_inferred)
    return marked


def _to_long_history(df_wide: pd.DataFrame, codes_map) -> pd.DataFrame:
    """
    yfinance.download の戻り(MultiIndex列)をロング形式にする。
    codes_map: { '7203': '7203.T', ... } 逆引きに使う。
    """
    if df_wide is None or df_wide.empty:
        return pd.DataFrame(columns=["日付","コード","始値","高値","安値","終値","出来高"])
    # P1-171: 保存キーは必ずcanonical code。
    codes_map = {canonical_code_for_db(k): v for k, v in dict(codes_map).items() if canonical_code_for_db(k)}
    if not codes_map:
        return pd.DataFrame(columns=["日付","コード","始値","高値","安値","終値","出来高"])
    # 単一銘柄のときは列がMultiIndexではない場合がある
    if isinstance(df_wide.columns, pd.MultiIndex):
        # P2-39: yfinance/Pandasの版やgroup_by設定で MultiIndex が
        # [Ticker, Price] / [Price, Ticker] のどちらにもなり得る。
        # level=0決め打ちをやめ、実際の問い合わせsymbolが含まれる階層を検出する。
        _symbols_u = {str(v).upper() for v in codes_map.values()}
        _level_scores = []
        for _lv in range(df_wide.columns.nlevels):
            _vals = {str(v).upper() for v in df_wide.columns.get_level_values(_lv)}
            _level_scores.append(len(_vals & _symbols_u))
        if not _level_scores or max(_level_scores) <= 0:
            raise RuntimeError(
                f"yfinance MultiIndex has no recognizable ticker level: "
                f"levels={list(df_wide.columns.names)} symbols={sorted(_symbols_u)[:5]}"
            )
        _ticker_level = int(max(range(len(_level_scores)), key=lambda j: _level_scores[j]))
        _work = df_wide.copy()
        _names = list(_work.columns.names)
        _ticker_name = "__p2_ticker__"
        _names[_ticker_level] = _ticker_name
        _work.columns = _work.columns.set_names(_names)
        # P2-47: pandas 2.1+ の新stack実装を優先し、旧版だけ互換fallback。
        # FutureWarningを抑えつつ、古い実行環境でも同じロング化経路を維持する。
        try:
            df = _work.stack(level=_ticker_level, future_stack=True).reset_index()
        except TypeError:
            df = _work.stack(level=_ticker_level).reset_index()
        df.rename(columns={_ticker_name:"Ticker", "Date":"日付"}, inplace=True)
        _reverse_codes = {str(v).upper(): k for k, v in codes_map.items()}
        df["コード"] = df["Ticker"].astype(str).str.upper().map(_reverse_codes)
    else:
        # 1銘柄のみ
        df = df_wide.reset_index().copy()
        df["Ticker"] = list(codes_map.values())[0]
        code = list(codes_map.keys())[0]
        df["コード"] = code
        df.rename(columns={"Date":"日付"}, inplace=True)

    df = df.rename(columns={
        "Open":"始値","High":"高値","Low":"安値","Close":"終値","Volume":"出来高"
    })
    # 欲しい列だけ、欠損行は落とす
    cols = ["日付","コード","始値","高値","安値","終値","出来高"]
    df = df[cols].dropna(subset=["日付","終値"])

    # P1-172: yfinanceのtz-aware indexをJST市場日付へ変換してからdate化。
    # UTC 16:00 は日本では翌日なので、単純 .dt.date では1日ずれる。
    df["日付"] = pd.to_datetime(df["日付"], utc=True, errors="coerce").dt.tz_convert("Asia/Tokyo").dt.date
    
    # ★追加：9:00前なら、APIが返してきた「今日」のデータを未来データとして削除する
    now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
    if now_jst.time() < dt_time(9, 0):
        df = df[df["日付"] < now_jst.date()]
    else:
        df = df[df["日付"] <= now_jst.date()]

    # 重複除去（同一日・同一コード）
    df = df.drop_duplicates(subset=["コード","日付"], keep="last")
    return df.sort_values(["コード","日付"])

def _upsert_price_history(conn, df_add: pd.DataFrame) -> int:
    if df_add is None or df_add.empty:
        return 0

    # P1-351: 旧実装はlegacy alias DELETEをcommitした後にpandas.to_sqlでappendしていた。
    # append失敗時に既存履歴だけ消えるため、DELETE+INSERTを同一SAVEPOINTで原子的に行う。
    cols = ["日付", "コード", "始値", "高値", "安値", "終値", "出来高"]
    work = df_add.copy()
    for c in cols:
        if c not in work.columns:
            work[c] = None
    # P1-462: DataFrame型bulk writerも同じ営業日guardを通す。
    _raw_write_rows = list(work[cols].itertuples(index=False, name=None))
    _raw_write_rows = _filter_price_history_write_rows(_raw_write_rows, date_index=0)
    if not _raw_write_rows:
        return 0
    work = pd.DataFrame(_raw_write_rows, columns=cols)
    rows = []
    del_rows = []
    for _, r in work[cols].iterrows():
        code = canonical_code_for_db(r["コード"])
        d = str(r["日付"])[:10]
        if not code or not d:
            continue
        vals = []
        for c in ("始値", "高値", "安値", "終値", "出来高"):
            v = r[c]
            if pd.isna(v):
                v = None
            elif c == "出来高":
                try: v = int(v)
                except Exception: v = None
            else:
                try: v = float(v)
                except Exception: v = None
            vals.append(v)
        rows.append((code, d, *vals))
        # canonical行はON CONFLICTで非NULL値を保持し、legacy aliasだけ削除する。
        for cv in code_query_variants(code):
            if str(cv) != str(code):
                del_rows.append((cv, d))

    if not rows:
        return 0

    sp = f"ph_upsert_{time.time_ns()}"
    cur = conn.cursor()
    try:
        conn.execute(f"SAVEPOINT {sp}")
        if del_rows:
            cur.executemany(
                "DELETE FROM price_history WHERE CAST(コード AS TEXT)=? AND 日付=?",
                del_rows
            )
        cur.executemany(
            """
            INSERT INTO price_history(コード,日付,始値,高値,安値,終値,出来高)
            VALUES(?,?,?,?,?,?,?)
            ON CONFLICT(コード,日付) DO UPDATE SET
              始値=COALESCE(excluded.始値, 始値),
              高値=COALESCE(excluded.高値, 高値),
              安値=COALESCE(excluded.安値, 安値),
              終値=COALESCE(excluded.終値, 終値),
              出来高=COALESCE(excluded.出来高, 出来高)
            """,
            rows
        )
        conn.execute(f"RELEASE SAVEPOINT {sp}")
        return len(rows)
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        raise
    finally:
        cur.close()


def _update_screener_from_history(conn, codes):
    """
    price_history の直近2日から
    現在値 / 前日終値 / 前日円差 / 前日終値比率(％) / 出来高 を更新する。
    すべて小数2桁（％含む）でDB保存する。
    """
    def _r2(x):
        try:
            return None if x is None else round(float(x), 2)
        except Exception:
            return None

    # P1-90: 市場全体（今回対象コード群）の最新価格日を先に確定する。
    # 個別銘柄だけ1営業日古い場合、その古い足を今日の値として screener へ再同期しない。
    # P1-173: history/screener結合キーをcanonical化。
    codes = list(dict.fromkeys(canonical_code_for_db(c) for c in codes if canonical_code_for_db(c)))
    # P1-247: JOIN/検索はcanonical、最終UPDATEはscreenerに実在するrawコードへ。
    _sraw = {}
    # P2-18: raw-key map取得失敗時にcanonical値でUPDATEを続けると0件更新を成功扱いし得る。
    # screener読込障害はfail-visibleにしてsnapshot混在を防ぐ。
    for (_rc,) in conn.execute("SELECT コード FROM screener").fetchall():
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _sraw:
            _sraw[_rk] = _rc
    # P1-594: PREOPENでは前営業日が価格正本。壁時計の今日まで読んでしまうと、
    # legacy/部分当日行が1本ある銘柄だけそれを先頭に拾い、本来使える前営業日価格まで
    # NULL化し得るためSQL上限はrun snapshotへ統一する。
    _cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    # P1-600: fresh判定の基準日を「対象codesの中で一番新しい日」にしない。
    # 全銘柄が同じ1営業日古い場合、_latest_valid_history_date(conn, codes)だとその古い日を
    # market_asofとして全員fresh扱いできる。今回到達すべきJPX snapshot日そのものを正本にする。
    market_asof = _cutoff

    # P2-17/P2-80: 200銘柄ごとのCAST条件は大きなprice_historyを約18回走査していた。
    # 表を1回だけ走査し、raw aliasごとの直近8行へSQL側で絞った後、
    # logical codeごとの直近2営業日へdedupeする。日付下限は置かないため、
    # 長期売買停止後の前回終値も維持する。
    _recent_history = {}
    _bulk_hist = pd.read_sql_query(
        """
        WITH ranked AS (
            SELECT rowid AS _rowid, コード, 日付, 終値, 出来高,
                   ROW_NUMBER() OVER (
                       PARTITION BY CAST(コード AS TEXT)
                       ORDER BY date(日付) DESC, rowid DESC
                   ) AS _rn
            FROM price_history
            WHERE date(日付) <= date(?)
        )
        SELECT _rowid, コード, 日付, 終値, 出来高
        FROM ranked
        WHERE _rn <= 8
        ORDER BY 日付 DESC, _rowid DESC
        """,
        conn, params=[_cutoff]
    )
    if not _bulk_hist.empty:
        _bulk_hist = _dedupe_price_history_df(_bulk_hist)
        _wanted = set(codes)
        _bulk_hist = _bulk_hist[
            _bulk_hist["コード"].map(canonical_code_for_db).isin(_wanted)
        ].copy()
        for _hcode, _hg in _bulk_hist.groupby("コード", sort=False):
            _hk = canonical_code_for_db(_hcode)
            if _hk:
                _recent_history[_hk] = (
                    _hg.sort_values("日付", ascending=False).head(2).reset_index(drop=True)
                )

    updated = []
    for code in codes:
        _ck = canonical_code_for_db(code)
        df = _recent_history.get(_ck)
        if df is None:
            df = pd.DataFrame(columns=["コード", "日付", "終値", "出来高"])
        if df.empty:
            # P1-564: 対象銘柄の価格履歴が今回まったく無い場合、前回の現在値/騰落率/出来高を
            # current EOD値として残さない。価格系はauthoritativeに不明へ戻す。
            updated.append((None, None, None, None, None, None, _sraw.get(_ck, _ck)))
            continue
        today = df.iloc[0]
        actual_date = str(today["日付"])[:10]
        # P1-564: 個別銘柄の最終足が市場as-ofより古い場合も、前回screener価格を保持しない。
        # 旧P1-90はskipだけだったため、昨日の現在値/前日比/出来高が今日の正式スコア入力へ残れた。
        if market_asof and actual_date != market_asof:
            updated.append((None, None, None, None, None, actual_date, _sraw.get(_ck, _ck)))
            continue
        close_t = today["終値"]
        vol_t   = today["出来高"]
        # P1-635: 期待as-ofの日付行が存在しても、終値がNULL/NaNならcurrent価格は成立しない。
        # 日付だけfreshな部分行で前回値やNaN派生値をcurrent扱いしない。
        if close_t is None or pd.isna(close_t):
            updated.append((None, None, None, None, None, actual_date, _sraw.get(_ck, _ck)))
            continue

        prev = df.iloc[1]["終値"] if len(df) >= 2 else None
        yen = pct = None
        if prev is not None and pd.notna(prev) and float(prev) != 0.0:
            yen = float(close_t) - float(prev)
            pct = yen / float(prev) * 100.0

        updated.append((
            _r2(close_t),                        # 現在値 → 2桁
            _r2(prev),                           # 前日終値 → 2桁
            _r2(yen),                            # 前日円差 → 2桁
            _r2(pct),                            # 前日終値比率(％) → 2桁
            int(vol_t) if pd.notna(vol_t) else None,  # 出来高
            actual_date,                         # P1-89: 実際の価格日をシグナル更新日に使う
            _sraw.get(canonical_code_for_db(code), canonical_code_for_db(code)),
        ))

    if updated:
        # P1-431: autocommit環境でも複数銘柄の価格同期を部分反映しない。
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_431_sync_screener_history")
            cur.executemany("""
                UPDATE screener
                   SET 現在値=?,
                       前日終値=?,
                       前日円差=?,
                       前日終値比率=?,
                       出来高=?,
                       シグナル更新日=?
                 WHERE コード=?
            """, updated)
            cur.execute("RELEASE SAVEPOINT p1_431_sync_screener_history")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_431_sync_screener_history")
                cur.execute("RELEASE SAVEPOINT p1_431_sync_screener_history")
            except Exception:
                pass
            raise
        finally:
            cur.close()
        
def filter_valid_yahoo_codes(conn: sqlite3.Connection, codes: list, *, for_history: bool = False) -> list:
    """Yahoo照会対象をcanonical化・重複除去し、自動soft sentinelを尊重する。

    P1-507:
      NONE         : 当日はYahoo全照会をskip
      HISTORY_NONE : 当日の履歴照会だけskip（quote/財務は通常symbolで継続）
      sentinelは翌JST日には期限切れとなり再試行される。
    """
    if not codes:
        return []
    base = list(dict.fromkeys(
        canonical_code_for_db(c) for c in codes if canonical_code_for_db(c)
    ))
    if not base or conn is None:
        return base

    # P2-27: _active_yahoo_override_sentinel()を銘柄ごとに呼ぶN+1を廃止。
    # canonical→legacy aliasの優先順位と「当日だけ有効」はそのままbulkで再現する。
    try:
        _ensure_override_table(conn)
        all_vars = expand_code_query_variants(base)
        rec_by_raw = {}
        for i in range(0, len(all_vars), 500):
            part = all_vars[i:i + 500]
            if not part:
                continue
            ph = ",".join("?" * len(part))
            for raw_code, sym, updated_at in conn.execute(
                f"SELECT CAST(コード AS TEXT), 問い合わせシンボル, updated_at "
                f"FROM yahoo_symbol_override WHERE CAST(コード AS TEXT) IN ({ph})",
                tuple(part),
            ).fetchall():
                rec_by_raw[str(raw_code)] = (sym, updated_at)
    except Exception as _e:
        # P2-37: sentinel表の読込障害を「sentinel無し」とみなすと、NONE指定銘柄を
        # 通常Yahoo銘柄として再照会し、当日snapshotの意味が崩れる。主要bulk処理ではfail-visible。
        raise RuntimeError(f"Yahoo sentinel lookup failed: {_e}") from _e

    today = date.fromisoformat(_today_jst())
    out = []
    for cc in base:
        rec = None
        for v in code_query_variants(cc):
            if str(v) in rec_by_raw:
                rec = rec_by_raw[str(v)]
                break
        state = None
        if rec:
            sym, updated_at = rec
            cand = str(sym or "").strip().upper()
            if cand in {"NONE", "HISTORY_NONE"} and updated_at:
                try:
                    d = datetime.strptime(str(updated_at)[:10], "%Y-%m-%d").date()
                    if d == today:
                        state = cand
                except Exception:
                    state = None
        if state == "NONE":
            continue
        if for_history and state == "HISTORY_NONE":
            continue
        out.append(cc)
    return out


def filter_yahoo_company_codes(conn: sqlite3.Connection, codes: list) -> list:
    """P1-549: Yahooの企業情報APIへ渡す対象から指数を除外する。

    price_history / intradayでは ^TOPX 等の指数も必要だが、marketCapや財務諸表は
    企業情報なので指数を混ぜない。P1-346以降の地方市場対応は維持し、指数だけを分離する。
    """
    base = filter_valid_yahoo_codes(conn, codes)
    if not base:
        return []
    # P2-48: 全screenerを読むのではなく対象codesのalias候補だけをbulk取得する。
    # さらにDB返却順には依存せず code_query_variants() のcanonical-first順で市場を確定する。
    # これにより 7203 / 7203.0 等のlegacy aliasが異なる市場値を持っていても結果が決定的になる。
    market_map = {}
    if conn is not None:
        _variants = expand_code_query_variants(base)
        _market_by_raw = {}
        try:
            for _i in range(0, len(_variants), 500):
                _part = _variants[_i:_i + 500]
                if not _part:
                    continue
                _ph = ",".join("?" for _ in _part)
                _rows = conn.execute(
                    f"SELECT CAST(コード AS TEXT), 市場 FROM screener "
                    f"WHERE CAST(コード AS TEXT) IN ({_ph})",
                    _part,
                ).fetchall()
                for _raw, _mkt in _rows:
                    _market_by_raw[str(_raw).strip()] = str(_mkt or "")
        except Exception as _e:
            # P2-33/P2-48: 市場map取得に失敗した状態で空mapへ落とすと、
            # 数字コードの指数等を企業情報APIへ誤送信し得る。DB障害は成功風に継続しない。
            raise RuntimeError(f"Yahoo company-universe market lookup failed: {_e}") from _e
        for _code in base:
            _cc = canonical_code_for_db(_code)
            if not _cc:
                continue
            _chosen = ""
            for _v in code_query_variants(_cc):
                if _v in _market_by_raw:
                    _chosen = _market_by_raw[_v]
                    break
            market_map[_cc] = _chosen
    out = []
    for c in base:
        cc = canonical_code_for_db(c)
        mkt = market_map.get(cc, "")
        if not cc or cc.startswith("^") or "指数" in mkt:
            continue
        out.append(cc)
    return list(dict.fromkeys(out))


def phase_yahoo_bulk_refresh(conn, codes, batch_size=100):
    """
    高速版:
      - 既存銘柄: period="2d", interval="1d" をバルクで取得し、差分だけ upsert
      - 未収録銘柄: period="12mo" をバルクで取得して初期投入
      - screener は price_history の直近2日から 前日終値比率/出来高/現在値 を更新
      - 時価総額は速度優先で更新しない（必要なら別フェーズで）
    """
    # P1-175: refresh universeをcanonical化し重複除去。
    codes = list(dict.fromkeys(canonical_code_for_db(c) for c in codes if canonical_code_for_db(c)))
    # P1-636: sentinelはYahoo履歴取得をskipするだけ。元universeから外すと
    # _update_screener_from_history()の鮮度clear対象からも消え、前回価格を残せるため保持する。
    _requested_price_codes = list(codes)
    # P1-509: HISTORY_NONEは当日の履歴取得だけskipするsoft sentinel。
    codes = filter_valid_yahoo_codes(conn, codes, for_history=True)
    have = _codes_with_data(conn)
    exist_codes = [c for c in codes if c in have]
    new_codes   = [c for c in codes if c not in have]

    # P2-46: 全universeで一度だけsymbol解決。別chunk/既存新規境界のcollisionも検知する。
    _bulk_symbols = resolve_yahoo_symbols_bulk(codes, conn)
    _bulk_symbol_map = dict(zip(codes, _bulk_symbols))

    total_added = 0
    # P1-478: 15:30直後などでYahoo日足がまだ前営業日までしか返らない場合、
    # 「レスポンスあり」だけで当日EOD完了markerを立てない。今回取得分に期待市場日が
    # 1行以上含まれることを最後に要求する。
    _expected_bulk_date = _expected_jpx_asof_date().isoformat()
    _fresh_expected_rows = 0
    # P1-113: 日次once処理では、チャンク失敗を握りつぶすと「本日完了」マーカーが立ち、
    # 同日中に再試行できなくなる。成功チャンクはupsertしてよいが、失敗が1件でもあれば
    # 後処理後に例外を返してdaily markerを書かせない。
    _failed_chunks = []

    # P1-683: 期待日の行が返っているのにOHLCVが部分欠損なら、
    # その行をEOD確定足として成功扱いしない。_upsert_price_history()はNULLをCOALESCEで
    # 既存値へ残すため、場中出来高/高安とEOD終値が混ざった足を作り得る。
    # 一方、期待日の行自体が無い銘柄（売買停止等）はここでは失敗にせず、既存のstale clearへ任せる。
    def _incomplete_expected_bar_codes(_df):
        if _df is None or getattr(_df, "empty", True):
            return []
        _need = {"コード", "日付", "始値", "高値", "安値", "終値", "出来高"}
        if not _need.issubset(_df.columns):
            return []
        _w = _df[list(_need)].copy()
        _w["コード"] = _w["コード"].map(canonical_code_for_db)
        _w["日付"] = _w["日付"].astype(str).str[:10]
        _w = _w[_w["日付"] == _expected_bulk_date].copy()
        if _w.empty:
            return []
        for _c in ("始値", "高値", "安値", "終値", "出来高"):
            _w[_c] = pd.to_numeric(_w[_c], errors="coerce")
        # P1-691: 指数は出来高をformal入力に使わないため、OHLCだけを完全性必須とする。
        # Yahoo側で指数VolumeがNULLでも、RS/Growth_Bias用の確定指数価格まで全EOD失敗にしない。
        _is_index_bar = _w["コード"].astype(str).str.startswith("^")
        _bad_price = (
            _w[["始値", "高値", "安値", "終値"]].isna().any(axis=1)
            | ~np.isfinite(_w[["始値", "高値", "安値", "終値"]]).all(axis=1)
            | (_w[["始値", "高値", "安値", "終値"]] <= 0).any(axis=1)
        )
        _bad_volume = (
            _w["出来高"].isna()
            | ~np.isfinite(_w["出来高"])
            | (_w["出来高"] < 0)
        )
        _bad = _bad_price | ((~_is_index_bar) & _bad_volume)
        return sorted({c for c in _w.loc[_bad, "コード"].dropna() if c})

    # 1) 既存銘柄: 2日分だけ一括取得（バッチ分割）
    for i in range(0, len(exist_codes), batch_size):
        chunk = exist_codes[i:i+batch_size]
        # P1-511: EOD bulk前のN+1 online probeを廃止。
        # P1-511: 新規12mo bulkでも個別probeを重ねない。
        tickers_map = {c: _bulk_symbol_map[c] for c in chunk}
        
        # すべてのシンボルをそのまま許可
        safe_tickers = list(tickers_map.values())
        
        if not safe_tickers:
            continue

        try:
            # ★修正: auto_adjust=False
            df_wide = _yfinance_download_repaired(safe_tickers, period="2d", interval="1d", group_by="ticker", threads=True, auto_adjust=False)
            df_add = _to_long_history(df_wide, tickers_map)
            # P1-116: downloadが例外を出さなくても、空/部分レスポンスなら未取得コードを失敗扱いにする。
            # P1-176: Yahoo部分応答の照合もcanonical codeで比較。
            _returned = set() if df_add is None or df_add.empty or "コード" not in df_add.columns else {canonical_code_for_db(x) for x in df_add["コード"].dropna()}
            _missing = [c for c in chunk if canonical_code_for_db(c) not in _returned]
            if _missing:
                _failed_chunks.append(("exist-partial", list(_missing), "empty/partial yahoo response"))
            _incomplete = _incomplete_expected_bar_codes(df_add)
            if _incomplete:
                _failed_chunks.append(("exist-incomplete-eod", list(_incomplete), "expected-date OHLCV incomplete"))
                # P1-686: 失敗判定した期待日partial barをDBへ残さない。
                # P1-683はmarkerを立てないだけだったため、再実行なしで翌PREOPENになると
                # 終値だけある不完全足がfresh判定を通れる余地があった。
                _inc_set = {canonical_code_for_db(c) for c in _incomplete if canonical_code_for_db(c)}
                _code_norm = df_add["コード"].map(canonical_code_for_db)
                _date_norm = df_add["日付"].astype(str).str[:10]
                df_add = df_add.loc[~((_date_norm == _expected_bulk_date) & _code_norm.isin(_inc_set))].copy()
            if df_add is not None and not df_add.empty and "日付" in df_add.columns:
                _fresh_expected_rows += int((df_add["日付"].astype(str).str[:10] == _expected_bulk_date).sum())
            total_added += _upsert_price_history(conn, df_add)
            print(f"[refresh/exist] {i+len(chunk)}/{len(exist_codes)} (+{len(df_add)} rows)")
        except Exception as e:
            _failed_chunks.append(("exist", list(chunk), str(e)))
            print(f"[refresh/exist] chunk error: {e}")

    # 2) 新規銘柄: 12ヶ月ぶんを一括取得（バッチ分割）
    for i in range(0, len(new_codes), batch_size):
        chunk = new_codes[i:i+batch_size]
        tickers_map = {c: _bulk_symbol_map[c] for c in chunk}
        
        # すべてのシンボルをそのまま許可
        safe_tickers = list(tickers_map.values())

        if not safe_tickers:
            continue

        try:
            # ★修正: auto_adjust=False
            df_wide = _yfinance_download_repaired(safe_tickers, period="12mo", interval="1d", group_by="ticker", threads=True, auto_adjust=False)
            df_add = _to_long_history(df_wide, tickers_map)
            # P1-176: Yahoo部分応答の照合もcanonical codeで比較。
            _returned = set() if df_add is None or df_add.empty or "コード" not in df_add.columns else {canonical_code_for_db(x) for x in df_add["コード"].dropna()}
            _missing = [c for c in chunk if canonical_code_for_db(c) not in _returned]
            if _missing:
                _failed_chunks.append(("new-partial", list(_missing), "empty/partial yahoo response"))
            _incomplete = _incomplete_expected_bar_codes(df_add)
            if _incomplete:
                _failed_chunks.append(("new-incomplete-eod", list(_incomplete), "expected-date OHLCV incomplete"))
                # P1-686: 新規12mo取得でも不完全な期待日barだけは保存しない。
                # それ以前の正常な履歴行は保持してbootstrap素材として利用できる。
                _inc_set = {canonical_code_for_db(c) for c in _incomplete if canonical_code_for_db(c)}
                _code_norm = df_add["コード"].map(canonical_code_for_db)
                _date_norm = df_add["日付"].astype(str).str[:10]
                df_add = df_add.loc[~((_date_norm == _expected_bulk_date) & _code_norm.isin(_inc_set))].copy()
            if df_add is not None and not df_add.empty and "日付" in df_add.columns:
                _fresh_expected_rows += int((df_add["日付"].astype(str).str[:10] == _expected_bulk_date).sum())
            total_added += _upsert_price_history(conn, df_add)
            # P1-681: 新規12mo取得も期待JPX日まで有効終値が到達したコードだけbootstrap完了。
            _bootstrap_fresh = set()
            if df_add is not None and not df_add.empty and {"コード", "日付", "終値"}.issubset(df_add.columns):
                _tmp_boot = df_add[["コード", "日付", "終値"]].copy()
                _tmp_boot["コード"] = _tmp_boot["コード"].map(canonical_code_for_db)
                _tmp_boot["日付"] = _tmp_boot["日付"].astype(str).str[:10]
                _tmp_boot["終値"] = pd.to_numeric(_tmp_boot["終値"], errors="coerce")
                _tmp_boot = _tmp_boot[
                    (_tmp_boot["日付"] == _expected_bulk_date)
                    & _tmp_boot["終値"].notna()
                    & np.isfinite(_tmp_boot["終値"])
                    & (_tmp_boot["終値"] > 0)
                ]
                _bootstrap_fresh = {c for c in _tmp_boot["コード"].dropna() if c}
            if _bootstrap_fresh:
                _mark_price_history_bootstrapped(conn, sorted(_bootstrap_fresh), "bulk-new-12mo")
            print(f"[refresh/new ] {i+len(chunk)}/{len(new_codes)} (+{len(df_add)} rows)")
        except Exception as e:
            _failed_chunks.append(("new", list(chunk), str(e)))
            print(f"[refresh/new] chunk error: {e}")

    # P1-478: API lagで今回取得が全部前営業日なら、古いDBを更新して成功markerを立てない。
    if codes and _fresh_expected_rows <= 0:
        raise RuntimeError(
            f"Yahoo EOD refresh returned no rows for expected JPX as-of {_expected_bulk_date}; retry later"
        )

    # 3) screener 更新（price_history 由来）
    # P1-636: Yahoo取得対象外sentinelも含む元universeでcurrent/staleを再確定する。
    _update_screener_from_history(conn, _requested_price_codes)
    apply_auto_metrics_eod(conn)
    apply_composite_score(conn)
    print(f"[refresh] 追記 {total_added} 行 / 銘柄 {len(codes)} 件（既存{len(exist_codes)}・新規{len(new_codes)}）")
    if _failed_chunks:
        failed_codes = [c for _, chunk, _ in _failed_chunks for c in chunk]
        raise RuntimeError(
            f"P1-113 yahoo_bulk_refresh: {len(_failed_chunks)} chunk(s) failed; "
            f"daily marker is intentionally not written. retry codes={failed_codes[:20]}"
        )
# ==== 時価総額取得

# ===== 全銘柄の時価総額を一括更新 =====

def _normalize_map(obj):
    """
    yahooquery.YQ(...).summary_detail / price の戻りを
    {symbol: { ... }} 形式の dict に正規化。文字列はスキップ。
    """
    if isinstance(obj, dict):
        # まれに '7203.T': 'Not Found' みたいな文字列が入るので弾く
        return {k: v for k, v in obj.items() if not isinstance(v, str)}
    if isinstance(obj, pd.DataFrame):
        if 'symbol' in obj.columns:
            d = obj.set_index('symbol').to_dict(orient='index')
            # 値が文字列の行は弾く
            return {k: v for k, v in d.items() if not isinstance(v, str)}
        try:
            d = obj.to_dict(orient='index')
            return {k: v for k, v in d.items() if not isinstance(v, str)}
        except Exception:
            return {}
    return {}

def _extract_mcap(entry):
    """
    entry から marketCap を float に取り出す。
    entry が dict/Series/list/str など何が来ても安全に None 返し。
    """
    # dict 以外を可能な限り dict 化（Series, list[dict] 等）
    if isinstance(entry, pd.Series):
        entry = entry.to_dict()
    elif isinstance(entry, (list, tuple)):
        entry = entry[0] if entry and isinstance(entry[0], dict) else {}
    elif isinstance(entry, str) or entry is None:
        return None
    elif not isinstance(entry, dict):
        entry = {}

    v = entry.get('marketCap')
    if v is None:
        return None
    if isinstance(v, dict):           # {'raw': 123..., 'fmt': '...'} 形式
        v = v.get('raw') or v.get('fmt') or v.get('longFmt')
    try:
        return float(str(v).replace(',', ''))
    except Exception:
        return None

def update_market_cap_all(conn, batch_size=300, max_workers=8):
    """
    全銘柄の時価総額（億円）を高速に更新。
    P1-440: API取得中はDBを触らず、取得フェーズ完了後に一括SAVEPOINT反映する。
    後半チャンク障害で前半だけ新時価総額になる状態を防ぐ。
    """
    _screener_rows_mcap = conn.execute("SELECT コード, 市場 FROM screener").fetchall()
    _mcap_raw = {}
    _mcap_market = {}
    for _rc, _rmkt in _screener_rows_mcap:
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _mcap_raw:
            _mcap_raw[_rk] = _rc
            _mcap_market[_rk] = str(_rmkt or "")
    _all_mcap_codes = list(dict.fromkeys(
        canonical_code_for_db(r[0]) for r in _screener_rows_mcap if canonical_code_for_db(r[0])
    ))
    # P1-643: company universeをsentinel filter前にも保持する。NONEはYahoo取得をskipするだけで、
    # 前回の時価総額をcurrent値として残す根拠にはならない。指数は従来どおり対象外。
    _requested_company_codes = [
        c for c in _all_mcap_codes
        if not str(c).startswith("^") and "指数" not in _mcap_market.get(c, "")
    ]
    # P1-549: 時価総額は企業情報。^TOPX等の指数をYahoo company APIへ送らない。
    codes = filter_yahoo_company_codes(conn, _all_mcap_codes)
    _mcap_skipped_codes = set(_requested_company_codes) - set(codes)

    def _write_rows_atomic(rows, sp_name):
        if not rows:
            return
        cur = conn.cursor()
        try:
            cur.execute(f"SAVEPOINT {sp_name}")
            cur.executemany("UPDATE screener SET 時価総額億円=? WHERE コード=?", rows)
            _affected = cur.rowcount
            if _affected != len(rows):
                raise RuntimeError(
                    f"market-cap UPDATE rowcount mismatch: expected={len(rows)} affected={_affected}"
                )
            cur.execute(f"RELEASE SAVEPOINT {sp_name}")
        except Exception:
            try:
                cur.execute(f"ROLLBACK TO SAVEPOINT {sp_name}")
                cur.execute(f"RELEASE SAVEPOINT {sp_name}")
            except Exception:
                pass
            raise
        finally:
            cur.close()

    if not codes and not _mcap_skipped_codes:
        print("[mcap] 対象なし")
        return

    # P2-46: 全対象を先に解決して、batch境界を跨ぐoverride衝突も検知する。
    _mcap_symbols_all = resolve_yahoo_symbols_bulk(codes, conn) if codes else []

    try:
        # P2-52: yahooquery未導入なら、このphaseだけ正式なyfinance fallbackへ入る。
        if Ticker is None:
            raise ImportError("yahooquery is not installed")
        # P1-643: sentinel skip銘柄は取得不能current snapshotとしてNULLへ戻す。
        all_rows = [(None, _mcap_raw.get(c, c)) for c in sorted(_mcap_skipped_codes)]
        _mcap_missing_payload = []
        for i in range(0, len(codes), batch_size):
            chunk = codes[i:i+batch_size]
            # P1-511: summary_detail自体が存在/取得可否を返すため事前5d probeは不要。
            symbols = _mcap_symbols_all[i:i+batch_size]
            tq = Ticker(symbols, asynchronous=True, max_workers=max_workers)
            sd = _normalize_map(tq.summary_detail)
            pr = _normalize_map(tq.price)
            rows = []
            for code, sym in zip(chunk, symbols):
                # P2-30: 更新先codeはYahoo symbolから逆算せず、問い合わせ元chunkを正本にする。
                # 特殊override symbolでも別銘柄へ誤更新しない。
                code = canonical_code_for_db(code)
                # P1-514: symbol自体が両responseから消えた場合はYahoo部分応答。
                # 前回時価総額を残して成功扱いせず、DB反映前にフェーズを失敗させる。
                if sym not in sd and sym not in pr:
                    _mcap_missing_payload.append(sym)
                    continue
                entry_sd = sd.get(sym)
                mcap = _extract_mcap(entry_sd)
                if mcap is None:
                    mcap = _extract_mcap(pr.get(sym))
                # 応答はあるがmarketCap自体が無い銘柄は「不明」として旧値をNULLへ戻す。
                mcap_oku = None if mcap is None else round(mcap / 1e8, 2)
                rows.append((mcap_oku, _mcap_raw.get(code, code)))
            all_rows.extend(rows)
            print(f"[mcap/all] {i+len(chunk)}/{len(codes)} 応答 {len(rows)} 件")

        if _mcap_missing_payload:
            raise RuntimeError(
                f"market-cap Yahoo partial response: missing={_mcap_missing_payload[:20]} "
                f"count={len(_mcap_missing_payload)}"
            )
        _valid_mcap_count = sum(1 for v, _ in all_rows if v is not None)
        if codes and _valid_mcap_count <= 0:
            raise RuntimeError("P1-120 market-cap refresh returned zero valid market caps")
        _write_rows_atomic(all_rows, "p1_440_market_cap")
        print(f"[mcap] 合計更新 {len(all_rows)} 件（全銘柄）")

    except ImportError:
        print("[mcap] yahooquery未導入 → yfinance fast_info にフォールバック（遅い）")
        if yfinance is None:
            raise RuntimeError("market-cap refresh requires yahooquery or yfinance; neither is installed")
        # P1-643: fallback経路でもsentinel skipのauthoritative clearを維持する。
        rows = [(None, _mcap_raw.get(c, c)) for c in sorted(_mcap_skipped_codes)]
        _mcap_fallback_failed = []
        # P2-36: fallbackでも1銘柄ずつDB参照するscalar resolverへ戻さない。
        _fallback_symbols = _mcap_symbols_all
        for c, _sym in zip(codes, _fallback_symbols):
            try:
                # P1-512: yfinanceにはYahooQuery用YQコンストラクタを前提にしない。
                # 正式なTicker.fast_infoを使い、ここでも事前online probeは重ねない。
                fi = yfinance.Ticker(_sym).fast_info
                mc = getattr(fi, "market_cap", None)
                if mc is None and hasattr(fi, "get"):
                    mc = fi.get("market_cap")
                rows.append((None if mc is None else float(mc)/1e8, _mcap_raw.get(c, c)))
            except Exception as _e:
                _mcap_fallback_failed.append((c, str(_e)))
        if _mcap_fallback_failed:
            raise RuntimeError(f"market-cap fallback partial failure: {_mcap_fallback_failed[:10]}")
        if codes and not any(v is not None for v, _ in rows):
            raise RuntimeError("P1-120 market-cap fallback returned zero valid market caps")
        _write_rows_atomic(rows, "p1_440_market_cap_fallback")
        print(f"[mcap/yf] 更新 {len(rows)} 件（フォールバック）")


def r2(x):
    """小数点2桁に丸め（None安全）"""
    try:
        return None if x is None else round(float(x), 2)
    except Exception:
        return None

def phase_yahoo_intraday_snapshot(conn: sqlite3.Connection):
    cur = conn.cursor()
    # P1-656: MIDDAY_FILTER_BY_FLAGS=True はquote取得対象を絞るだけで、
    # filter対象外の前回EOD/live値を今回MIDDAY値として残してよい意味ではない。
    # production runでは全screener raw keyも保持し、対象外を後段のlive-only clearへ回す。
    _all_mid_raw = {}
    if MIDDAY_FILTER_BY_FLAGS and not TEST_MODE:
        cur.execute("SELECT コード FROM screener")
        for (_arc,) in cur.fetchall():
            _ak = canonical_code_for_db(_arc)
            if _ak and _ak not in _all_mid_raw:
                _all_mid_raw[_ak] = _arc

    if MIDDAY_FILTER_BY_FLAGS:
        # P1-178: 旧SQLは絞込条件をLEFT JOINのON句へORで入れており、
        # 全件残留・重複・「コード」曖昧参照を起こす。明示されている時価総額条件をWHEREへ。
        cur.execute("""
            SELECT DISTINCT s.コード
            FROM screener s
            WHERE s.時価総額億円 BETWEEN 50 AND 5000
        """)
    else:
        cur.execute("SELECT コード FROM screener")
    _mid_rows = cur.fetchall()
    _mid_raw = {}
    for (_rc,) in _mid_rows:
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _mid_raw:
            _mid_raw[_rk] = _rc
    codes = list(dict.fromkeys(canonical_code_for_db(r[0]) for r in _mid_rows if canonical_code_for_db(r[0])))
    cur.close()

    # P2-81: 時価総額filterで企業だけに絞ると、地合い/RSの正本である
    # TOPIXまで取得対象外になる。DB内コードは^TOPXのまま、Yahoo照会時だけ
    # 998405.Tへ変換してprice_historyへ当日指数を保存する。
    # P2-81/P2-87: 正式指数がYahooの場中quoteへ未到達でも、価格系列を
    # 混ぜずに期間returnだけ代理利用できるよう連動ETFもcurrent取得する。
    _benchmark_codes = ("^TOPX", "^N225", "2516", "1306")
    for _bench in _benchmark_codes:
        if _bench not in codes:
            codes.append(_bench)
            _mid_raw[_bench] = _all_mid_raw.get(_bench, _bench)
    _mid_requested_codes = list(codes)
    # P1-656: filter対象外はproduction MIDDAYでは「今回live値なし」。
    # TEST_MODEは意図的な部分実行なので、テスト対象外の実DB行までは破壊しない。
    _mid_filter_excluded_raw = set()
    if MIDDAY_FILTER_BY_FLAGS and not TEST_MODE and _all_mid_raw:
        _selected_keys = set(_mid_requested_codes)
        _mid_filter_excluded_raw = {
            _raw for _key, _raw in _all_mid_raw.items() if _key not in _selected_keys
        }
    
    # P1-179: MIDDAY問い合わせ・更新キーもcanonical化。
    codes = filter_valid_yahoo_codes(conn, codes) # ★ここで除外！
    # P1-637: sentinelはquote取得対象外なだけで、旧live値の鮮度clear対象には残す。
    _mid_skipped_codes = set(_mid_requested_codes) - set(codes)

    if not codes:
        # 全件sentinelでも早期returnせず、下段で旧live値をauthoritativeにclearする。
        print("対象quoteコードなし：取得はスキップし、旧live値の鮮度clearのみ実行")
    if TEST_MODE:
        codes = codes[:TEST_LIMIT]
        print(f"[TEST] {len(codes)}銘柄(MIDDAY)に絞って実行")

    if Ticker is None:
        raise RuntimeError("MIDDAY quote refresh requires yahooquery; install yahooquery before running this phase")
    symbols_all = resolve_yahoo_symbols_bulk(codes, conn)
    print(f"[MIDDAY] quotes取得: {len(symbols_all)}銘柄")
    trade_date = None  # per-symbol from quote

    up_screener, up_hist = [], []
    # P1-515/P1-637: 部分レスポンスに加え、sentinelでquote自体をskipした銘柄も
    # 前回live値を残さないため今回invalidとして扱う。
    _mid_invalid_raw = {_mid_raw.get(c, c) for c in _mid_skipped_codes}
    # P1-656: quote filter対象外も旧live-only snapshotを残さない。
    _mid_invalid_raw.update(_mid_filter_excluded_raw)
    # P1-566: 個別quoteも当日JPX日付に到達したものだけcurrent MIDDAY値として採用する。
    _mid_expected = _expected_jpx_asof_date().isoformat()
    # P2-31: quoteごとのget_prev_close_db_first() N+1を廃止。
    # 当日quoteだけを採用するため、必要なのは共通の直前営業日1日分だけ。
    _mid_prev_map = _load_prev_close_map_for_trade_date(conn, codes, _mid_expected)
    batch = YQ_BATCH_MID
    for i in range(0, len(symbols_all), batch):
        symbols = symbols_all[i:i+batch]
        batch_codes = codes[i:i+batch]
        t = Ticker(symbols, max_workers=YQ_MAX_WORKERS)
        # P1-556: yahooquery propertyをisinstance判定と代入で2回評価しない。
        # t.quotes はネットワーク取得なので、成功時に同じbatchを二重要求して429/部分応答を増やし得た。
        _quotes_payload = t.quotes
        quotes = _quotes_payload if isinstance(_quotes_payload, dict) else {}

        for code, sym in zip(batch_codes, symbols):
            q = quotes.get(sym)
            # P2-30: 特殊Yahoo overrideでも問い合わせ元logical codeへ戻す。
            code = canonical_code_for_db(code)
            _raw_db_code = _mid_raw.get(code, code)

            # P1-515: quote欠落/エラー/価格・取引日欠損は「今回不明」。
            # 前回の現在値を今日のlive値として残さず、後段の同一SAVEPOINTでNULLへ戻す。
            if not isinstance(q, dict):
                _mid_invalid_raw.add(_raw_db_code)
                continue
            last = ffloat(q.get("regularMarketPrice"), None)
            if last is None:
                _mid_invalid_raw.add(_raw_db_code)
                continue
            trade_date = _trade_date_from_quote(q)
            if not trade_date:
                _mid_invalid_raw.add(_raw_db_code)
                continue
            # P1-566: 市場全体では当日quoteが取れていても、個別休止/キャッシュされた前日quoteを
            # current price・RVOL・正式スコアへ混ぜない。前日quoteは今回不明として価格欄をclearする。
            if str(trade_date)[:10] != _mid_expected:
                _mid_invalid_raw.add(_raw_db_code)
                continue

            prev_api = ffloat(q.get("regularMarketPreviousClose"), None)

            # P2-31: DBの直前営業日終値をbulk mapから優先し、無ければYahoo previousClose。
            prev = _mid_prev_map.get(code)
            if prev is None and prev_api is not None:
                try:
                    _qp = float(prev_api)
                    if np.isfinite(_qp) and _qp != 0.0:
                        prev = _qp
                except Exception:
                    prev = None

            yen = pct = None
            if last is not None and prev is not None and prev != 0:
                yen = last - prev
                pct = yen / prev * 100.0

            vol  = fint(q.get("regularMarketVolume"), None)  # P1-87: 欠損出来高を0株と捏造しない
            mcap = ffloat(q.get("marketCap"), 0.0)
            zika_oku = None if not mcap else round(mcap / 100_000_000.0, 2)

            # ← tupleの順序を変更：前日終値・前日円差・前日終値比率を全部入れる
            # ここでDB側の正規コードに寄せる（^TOPIX → ^TOPX など）
            code = canonical_code_for_db(code)
            up_screener.append((
                None if last is None else round(float(last), 2),   # 現在値 2桁
                None if prev is None else round(float(prev), 2),   # 前日終値 2桁
                None if yen  is None else round(float(yen),  2),   # 前日円差 2桁
                None if pct  is None else round(float(pct),  2),   # 前日終値比率(％) 2桁
                None if vol is None else int(vol),                    # 出来高（欠損はNULL）
                zika_oku,
                trade_date,
                _mid_raw.get(code, code)
            ))

            o1 = ffloat(q.get("regularMarketOpen"), None)
            h1 = ffloat(q.get("regularMarketDayHigh"), None)
            l1 = ffloat(q.get("regularMarketDayLow"), None)
            c1 = last
            # ここでDB側の正規コードに寄せる（^TOPIX → ^TOPX など）
            code = canonical_code_for_db(code)
            up_hist.append((
                code, trade_date,
                None if o1 is None else round(float(o1), 2),
                None if h1 is None else round(float(h1), 2),
                None if l1 is None else round(float(l1), 2),
                None if c1 is None else round(float(c1), 2),
                None if vol is None else int(vol)
            ))

        time.sleep(YQ_SLEEP_MID)

    # P1-397: 問い合わせ対象があるのに有効価格が1件も取れない回を「正常更新」としない。
    # staleな前回価格のまま派生スコア/HTMLへ進むのを防ぐ。
    if symbols_all and not up_screener:
        raise RuntimeError(f"MIDDAY quote snapshot returned zero valid prices for {len(symbols_all)} symbols")
    # P1-480/P1-566: 営業日の場中なのに当日quoteが1件も無ければYahoo全体のstale/cacheを疑い、
    # 今回snapshotとして公開しない。P1-566以降は個別前日quoteもcurrent値としては採用しない。
    if symbols_all and not any(str(_r[1])[:10] == _mid_expected for _r in up_hist):
        raise RuntimeError(f"MIDDAY quote snapshot has no quote timestamp for expected JPX date {_mid_expected}")

    if up_screener or up_hist or _mid_invalid_raw:
        # P1-441/P1-637: valid quoteが0件でもsentinel/invalid clearは同一snapshotで確定。
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_441_midday_snapshot")
            if _mid_invalid_raw:
                cur.executemany(
                    "UPDATE screener SET 現在値=NULL, 前日終値=NULL, 前日円差=NULL, "
                    "前日終値比率=NULL, 出来高=NULL, 時価総額億円=NULL, シグナル更新日=NULL, "
                    # P1-638: price clear後も前回の当日売買代金/RVOLだけ残さない。
                    "売買代金億=NULL, RVOL代金=NULL WHERE コード=?",
                    [(c,) for c in sorted(_mid_invalid_raw, key=str)]
                )
            if up_screener:
                cur.executemany(
                    "UPDATE screener SET "
                    "現在値=ROUND(?,2), 前日終値=ROUND(?,2), 前日円差=ROUND(?,2), 前日終値比率=ROUND(?,2), "
                    # P1-527: valid quoteでもmarketCap欠損なら旧時価総額をfresh値として残さない。
                    "出来高=?, 時価総額億円=?, 更新日=? WHERE コード=?",
                    up_screener
                )
            if up_hist:
                # P1-528: MIDDAYは日中累積値を単調マージ。EOD確定書込みは通常上書きのまま。
                _atomic_write_price_history_rows(conn, up_hist, close_only=False, intraday_merge=True)
            cur.execute("RELEASE SAVEPOINT p1_441_midday_snapshot")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_441_midday_snapshot")
                cur.execute("RELEASE SAVEPOINT p1_441_midday_snapshot")
            except Exception:
                pass
            raise
        finally:
            cur.close()

    if up_hist or _mid_invalid_raw:
        # P1-639: sentinel/invalid clearだけのrunでも、旧売買代金/RVOL/合成スコアを
        # 残さず、clear後のcurrent DB状態から派生値を再確定する。
        apply_auto_metrics_midday(conn, use_time_progress=True)
        apply_composite_score(conn)

# ===== 派生指標の更新 =====

def phase_snapshot_shodou_baseline(conn):
    """
    初動フラグ='候補' で、まだ基準が未設定(初動株価/初動出来高 が NULL)の銘柄に対して、
    その時点の 現在値/出来高 をスナップショットして基準化する。
    ・CSVは一切参照しない
    ・倍率は 1.0 で初期化
    """
    cur = conn.cursor()
    # 候補 かつ 基準が未設定のものを抽出
    cur.execute("""
        SELECT コード, 現在値, 出来高, 初動株価, 初動出来高
        FROM screener
        WHERE 初動フラグ='候補'
          AND (初動株価 IS NULL OR 初動出来高 IS NULL)
    """)
    rows = cur.fetchall()

    updates = []
    for code, now_price, now_vol, old_price, old_vol in rows:
        # P1-58: 欠損出来高を0株/倍率1.0として固定しない。
        # また、後日出来高だけ取得できた際に既存の初動株価を上書きしない。
        try:
            ip = float(now_price) if now_price is not None else None
        except Exception:
            ip = None
        try:
            iv = int(now_vol) if now_vol is not None else None
        except Exception:
            iv = None

        new_price = ip if old_price is None and ip is not None else None
        new_vol = iv if old_vol is None and iv is not None else None
        if new_price is None and new_vol is None:
            continue
        updates.append((new_price, new_price, new_vol, new_vol, code))

    if updates:
        # P1-435: 初動baselineを銘柄途中で部分反映しない。
        try:
            cur.execute("SAVEPOINT p1_435_shodou_baseline")
            cur.executemany("""
                UPDATE screener
                   SET 初動株価=COALESCE(初動株価, ?),
                       初動株価倍率=CASE WHEN 初動株価 IS NULL AND ? IS NOT NULL THEN 1.0 ELSE 初動株価倍率 END,
                       初動出来高=COALESCE(初動出来高, ?),
                       初動出来高倍率=CASE WHEN 初動出来高 IS NULL AND ? IS NOT NULL THEN 1.0 ELSE 初動出来高倍率 END
                 WHERE コード=?
            """, updates)
            cur.execute("RELEASE SAVEPOINT p1_435_shodou_baseline")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_435_shodou_baseline")
                cur.execute("RELEASE SAVEPOINT p1_435_shodou_baseline")
            except Exception:
                pass
            raise
        print(f"[shodou-baseline] snapshotted {len(updates)} symbols")
    else:
        print("[shodou-baseline] no new baseline")
    cur.close()

def phase_update_shodou_multipliers(conn):
    """
    既に基準(初動株価/初動出来高)がある銘柄の倍率を、最新の 現在値/出来高 から再計算して反映。
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 現在値, 出来高, 初動株価, 初動出来高
        FROM screener
        WHERE 初動株価 IS NOT NULL OR 初動出来高 IS NOT NULL
    """)
    rows = cur.fetchall()

    updates = []

    # P1-572: 1項目の型崩れでその銘柄全体をcontinueすると、前回の倍率が残る。
    # 各入力を独立にNone化し、今回計算不能な倍率は必ずNULLで上書きする。
    def _mul_float(v):
        try:
            return float(v) if v is not None else None
        except Exception:
            return None

    def _mul_int(v):
        try:
            return int(v) if v is not None else None
        except Exception:
            return None

    for code, now_price, now_vol, base_price, base_vol in rows:
        cp = _mul_float(now_price)
        bp = _mul_float(base_price)
        cv = _mul_int(now_vol)
        bv = _mul_int(base_vol)

        # 価格倍率
        mul_price = None
        if cp is not None and bp not in (None, 0):
            mul_price = cp / bp

        # 出来高倍率
        mul_vol = None
        if cv is not None and bv not in (None, 0):
            mul_vol = cv / bv

        # P1-60: 今日の入力が欠損なら昨日の倍率を残さずNULLへ戻す。
        # 倍率は「現在/初動」の時点値であり、last-known-valueではない。
        updates.append((mul_price, mul_vol, code))

    if updates:
        # P1-436: 倍率snapshotも一括確定。
        try:
            cur.execute("SAVEPOINT p1_436_shodou_multipliers")
            cur.executemany("""
                UPDATE screener
                   SET 初動株価倍率 = ?,
                       初動出来高倍率 = ?
                 WHERE コード=?
            """, updates)
            cur.execute("RELEASE SAVEPOINT p1_436_shodou_multipliers")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_436_shodou_multipliers")
                cur.execute("RELEASE SAVEPOINT p1_436_shodou_multipliers")
            except Exception:
                pass
            raise
        print(f"[shodou-mults] updated {len(updates)} symbols")
    else:
        print("[shodou-mults] no updates")
    cur.close()

# ==========================================
# ★ 追加: 相対強度(RS)・地合い・逆行フラグのDB永続化
# ==========================================
def phase_update_market_metrics(conn: sqlite3.Connection):
    """
    個別株のRS/地合い/逆行フラグをDB保存。
    P1-105: 個別株と指数の観測終了日を必ず一致させ、異なる日付同士のリターンを引かない。
    """
    columns_to_add = [
        ("RS_5", "REAL"), ("RS_20", "REAL"), ("Growth_Bias", "REAL"),
        ("地合いフラグ", "INTEGER"), ("逆行強フラグ", "INTEGER"), ("逆行弱フラグ", "INTEGER")
    ]
    cur = conn.cursor()
    # P2-16: duplicate columnだけを想定したbroad OperationalError無視を廃止。
    _market_schema_cols = {r[1] for r in cur.execute("PRAGMA table_info(screener)").fetchall()}
    for col_name, dtype in columns_to_add:
        if col_name not in _market_schema_cols:
            cur.execute(f'ALTER TABLE screener ADD COLUMN "{col_name}" {dtype}')
            _market_schema_cols.add(col_name)
    conn.commit()

    print("[MarketMetrics] RS・地合い・逆行フラグの計算とDB保存を開始します...")
    df_cand = pd.read_sql_query("SELECT コード, 市場, 現在値, 前日終値 FROM screener", conn)
    if df_cand.empty:
        cur.close(); return
    # P1-250: 計算JOINはcanonical、DB更新はscreenerの実rawコードへ。
    df_cand["_screener_code_raw"] = df_cand["コード"]
    # P1-180: RS/地合い各DataFrameのコードを共通キーへ。
    df_cand["コード"] = df_cand["コード"].map(canonical_code_for_db)
    df_cand["現在値"] = pd.to_numeric(df_cand["現在値"], errors="coerce")
    df_cand["前日終値"] = pd.to_numeric(df_cand["前日終値"], errors="coerce")

    # P1-285: raw aliasごとのOFFSETではなく、論理銘柄の異なる取引日列から5/20日前を取る。
    _market_asof = _latest_valid_history_date(conn)
    if not _market_asof:
        cur.close(); return
    _mh = pd.read_sql_query("""
        SELECT rowid AS _rowid, コード, 日付, 終値
        FROM price_history
        WHERE date(日付) >= date(?, '-140 day') AND date(日付) <= date(?)
        ORDER BY 日付, rowid
    """, conn, params=[_market_asof, _market_asof])
    _mh = _dedupe_price_history_df(_mh)
    # P1-676: 5日/20日前はprice_historyの行数ではなく有効終値の観測数で数える。
    if not _mh.empty:
        _mh["終値"] = pd.to_numeric(_mh["終値"], errors="coerce")
        _mh = _mh[_mh["終値"].notna() & np.isfinite(_mh["終値"]) & (_mh["終値"] > 0)].copy()
    _recs = []
    for _code, _g in _mh.groupby("コード", sort=False) if not _mh.empty else []:
        _g = _g.sort_values("日付")
        _vals = pd.to_numeric(_g["終値"], errors="coerce")
        _dates = pd.to_datetime(_g["日付"], errors="coerce").dt.normalize()
        _recs.append({
            "コード": _code,
            "株価基準日": _dates.iloc[-1] if len(_dates) else pd.NaT,
            "終値5日前": float(_vals.iloc[-6]) if len(_vals) >= 6 and pd.notna(_vals.iloc[-6]) else np.nan,
            "終値20日前": float(_vals.iloc[-21]) if len(_vals) >= 21 and pd.notna(_vals.iloc[-21]) else np.nan,
        })
    _offsets = pd.DataFrame(_recs)
    if _offsets.empty:
        df_cand["株価基準日"] = pd.NaT
        df_cand["終値5日前"] = np.nan
        df_cand["終値20日前"] = np.nan
    else:
        df_cand = df_cand.merge(_offsets, on="コード", how="left")

    def _get_index_returns(code, days_list=(1,5,20)):
        aliases = [code]
        if code in ("^TOPX", "^TOPIX"):
            aliases = ["^TOPX", "^TOPIX", "TOPIX", "998405.T"]
        qs = ",".join("?" for _ in aliases)
        df = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, 日付, 終値 FROM price_history WHERE コード IN ({qs}) AND date(日付) <= date(?) ORDER BY 日付 ASC, rowid ASC",
            conn, params=[*aliases, _market_asof]
        )
        out = {d: None for d in days_list}; out["asof"] = None
        if df.empty: return out
        # P1-310: TOPIX旧aliasが同日に複数残っていても指数リターンを任意行へ依存させない。
        df = _dedupe_price_history_df(df)
        df["終値"] = pd.to_numeric(df["終値"], errors="coerce")
        df["日付"] = pd.to_datetime(df["日付"], errors="coerce").dt.normalize()
        df = df.dropna(subset=["日付","終値"]).sort_values("日付")
        # P1-676: 指数側も0/非有限終値を1観測日に数えない。
        df = df[np.isfinite(df["終値"]) & (df["終値"] > 0)].copy()
        if df.empty: return out
        out["asof"] = df["日付"].iloc[-1]
        last = float(df["終値"].iloc[-1])
        for d in days_list:
            if len(df) > d:
                before = float(df["終値"].iloc[-(d+1)])
                if before != 0: out[d] = (last - before) / before
        return out

    idx_cache = {sym: _get_index_returns(sym) for sym in ("^TOPX","^N225","^GRT250")}
    # P2-87: Yahoo JapanのTOPIXページ(998405.T)が存在しても、yahooqueryの
    # 場中quoteは当日値を返さない場合がある。正式^TOPXを第一優先とし、
    # 今回as-ofへ未到達のときだけTOPIX連動ETF 1306の同期間returnを代理使用する。
    # 1306の価格水準そのものを^TOPXへ保存しないため、指数/ETFの系列混在は起こらない。
    _required_market_ts = pd.Timestamp(_market_asof).normalize() if _market_asof else None
    _topx_exact = idx_cache["^TOPX"]
    _topx_proxy = _get_index_returns("1306")
    _topx_exact_current = (
        _topx_exact.get("asof") is not None and _required_market_ts is not None
        and pd.Timestamp(_topx_exact.get("asof")).normalize() == _required_market_ts
    )
    _topx_proxy_current = (
        _topx_proxy.get("asof") is not None and _required_market_ts is not None
        and pd.Timestamp(_topx_proxy.get("asof")).normalize() == _required_market_ts
    )
    if (not _topx_exact_current) and _topx_proxy_current:
        idx_cache["^TOPX"] = _topx_proxy
        print("[MarketMetrics] TOPIX正式指数が未到達のため1306.T ETFリターンを代理使用")

    # P2-82: Yahooには東証グロース250指数の安定したquoteシンボルがない。
    # 正式指数^GRT250が今回as-ofへ未到達のときだけ、連動ETF 2516の
    # 同期間リターンを代理使用する。価格水準を^GRT250履歴へ書かないため、
    # 異なる水準の系列連結は起こらない。
    _growth_exact = idx_cache["^GRT250"]
    _growth_proxy = _get_index_returns("2516")
    _growth_exact_current = (
        _growth_exact.get("asof") is not None and _required_market_ts is not None
        and pd.Timestamp(_growth_exact.get("asof")).normalize() == _required_market_ts
    )
    _growth_proxy_current = (
        _growth_proxy.get("asof") is not None and _required_market_ts is not None
        and pd.Timestamp(_growth_proxy.get("asof")).normalize() == _required_market_ts
    )
    if (not _growth_exact_current) and _growth_proxy_current:
        idx_cache["^GRT250"] = _growth_proxy
        print("[MarketMetrics] Growth250正式指数が未到達のため2516.T ETFリターンを代理使用")

    def _resolve_index_symbol(market_str):
        mkt = str(market_str or "").strip(); mu = mkt.upper()
        if "グロース" in mkt or mu in {"東G","東証G","東証GRT","G","GRT","GROWTH"} or "GROWTH" in mu:
            return "^GRT250"
        return "^TOPX"

    # 地合いは同じ最新指数日だけを材料にする。古い指数を今日の地合いへ混ぜない。
    index_asofs = [v.get("asof") for v in idx_cache.values() if v.get("asof") is not None]
    market_asof = max(index_asofs) if index_asofs else None
    topx1 = idx_cache["^TOPX"].get(1) if idx_cache["^TOPX"].get("asof") == market_asof else None
    nikkei1 = idx_cache["^N225"].get(1) if idx_cache["^N225"].get("asof") == market_asof else None

    def _market_flag(t_val, n_val):
        vals = [v for v in (t_val, n_val) if v is not None and np.isfinite(v)]
        if not vals: return None
        if any(v <= -0.01 for v in vals): return -1
        if any(v >= 0.01 for v in vals): return 1
        return 0
    market_flag_today = _market_flag(topx1, nikkei1)

    rs5_list=[]; rs20_list=[]; rev_s=[]; rev_w=[]
    for _, r in df_cand.iterrows():
        idx = _resolve_index_symbol(r.get("市場")); idx_rec = idx_cache.get(idx, {})
        stock_asof = r.get("株価基準日")
        same_day = pd.notna(stock_asof) and idx_rec.get("asof") is not None and pd.Timestamp(stock_asof) == pd.Timestamp(idx_rec.get("asof"))
        idx5 = idx_rec.get(5) if same_day else None
        idx20 = idx_rec.get(20) if same_day else None
        try:
            st5 = (float(r["現在値"]) - float(r["終値5日前"])) / float(r["終値5日前"]) if pd.notna(r.get("現在値")) and pd.notna(r.get("終値5日前")) and float(r["終値5日前"]) != 0 else None
        except Exception: st5 = None
        try:
            st20 = (float(r["現在値"]) - float(r["終値20日前"])) / float(r["終値20日前"]) if pd.notna(r.get("現在値")) and pd.notna(r.get("終値20日前")) and float(r["終値20日前"]) != 0 else None
        except Exception: st20 = None
        rs5_list.append(st5 - idx5 if st5 is not None and idx5 is not None else None)
        rs20_list.append(st20 - idx20 if st20 is not None and idx20 is not None else None)

        same_market_day = pd.notna(stock_asof) and market_asof is not None and pd.Timestamp(stock_asof) == pd.Timestamp(market_asof)
        try:
            stock_pct = (float(r["現在値"]) - float(r["前日終値"])) / float(r["前日終値"]) if pd.notna(r.get("現在値")) and pd.notna(r.get("前日終値")) and float(r["前日終値"]) != 0 else None
        except Exception: stock_pct = None
        if (not same_market_day) or stock_pct is None or market_flag_today is None or not np.isfinite(stock_pct):
            rev_s.append(None); rev_w.append(None)
        else:
            rev_s.append(1 if market_flag_today == -1 and stock_pct >= 0.01 else 0)
            rev_w.append(1 if market_flag_today == 1 and stock_pct <= -0.01 else 0)

    df_cand["RS_5"] = rs5_list; df_cand["RS_20"] = rs20_list
    df_cand["逆行強フラグ"] = rev_s; df_cand["逆行弱フラグ"] = rev_w
    # 地合いフラグは市場最新日と同じ株価基準日の銘柄だけへ保存。
    df_cand["地合いフラグ"] = [market_flag_today if (pd.notna(d) and market_asof is not None and pd.Timestamp(d)==pd.Timestamp(market_asof)) else None for d in df_cand["株価基準日"]]

    topx20 = idx_cache["^TOPX"].get(20); grt20 = idx_cache["^GRT250"].get(20)
    # P1-599: TOPIX/Growth250同士が同じ日でも、市場全体の最新as-ofより古ければ
    # stale Growth_Biasをcurrent fair-value入力として全銘柄へ配らない。
    _topx_asof = idx_cache["^TOPX"].get("asof")
    _grt_asof = idx_cache["^GRT250"].get("asof")
    # P1-623: 指数群だけで同じ古い日に揃ってもcurrent Growth_Biasにしない。
    # market_asofは指数群の最大日なので、3指数すべて前営業日なら前営業日で一致してしまう。
    # 個別価格側の今回as-of(_market_asof)にも到達している場合だけ有効化する。
    _required_growth_asof = pd.Timestamp(_market_asof).normalize() if _market_asof else None
    same_growth_day = (
        _topx_asof is not None and _grt_asof is not None and _required_growth_asof is not None
        and pd.Timestamp(_topx_asof).normalize() == pd.Timestamp(_grt_asof).normalize() == _required_growth_asof
    )
    growth_bias = grt20 - topx20 if same_growth_day and topx20 is not None and grt20 is not None else None
    df_cand["Growth_Bias"] = growth_bias

    updates=[]
    for _, r in df_cand.iterrows():
        updates.append((
            round(r["RS_5"],4) if pd.notna(r["RS_5"]) else None,
            round(r["RS_20"],4) if pd.notna(r["RS_20"]) else None,
            round(r["Growth_Bias"],4) if pd.notna(r["Growth_Bias"]) else None,
            int(r["地合いフラグ"]) if pd.notna(r["地合いフラグ"]) else None,
            int(r["逆行強フラグ"]) if pd.notna(r["逆行強フラグ"]) else None,
            int(r["逆行弱フラグ"]) if pd.notna(r["逆行弱フラグ"]) else None,
            str(r["_screener_code_raw"])
        ))
    _sp = f"sp_market_metrics_{time.time_ns()}"
    conn.execute(f"SAVEPOINT {_sp}")
    try:
        cur.executemany("""UPDATE screener SET RS_5=?, RS_20=?, Growth_Bias=?, 地合いフラグ=?, 逆行強フラグ=?, 逆行弱フラグ=? WHERE コード=?""", updates)
        conn.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        cur.close()
        raise
    # P1-427: RS/地合い書戻しを原子的に。
    cur.close()
    print(f"[MarketMetrics] RS等のDB保存が完了しました: {len(updates)} 銘柄")

def phase_derive_update(conn: sqlite3.Connection):
    cur = conn.cursor()
    cur.execute("SELECT コード, 初動株価, 現在値, UP継続回数, DOWN継続回数, 登録日, 時価総額億円 FROM screener")
    rows = cur.fetchall()
    cur.close()
    d_today = today_str()
    cal_today = date.fromisoformat(_today_jst())
    # P1-221: workdaysへ空holidayを渡さず、日本市場祝日＋追加休場日を反映。
    # P1-423: configured extra closure fileが壊れていれば派生日数を推測しない。
    _extra_closed_derive = _load_extra_closed(EXTRA_CLOSED_PATH)

    def _derive_holidays(a: date, b: date):
        lo, hi = (a, b) if a <= b else (b, a)
        # 10営業日先の計算余白も含める。weekendはworkdays側も除外するが重複しても害はない。
        hi = max(hi, lo + timedelta(days=45))
        out = []
        d = lo
        while d <= hi:
            try:
                closed = is_jp_market_holiday(d, _extra_closed_derive)
            except Exception as e:
                # P1-412: 祝日判定障害時に祝日を営業日としてworkdaysへ渡さない。
                raise RuntimeError(f"derive holiday calendar failed for {d}: {e}") from e
            if closed:
                out.append(d)
            d += timedelta(days=1)
        return out

    # P2-19: 登録日ごとに同じ祝日カレンダーを数千回再生成しない。
    _parsed_rows = []
    _min_reg = cal_today
    for _row in rows:
        try:
            _db_reg = date.fromisoformat(str(_row[5]))
        except Exception:
            _db_reg = cal_today
        _parsed_rows.append((_row, _db_reg))
        if _db_reg < _min_reg:
            _min_reg = _db_reg
    # 今日登録銘柄の10営業日先まで必要なのでfuture 45日分も一度に含める。
    _derive_holidays_all = _derive_holidays(_min_reg, cal_today + timedelta(days=45)) if _parsed_rows else []
    _derive_cutoff = _expected_jpx_asof_date().isoformat()

    # P2-19/P2-83: 必要なのは銘柄ごとの直近32本だけ。
    # 200銘柄ごとのCAST条件で大表を約18回走査せず、price_historyを1回だけ
    # 走査してraw aliasごとの直近40行へSQL側で絞り、logical 32本へdedupeする。
    _derive_hist = {}
    _derive_codes = list(dict.fromkeys(canonical_code_for_db(_r[0]) for _r, _ in _parsed_rows if canonical_code_for_db(_r[0])))
    _bulk_ph = pd.read_sql_query(
        """
        WITH ranked AS (
            SELECT rowid AS _rowid, コード, 日付, 終値,
                   ROW_NUMBER() OVER (
                       PARTITION BY CAST(コード AS TEXT)
                       ORDER BY date(日付) DESC, rowid DESC
                   ) AS _rn
            FROM price_history
            WHERE date(日付) <= date(?)
        )
        SELECT _rowid, コード, 日付, 終値
        FROM ranked
        WHERE _rn <= 40
        ORDER BY 日付 DESC, _rowid DESC
        """,
        conn, params=[_derive_cutoff]
    )
    if not _bulk_ph.empty:
        _bulk_ph = _dedupe_price_history_df(_bulk_ph)
        _derive_wanted = set(_derive_codes)
        _bulk_ph = _bulk_ph[
            _bulk_ph["コード"].map(canonical_code_for_db).isin(_derive_wanted)
        ].copy()
        for _hcode, _hg in _bulk_ph.groupby("コード", sort=False):
            _hk = canonical_code_for_db(_hcode)
            if _hk:
                _derive_hist[_hk] = _hg.sort_values("日付", ascending=False).head(32).reset_index(drop=True)

    _derive_updates = []
    _derive_resets = []
    for ((code, initial_price, current_price, up_con, down_con, _regist_date, zika_oku), db_reg) in _parsed_rows:
        up_con = int(up_con or 0)
        down_con = int(down_con or 0)

        holidays = _derive_holidays_all
        last_date = workdays.workday(db_reg, 10, holidays)
        diff_days = max(0, (last_date - db_reg).days)
        eigyo_sabun = workdays.networkdays(db_reg, cal_today, holidays)

        # P1-110: UP/DOWN継続回数を状態へ加算せず、price_historyから毎回再計算する。
        # 同日にMIDDAY/EOD/19時バッチで複数回呼ばれても同じ結果になり、二重三重加算しない。
        _code_key = canonical_code_for_db(code)
        _ph = _derive_hist.get(_code_key)
        if _ph is None:
            _ph = pd.DataFrame(columns=["コード", "日付", "終値"])
        if len(_ph) >= 2:
            _ph["終値"] = pd.to_numeric(_ph["終値"], errors="coerce")
            _ph = _ph.dropna(subset=["終値"]).reset_index(drop=True)

        # P1-569: 履歴本数が十分でも個別最終足が今回JPX as-ofより古ければ、
        # 前営業日までのUPDOWNを「今日の継続状態」として更新しない。
        _derive_price_fresh = False
        if not _ph.empty:
            try:
                _derive_last_date = pd.to_datetime(_ph.loc[0, "日付"], errors="coerce")
                _derive_price_fresh = (
                    pd.notna(_derive_last_date)
                    and _derive_last_date.date().isoformat() == _derive_cutoff
                )
            except Exception:
                _derive_price_fresh = False

        if len(_ph) >= 2 and _derive_price_fresh:
            _signs = []
            for _i in range(len(_ph)-1):
                _a=float(_ph.loc[_i,"終値"]); _b=float(_ph.loc[_i+1,"終値"])
                _signs.append(1 if _a>_b else -1 if _a<_b else 0)
            _cur_sign = _signs[0] if _signs else 0
            _streak = 0
            if _cur_sign != 0:
                for _sgn in _signs:
                    if _sgn == _cur_sign: _streak += 1
                    else: break
            if _cur_sign > 0:
                up_con=_streak; down_con=0; updown="↑" if _streak<=1 else f"↑{_streak}"
            elif _cur_sign < 0:
                down_con=_streak; up_con=0; updown="↓" if _streak<=1 else f"↓{_streak}"
            else:
                up_con=0; down_con=0; updown="同値"
        else:
            # P1-545/P1-569: 履歴不足だけでなく個別最終足が今回as-of未到達のときも、
            # 前回UPDOWN/継続回数をcurrent状態として残さない。今回計算不能は明示不明へ。
            up_con = 0
            down_con = 0
            updown = ""

        # P1-74: 時価総額未取得を0億円へ変換して「組入前?:0」と表示しない。
        try:
            z = float(zika_oku) if zika_oku is not None else None
            if z is not None and (z != z):  # NaN
                z = None
        except Exception:
            z = None
        if z is None:
            maru = "情報不足"
        elif z >= 100:
            maru = f"組入済?:{int(z)}"
        elif 90 < z < 100:
            maru = f"組入期待:{int(z)}"
        else:
            maru = f"組入前?:{int(z)}"

        _derive_updates.append((maru, diff_days, eigyo_sabun, updown, up_con, down_con, d_today, code))

        try:
            ip = float(initial_price) if initial_price is not None else None
            cp = float(current_price) if current_price is not None else None
            if ip is not None and cp is not None and eigyo_sabun is not None:
                if cp < ip and eigyo_sabun > 14:
                    _derive_resets.append(("失敗または未検知", code))
        except Exception:
            pass

    # P1-442: 派生状態と初動失敗resetを全銘柄同一トランザクションで確定。
    if _derive_updates or _derive_resets:
        cur2 = conn.cursor()
        try:
            cur2.execute("SAVEPOINT p1_442_derive")
            if _derive_updates:
                cur2.executemany(
                    "UPDATE screener SET 機関組入時価総額=?, 残日=?, 経過日数=?, UPDOWN=?, UP継続回数=?, DOWN継続回数=?, 更新日=? WHERE コード=?",
                    _derive_updates,
                )
            if _derive_resets:
                cur2.executemany(
                    'UPDATE screener SET 初動検知成功=?, 初動株価=NULL, 初動株価倍率=NULL, 初動出来高=NULL, 初動出来高倍率=NULL, UP継続回数=0 WHERE コード=?',
                    _derive_resets,
                )
            cur2.execute("RELEASE SAVEPOINT p1_442_derive")
        except Exception:
            try:
                cur2.execute("ROLLBACK TO SAVEPOINT p1_442_derive")
                cur2.execute("RELEASE SAVEPOINT p1_442_derive")
            except Exception:
                pass
            raise
        finally:
            cur2.close()


# ===== シグナル判定（初動/底打ち/上昇余地/Migikata） =====

def _pivot_ratio_higher_lows(high: pd.Series, low: pd.Series, win=5):
    """5日窓などで谷(安値)のピボットを取り、連続して切り上げている比率を返す"""
    n = len(low)
    if n < win*2+1: return 0.0
    is_trough = []
    for i in range(win, n-win):
        if low.iloc[i] == low.iloc[i-win:i+win+1].min():
            is_trough.append(i)
    if len(is_trough) < 2: return 0.0
    cnt = 0
    for a,b in zip(is_trough, is_trough[1:]):
        if low.iloc[b] > low.iloc[a]: cnt += 1
    return cnt / max(1, (len(is_trough)-1))

def _trend_metrics_df(g: pd.DataFrame):
    """必要メトリクスをまとめて計算。

    P0-8修正:
    - 傾き/R2/週足/DD/HLなどの採点期間は従来どおり直近 LOOKBACK 日。
    - MA20/50/100だけは、その前の助走データも含めた履歴で計算する。
      これにより LOOKBACK=90 のままでも、直近30日について 20>50>100 を判定できる。
    """
    g = g.sort_values("日付").copy()
    g_score = g.tail(LOOKBACK).copy()

    # 採点対象（従来どおり直近90営業日）
    px = g_score["終値"].astype(float).copy()
    hi = (g_score["高値"] if "高値" in g_score else g_score["終値"]).astype(float)
    lo = (g_score["安値"] if "安値" in g_score else g_score["終値"]).astype(float)

    # 1) 回帰（log終値 ~ 日数）
    y = np.log(px.values)
    x = np.arange(len(px), dtype=float)
    b1, b0 = np.polyfit(x, y, 1)
    y_hat = b0 + b1*x
    ss_res = np.sum((y - y_hat)**2)
    ss_tot = np.sum((y - np.mean(y))**2)
    r2 = 1 - (ss_res/ss_tot) if ss_tot > 0 else 0.0
    slope_ann = np.exp(b1*252) - 1.0  # 年率換算

    # 2) MAs：MA100用の助走期間を含む全履歴で作る
    px_full = g["終値"].astype(float).copy()
    s20_full  = px_full.rolling(20,  min_periods=20).mean()
    s50_full  = px_full.rolling(50,  min_periods=50).mean()
    s100_full = px_full.rolling(100, min_periods=100).mean()

    # 直近RIBBON_KEEP_DAYSで 20>50>100 を維持した日の比率。
    # rolling(100) は0始まりindex=99から有効なので、i>=100の旧条件は使わない。
    ribbon_days = min(RIBBON_KEEP_DAYS, len(g_score))
    rib_ok = 0
    start_i = max(0, len(g) - ribbon_days)
    for i in range(start_i, len(g)):
        a, b, c = s20_full.iloc[i], s50_full.iloc[i], s100_full.iloc[i]
        if pd.notna(a) and pd.notna(b) and pd.notna(c) and a > b > c:
            rib_ok += 1
    ribbon_ratio = rib_ok / max(1, ribbon_days)

    # SMA50上回り比率は従来挙動を維持（直近LOOKBACK窓の中だけでSMA50を作る）。
    # P0-8ではリボン20点だけを正常化し、他の採点軸は変えない。
    s50_score = px.rolling(50, min_periods=50).mean()
    above50_ratio = float((px > s50_score).sum()) / max(1, (~s50_score.isna()).sum())

    # 3) 週次の上昇継続（“上昇週”の割合）
    w = g_score.set_index("日付")["終値"].resample("W-FRI").last().dropna()
    wk_ratio = float((w.diff() > 0).sum()) / max(1, (w.diff().dropna().shape[0]))

    # 4) 最大ドローダウン
    cummax = px.cummax()
    mdd = float((px/cummax - 1.0).min()) * -1.0  # 正の値

    # 5) 安値の切り上げ比率（ピボット）
    hl_ratio = _pivot_ratio_higher_lows(hi, lo, win=HL_WIN)

    return dict(
        slope_ann=float(slope_ann), r2=float(max(0,min(1,r2))),
        ribbon_ratio=float(ribbon_ratio), above50_ratio=float(above50_ratio),
        week_up_ratio=float(wk_ratio), mdd=float(mdd), hl_ratio=float(hl_ratio)
    )

def compute_right_up_persistent(conn, as_of=None, log_datetime=None, replace_log_day=False):
    """『ずーーっと右肩上がり』をスコア化して screener を更新"""
    # 対象日
    if as_of is not None:
        today = pd.to_datetime(as_of)
    else:
        _valid_asof = _latest_valid_history_date(conn)
        if not _valid_asof:
            print("[右肩上がり] price_history空"); return
        # P1-408: legacy future/weekend MAX(日付)を現在の右肩基準日にしない。
        today = pd.to_datetime(_valid_asof)

    # P0-8: 直近30日すべてでMA100を判定するには、最低約129営業日の履歴が必要。
    # カレンダー日換算には余裕を持たせ、採点窓90日は変えずMA計算用の助走だけ取得する。
    _trend_hist_rows = max(LOOKBACK, 100 + RIBBON_KEEP_DAYS)
    start = (today - pd.Timedelta(days=int(_trend_hist_rows * 1.8))).strftime("%Y-%m-%d")
    ph = pd.read_sql_query(
        "SELECT rowid AS _rowid, 日付, コード, 終値, 高値, 安値 FROM price_history "
        "WHERE date(日付)>=date(?) AND date(日付)<=date(?) ORDER BY 日付, rowid",
        conn, params=[start, today.strftime("%Y-%m-%d")], parse_dates=["日付"]
    )
    # P1-287: split aliasを別系列のままMA/R2へ入れない。
    ph = _dedupe_price_history_df(ph)
    # P1-676: 持続右肩のMA/回帰も有効な正の終値だけを観測列にする。
    if not ph.empty:
        ph["終値"] = pd.to_numeric(ph["終値"], errors="coerce")
        ph = ph[ph["終値"].notna() & np.isfinite(ph["終値"]) & (ph["終値"] > 0)].copy()
    ph = add_price_features(ph)  # v10: unify price feature calc
    if ph.empty: 
        print("[右肩上がり] データ無し"); return

    # P1-579: 持続右肩はphase_signal_detection側の別ロジック(tob_score)ログと分離する。
    # 専用種別「右肩上がり-持続」を同じsnapshot内で記録し、開始日もこの履歴から算出する。
    if log_datetime is not None:
        _ru_log_dt = pd.Timestamp(log_datetime)
        if _ru_log_dt.tzinfo is not None:
            _ru_log_dt = _ru_log_dt.tz_convert("Asia/Tokyo")
    else:
        _ru_log_dt = pd.Timestamp(_now_jst())
    _ru_dt_str = _ru_log_dt.strftime("%Y-%m-%d %H:%M:%S")
    _ru_day_str = pd.to_datetime(today).date().isoformat()

    # P1-254: 価格履歴キーとscreener rawキーが違っても候補更新を落とさない。
    _ru_raw = {}
    # P2-18: 更新先raw-key mapは必須。取得不能なら候補更新を黙って落とさない。
    for (_rc,) in conn.execute("SELECT コード FROM screener").fetchall():
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _ru_raw:
            _ru_raw[_rk] = _rc
    outs = []
    _ru_logs = []
    for code, g0 in ph.groupby("コード", sort=False):
        # 採点は関数内で直近LOOKBACK日に限定。ここではMA100の助走分を残して渡す。
        g = g0[g0["日付"]<=today].tail(_trend_hist_rows).copy()
        # P1-81: 当該銘柄の最終価格日が市場全体as-ofより古ければstale。
        # 古い足を現在の持続右肩として再利用せず、後段の全件クリアを維持する。
        if g.empty or pd.to_datetime(g["日付"].iloc[-1]).normalize() != pd.to_datetime(today).normalize():
            continue
        # P3-37: 持続右肩の20>50>100リボンを直近RIBBON_KEEP_DAYSすべて判定するには、
        # MA100初回有効行(100営業日目) + その後の観測が必要。旧式はMIN_DAYS=60だけで
        # 正式採点し、未観測のribbon日を0点として新規上場銘柄を「弱いトレンド」と誤表現していた。
        _persistent_required_rows = max(MIN_DAYS, 100 + int(RIBBON_KEEP_DAYS) - 1)
        if len(g) < _persistent_required_rows:
            continue
        if len(g.tail(LOOKBACK)) < MIN_DAYS:
            continue
        met = _trend_metrics_df(g)

        # ---- スコア（0-100） ----
        # 回帰傾き（0→40点）：12%/年で0点、50%/年で満点
        slope = met["slope_ann"]
        slope_score = 0.0 if slope <= SLOPE_MIN_ANN else min(1.0, (slope - SLOPE_MIN_ANN)/0.38)*40.0

        # R^2（0→15点）
        r2_score = met["r2"]*15.0

        # リボン維持（0→20点）: 直近でどれだけ20>50>100を維持
        ribbon_score = min(1.0, met["ribbon_ratio"]/0.8)*20.0  # 80%維持で満点

        # 週足の上昇比率（0→10点）
        week_score = 0.0 if met["week_up_ratio"] <= WEEK_UP_MIN else min(1.0,(met["week_up_ratio"]-WEEK_UP_MIN)/(0.9-WEEK_UP_MIN))*10.0

        # SMA50上回り比率（0→10点）
        above50_score = min(1.0, max(0.0, (met["above50_ratio"]-0.6)/(0.9-0.6)))*10.0

        # 安値の切り上げ（0→10点）
        hl_score = min(1.0, met["hl_ratio"]/0.7)*10.0  # 70%がHLなら満点

        # DDペナルティ（～-15点）
        dd_pen = 0.0
        if met["mdd"] > MDD_MAX:
            dd_pen = min(1.0, (met["mdd"]-MDD_MAX)/0.2)*15.0  # 30%超→減点、50%で最大

        # P1-83: 配点素点は最大105点なので、仕様どおり0〜100へクリップする。
        score = min(100.0, max(0.0, slope_score + r2_score + ribbon_score + week_score + above50_score + hl_score - dd_pen))

        # 最低限の基礎条件
        base_ok = (slope > 0) and (met["r2"] >= R2_MIN)
        flag = "候補" if (base_ok and score >= THRESH_SCORE) else ""

        _ru_code = canonical_code_for_db(code)
        outs.append((round(score,1), flag, _ru_raw.get(_ru_code, str(code))))
        if flag:
            _last = g.iloc[-1]
            _ru_logs.append((
                _ru_code, _ru_dt_str, "右肩上がり-持続",
                f"persistent score={round(float(score),1)}",
                float(_last["終値"]) if pd.notna(_last.get("終値")) else None,
                float(_last["高値"]) if pd.notna(_last.get("高値")) else None,
                float(_last["安値"]) if pd.notna(_last.get("安値")) else None,
                round(float(score),1), 0
            ))

    # P1-66/P1-438/P1-579: screener状態と持続右肩ログを同一snapshotで確定する。
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_438_right_up_persistent")
        cur.execute("UPDATE screener SET 右肩上がりフラグ='', 右肩上がりスコア=NULL")
        if outs:
            cur.executemany("""
                UPDATE screener SET 右肩上がりスコア=?, 右肩上がりフラグ=? WHERE コード=?
            """, outs)
        if replace_log_day:
            cur.execute(
                "DELETE FROM signals_log WHERE 種別='右肩上がり-持続' AND substr(日時,1,10)=?",
                (_ru_day_str,)
            )
        if _ru_logs:
            try:
                cur.executemany("""
                    INSERT INTO signals_log
                        (コード, 日時, 種別, 詳細, 終値, 高値, 安値, スコア, 検証済み)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(コード, 日時, 種別) DO UPDATE SET
                        詳細=excluded.詳細, 終値=excluded.終値, 高値=excluded.高値,
                        安値=excluded.安値, スコア=excluded.スコア, 検証済み=excluded.検証済み
                """, _ru_logs)
            except sqlite3.OperationalError:
                # 旧DBに対応unique制約が無い場合も、当日snapshot削除後なので単純INSERTで安全。
                cur.executemany("""
                    INSERT INTO signals_log
                        (コード, 日時, 種別, 詳細, 終値, 高値, 安値, スコア, 検証済み)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, _ru_logs)
        cur.execute("RELEASE SAVEPOINT p1_438_right_up_persistent")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_438_right_up_persistent")
            cur.execute("RELEASE SAVEPOINT p1_438_right_up_persistent")
        except Exception:
            pass
        raise
    finally:
        cur.close()
    if not outs:
        print("[右肩上がり] 該当なし（前回値をクリア）"); return
    print(f"[右肩上がり] 持続トレンド版 {len(outs)} 銘柄を更新 / 閾値={THRESH_SCORE}")

# ========================= 右肩上がり・早期トリガー（完全版：置換用） =========================

# ---- スキーマ確保（screener の列/ 最小列）----

# ---- メイン：早期トリガー計算・DB更新・ログ記録（日時で記録）----
def compute_right_up_early_triggers(conn, as_of=None, log_datetime=None, replace_log_day=False):

    if as_of is not None:
        as_of_date = pd.to_datetime(as_of).date()
    else:
        _valid_asof = _latest_valid_history_date(conn)
        if not _valid_asof:
            print("[右肩早期] price_history が空です"); return
        # P1-409: legacy future/weekend MAX(日付)を早期トリガー基準日にしない。
        as_of_date = date.fromisoformat(_valid_asof)

    # P1-142: 右肩早期ログの既定時刻もOSローカルではなくJST固定。
    if log_datetime is not None:
        dt_log = pd.Timestamp(log_datetime)
        if dt_log.tzinfo is not None:
            dt_log = dt_log.tz_convert("Asia/Tokyo")
    else:
        dt_log = pd.Timestamp(_now_jst())
    dt_str = dt_log.strftime("%Y-%m-%d %H:%M:%S")

    print("[右肩早期] データを一括計算中...")
    start = (pd.Timestamp(as_of_date) - pd.Timedelta(days=320)).strftime("%Y-%m-%d")
    ph = pd.read_sql_query(
        "SELECT rowid AS _rowid, 日付, コード, 終値, 高値, 安値, 出来高 "
        "FROM price_history WHERE 日付 >= date(?) AND 日付 <= date(?) "
        "ORDER BY 日付, rowid",
        conn, params=(start, as_of_date)
    )
    if ph.empty:
        print("[右肩早期] データなし"); return
    ph = _dedupe_price_history_df(ph)
    ph["日付"] = pd.to_datetime(ph["日付"])
    # P1-676: 早期右肩も無効終値をMA/高値窓の1観測日に数えない。
    ph["終値"] = pd.to_numeric(ph["終値"], errors="coerce")
    ph = ph[ph["終値"].notna() & np.isfinite(ph["終値"]) & (ph["終値"] > 0)].copy()
    if ph.empty:
        print("[右肩早期] 有効終値データなし"); return

    # P1-288: canonicalな1本の系列にしてからMAを計算。
    grp = ph.groupby("コード", sort=False)
    def _early_roll(series, window, min_periods=None, op="mean"):
        roller = series.groupby(ph["コード"], sort=False).rolling(
            window, min_periods=(window if min_periods is None else min_periods)
        )
        calculated = getattr(roller, op)()
        return calculated.reset_index(level=0, drop=True).reindex(ph.index)

    ph["s10"] = _early_roll(ph["終値"], 10)
    ph["s20"] = _early_roll(ph["終値"], 20)
    ph["s50"] = _early_roll(ph["終値"], 50)
    ph["s100"] = _early_roll(ph["終値"], 100)
    ph["s200"] = _early_roll(ph["終値"], 200)
    ph["v20"] = _early_roll(ph["出来高"], 20)
    # P1-12: ブレイク/20MAリバの出来高基準は直前20営業日（当日除外）。
    ph["v20_prev"] = _early_roll(grp["出来高"].shift(1), 20, 20)
    ph["hh60"] = _early_roll(grp["高値"].shift(1), HH_N, HH_N, op="max")

    # P1-255: early右肩も計算キーとscreener raw更新キーを分離。
    _early_raw = {}
    # P2-18: early右肩もraw-key map取得不能をsilent successにしない。
    for (_rc,) in conn.execute("SELECT コード FROM screener").fetchall():
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _early_raw:
            _early_raw[_rk] = _rc
    results, logs = [], []
    cnt_flag = 0

    def _best_signal_vectorized(g):
        if len(g) < max(60, 50): return 0.0, "", ""
        last = g.iloc[-1]
        close_t = float(last["終値"])
        vol_t   = float(last["出来高"])
        
        s10_t = last["s10"]
        s20_t = last["s20"]
        s50_t = last["s50"]
        s100_t = last["s100"]
        s200_t = last["s200"]
        v20_t = last["v20_prev"]
        hh60_t = last["hh60"]
        
        if pd.isna(s20_t) or pd.isna(s50_t): return 0.0, "", ""
        ext20 = (close_t - s20_t) / s20_t if s20_t > 0 else 0.0
        ext50 = (close_t - s50_t) / s50_t if s50_t > 0 else 0.0
        
        sigs = []
        # A) ブレイク
        if not pd.isna(hh60_t):
            cond_break = (close_t >= hh60_t * (1.0 + PIVOT_EPS))
            cond_vol   = (not pd.isna(v20_t) and vol_t >= v20_t * VOL_BOOST)
            cond_ma    = (s20_t > s50_t) and (len(g["s50"]) >= 2 and not pd.isna(g["s50"].iloc[-2]) and s50_t > g["s50"].iloc[-2])
            cond_ext   = (ext20 <= EXT_20_MAX) and (ext50 <= EXT_50_MAX)
            # P0-9: vectorized経路も同じく出来高条件を必須にする
            if cond_break and cond_vol and cond_ma and cond_ext:
                near = max(0.0, 1.0 - (close_t / hh60_t - 1.0) / 0.05)
                vol_score = 0.0 if pd.isna(v20_t) else min(1.0, (vol_t / max(1.0, v20_t)) / 2.5)
                ma_gap = min(1.0, (s20_t/s50_t - 1.0) / 0.05) if s50_t > 0 else 0.0
                score = 55*near + 25*vol_score + 20*ma_gap
                sigs.append((score, "ブレイク", f"HH{HH_N}+{PIVOT_EPS*100:.1f}%, vol≥{VOL_BOOST}x, 20>50"))

        # B) ポケット
        if not pd.isna(s10_t) and not pd.isna(hh60_t):
            down_mask = g["終値"].diff() < 0
            # P1-12: 当日を比較窓から除外する。
            down_vol_max = g["出来高"].where(down_mask).shift(1).tail(POCKET_WIN).max()
            near_pivot = (close_t / hh60_t - 1.0)
            cond_pp = (close_t > s10_t) and (not pd.isna(down_vol_max)) and (vol_t > down_vol_max)
            cond_near = (-0.03 <= near_pivot <= 0.02)
            if cond_pp and cond_near:
                tight = g["終値"].tail(10).pct_change().dropna().std()
                tight_score = max(0.0, 1.0 - (tight / 0.025)) if pd.notna(tight) else 0.0
                vol_score = min(1.0, vol_t / max(1.0, down_vol_max) / 2.0)
                score = 35 + 35*vol_score + 30*tight_score
                sigs.append((score, "ポケット", f">10MA, vol>{POCKET_WIN}dDownMax, near HH{HH_N}"))

        # C) 20MAリバ
        below20 = (g["終値"] < g["s20"]) & g["s20"].notna()
        cross_up = (below20.shift(1, fill_value=False) & (~below20)).tail(REB_WIN).any()
        cond_c = (cross_up and close_t >= s20_t and (not pd.isna(s50_t)) and close_t >= s50_t and (not pd.isna(v20_t)) and vol_t >= v20_t)
        if cond_c:
            near20 = max(0.0, 1.0 - abs(ext20)/0.04)
            score = 30 + 40*near20 + 30*min(1.0, vol_t/max(1.0, v20_t))
            sigs.append((score, "20MAリバ", "20MA reclaim & vol≥Avg20 & ≥50MA"))

        # D) 200MAリクレイム
        if not pd.isna(s200_t):
            above200 = (g["終値"] >= g["s200"]) & g["s200"].notna()
            crossed  = ((~above200.shift(1, fill_value=False)) & above200).tail(RECLAIM_WIN).any()
            stay3    = above200.tail(3).all()
            slope50_up   = (len(g["s50"].dropna()) >= 6 and s50_t > g["s50"].iloc[-5])
            slope100_ok  = (len(g["s100"].dropna()) >= 6 and s100_t >= g["s100"].iloc[-5])
            cond_d = crossed and stay3 and slope50_up and slope100_ok
            if cond_d:
                ext200 = (close_t - s200_t)/s200_t if s200_t > 0 else 0.0
                near200   = max(0.0, 1.0 - abs(ext200)/0.06)
                # P1-585: vectorized正本も当日出来高欠損を満点化しない。
                vol_score = 0.0 if (pd.isna(vol_t) or pd.isna(v20_t)) else min(1.0, vol_t/max(1.0, v20_t))
                score = 25 + 45*near200 + 30*vol_score
                sigs.append((score, "200MAリクレイム", "cross&stay3d, 50MA↑,100MA↔↑"))

        if not sigs: return 0.0, "", ""
        sigs.sort(key=lambda x: x[0], reverse=True)
        return sigs[0]

    for code, g in ph.groupby("コード", sort=False):
        # P1-82: 銘柄の最終価格日が市場全体as-ofより古ければ早期右肩候補にしない。
        if g.empty or pd.to_datetime(g["日付"].iloc[-1]).date() != as_of_date:
            continue
        score, tag, detail = _best_signal_vectorized(g)
        flag = "候補" if score >= SCORE_TH and tag else ""
        results.append((
            None if score==0 else round(float(score),1),
            (tag if tag else None),
            (flag if flag else ""),
            _early_raw.get(canonical_code_for_db(code), str(code))
        ))
        if flag:
            cnt_flag += 1
            # P1-582: 翌日検証は基準終値/OHLCを必須にするため、早期右肩ログにも価格とスコアを保存する。
            _last = g.iloc[-1]
            logs.append((
                dt_str, canonical_code_for_db(code), "右肩上がり-早期",
                f"{tag} | score={round(float(score),1)} | {detail}",
                float(_last["終値"]) if pd.notna(_last.get("終値")) else None,
                float(_last["高値"]) if pd.notna(_last.get("高値")) else None,
                float(_last["安値"]) if pd.notna(_last.get("安値")) else None,
                int(_last["出来高"]) if pd.notna(_last.get("出来高")) else None,
                round(float(score), 1), 0,
            ))

    # P1-439: 早期右肩のclear/再付与と対応ログを同じsnapshotで確定する。
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_439_right_up_early")
        cur.execute("UPDATE screener SET 右肩早期フラグ='', 右肩早期種別=NULL, 右肩早期スコア=NULL")
        if results:
            cur.executemany("""
                UPDATE screener
                   SET 右肩早期スコア=?, 右肩早期種別=?, 右肩早期フラグ=? WHERE コード=?
            """, results)
        # P1-574: 場中に何度再計算しても同一営業日の早期ログを積み増さない。
        # 当日分をsnapshotとして置換し、EOD実行ではその日の最終状態へ確定する。
        if replace_log_day:
            cur.execute(
                "DELETE FROM signals_log WHERE 種別='右肩上がり-早期' AND substr(日時,1,10)=?",
                (as_of_date.isoformat(),)
            )
        if logs:
            try:
                cur.executemany("""
                    INSERT INTO signals_log
                        (日時, コード, 種別, 詳細, 終値, 高値, 安値, 出来高, スコア, 検証済み)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                    ON CONFLICT(コード, 日時, 種別) DO UPDATE SET
                        詳細=excluded.詳細, 終値=excluded.終値, 高値=excluded.高値,
                        安値=excluded.安値, 出来高=excluded.出来高,
                        スコア=excluded.スコア, 検証済み=excluded.検証済み
                """, logs)
            except sqlite3.OperationalError:
                # 旧DBに対応unique制約が無い場合も、replace_log_day時は当日snapshot削除後なので安全。
                cur.executemany("""
                    INSERT INTO signals_log
                        (日時, コード, 種別, 詳細, 終値, 高値, 安値, 出来高, スコア, 検証済み)
                    VALUES(?,?,?,?,?,?,?,?,?,?)
                """, logs)
        cur.execute("RELEASE SAVEPOINT p1_439_right_up_early")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_439_right_up_early")
            cur.execute("RELEASE SAVEPOINT p1_439_right_up_early")
        except Exception:
            pass
        raise
    finally:
        cur.close()

    print(f"[右肩早期] 候補 {cnt_flag} 件 / 総{len(results)}件  as_of={as_of_date}  閾値{SCORE_TH}  dt={dt_str}")
# ========================= /右肩上がり・早期トリガー（完全版：置換用） =========================

# ===== シグナル判定（初動/底打ち/上昇余地） =====
def _calc_upside_potential_score(mcap_oku, close, ma20, volume_ratio):
    """P3-13: 上昇余地(時価40/トレンド40/出来高20)を欠損安全に0-100へ正規化。"""
    comps = []
    def _finite(v):
        try:
            x = float(v)
            return x if math.isfinite(x) else None
        except Exception:
            return None

    mcap = _finite(mcap_oku)
    if mcap is not None and mcap >= 0:
        comps.append((max(0.0, min(40.0, (100.0 - mcap) / 100.0 * 40.0)), 40.0))

    c = _finite(close); ma = _finite(ma20)
    if c is not None and ma is not None and c > 0 and ma > 0:
        comps.append((max(0.0, min(40.0, (c / ma - 1.0) * 200.0)), 40.0))

    vr = _finite(volume_ratio)
    if vr is not None and vr >= 0:
        comps.append((max(0.0, min(20.0, (vr - 1.0) * 20.0)), 20.0))

    if len(comps) < 2:
        return None
    known_score = sum(score for score, _ in comps)
    known_weight = sum(weight for _, weight in comps)
    return round(max(0.0, min(100.0, known_score * 100.0 / known_weight)), 1)


def phase_signal_detection(conn: sqlite3.Connection):
    
    cur = conn.cursor()
    _valid_max = _latest_valid_history_date(conn)
    _max_row = (_valid_max,) if _valid_max else None
    cur.close()
    if not _max_row or not _max_row[0]:
        return

    # P1-80: 壁時計の今日ではなく、price_history全体の最新日をシグナル基準日にする。
    # 休日/取込前に昨日の足を「今日のシグナル」として再ラベルしない。
    today = str(_max_row[0])[:10]
    # P1-253: price_historyの計算キーとscreenerのraw更新キーを分離。
    _signal_sraw = {}
    _signal_mcap = {}
    # P2-18: signal更新先/時価総額mapは判定の必須入力。DB読込失敗は明示失敗にする。
    for _rc, _mc in conn.execute("SELECT コード, 時価総額億円 FROM screener").fetchall():
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _signal_sraw:
            _signal_sraw[_rk] = _rc
            _signal_mcap[_rk] = _mc
    # P2-88: price_history全体のDISTINCT codeではなく、実際に更新するscreenerの
    # logical codeだけを対象にする。指数・上場廃止済み履歴を計算対象へ混ぜない。
    codes = list(_signal_sraw.keys())
    if not codes:
        return
    try:
        signal_asof_date = date.fromisoformat(today)
    except Exception:
        signal_asof_date = date.fromisoformat(_today_jst())
    upd_rows, log_rows = [], []
    # P1-544: price_historyに今回1行も無いscreener銘柄も最後に明示clearするため、
    # current runで判定/clear対象へ触れたlogical codeを記録する。
    _signal_touched = set()
    # P1-32: 52週高値(252営業日)を計算するのに300暦日では履歴が不足する。
    # 400暦日以上を取得し、252観測揃った銘柄だけ52週高値を有効化する。
    signal_history_days = max(int(SIGNAL_LOOKBACK_DAYS), 400)
    start_cut = (signal_asof_date - timedelta(days=signal_history_days)).strftime("%Y-%m-%d")
    
    print("[シグナル] データを一括計算中...")

    # P2-88: 旧実装は300銘柄ごとにCAST(code AS TEXT) IN (...)を発行し、
    # 約400日分の巨大price_historyを約12回反復走査して1分50秒を要した。
    # 必要期間をDBから1回だけ読み、canonical/休日/同日alias重複を1回だけ除去する。
    # 特徴量列はメモリ膨張を避けるため、下段で従来どおり300銘柄ずつ生成する。
    _signal_history_all = pd.read_sql_query("""
        SELECT rowid AS _rowid, コード, 日付, 始値, 終値, 高値, 安値, 出来高
        FROM price_history
        WHERE date(日付) >= date(?) AND date(日付) <= date(?)
        ORDER BY 日付, rowid
    """, conn, params=[start_cut, today])
    if not _signal_history_all.empty:
        _signal_history_all = _dedupe_price_history_df(_signal_history_all)
        _signal_history_all["終値"] = pd.to_numeric(_signal_history_all["終値"], errors="coerce")
        _signal_history_all = _signal_history_all[
            _signal_history_all["終値"].notna()
            & np.isfinite(_signal_history_all["終値"])
            & (_signal_history_all["終値"] > 0)
            & _signal_history_all["コード"].isin(set(codes))
        ].copy()

    # バッチ処理（特徴量DataFrameの大きすぎるメモリ消費を防ぐため300件ずつ）
    for i in range(0, len(codes), 300):
        part = codes[i:i+300]
        if _signal_history_all.empty:
            continue
        df = _signal_history_all[_signal_history_all["コード"].isin(part)].copy()

        if df.empty:
            continue
        # P1-289: alias分断前ではなく論理1系列へ統合してからMA/RSI等を計算。
        df = add_price_features(df)
        # === 【一括計算パッチ】 ===
        grp = df.groupby("コード", sort=False)

        def _signal_roll(series, window, min_periods=None, op="mean"):
            roller = series.groupby(df["コード"], sort=False).rolling(
                window, min_periods=(window if min_periods is None else min_periods)
            )
            calculated = getattr(roller, op)()
            return calculated.reset_index(level=0, drop=True).reindex(df.index)

        # P1-12: 「当日の出来高 ÷ 過去平均」を見るイベント判定では、
        # 当日出来高を平均側へ混ぜない。AI学習用など既存の inclusive rolling は変更せず、
        # シグナル判定専用の prior-only 平均を局所的に持つ。
        _volume_prev = grp["出来高"].shift(1)
        df["出来高_ma5_prev"] = _signal_roll(_volume_prev, 5, 5)
        df["出来高_ma20_prev"] = _signal_roll(_volume_prev, 20, 20)
        
        # RSI14 一括計算
        diff = grp["終値"].diff()
        up = diff.clip(lower=0)
        down = -diff.clip(upper=0)
        up_ma = _signal_roll(up, 14, 14)
        down_ma = _signal_roll(down, 14, 14)
        rs = up_ma / down_ma.replace(0, 1e-9)
        df["RSI14"] = 100 - (100 / (1 + rs))
        
        # 52週高値 一括計算
        df["high_52"] = _signal_roll(df["終値"], 252, 252, op="max")
        
        # vol60 一括計算
        df["vol60"] = _signal_roll(df["出来高"], 60, 60)
        
        # MA13 / MA26 一括計算（add_price_featuresで未計算の場合の保険）
        if "MA13" not in df.columns:
            df["MA13"] = _signal_roll(df["終値"], 13, 13)
        # ▼▼▼ これを追加 ▼▼▼
        if "MA25" not in df.columns:
            df["MA25"] = _signal_roll(df["終値"], 25, 25)
        # ▲▲▲ ここまで ▲▲▲
        if "MA26" not in df.columns:
            df["MA26"] = _signal_roll(df["終値"], 26, 26)
            
        # P1-253: 時価総額はcanonical mapから参照し、rawコード差で欠損扱いしない。
        mcap_map = _signal_mcap

        for code, g in df.groupby("コード", sort=False):
            last = g.iloc[-1]
            # P1-80: 市場全体のas-of日に当該銘柄の価格行が無ければstale。前日の足を今日扱いしない。
            last_date = str(last["日付"])[:10]
            if last_date != today:
                _ck = canonical_code_for_db(code)
                _signal_touched.add(_ck)
                # P1-578: stale銘柄を評価した「処理日=today」でシグナル更新日へ再ラベルしない。
                # フラグはcurrent-runでNULL化しつつ、更新日は実際に存在する最終価格日を保持する。
                upd_rows.append((None, None, None, last_date, _signal_sraw.get(_ck, code)))
                continue
            if len(g) < 21 or pd.isna(last["終値_ma5"]) or pd.isna(last["終値_ma20"]) or pd.isna(last["出来高_ma5_prev"]):
                # P1-67: 今回計算不能の銘柄に前回の初動/底打ち/上昇余地を残さない。
                _ck = canonical_code_for_db(code)
                _signal_touched.add(_ck)
                upd_rows.append((None, None, None, today, _signal_sraw.get(_ck, code)))
                continue

            prev_close = g["終値"].iloc[-2] if len(g) >= 2 else last["終値"]
            zenhi = (last["終値"] - prev_close)

            # --- 初動 ---
            # P1-12: 今日を含む5日平均ではなく「直前5営業日の平均」と比較する。
            # P1-584: 当日出来高NULLをNaN比率のままbuilt-in min/maxへ渡すと、
            # min(20, NaN)が20を返して出来高成分が満点化し得る。欠損/非有限/分母0は評価不能(None)。
            try:
                _cur_vol = float(last["出来高"]) if pd.notna(last["出来高"]) else None
                _prev5_vol = float(last["出来高_ma5_prev"]) if pd.notna(last["出来高_ma5_prev"]) else None
                if _cur_vol is not None and not np.isfinite(_cur_vol): _cur_vol = None
                if _prev5_vol is not None and (not np.isfinite(_prev5_vol) or _prev5_vol <= 0): _prev5_vol = None
            except Exception:
                _cur_vol = None
                _prev5_vol = None
            vol_bai = (_cur_vol / _prev5_vol) if (_cur_vol is not None and _prev5_vol is not None) else None
            price_ma5_ratio = last["終値"] / last["終値_ma5"] if last["終値_ma5"] else 0
            
            # ① 既存ロジック：急騰ブレイク初動（出来高2倍以上、5日線から+3%以上乖離、前日比プラス）
            cond_shodou_normal = (vol_bai is not None and vol_bai >= 2 and price_ma5_ratio >= 1.03 and zenhi > 0)
            
            # ② 新規ロジック：25日線反転初動（押し目からの反発）
            cond_shodou_reversal = False
            if "MA25" in last and pd.notna(last["MA25"]) and pd.notna(last["始値"]):
                ma25 = last["MA25"]
                # 条件1: 安値が25日線に接近(上2%以内)、または下抜けしてタッチした
                near_ma25 = (last["安値"] <= ma25 * 1.02)
                # 条件2: 終値はしっかり25日線を上回って維持している
                close_above = (last["終値"] > ma25)
                # 条件3: 陽線である（反発の強い証拠：終値 > 始値）
                is_yousen = (last["終値"] > last["始値"])
                # 条件4: 出来高が伴っている（5日平均の1.5倍以上。急騰時より少し条件を緩和）
                
                cond_shodou_reversal = (near_ma25 and close_above and is_yousen and vol_bai is not None and vol_bai >= 1.5)

            # どちらか一方でも満たせば「候補」とする
            shodou = "候補" if (cond_shodou_normal or cond_shodou_reversal) else None

            # --- 底打ち ---
            range_ok = False
            if pd.notna(last["高値"]) and pd.notna(last["安値"]) and last["高値"] > last["安値"]:
                pos = (last["終値"] - last["安値"]) / (last["高値"] - last["安値"])
                range_ok = pos >= 0.6
            bottom = "候補" if (pd.notna(last["RSI14"]) and last["RSI14"] <= 30 and zenhi > 0 and range_ok) else None

            # --- 上昇余地スコア ---
            # P1-31: 時価総額欠損を0億円（最小型株）と捏造して40点満額にしない。
            try:
                _z = mcap_map.get(canonical_code_for_db(code))
                zika_oku = float(_z) if _z is not None and pd.notna(_z) else None
            except Exception:
                zika_oku = None

            # P3-13: Pythonのmin/maxはNaNに対して直感と異なる結果を返し、
            # MA20=NaNでもtrend満点40になる場合がある。各成分を有限値検証し、
            # 3要素中2要素以上が揃った時だけ利用可能配点を100点へ再正規化する。
            potential_score = _calc_upside_potential_score(
                zika_oku, last.get("終値"), last.get("終値_ma20"), vol_bai
            )

            # --- 右肩上がりモメンタムスコア ---
            high_52 = last["high_52"] if pd.notna(last["high_52"]) else None
            near_high = bool(high_52 is not None and high_52 > 0 and last["終値"] and (last["終値"] / high_52 >= 0.95))

            slope13 = (last["MA13"] / g["MA13"].iloc[-13] - 1) if len(g) >= 26 and not pd.isna(g["MA13"].iloc[-13]) else 0
            slope26 = (last["MA26"] / g["MA26"].iloc[-26] - 1) if len(g) >= 52 and not pd.isna(g["MA26"].iloc[-26]) else 0

            above_ma20_ratio = (g["終値"].tail(60) > g["終値_ma20"].tail(60)).mean() if len(g) >= 60 else 0
            vol_contraction = (last["ATR20"] / last["終値"] <= 0.03) if (last["終値"] and not pd.isna(last["ATR20"])) else False

            vol20 = last["出来高_ma20"]
            vol60 = last["vol60"]
            dryup = (pd.notna(vol20) and pd.notna(vol60) and vol20 < vol60 * 0.8)

            # P1-12: 「今日の出来高急増」は直前20営業日の平均と比較する。
            vol20_prev = last.get("出来高_ma20_prev", np.nan)
            rumor_spike = (pd.notna(vol20_prev) and pd.notna(last["出来高"]) and last["出来高"] >= vol20_prev * 1.5 and zenhi > 0)

            tob_score = 0
            tob_score += 20 if near_high else 0
            tob_score += 15 if slope13 > 0 else 0
            tob_score += 15 if slope26 > 0 else 0
            tob_score += 20 if above_ma20_ratio >= 0.80 else 0
            tob_score += 15 if vol_contraction else 0
            tob_score += 10 if dryup else 0
            tob_score += 5  if rumor_spike else 0
            tob_flag = "候補" if tob_score >= 60 else None

            _ck = canonical_code_for_db(code)
            _signal_touched.add(_ck)
            upd_rows.append((shodou, bottom, potential_score, today, _signal_sraw.get(_ck, code)))

            def _append_log(kind, score_value):
                log_rows.append((
                    code, today, kind,
                    last['終値'], last['高値'], last['安値'],
                    int(last['出来高']) if pd.notna(last['出来高']) else None,
                    score_value,
                    0,
                    None, None, None, None,
                    None, None, None,
                    None, None
                ))

            if shodou: _append_log('初動', potential_score if potential_score is not None else None)
            if bottom: _append_log('底打ち', potential_score if potential_score is not None else None)
            if potential_score is not None and potential_score >= 60: _append_log('上昇余地', potential_score)
            if tob_flag: _append_log('右肩上がり', float(tob_score))

    # P1-544: screenerには存在するが今回の有効price_history系列が1本も無かった銘柄は、
    # 旧初動/底打ち/上昇余地をcurrent signalとして残さない。
    for _ck, _raw in _signal_sraw.items():
        if _ck not in _signal_touched:
            # P1-578: 今回の有効price_history系列が1本も無い銘柄は、
            # シグナル更新日まで「今日」に進めずNULLへ戻す。
            upd_rows.append((None, None, None, None, _raw))

    if upd_rows or log_rows:
        # P1-434: screenerフラグと対応signals_logを同一トランザクションで確定。
        # ログだけ/フラグだけ新状態になる途中失敗を防ぐ。
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_434_signal_detection")
            if upd_rows:
                cur.executemany("""
                    UPDATE screener
                       SET 初動フラグ=?,
                           底打ちフラグ=?,
                           上昇余地スコア=?,
                           シグナル更新日=?
                     WHERE コード=?
                """, upd_rows)
            if log_rows:
                cur.executemany("""
                    INSERT OR IGNORE INTO signals_log
                      (コード,日時,種別,終値,高値,安値,出来高,スコア,検証済み,
                       次日始値,次日終値,次日高値,次日安値,
                       リターン終値pct,フォロー高値pct,最大逆行pct,判定,理由)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, log_rows)
            cur.execute("RELEASE SAVEPOINT p1_434_signal_detection")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_434_signal_detection")
                cur.execute("RELEASE SAVEPOINT p1_434_signal_detection")
            except Exception:
                pass
            raise
        finally:
            cur.close()

# ===== 翌営業日検証 =====
def phase_validate_prev_business_day(conn: sqlite3.Connection):
    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    today = date.fromisoformat(_today_jst())
    d0 = prev_business_day_jp(today, extra_closed)
    d0s = d0.strftime("%Y-%m-%d")

    cur = conn.cursor()
    cur.execute("""
        SELECT 日時, コード, 種別, 終値, スコア
        FROM signals_log
        -- P1-72: 価格取り込み遅延で前日に検証できなかったシグナルも30日間は再試行する。
        -- P1-68: 早期右肩は YYYY-MM-DD HH:MM:SS で記録されるため、日付部分で抽出する。
        WHERE substr(日時, 1, 10) BETWEEN date(?, '-30 day') AND ?
          AND (検証済み IS NULL OR 検証済み=0)
    """, (d0s, d0s))
    sigs = cur.fetchall()
    cur.close()
    if not sigs:
        print(f"直近30日〜前営業日({d0s})の未検証シグナルなし")
        return

    # 各シグナル固有の「次営業日」を計算する。古い未検証も今日までなら追試する。
    sig_next_date = {}
    eligible_sigs = []
    for sig in sigs:
        signal_ts = sig[0]
        try:
            signal_day = date.fromisoformat(str(signal_ts)[:10])
        except (TypeError, ValueError):
            # P2-71: 個別の壊れた日時はその行だけ対象外。カレンダー障害とは分離する。
            print(f"[validate-prev][WARN] invalid signal timestamp skipped: {signal_ts!r}")
            continue
        # 営業日計算が失敗した場合は全体の基準日が信用できないため上位へ伝播。
        next_day = next_business_day_jp(signal_day, extra_closed)
        if next_day <= today:
            sig_next_date[(signal_ts, sig[1], sig[2])] = next_day.strftime("%Y-%m-%d")
            eligible_sigs.append(sig)

    if not eligible_sigs:
        return

    sigs = eligible_sigs
    # P1-219: signals_log と price_history のコード表記揺れ（7203.0/7203, 285a/285A）を吸収。
    codes = sorted(set(
        c for c in (canonical_code_for_db(s[1]) for s in sigs) if c
    ))
    target_dates = sorted(set(sig_next_date.values()))
    # P1-244: price_history側にlegacy 7203.0 / 小文字英字が残っていても候補へ含める。
    # P1-337: 翌日検証も共通alias候補を使い、.N/.S/.F旧行を検証不能にしない。
    query_codes = sorted(expand_code_query_variants(codes))
    qmarks = ",".join("?"*len(query_codes))
    df1 = pd.read_sql_query(f"""
        SELECT コード, 日付, 始値, 高値, 安値, 終値
        FROM price_history
        WHERE 日付 BETWEEN ? AND ? AND CAST(コード AS TEXT) IN ({qmarks})
        ORDER BY コード, 日付
    """, conn, params=[target_dates[0], target_dates[-1], *query_codes])

    if df1.empty:
        # P1-678: eligibleな次営業日価格がまだ無い場合は成功markerを立てず、同日後続runで再試行する。
        return False

    # P1-291: 同じ次営業日にaliasが複数行あっても任意の最後の行を使わない。
    df1 = _dedupe_price_history_df(df1)
    df1["コード_norm"] = df1["コード"].map(canonical_code_for_db)
    rows = {(r["コード_norm"], str(r["日付"])[:10]): r for _, r in df1.iterrows()}
    updates = []
    # P1-678: 次日価格/OHLCがまだ未完成の件数。検証可能分は確定し、残件だけ同日再試行する。
    _pending_validation = 0
    for signal_ts, code, kind, base_close, score in sigs:
        signal_d1s = sig_next_date.get((signal_ts, code, kind))
        if not signal_d1s:
            continue
        r_next = rows.get((canonical_code_for_db(code), signal_d1s))
        # P1-53: pandas.Series を真偽値評価すると ValueError。存在判定はNoneで行う。
        if r_next is None:
            _pending_validation += 1
            continue
        o1 = ffloat(r_next["始値"], None)
        h1 = ffloat(r_next["高値"], None)
        l1 = ffloat(r_next["安値"], None)
        c1 = ffloat(r_next["終値"], None)

        # P1-678: signal側の基準終値欠損/0/非有限は、後からprice_historyが埋まっても直らない。
        # 無限retryにせず「検証不能」と確定する。
        _base = ffloat(base_close, None)
        if _base is None or not math.isfinite(float(_base)) or float(_base) <= 0:
            updates.append((1, o1, c1, h1, l1, None, None, None,
                            "検証不能", "signal base close missing", code, signal_ts, kind))
            continue
        base_close = float(_base)

        # P1-218/P1-678: 次営業日のOHLCが未完成/無効なら未検証のまま同日再試行。
        _next_core = (c1, h1, l1)
        if any(v is None or not math.isfinite(float(v)) or float(v) <= 0 for v in _next_core):
            _pending_validation += 1
            continue

        ret_close = (c1 / base_close - 1) * 100
        follow_high = (h1 / base_close - 1) * 100
        mae = (l1 / base_close - 1) * 100

        # P1-64: 条件未達は「外れ」、データ不足は「検証不能」と明示。
        # 旧「見送り」では relax_rejudge_signals(判定='外れ') に一件も流れなかった。
        metrics_ready = (ret_close is not None and follow_high is not None and mae is not None)
        verdict, reason = ("外れ", "") if metrics_ready else ("検証不能", "insufficient data")
        if kind == "初動":
            if (follow_high is not None and follow_high >= 2.0) or (ret_close is not None and ret_close > 0):
                verdict = "的中"
            reason = (
                f"follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%"
                if follow_high is not None and ret_close is not None else "insufficient data"
            ) if (follow_high is not None and ret_close is not None) else "insufficient data"
        elif kind == "底打ち":
            pos = (c1 - l1) / (h1 - l1) if (c1 is not None and h1 is not None and l1 is not None and h1 > l1) else 0
            if (ret_close is not None and ret_close > 0) and pos >= 0.5:
                verdict = "的中"
            reason = f"close_ret={ret_close:.2f}% pos={pos:.2f}" if ret_close is not None else f"close_ret=N/A pos={pos:.2f}"
        elif kind == "上昇余地":
            need = 1.0
            if score is not None:
                s = float(score)
                if s >= 90: need = 3.0
                elif s >= 80: need = 2.0
                elif s >= 60: need = 1.0
            if (follow_high is not None and follow_high >= need) or (ret_close is not None and ret_close > 0):
                verdict = "的中"
            reason = (
                f"need={need:.1f}% follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%"
                if follow_high is not None and ret_close is not None else f"need={need:.1f}% insufficient data"
            )
        elif kind in ("右肩上がり", "右肩上がり-早期", "右肩上がり-持続"):
            # P1-69/P1-579: 早期/持続右肩も通常右肩と同じ翌日継続基準で検証する。
            if (follow_high is not None and follow_high >= 1.0) or (ret_close is not None and ret_close >= 0):
                verdict = "傾向維持"
            # P1-48: 欠損リターンを :.2f でフォーマットして検証全体を落とさない。
            if follow_high is not None and ret_close is not None:
                reason = f"follow_high={follow_high:.2f}% close_ret={ret_close:.2f}%"
            else:
                reason = "follow_high=N/A close_ret=N/A"
        else:
            verdict = "検証不能"
            reason = "unknown kind"

        updates.append((1, o1, c1, h1, l1,
                        None if ret_close is None else round(ret_close, 2),
                        None if follow_high is None else round(follow_high, 2),
                        None if mae is None else round(mae, 2),
                        verdict, reason, code, signal_ts, kind))

    if updates:
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_450_validate_next_day")
            cur.executemany("""
                UPDATE signals_log
                SET 検証済み=?,
                    次日始値=?, 次日終値=?, 次日高値=?, 次日安値=?,
                    リターン終値pct=?, フォロー高値pct=?, 最大逆行pct=?,
                    判定=?, 理由=?
                WHERE コード=? AND 日時=? AND 種別=?
            """, updates)
            cur.execute("RELEASE SAVEPOINT p1_450_validate_next_day")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_450_validate_next_day")
                cur.execute("RELEASE SAVEPOINT p1_450_validate_next_day")
            except Exception:
                pass
            raise
        finally:
            cur.close()

    # P1-583: P1-72で30日分の未検証シグナルを追試できる一方、旧出力は
    # 常に直前営業日(d0s)だけをCSV化していた。価格遅延で数日前のシグナルが
    # 今日初めて検証できてもDBだけ検証済みになり、その日のレポートへ永久に現れない。
    # 今回実際に検証したsignal dayごとに標準の validate_<signal>_vs_<next>.csv を再生成する。
    _report_days = set()
    for _u in updates:
        try:
            _report_days.add(str(_u[-2])[:10])  # signal_ts
        except Exception:
            pass
    # 従来互換: 直前営業日の既存検証済み行も通常どおりレポート対象にする。
    _report_days.add(d0s)

    _report_total = 0
    _report_written = []
    for _sig_day_s in sorted(_report_days):
        try:
            _sig_day = date.fromisoformat(_sig_day_s)
        except (TypeError, ValueError):
            print(f"[validate-prev][WARN] invalid report signal date skipped: {_sig_day_s!r}")
            continue
        # P2-71: report名だけの問題でも営業日カレンダー障害をsilent skipしない。
        _next_day_s = next_business_day_jp(_sig_day, extra_closed).isoformat()

        cur = conn.cursor()
        try:
            cur.execute("""
                SELECT コード, 種別, 日時, 終値, 次日始値, 次日終値, 次日高値, 次日安値,
                       リターン終値pct, フォロー高値pct, 最大逆行pct, 判定, 理由, スコア
                FROM signals_log
                WHERE substr(日時, 1, 10)=? AND 検証済み=1
                ORDER BY 判定 DESC, フォロー高値pct DESC
            """, (_sig_day_s,))
            _report_rows = cur.fetchall()
        finally:
            cur.close()

        if not _report_rows:
            continue
        _out = os.path.join(OUTPUT_DIR, f"validate_{_sig_day_s}_vs_{_next_day_s}.csv")
        # P1-450/P2-45: atomic replaceに加え、理由などにcomma/quote/newlineが入っても
        # 列崩れしないよう標準csv.writerでRFC互換escapeする。
        _csv_buf = io.StringIO(newline="")
        _csv_writer = csv.writer(_csv_buf, lineterminator="\n")
        _csv_writer.writerow([
            "コード", "種別", "日時", "終値", "次日始値", "次日終値", "次日高値", "次日安値",
            "リターン終値pct", "フォロー高値pct", "最大逆行pct", "判定", "理由", "スコア"
        ])
        _csv_writer.writerows([
            [x if x is not None else "" for x in r] for r in _report_rows
        ])
        _atomic_write_text_file(_out, _csv_buf.getvalue())
        _report_total += len(_report_rows)
        _report_written.append(f"{_sig_day_s}→{_next_day_s}:{len(_report_rows)}")

    if _report_written:
        try:
            notification.notify(
                title="翌日検証レポート",
                message=f"検証レポート更新: {_report_total}件 / " + ", ".join(_report_written[:4]),
                timeout=5,
            )
        except Exception:
            pass

    # P1-678: 一部未完成なら検証可能分は保持しつつ成功markerを立てない。
    # 次回runでは未検証残件だけが再試行される。
    if _pending_validation > 0:
        print(f"[validate-prev] pending={_pending_validation} → 同日再試行のためdaily marker未確定")
        return False
    return True

# ===== HTML（オフライン/5タブ/各表にリンク列/Migikataフィルタ追加） =====

# ========== Template exporter（CDNなし・JSON直埋め・フォールバック強化・完全置換） ==========

# しきい値（ヘルプ表示用：未使用でも残す）
HH_N        = globals().get('HH_N', 60)
PIVOT_EPS   = globals().get('PIVOT_EPS', 0.002)
VOL_BOOST   = globals().get('VOL_BOOST', 1.5)
EXT_20_MAX  = globals().get('EXT_20_MAX', 0.05)
EXT_50_MAX  = globals().get('EXT_50_MAX', 0.10)
POCKET_WIN  = globals().get('POCKET_WIN', 10)
REB_WIN     = globals().get('REB_WIN', 10)
RECLAIM_WIN = globals().get('RECLAIM_WIN', 10)
SCORE_TH    = globals().get('SCORE_TH', 70)

# ---------- 安全整形 ----------
def _to_float(v):
    if v is None: return None
    try:
        s = str(v).replace(',', '').replace('％','').replace('%','').strip()
        if s == '': return None
        x = float(s)
        return x if math.isfinite(x) else None
    except Exception:
        return None

def _optional_bool(v):
    """True/False/不明(None)を区別して解釈する。NaNをTrue扱いしない。"""
    if v is None:
        return None
    try:
        if pd.isna(v):
            return None
    except Exception:
        pass
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, np.integer)) and not isinstance(v, bool):
        if int(v) == 1:
            return True
        if int(v) == 0:
            return False
    if isinstance(v, (float, np.floating)) and math.isfinite(float(v)):
        if float(v) == 1.0:
            return True
        if float(v) == 0.0:
            return False
    t = str(v).strip().lower()
    if t in {"1", "true", "yes", "y", "on", "あり", "○", "◯"}:
        return True
    if t in {"0", "false", "no", "n", "off", "なし", "×"}:
        return False
    return None


def _profit_acceleration_quality(data) -> str:
    """利益加速フラグとYoYの整合状態を返す。

    ``直近営業益YoY>0`` は単なる増益であり、加速（前期/前四半期より伸びが強まった）とは
    別概念なのでfallbackに使わない。フラグが立った時だけ、売上YoYとの比較を
    マージン面の品質確認として使う。
    """
    get = data.get if hasattr(data, "get") else lambda k, d=None: d
    flag = _optional_bool(get("利益加速フラグ"))
    if flag is None:
        return "unknown"
    if not flag:
        return "inactive"
    sales_yoy = _to_float(get("直近売上YoY"))
    op_yoy = _to_float(get("直近営業益YoY"))
    if sales_yoy is None or op_yoy is None:
        return "unverified"
    return "quality" if op_yoy >= sales_yoy else "margin_lag"


def _fmt_cell(v):
    """HTML混在・数値を安全に整形して返す。<span>/<br>/<div> を含む場合は safe 出力。"""
    try:
        if v is None or (isinstance(v,float) and math.isnan(v)): 
            return ""
        s = str(v)
        if any(tag in s for tag in ("<a ", "<img", "<span", "<br", "<div")):
            return Markup(s)
        if isinstance(v,int): 
            return f"{v:,}"
        if isinstance(v,float):
            if abs(v-round(v))<1e-9: 
                return f"{int(round(v)):,}"
            return f"{v:.2f}"
        fv = _to_float(v)
        if fv is not None: 
            return _fmt_cell(fv)
        return escape(s)
    except Exception:
        return escape(str(v)) if v is not None else ""

def _noshor_from_agency(val) -> str:
    """P1-39: 明示的な「機関空売りなし」と、未取得/不明を分離する。"""
    if val is None:
        return ""
    s = str(val).strip()
    sl = s.lower()
    if sl in ("", "-", "nan", "none", "n/a", "na"):
        return ""  # 不明
    if s == "なし" or sl == "0":
        return "1"  # 明示的になし
    return "0"      # 機関名等が実在

def _noshor_flag_from_current_snapshot(d: dict) -> str:
    """P3-45: 当日crawl成功で確認できた現在残高だけを1/0化する。

    取得失敗/未取得は空欄=不明。旧screener.空売り機関は過去履歴や旧ジョブの
    表示を含む曖昧列のため、現在判定のfallbackに使わない。
    """
    if not isinstance(d, dict):
        return ""

    acquisition = str(d.get("機関空売り取得状態") or "").strip()
    current_state = str(d.get("機関空売り現在状態") or "").strip()
    if acquisition or current_state:
        if acquisition != "成功":
            return ""
        if current_state == "なし":
            return "1"
        if current_state == "あり":
            return "0"
        return ""

    # 互換経路: P3-45フィールド付与前の対象テスト等では、
    # 今回集計の数値が明示されている場合だけ判定する。
    raw_total = d.get("機関空売り合計株数")
    try:
        total = float(raw_total) if raw_total not in (None, "") else None
        if total is not None and math.isfinite(total):
            return "1" if total <= 0 else "0"
    except Exception:
        pass
    return ""

def _op_ratio_flag(d):
    """営利対時価>=10%を 1/0、必要財務が無い場合は空欄(未判定)で返す。"""
    e = _to_float(d.get("営業利益"))
    z = _to_float(d.get("時価総額億円"))
    if e is None or z is None or z <= 0:
        return ""
    try:
        ratio = (e / z) * 100.0
        return "1" if ratio >= 10.0 else "0"
    except Exception:
        return ""

def _bucket_turn(v):
    if v is None: return "売買:<不明>"
    v=float(v);  return "売買:<5" if v<5 else ("売買:5-10" if v<10 else ("売買:10-50" if v<50 else ("売買:50-100" if v<100 else "売買:100+")))
def _bucket_rvol(v):
    if v is None: return "RVOL:<不明>"
    v=float(v);  return "RVOL:<1" if v<1 else ("RVOL:1-2" if v<2 else ("RVOL:2-3" if v<3 else ("RVOL:3-5" if v<5 else "RVOL:5+")))
def _bucket_vol(v):
    if v is None: return "出来:<不明>"
    v=float(v);  return "出来:<10万" if v<1e5 else ("出来:10-100万" if v<1e6 else ("出来:100-500万" if v<5e6 else ("出来:500-1000万" if v<1e7 else "出来:1000万+")))
def _bucket_atr(v):
    if v is None: return "ATR:<不明>"
    v=float(v);  return "ATR:<4" if v<4 else ("ATR:4-6" if v<6 else ("ATR:6-10" if v<10 else "ATR:10+"))
def _bucket_mcap(v):
    if v is None: return "時価:<不明>"
    v=float(v);  return "時価:<300" if v<300 else ("時価:300-1000" if v<1000 else ("時価:1000-5000" if v<5000 else ("時価:5000-20000" if v<20000 else "時価:20000+")))
def _bucket_comp(v):
    if v is None: return "合成S:<不明>"
    v=float(v);  return "合成S:<70" if v<70 else ("合成S:70-80" if v<80 else ("合成S:80-90" if v<90 else "合成S:90+"))
def _bucket_rate(v):
    if v is None: return "上昇率:不明"
    v=float(v);  return "上昇率:0-1" if v<1 else ("上昇率:1-3" if v<3 else ("上昇率:3-5" if v<5 else "上昇率:5+"))

def _build_reason(d):
    tags = []
    tags.append(_bucket_rate(_to_float(d.get("前日終値比率"))))
    tags.append(_bucket_turn(_to_float(d.get("売買代金(億)"))))
    tags.append(_bucket_rvol(_to_float(d.get("RVOL代金"))))
    tags.append(_bucket_vol(_to_float(d.get("出来高"))))
    tags.append(_bucket_atr(_to_float(d.get("ATR14%"))))
    tags.append(_bucket_mcap(_to_float(d.get("時価総額億円"))))
    tags.append(_bucket_comp(_to_float(d.get("合成スコア"))))
    etype = (d.get("右肩早期種別") or "").strip()
    if etype: tags.append(f"早期:{etype}")
    # P1-560: HTML exportではフラグが「候補 + 開始日mini HTML」へ先に装飾される。
    # 完全一致だと判定理由から本来のシグナルタグだけ消えるため部分一致で判定する。
    if _to_float(d.get("INITIAL_MOMENTUM")) == 1.0: tags.append("IM初動")
    if "候補" in str(d.get("初動フラグ") or ""): tags.append("初動")
    if "候補" in str(d.get("底打ちフラグ") or ""): tags.append("底打ち")
    if "候補" in str(d.get("右肩上がりフラグ") or ""): tags.append("右肩")
    if "候補" in str(d.get("右肩早期フラグ") or ""): tags.append("早期")
    if _op_ratio_flag(d) == "1": tags.append("割安")
    if d.get("空売り機関なし_flag","0") == "1": tags.append("機関:なし")
    return " / ".join(tags)

# === 推奨／比率・営利対時価の自動算出（DB非依存）========================

# === helper（無ければ追記） ===
def _clamp(x, lo, hi):
    try:
        x = float(x)
    except Exception:
        return lo
    return max(lo, min(hi, x))

def _to_float_safe(d, key, default=None):
    try:
        v = d.get(key, None)
        if v is None or (isinstance(v, str) and not v.strip()):
            return default
        return float(str(v).replace('%',''))
    except Exception:
        return default

# === 推奨ロジック（置換） ===
def _derive_recommendation(d: dict):
    """
    連続比率（0〜1）を作り、UI/運用は離散バンド（1.0/0.75/0.5/0.25/—）で安定化。
    ・比率は comp（合成スコア）を中核に、流動性と上昇率で補正
    ・境界付近はヒステリシス（±0.02）でフリップ抑制
    返り値: (label:str or "", ratio_band:float or None, ratio_raw:float or None)
    """
    # 参照値の取得
    comp = _to_float_safe(d, "合成スコア", None)           # 0〜100想定

    # P1-657: RVOL代金はcurrent-runのauthoritative列。
    # writerの無い旧RVOL_売買代金へfallbackすると、current列をNULL clearした銘柄で旧値が復活する。
    rvol = _to_float_safe(d, "RVOL代金", None)             # ≥2が目安

    turn = _to_float_safe(d, "売買代金(億)", None)         # ≥5が目安

    # _prepare_rows 内では直前に raw を生成しているので最優先で利用。
    # 表示済みの "1.23%" 文字列や旧列名にも互換対応する。
    rate = _to_float_safe(d, "前日終値比率_raw", None)      # 上昇率（％）
    if rate is None:
        rate = _to_float_safe(d, "前日終値比率", None)
    # P1-657: writerの無い旧「前日終値比率（％）」へはfallbackしない。

    # 候補フラグ（どれか一つでも「候補」）。
    # HTML装飾後は "候補&nbsp;<span ...>" になるため完全一致ではなく包含判定。
    flags = [
        str(d.get("右肩早期フラグ", "") or "").strip(),
        str(d.get("右肩上がりフラグ", "") or "").strip(),
        str(d.get("初動フラグ", "") or "").strip(),
    ]
    is_setup = any("候補" in f for f in flags)

    # 連続化：合成S=65→0.0, 100→1.0（下駄・上限付き）
    if comp is None:
        return ("", None, None)
    # P1-669: 合成スコアは3成分中2成分でも参考値として成立するが、
    # actionableな推奨比率では欠損成分を「減点なし」として有利に扱わない。
    # RVOL・売買代金・前日比が全てcurrentに揃った時だけ推奨を確定する。
    if rvol is None or turn is None or rate is None:
        return ("", None, None)
    ratio = _clamp((comp - 65.0) / 35.0, 0.0, 1.0)

    # 流動性補正（満たない場合は減点方式）
    if rvol is not None and rvol < 2.0:
        ratio *= 0.7
    if turn is not None and turn < 5.0:
        ratio *= 0.8

    # 上昇率レンジ補正（小口レンジ外は減点、有力レンジは微加点）
    if rate is not None:
        if 0.0 <= rate <= 2.0:
            ratio = min(1.0, ratio * 1.10)   # 有力レンジ微ブースト
        elif -2.0 <= rate <= 3.0:
            pass                              # 小口レンジ＝補正なし
        else:
            ratio *= 0.6                      # レンジ外は減点

    # 候補でない場合は弱め扱い（完全にゼロにせず、わずかに残す選択も可能）
    if not is_setup:
        ratio *= 0.5

    ratio = _clamp(ratio, 0.0, 1.0)
    ratio_raw = ratio  # 生値を保持

    # ===== バンド分け（UI/運用向け）＋ヒステリシス =====
    # 直近の raw が dict にあれば取得（ダッシュボード側で前回値を入れてくれていれば粘りが効く）
    prev_raw = _to_float_safe(d, "推奨比率_raw", None)

    # バンド（下限しきい値, ラベル, 表示倍率）
    BANDS = [
        (0.88, "エントリー有力", 1.00),
        (0.63, "中強度",        0.75),
        (0.38, "小口提案",      0.50),
        (0.13, "微小口",        0.25),
    ]
    EPS = 0.02  # ヒステリシス幅

    def _band_index(val):
        for i, (thr, _, _) in enumerate(BANDS):
            if val >= thr:
                return i
        return None

    cand_idx = _band_index(ratio)
    prev_idx = _band_index(prev_raw) if prev_raw is not None else None

    # ヒステリシス：境界±EPSで前回バンドを維持
    if prev_idx is not None and cand_idx is not None:
        # 上方向遷移のときは “thr + EPS” まで到達しない限り据え置き
        if cand_idx < prev_idx:
            thr_up = BANDS[cand_idx][0]
            if ratio < (thr_up + EPS):
                cand_idx = prev_idx
        # 下方向遷移のときは “prev_thr - EPS” を割るまで据え置き
        elif cand_idx > prev_idx:
            prev_thr = BANDS[prev_idx][0]
            if ratio >= (prev_thr - EPS):
                cand_idx = prev_idx

    # バンド決定
    if cand_idx is None:
        # しきい値未満は「空欄」
        return ("", None, ratio_raw)
    label, ratio_band = BANDS[cand_idx][1], BANDS[cand_idx][2]

    return (label, ratio_band, ratio_raw)
def _derive_triangle_scores(d):
    """三角スコア（上昇・安定・短期ボラ）を計算して (g, s, v) を返す"""

    # ---------- 1) 上昇スコア（Growth ▲） ----------
    # P3-6: 旧式は0-100の合成スコアへ最大40点のシグナル、RVOL12点、RS20点を
    # そのまま足していたため、有力候補ほど100点へ大量飽和して順位情報を失っていた。
    # 100点の成分予算を先に固定し、合成70 + setup12 + RVOL8 + RS10 = 最大100へ再配分する。
    base = _to_float(d.get("合成スコア"))
    # P3-29: Growthの70%を占める主成分が欠損なら、弱い0点ではなく未判定。
    g = None if base is None else _clamp(base, 0.0, 100.0) * 0.70

    # 初動・右肩・早期・底打ちは相関が高いため、1本3点・最大12点に抑える。
    setup_count = 0
    for key in ["初動フラグ", "右肩上がりフラグ", "右肩早期フラグ", "底打ちフラグ"]:
        flag_val = d.get(key)
        flag_txt = str(flag_val or "").strip()
        if flag_val in [1, True] or flag_txt in ["○", "◯", "あり"] or "候補" in flag_txt:
            setup_count += 1
    if g is not None:
        g += min(setup_count, 4) * 3.0

    # RVOLは合成Sにも含まれるため補助加点を最大8点へ制限。低RVOLは-5点。
    rvol = _to_float(d.get("RVOL代金"))
    if g is not None and rvol is not None:
        if rvol >= 5:
            g += 8.0
        elif rvol >= 2:
            g += 5.0
        elif rvol < 1:
            g -= 5.0

    # RS_5/RS_20は各最大5点。+10%で各上限。
    for key in ["RS_5", "RS_20"]:
        rs = _to_float(d.get(key))
        if g is not None and rs is not None and rs > 0:
            rs_pct = rs * 100.0
            g += min(rs_pct * 0.5, 5.0)

    # 前日比（飛びすぎ陽線は追いかけリスクとして減点）。
    pct = _to_float(d.get("前日終値比率_raw"))
    if g is not None and pct is not None:
        if pct > 6:
            g -= 8.0
        elif pct > 3:
            g -= 3.0

    if g is not None:
        g = max(0.0, min(100.0, g))

    # ---------- 2) 安定スコア（Safety ◆） ----------
    # P2-1: 欠損が多いだけで「やや安全」に見えないよう、真の中立50から開始する。
    # 各根拠が取得できた場合だけ加減点し、全欠損なら未判定(None)を返す。
    s = 50
    _safety_obs = 0

    # 空売り機関なし
    # P0-7: 前処理は "1"/"0" の文字列を返すため、int/string/boolを共通判定する。
    no_short_flag = str(d.get("空売り機関なし_flag") or "").strip().lower()
    if no_short_flag in {"1", "true"}:
        _safety_obs += 1
        s += 15
    elif no_short_flag in {"0", "false"}:
        _safety_obs += 1
        s -= 5
    # 不明("")は安全/危険どちらにも寄せない。

    # 割安業績
    # 営利対時価_flag も前処理では "1"/"0" の文字列になり得る。
    op_ratio_flag = str(d.get("営利対時価_flag") or "").strip().lower()
    _op_for_ratio = _to_float(d.get("営業利益"))
    _mcap_for_ratio = _to_float(d.get("時価総額億円"))
    if _op_for_ratio is not None and _mcap_for_ratio is not None and _mcap_for_ratio > 0:
        _safety_obs += 1
        if op_ratio_flag in {"1", "true"}:
            s += 10

    # 営業利益YoY
    # P1-657: 現行FUND_SCRIPTがauthoritativeに更新するのは「直近営業益YoY」のみ。
    # current値がNULLのときwriterless旧「営利YoY」を復活させず、情報不足として中立に扱う。
    yoy = _to_float(d.get("直近営業益YoY"))
    if yoy is not None:
        _safety_obs += 1
        if yoy > 0:
            s += 5
        else:
            s -= 5

    # 増資リスク
    zr = _to_float(d.get("増資スコア"))
    if zr is not None:
        _safety_obs += 1
        if zr >= 70:
            s -= 15
        elif zr >= 40:
            s -= 8
        else:
            s += 3

    # ATR（ボラ高すぎ＝減点）
    atr = _to_float(d.get("ATR14_PCT"))
    if atr is not None:
        _safety_obs += 1
        if atr <= 3:
            s += 10
        elif atr >= 7:
            s -= 10

    # 支持帯の距離
    # （DB由来で文字列になっていることがあるので、安全に float 変換してから使う）
    support = _to_float(d.get("最寄り支持"))
    price   = _to_float(d.get("現在値"))

    if price is not None and support is not None and price > 0:
        _safety_obs += 1
        dist = (price - support) / price * 100
        if 0 <= dist <= 3:
            s += 8
    
    # ★ 追加：需給OH（DTC）による減点処理
    oh = _to_float(d.get("需給OH"))
    if oh is not None:
        _safety_obs += 1
        if oh >= 5.0:
            s -= 15  # 5日分以上は上値が相当重いため大幅減点
        elif oh >= 3.0:
            s -= 5   # 3日分以上でやや重い
    
    s = None if _safety_obs == 0 else max(0, min(100, s))

    # ---------- 3) 短期ボラスコア（Vol ■） ----------
    # P3-8: ATRはこの指標の一次情報。ATR欠損を0点にすると「極めて低ボラ」と
    # 誤読できるため、未判定(None)として明示する。RVOLだけでは価格ボラを代用しない。
    v = None
    if atr is not None:
        v = 0
        if atr <= 1:
            v += 20
        elif atr <= 3:
            v += 50
        elif atr <= 6:
            v += 70
        elif atr <= 9:
            v += 85
        else:
            v += 95

        # RVOLはATRが観測できた場合だけ補助情報として加点する。
        if rvol is not None:
            if rvol >= 5:
                v += 10
            elif rvol >= 2:
                v += 5

        v = int(max(0, min(100, v)))

    return (None if g is None else int(g)), (None if s is None else int(s)), v




def _derive_opratio_flag(d, threshold_pct: float = 10.0) -> str:
    """営業利益/時価総額が閾値以上なら1、条件外0、必要値欠損は空欄。"""
    op   = _to_float(d.get("営業利益"))
    mcap = _to_float(d.get("時価総額億円"))
    if op is None or mcap is None or mcap <= 0:
        return ""
    ratio_pct = (op / mcap) * 100.0
    return "1" if ratio_pct >= threshold_pct else "0"
# =======================================================================
from typing import Union

def _safe_get_numeric(df: pd.DataFrame, col_names: Union[str, list[str]], default_val: float = 0.0) -> pd.Series:
    """
    指定カラム（複数候補可）を安全に数値Seriesとして取得し、欠損値やカラムが存在しない場合はデフォルト値で埋める。
    空間計算量: O(N) - 新しいSeriesを生成
    時間計算量: O(N) - Pandasのベクトル化処理に依存
    """
    if isinstance(col_names, str):
        col_names = [col_names]
        
    for col in col_names:
        if col in df.columns:
            return pd.to_numeric(df[col], errors="coerce").fillna(default_val)
            
    # 全ての候補が存在しない場合はデフォルト値のSeriesを返す
    return pd.Series(default_val, index=df.index, dtype=float)

def apply_3algo_labels(df: pd.DataFrame) -> pd.DataFrame:
    """3大アルゴリズムをグラデーション（連続評価）で100点満点で採点し、最終的な売買判定を下す"""
    if df is None or getattr(df, "empty", True):
        return df

    import numpy as np
    import pandas as pd

    # --- 1. モメンタムCTA (0〜100点) ---
    # P1-33: RS欠損/列欠如を実測0（指数並み）へ変換しない。
    # 実測0には従来どおり中立点、欠損にはRS成分0点を与える。
    rs20_raw = pd.to_numeric(df["RS_20"], errors="coerce") if "RS_20" in df.columns else pd.Series(np.nan, index=df.index, dtype=float)
    rs5_raw = pd.to_numeric(df["RS_5"], errors="coerce") if "RS_5" in df.columns else pd.Series(np.nan, index=df.index, dtype=float)
    rs20 = rs20_raw.fillna(0.0)
    rs5 = rs5_raw.fillna(0.0)
    
    # P1-77: 前日比が欠損/列欠如でも0%という実測値にしない。
    pct_raw = pd.Series(np.nan, index=df.index, dtype=float)
    for _c in ["前日終値比率_raw", "前日終値比率"]:
        if _c in df.columns:
            pct_raw = pct_raw.combine_first(pd.to_numeric(df[_c], errors="coerce"))
    pct_change = pct_raw.fillna(0.0)
    
    # ベース点（フラグが出ている場合は下駄をはかせる）
    # P1-49: フラグ列自体が無い場合も、dfと同じindex長のFalse列を使う。
    # 空Seriesをdefaultにすると non-empty df とブロードキャストできず例外になる。
    _mom_flag_cols = ["右肩上がりフラグ", "右肩早期フラグ", "初動フラグ"]
    has_rightup = df.get("右肩上がりフラグ", pd.Series("", index=df.index, dtype=str)).astype(str).str.contains("候補")
    has_early = df.get("右肩早期フラグ", pd.Series("", index=df.index, dtype=str)).astype(str).str.contains("候補")
    has_shodou = df.get("初動フラグ", pd.Series("", index=df.index, dtype=str)).astype(str).str.contains("候補")
    # P1-79: 「列がDataFrameに存在する」だけで全行を情報あり扱いしない。
    # 空文字は計算済み・非候補として有効、NaN/Noneはその行の未取得として扱う。
    flag_info_available = pd.Series(False, index=df.index, dtype=bool)
    flag_info_complete = pd.Series(True, index=df.index, dtype=bool)
    for _c in _mom_flag_cols:
        if _c in df.columns:
            flag_info_available = flag_info_available | df[_c].notna()
            flag_info_complete = flag_info_complete & df[_c].notna()
        else:
            flag_info_complete[:] = False
    # P1-551: Momentumの正式判定は構成要素が揃った時だけ。
    # 旧実装はRS20/RS5/前日比/flagのどれか1つでvalidとなり、欠損成分を0点として
    # 総合の買い/売りまで確定し得た。部分スコアは参考表示に残すが総合判定から除外する。
    momentum_valid = rs20_raw.notna() & rs5_raw.notna() & pct_raw.notna() & flag_info_complete
    base_mom = np.where(has_rightup | has_early | has_shodou, 40, 0)
    
    # ★追加：前日比 +5%〜+15% の範囲で 0〜40点 を加算（単日急騰を逃さない）
    pct_score = np.clip((pct_change - 5.0) / 10.0 * 40, 0, 40)
    
    # RS_5 / RS_20 はDBでは小数リターン差（例: +5% = 0.05）で保存される。
    # この採点式の閾値は%ポイント（+5, +10）前提なので、ここでのみ ×100 して単位を合わせる。
    rs20_pct = rs20 * 100.0
    rs5_pct = rs5 * 100.0
    rs20_score = pd.Series(np.clip((rs20_pct + 10.0) / 40.0 * 30.0, 0, 30), index=df.index).where(rs20_raw.notna(), 0.0)
    rs5_score = pd.Series(np.clip((rs5_pct + 5.0) / 20.0 * 30.0, 0, 30), index=df.index).where(rs5_raw.notna(), 0.0)
    
    # ★合算にpct_scoreを追加
    score_mom = np.round(np.clip(base_mom + rs20_score + rs5_score + pct_score, 0, 100)).astype(int)
    
    cond_mom_strong = score_mom >= 80
    cond_mom_break = (score_mom >= 60) & (has_early | has_shodou)
    cond_mom_adj = (rs20 > 0) & (rs5 < 0) & (score_mom >= 40)

    label_mom = np.select(
        [~momentum_valid, cond_mom_strong, cond_mom_break, cond_mom_adj], 
        ["情報不足", "🔥強気", "🚀初動", "⚠️調整"], 
        default="❄️弱気"
    )
    _mom_label_s = pd.Series(label_mom, index=df.index).astype(str)
    _mom_score_s = pd.Series(score_mom, index=df.index).astype(str)
    # P3-31: 構成要素不足時の部分点は内部参考値。正式出力へ数値を出すと
    # 「情報不足 (92点)」のような自己矛盾になるため、Vol/総合と同じ--点へ統一する。
    df["Algo_Momentum"] = np.where(
        momentum_valid,
        _mom_label_s + " (" + _mom_score_s + "点)",
        "情報不足 (--点)",
    )


    # --- 2. ボラティリティターゲット (0〜100点) ---
    # P1-661: current価格の正本は「現在値」のみ。
    # P1-564/635等で現在値をauthoritative NULLにした銘柄で、writerの異なる旧「終値」を
    # current価格として復活させない。
    price = _safe_get_numeric(df, ["現在値"], 0.0)

    # ATR14_PCT / ATR14% はすでに「株価に対するATRの%」なので、そのまま daily_risk(%) として使う。
    # P1-662: 正式出力の円ATR正本は current overlay の ATR14 のみ。
    # ATR20 は price_history特徴量の一時列でありscreener current writerがないため、
    # ATR14がauthoritative NULLになった銘柄へ旧ATR20を復活させない。
    atr_pct = pd.Series(np.nan, index=df.index, dtype=float)
    for col in ["ATR14_PCT", "ATR14%"]:
        if col in df.columns:
            vals = pd.to_numeric(df[col], errors="coerce")
            atr_pct = atr_pct.combine_first(vals)

    atr_abs = (
        pd.to_numeric(df["ATR14"], errors="coerce")
        if "ATR14" in df.columns
        else pd.Series(np.nan, index=df.index, dtype=float)
    )

    atr_abs_pct = pd.Series(
        np.where((price > 0) & atr_abs.notna(), (atr_abs / price) * 100.0, np.nan),
        index=df.index,
        dtype=float,
    )
    daily_risk_s = atr_pct.combine_first(atr_abs_pct)
    valid_vol = daily_risk_s.notna() & np.isfinite(daily_risk_s) & (daily_risk_s >= 0)

    # 連続的スコアリング: 1日の値幅リスクが2%以下なら100点。そこから8%に向けて徐々に0点へ減点する
    score_vol_raw = pd.Series(np.nan, index=df.index, dtype=float)
    vr = daily_risk_s[valid_vol]
    score_vol_raw.loc[valid_vol] = np.where(
        vr <= 2.0,
        100,
        np.where(vr >= 8.0, 0, 100 - ((vr - 2.0) / 6.0) * 100),
    )
    # ATR欠損は「低リスク100点」にせず、安全側で0点。ただし表示ラベルは「情報不足」。
    score_vol = score_vol_raw.fillna(0).round().clip(0, 100).astype(int)

    cond_vol_low = valid_vol & (score_vol >= 80)
    cond_vol_mid = valid_vol & (score_vol >= 50)
    cond_vol_high = valid_vol & (score_vol >= 20)
    cond_vol_danger = valid_vol & (score_vol < 20)

    label_vol = np.select(
        [cond_vol_low, cond_vol_mid, cond_vol_high, cond_vol_danger],
        ["🟢低ﾘｽｸ", "🟡中ﾘｽｸ", "🟠高ﾘｽｸ", "🔴危険"],
        default="情報不足",
    )
    _vol_label_s = pd.Series(label_vol, index=df.index).astype(str)
    df["Algo_VolTarget"] = np.where(
        valid_vol,
        _vol_label_s + " (" + score_vol.astype(str) + "点)",
        "情報不足 (--点)"
    )


    # --- 3. クオンツ・ファクター (0〜100点) ---
    # P1-34: 欠損を実測0へ変換しない。特に割安度欠損=0%扱いは20点を自動付与していた。
    val_raw = pd.to_numeric(df["割安度"], errors="coerce") if "割安度" in df.columns else pd.Series(np.nan, index=df.index, dtype=float)
    if "営利対時価_pct" in df.columns:
        op_raw = pd.to_numeric(df["営利対時価_pct"], errors="coerce")
    elif "営利対時価" in df.columns:
        op_raw = pd.to_numeric(df["営利対時価"], errors="coerce")
    else:
        op_raw = pd.Series(np.nan, index=df.index, dtype=float)
    roe_raw = pd.to_numeric(df["ROE"], errors="coerce") if "ROE" in df.columns else pd.Series(np.nan, index=df.index, dtype=float)

    v_score = pd.Series(np.clip((val_raw.fillna(0.0) + 20) / 50 * 50, 0, 50), index=df.index).where(val_raw.notna(), 0.0)
    o_score = pd.Series(np.clip(op_raw.fillna(0.0) / 10 * 30, 0, 30), index=df.index).where(op_raw.notna(), 0.0)
    r_score = pd.Series(np.clip(roe_raw.fillna(0.0) / 15 * 20, 0, 20), index=df.index).where(roe_raw.notna(), 0.0)

    factor_info_count = val_raw.notna().astype(int) + op_raw.notna().astype(int) + roe_raw.notna().astype(int)
    score_fac = np.round(np.clip(v_score + o_score + r_score, 0, 100)).astype(int)

    cond_fac_super = score_fac >= 80
    cond_fac_val = score_fac >= 60
    cond_fac_fair = score_fac >= 40
    
    # P1-98: Factor 3要素の一部しか無い行を「割高/割安」と断定しない。
    # 3要素すべて揃った時だけFactor判定を確定し、部分値は参考点として残す。
    factor_complete = factor_info_count == 3
    label_fac = np.select(
        [~factor_complete, cond_fac_super, cond_fac_val, cond_fac_fair],
        ["情報不足", "🌟超割安", "✨割安", "⚖️妥当"],
        default="📉割高"
    )
    _fac_label_s = pd.Series(label_fac, index=df.index).astype(str)
    _fac_score_s = pd.Series(score_fac, index=df.index).astype(str)
    # P3-31: 3要素未完備時の部分点も正式表示しない。
    df["Algo_Factor"] = np.where(
        factor_complete,
        _fac_label_s + " (" + _fac_score_s + "点)",
        "情報不足 (--点)",
    )


    # --- 4. 総合判定 ---
    # P1-78: 「情報不足」のサブAlgoを0点として平均し、総合を🔴売りへ落とさない。
    factor_valid = factor_complete
    component_count = momentum_valid.astype(int) + valid_vol.astype(int) + factor_valid.astype(int)
    component_sum = (
        pd.Series(score_mom, index=df.index, dtype=float).where(momentum_valid, 0.0)
        + pd.Series(score_vol, index=df.index, dtype=float).where(valid_vol, 0.0)
        + pd.Series(score_fac, index=df.index, dtype=float).where(factor_valid, 0.0)
    )
    total_score_f = pd.Series(np.nan, index=df.index, dtype=float)
    has_any_algo = component_count > 0
    total_score_f.loc[has_any_algo] = component_sum.loc[has_any_algo] / component_count.loc[has_any_algo]
    total_score = total_score_f.fillna(0.0).round().astype(int)

    # 売買ラベルは3要素すべて計測できた時だけ確定。欠損があれば参考点のみ表示。
    complete_algo_info = momentum_valid & valid_vol & factor_valid
    cond_buy_strong = complete_algo_info & (total_score >= 80)
    cond_buy = complete_algo_info & (total_score >= 60) & (total_score < 80)
    cond_wait = complete_algo_info & (total_score >= 40) & (total_score < 60)
    cond_sell = complete_algo_info & (total_score < 40)

    label_total = np.select(
        [~complete_algo_info, cond_buy_strong, cond_buy, cond_wait, cond_sell],
        ["⚪情報不足", "🚀強い買い", "🟢買い", "🟡様子見", "🔴売り"],
        default="⚪情報不足"
    )
    _total_label_s = pd.Series(label_total, index=df.index).astype(str)
    _total_score_s = pd.Series(total_score, index=df.index).astype(str)
    # P3-17: formal判定は3要素完備時だけ。未完備時のavailable-component平均は
    # 内部参考計算に留め、HTML/LLMへ「情報不足なのに高得点」という矛盾を出さない。
    df["Algo_総合判定"] = np.where(
        complete_algo_info,
        _total_label_s + " (" + _total_score_s + "点)",
        "⚪情報不足 (--点)"
    )

    return df

def apply_volume_quality_labels(df: pd.DataFrame) -> pd.DataFrame:
    """出来高と値動きの質を判定する（短期需給分析）"""
    if df is None or getattr(df, "empty", True):
        return df

    import numpy as np
    import pandas as pd

    # P1-50: 欠損を「前日比0% / RVOL1倍」という実測値に置換しない。
    def _first_numeric(cols):
        out = pd.Series(np.nan, index=df.index, dtype=float)
        for col in cols:
            if col not in df.columns:
                continue
            ser = pd.to_numeric(
                df[col].astype(str).str.replace(r"[^\d\.\-]", "", regex=True),
                errors="coerce",
            )
            out = out.combine_first(ser)
        return out

    pct_raw = _first_numeric(["前日終値比率_raw", "前日終値比率"])
    # P1-657: current RVOL代金のみを判定入力にする。
    rvol_raw = _first_numeric(["RVOL代金"])
    pct = pct_raw.fillna(0.0)
    rvol = rvol_raw.fillna(0.0)
    valid_quality = pct_raw.notna() & rvol_raw.notna()
    
    # 上げ・下げの判定
    is_up = pct > 0
    is_down = pct < 0
    
    # 出来高の伴い具合 (RVOL 1.5倍以上を「伴う」、1.0未満を「伴わない」とする)
    high_vol = rvol >= 1.5
    low_vol = rvol < 1.0
    
    cond_real_up = is_up & high_vol
    cond_fake_up = is_up & low_vol
    cond_real_down = is_down & high_vol
    cond_fake_down = is_down & low_vol
    
    label = np.select(
        [cond_real_up, cond_fake_up, cond_real_down, cond_fake_down],
        ["🚀本物/吸い上げ(↑)", "🎈値飛び(↑)", "💥強い売り/吸収(↓)", "🍂需給薄(↓)"],
        default="様子見"
    )
    # 入力不足は「様子見」ではなく情報不足として区別する。
    label = np.where(valid_quality, label, "⚪情報不足")
    df["短期需給判定"] = label
    return df


# ---------- 行整形（欠損安全 & 外部列に依存しない） ----------
def _prepare_rows(df: pd.DataFrame, conn: sqlite3.Connection | None = None):
    rows: list[dict] = []

    # P2-41: Yahoo URLは全行を1銘柄ずつresolverへ通さず、DB読込をbulkで1回にまとめる。
    _yahoo_symbol_map = {}
    if conn is not None and df is not None and not df.empty and "コード" in df.columns:
        _link_codes = list(dict.fromkeys(
            canonical_code_for_db(c) for c in df["コード"].tolist() if canonical_code_for_db(c)
        ))
        if _link_codes:
            _link_symbols = resolve_yahoo_symbols_bulk(_link_codes, conn)
            _yahoo_symbol_map = dict(zip(_link_codes, _link_symbols))

    # NaN 判定は pandas.isna を使うと型に強い
    def _clean(val):
        return None if pd.isna(val) else val

    for _, r in df.iterrows():
        d = {k: _clean(r.get(k)) for k in df.columns}

        # コード/銘柄
        if "コード" in d:
            # P1-150: CSVの7203.0や285Aも共通正規化。
            d["コード"] = _normalize_jp_security_code(d.get("コード"))
        if "銘柄名" in d:
            d["銘柄名"] = str(d.get("銘柄名") or "")

        # Yahoo / X
        code4 = _normalize_jp_security_code(d.get("コード"))
        _link_key = canonical_code_for_db(code4) if code4 else ""
        _resolved_symbol = _yahoo_symbol_map.get(_link_key)
        if _resolved_symbol:
            d["yahoo_url"] = f"https://finance.yahoo.co.jp/quote/{_resolved_symbol}"
        else:
            d["yahoo_url"] = _yahoo_quote_url(code4, d.get("市場"), conn) if code4 else ""
        d["x_url"] = f"https://x.com/search?q={quote(d.get('銘柄名') or '')}" if d.get("銘柄名") else ""

        # 売買代金(億) 補完
        # P1-631: current-run正本の「売買代金億」を表示aliasへ同期する。
        # 正本列が存在してNULLなら、旧「売買代金(億)」や価格×出来高から復活させない。
        if "売買代金億" in d:
            d["売買代金(億)"] = _to_float(d.get("売買代金億"))
        elif d.get("売買代金(億)") is None:
            fv = _to_float(d.get("現在値")); fvol = _to_float(d.get("出来高"))
            if fv is not None and fvol is not None:
                d["売買代金(億)"] = round(fv * fvol / 1e8, 2)

        # RVOL代金 補完
        fturn = _to_float(d.get("売買代金(億)"))
        favg20 = _to_float(d.get("売買代金20日平均億"))
        if d.get("RVOL代金") is None and (fturn is not None) and (favg20 and favg20 != 0):
            d["RVOL代金"] = round(fturn / favg20, 2)

        # 前日比 補完
        now_ = _to_float(d.get("現在値")); prev_ = _to_float(d.get("前日終値"))
        if d.get("前日円差") is None and (now_ is not None and prev_ is not None):
            d["前日円差"] = now_ - prev_
        if d.get("前日終値比率") is None and (now_ is not None and prev_ not in (None, 0)):
            d["前日終値比率"] = (now_ / prev_ - 1.0) * 100.0

        # 現在値 raw
        cv = _to_float(d.get("現在値"))
        d["現在値_raw"] = cv if cv is not None else ""

        # 前日終値比率 raw
        # P1-657: current authoritative「前日終値比率」がNULLなら旧legacy％列を復活させない。
        pct_val = d.get("前日終値比率")
        pctf = _to_float(pct_val)
        d["前日終値比率_raw"] = pctf if pctf is not None else ""

        # 表示は％文字列に統一（rawは数値のまま残す）
        if pctf is not None:
            d["前日終値比率"] = f"{round(float(pctf), 2)}%"

        # 付加フラグ群
        d["空売り機関なし_flag"] = _noshor_flag_from_current_snapshot(d)
        d["営利対時価_flag"]     = _op_ratio_flag(d)

        # 判定/理由
        # P1-52: 「当たり」は検証済みシグナルの結果であるべきで、単に当日プラスだけで捏造しない。
        # DB/入力に既存の検証判定がある場合だけ保持し、無ければ空欄。
        existing_judge = d.get("判定")
        d["判定"] = str(existing_judge).strip() if existing_judge not in (None, "") else ""
        d["判定理由"] = _build_reason(d)

        # 推奨ロジック
        rec, ratio_band, ratio_raw = _derive_recommendation(d)
        # P1-109: 現在条件で推奨が消えた/合成Sが情報不足になった時、前回の推奨を残さない。
        # 推奨はlast-known-valueではなく「今回の計算結果」。
        d["推奨アクション"] = rec or ""
        d["推奨比率_raw"] = ratio_raw if ratio_raw is not None else ""
        d["推奨比率"] = "" if ratio_band is None else f"{int(round(float(ratio_band) * 100))}%"

        # 営利対時価_flag（DB列が無い/空なら導出）
        if not (d.get("営利対時価_flag") or "").strip():
            d["営利対時価_flag"] = _derive_opratio_flag(d)

        # 念のため：空売り機関なし_flag を維持
        d["空売り機関なし_flag"] = _noshor_flag_from_current_snapshot(d)
        
        # 三角スコア計算
        g, s, v = _derive_triangle_scores(d)
        d["tri_growth"] = g
        d["tri_safety"] = s
        d["tri_vol"] = v
        _tri_growth_text = "--" if g is None else f"{int(g):02d}"
        _tri_safety_text = "--" if s is None else f"{int(s):02d}"
        _tri_vol_text = "--" if v is None else f"{int(v):02d}"
        d["三角スコア"] = f"▲{_tri_growth_text} ◆{_tri_safety_text} ■{_tri_vol_text}"
        
        # ----------------------------------------------------
        # ★ 【追加】AGCの教訓に基づく「利益加速ステータス」の判定
        # ----------------------------------------------------
        _accel_quality = _profit_acceleration_quality(d)
        if _accel_quality == "quality":
            d["利益加速ステータス"] = "🚀加速"
        elif _accel_quality == "margin_lag":
            # P3-4: 売上成長に利益成長が追いつかない事実だけから「出尽くし」とは断定しない。
            d["利益加速ステータス"] = "⚠️利益率鈍化"
        elif _accel_quality in {"unknown", "unverified"}:
            d["利益加速ステータス"] = "⚪情報不足"
        else:
            d["利益加速ステータス"] = "-"
        # ----------------------------------------------------

        
        d = _ensure_links(d)
        rows.append(d)

    # ここでは `_conn` を閉じない（呼び出し側 or シングルトン管理側の責務）
    return rows

# =================== HTMLテンプレ ===================

# =================== HTMLテンプレートの保存 ===================
def _ensure_template_file(template_dir: str, overwrite=True):
    os.makedirs(template_dir, exist_ok=True)
    path = os.path.join(template_dir, "dashboard.html")
    if overwrite or not os.path.exists(path):
        # P1-396: truncate途中で落ちて壊れたtemplateを残さない。
        _atomic_write_text_file(path, _load_dashboard_template_str())
    return path

# =================== ダッシュボード書き出し（完全版） ===================

def _normalize_jp_security_code(code: object) -> str:
    """
    P1-130: 東証の英数字コード（例: 285A）を壊さず正規化する。
    数値CSV由来の ``7203.0`` だけ整数表記へ戻し、数字コードのみ4桁ゼロ埋め。
    """
    if code is None:
        return ""
    s = str(code).strip().upper()
    if not s or s.lower() in {"nan", "none"}:
        return ""
    # P1-329: HTML/CSV側の共通正規化も .N/.S/.F を同一銘柄として扱う。
    for suf in (".T", ".N", ".S", ".F", "-T", "-N", "-S", "-F", ".JP", "-JP"):
        if s.endswith(suf):
            s = s[:-len(suf)]
            break
    if re.fullmatch(r"[0-9]+\.0+", s):
        s = s.split(".", 1)[0]
    if s.isdigit():
        return s.zfill(4)
    return s

def _load_offering_codes_from_db(conn, days=400):
    """
    offerings_events から直近 days 日の増資/行使/売出/CB/EB などの履歴があるコードを集合で返す。
    テーブル自体が未導入なら空集合。存在するのに読めない場合は希薄化警告を黙って消さずfatal。
    """
    try:
        _exists = conn.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name='offerings_events'"
        ).fetchone()
        if not _exists:
            return set()
        cur = conn.cursor()
        # P1-490: 実提出イベントは未来日を採用しない。壊れた未来行で希薄化警告を誤表示しない。
        cutoff_date = (date.fromisoformat(_today_jst()) - timedelta(days=int(days))).isoformat()
        today_date = _today_jst()
        cur.execute(
            "SELECT DISTINCT コード "
            "FROM offerings_events "
            "WHERE date(提出時刻) >= date(?) AND date(提出時刻) <= date(?)",
            (cutoff_date, today_date)
        )
        # P1-131: CAST AS INTEGERを通さず285A等をそのまま保持。
        return {_normalize_jp_security_code(r[0]) for r in cur.fetchall() if r and _normalize_jp_security_code(r[0])}
    except Exception as _e:
        raise RuntimeError(f"offerings_events read failed; refusing to hide dilution alerts: {_e}") from _e
    finally:
        try:
            cur.close()
        except Exception:
            pass

# ==== [FAST PATH HELPERS - REAL IMPL] ===================================



    



# ==== [/FAST PATH HELPERS - REAL IMPL] ==================================


# ==== [BULK PRICE SUMMARIES - TOP LEVEL] ================================
def preload_price_summaries(conn, codes, window_days=200): # ← 期間を200日に延長
    """対象コードをまとめて読み、直近の終値/高値/安値と MA5/25/75 を返す"""
    if not codes:
        return {}
    # P1-157: 価格サマリのキーも共通正規化。
    codes = [_normalize_jp_security_code(c) for c in codes]
    codes = [c for c in codes if c]
    start_date = (date.fromisoformat(_today_jst()) - timedelta(days=window_days)).isoformat()
    out = {}
    # P1-570: 価格サマリもrun sessionの期待as-ofを基準に個別鮮度を判定する。
    # PREOPENは前営業日、MIDDAY/EODは当日（休場日は直前営業日）。
    _summary_expected_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    # P1-281: legacy候補展開でSQL変数上限を超えにくいサイズへ。
    CH = 300
    
    # 当日のライブ現在値をscreenerから取得（MA計算の末尾に結合するため）
    live_prices = {}
    try:
        cur = conn.cursor()
        cur.execute("SELECT コード, 現在値 FROM screener WHERE 現在値 IS NOT NULL")
        for row in cur.fetchall():
            c4 = _normalize_jp_security_code(row[0])
            val = str(row[1]).replace(",", "")
            try: live_prices[c4] = float(val)
            except ValueError: pass
        cur.close()
    except Exception as e:
        print(f"[bulkprice][WARN] live prices fetch failed: {e}")

    # P2-85: 旧実装は300銘柄ごとのCAST INで同じprice_historyを約12回走査し、
    # HTML価格サマリだけで約46秒を消費していた。対象は実質全screenerなので、
    # 200日窓を1回だけ読み、後段のchunkはメモリ上で分割する。
    try:
        dfp_all = pd.read_sql_query(
            """
            SELECT rowid AS _rowid, コード, 日付, 終値, 高値, 安値
            FROM price_history
            WHERE date(日付) >= date(?) AND date(日付) <= date(?)
            ORDER BY 日付, rowid
            """,
            conn,
            params=[start_date, _summary_expected_asof],
            parse_dates=["日付"],
        )
    except Exception as _e:
        print(f"[bulkprice][ERROR] single-pass history read failed: {_e}")
        raise RuntimeError("bulk price summary single-pass read failed") from _e
    if dfp_all.empty:
        return out
    dfp_all["_summary_key"] = dfp_all["コード"].map(_normalize_jp_security_code)

    for i in range(0, len(codes), CH):
        part = codes[i:i+CH]
        part_set = set(part)
        dfp = dfp_all[dfp_all["_summary_key"].isin(part_set)].drop(
            columns=["_summary_key"], errors="ignore"
        ).copy()
        if dfp.empty:
            continue
        for c in ("終値","高値","安値"):
            if c in dfp.columns:
                dfp[c] = pd.to_numeric(dfp[c], errors="coerce")
        dfp = _dedupe_price_history_df(dfp)
        for code, g in dfp.groupby("コード", sort=False):
            c4 = _normalize_jp_security_code(code)
            g = g.sort_values("日付")
            
            # ▼【修正：ここに追加】ライブ価格追加前の「DB上の確実な直近2営業日」の終値を確保
            db_last = g.iloc[-1] if len(g) > 0 else None
            db_prev = g.iloc[-2] if len(g) > 1 else None
            db_last_close = float(db_last["終値"]) if db_last is not None and pd.notna(db_last["終値"]) else None
            db_prev_close = float(db_prev["終値"]) if db_prev is not None and pd.notna(db_prev["終値"]) else None
            try:
                db_last_date = str(pd.Timestamp(db_last["日付"]).date()) if db_last is not None and pd.notna(db_last["日付"]) else None
            except Exception:
                db_last_date = None
            # P1-663: 日付だけ当日でも終値NULLならcurrent price summaryとして扱わない。
            # P1-635のscreener価格fresh条件と揃え、MA/高安だけをcurrent表示へ残さない。
            _summary_fresh = bool(
                db_last_date
                and db_last_date == _summary_expected_asof
                and db_last_close is not None
                and np.isfinite(db_last_close)
                and db_last_close > 0
            )
            
            # ▼当日の現在値をMA計算の末尾に連結
            today_ts = pd.Timestamp(_today_jst())
            live_px = live_prices.get(c4)
            # P1-231: screener.現在値をMAへ混ぜるのは実際の場中だけ。
            # P1-158は休日/9時前だけを防いだため、引け後に古いscreener値で確定終値を上書きできた。
            now_jst = _now_jst()
            try:
                live_override_allowed = _is_trading_session_now(now_jst)
            except Exception:
                live_override_allowed = (now_jst.weekday() < 5) and (dt_time(9,0) <= now_jst.time() < dt_time(15,30))  # P1-382
            if live_px is not None and live_override_allowed:
                if not g.empty and g["日付"].iloc[-1].date() == today_ts.date():
                    g.loc[g.index[-1], "終値"] = live_px
                else:
                    new_row = pd.DataFrame({"コード": [c4], "日付": [today_ts], "終値": [live_px], "高値": [np.nan], "安値": [np.nan]})
                    g = pd.concat([g, new_row], ignore_index=True)
            
            # P1-676: MAの窓数も有効な正の終値だけで数える。
            close = pd.to_numeric(g["終値"], errors="coerce")
            close = close[close.notna() & np.isfinite(close) & (close > 0)]
            
            # min_periods を期間と一致させ、データ不足時の異常値を防ぐ
            ma5  = close.rolling(5,  min_periods=5).mean().iloc[-1] if len(close) >= 5 else None
            ma25 = close.rolling(25, min_periods=25).mean().iloc[-1] if len(close) >= 25 else None
            ma75 = close.rolling(75, min_periods=75).mean().iloc[-1] if len(close) >= 75 else None
            
            last = g.iloc[-1] if len(g) > 0 else None
            prev = g.iloc[-2] if len(g) > 1 else None
            out[c4] = {
                "last_date": None if last is None else str(last["日付"].date()),
                "last_close": None if last is None or pd.isna(last["終値"]) else float(last["終値"]),
                "last_high":  None if last is None or pd.isna(last["高値"]) else float(last["高値"]),
                "last_low":   None if last is None or pd.isna(last["安値"]) else float(last["安値"]),
                "prev_date":  None if prev is None else str(prev["日付"].date()),
                "prev_close": None if prev is None or pd.isna(prev["終値"]) else float(prev["終値"]),
                "ma5":  None if pd.isna(ma5)  else float(ma5),
                "ma25": None if pd.isna(ma25) else float(ma25),
                "ma75": None if pd.isna(ma75) else float(ma75),
                # ▼【修正：ここに追加】
                "db_last_close": db_last_close,
                "db_prev_close": db_prev_close,
                # P1-570: stale個別系列を表示current/MAへ復活させないための鮮度フラグ。
                "fresh": _summary_fresh,
                "expected_asof": _summary_expected_asof,
            }
    return out

def enrich_rows_with_price_summary(rows, summary_map):
    if not rows:
        return rows

    # --- 【修正：ここに追加】価格ガード（時間帯判定） ---
    do_guard = False
    if globals().get("PRICE_GUARD_ENABLED", False):
        try:
            from zoneinfo import ZoneInfo
            JST = ZoneInfo("Asia/Tokyo")
            now_jst = datetime.now(JST)
        except Exception:
            now_jst = _now_jst()
        
        # すでに定義済みの「場中判定関数」を呼び出し
        try:
            is_session = _is_trading_session_now(now_jst)
        except NameError:
            # 万が一関数が見えない場合のフォールバック（土日、または9:00前・15:30以降）
            try:
                import jpholiday
                is_biz = (now_jst.weekday() < 5) and (not jpholiday.is_holiday(now_jst.date()))
            except:
                is_biz = (now_jst.weekday() < 5)
            
            t = now_jst.time()
            is_session = is_biz and ((dt_time(9,0) <= t <= dt_time(11,30)) or (dt_time(12,30) <= t < dt_time(15,30)))  # P1-382

        # 場中（9:00-11:30, 12:30-15:30）以外ならガード発動
        if not is_session:
            do_guard = True
            print("[price-guard] 場外時間のため、表示価格を確定した履歴データ(EOD)で上書き保護します")
    # ------------------------------------------------

    for r in rows:
        # P1-543: summaryが今回作れなかった銘柄に、_prepare_rows由来の前回MA/高安を残さない。
        # current summaryが無いものは「不明」として空欄にし、成功銘柄だけ下で再付与する。
        for _k in ("高値", "安値", "MA5", "5日", "MA25", "25日", "MA75", "75日"):
            r[_k] = ""

        # P1-181: summary_map照合もcanonical code。
        c4 = canonical_code_for_db(r.get("コード"))
        _s_raw = summary_map.get(c4) or {}
        # P1-570: 個別最終価格日が今回sessionの期待as-ofに届いていないsummaryは、
        # 場外price guardで前営業日値をcurrentへ復活させず、MA/高安もcurrent summaryとして表示しない。
        s = _s_raw if bool(_s_raw.get("fresh")) else {}
        
        # --- 【修正：ここに追加】価格ガードの上書き適用 ---
        if do_guard and s:
            db_c = s.get("db_last_close")
            db_p = s.get("db_prev_close")
            
            if db_c is not None:
                r["現在値_raw"] = db_c
                # 小数点以下が0なら整数表記、それ以外は2桁
                r["現在値"] = f"{db_c:,.0f}" if abs(db_c - round(db_c)) < 1e-9 else f"{db_c:,.2f}"
                
                if db_p is not None and db_p != 0:
                    r["前日終値"] = f"{db_p:,.0f}" if abs(db_p - round(db_p)) < 1e-9 else f"{db_p:,.2f}"
                    diff = db_c - db_p
                    pct = (db_c / db_p - 1.0) * 100.0
                    r["前日円差"] = f"{diff:,.0f}" if abs(diff - round(diff)) < 1e-9 else f"{diff:,.2f}"
                    r["前日終値比率_raw"] = pct
                    r["前日終値比率"] = f"{pct:.2f}%"
                else:
                    r["前日終値"] = ""
                    r["前日円差"] = ""
                    r["前日終値比率_raw"] = None
                    r["前日終値比率"] = ""
        # ------------------------------------------------

        if s:
            r["高値"]  = "" if s.get("last_high")  is None else f"{s['last_high']:,.0f}"
            r["安値"]  = "" if s.get("last_low")   is None else f"{s['last_low']:,.0f}"
            r["MA5"]   = "" if s.get("ma5")        is None else f"{s['ma5']:,.0f}"
            r["5日"]   = r.get("MA5","" )
            r["MA25"]  = "" if s.get("ma25")       is None else f"{s['ma25']:,.0f}"
            r["25日"]  = r.get("MA25","" )
            r["MA75"]  = "" if s.get("ma75")       is None else f"{s['ma75']:,.0f}"
            r["75日"]  = r.get("MA75","" )
    return rows
# ==== [/BULK PRICE SUMMARIES - TOP LEVEL] ===============================

def _earnings_reaction_metrics(rets, min_score_obs: int = 3):
    """P3-15: 過去D1反応のraw統計と、最低観測数を満たすformal scoreを返す。"""
    clean = []
    for v in rets or []:
        try:
            x = float(v)
            if math.isfinite(x):
                clean.append(x)
        except Exception:
            continue
    if not clean:
        return None
    ups = [v for v in clean if v > 0]
    downs = [v for v in clean if v <= 0]
    win_prob = len(ups) / len(clean)
    lose_prob = len(downs) / len(clean)
    avg_up = sum(ups) / len(ups) if ups else 0.0
    avg_down = sum(downs) / len(downs) if downs else 0.0
    expected_value = (win_prob * avg_up) + (lose_prob * avg_down)
    score = None
    if len(clean) >= max(1, int(min_score_obs)):
        score = max(0.0, min(100.0, (expected_value + 2.0) / 7.0 * 100.0))
    return {
        "n": len(clean),
        "win_rate_pct": win_prob * 100.0,
        "expected_value": expected_value,
        "score": score,
        "spark": ",".join(f"{v:.1f}" for v in clean),
    }


def enrich_earnings_reaction(conn: sqlite3.Connection, df_cand: pd.DataFrame) -> pd.DataFrame:
    """
    過去8回の決算翌日リアクションを付与。

    優先:
      earnings_reaction_labels（fetch_allで作った実反応ラベル）
    フォールバック:
      旧 finance_notes.past_earnings_dates + price_history
    """
    if df_cand is None or df_cand.empty or "コード" not in df_cand.columns:
        return df_cand

    df_cand = df_cand.copy()
    # P1-210: earnings reaction主経路も共通canonical codeへ。
    df_cand["コード"] = df_cand["コード"].map(canonical_code_for_db)
    # P3-19: live/export snapshotより未来のeventはDBに混入していても使用しない。
    _reaction_asof = pd.Timestamp(_expected_snapshot_date_for_run(_auto_run_mode())).normalize()

    reaction_cols = ["決算勝率", "決算期待値", "過去決算D1期待値", "決算リアクション件数", "決算リアクションスコア", "決算リアクション履歴"]
    # P1-539: screenerから読み込んだ前回リアクション値を今回計算失敗時に再表示しない。
    # DBのlast-good値は保持し、export用DataFrameだけ先に「未計算」へ戻す。
    for _rc in reaction_cols:
        df_cand[_rc] = "" if _rc == "決算リアクション履歴" else np.nan

    try:
        tables = {
            r[0] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }

        # ============================================================
        # 新ルート: earnings_reaction_labels を直接利用
        # ============================================================
        if "earnings_reaction_labels" in tables:
            target_codes = sorted(set(df_cand["コード"].dropna().astype(str)))
            if target_codes:
                query_codes = expand_code_query_variants(target_codes)
                qmarks = ",".join(["?"] * len(query_codes))
                # P1-626: alias重複の正本を引け後/D1有効性より先に確定する。
                # UPSERT更新ではrowidが進まないためupdated_atを主、canonical rawをtie-breakに使う。
                _erl_cols = {r[1] for r in conn.execute("PRAGMA table_info(earnings_reaction_labels)").fetchall()}
                _erl_updated_expr = "updated_at" if "updated_at" in _erl_cols else "NULL AS updated_at"
                rdf = pd.read_sql_query(
                    f"""
                    SELECT rowid AS _rowid, コード, 発表日時, 引け後, D1終値騰落率,
                           {_erl_updated_expr}
                    FROM earnings_reaction_labels
                    WHERE CAST(コード AS TEXT) IN ({qmarks})
                    ORDER BY 発表日時
                    """,
                    conn,
                    params=query_codes,
                )
            else:
                rdf = pd.DataFrame()

            if not rdf.empty:
                rdf["_raw_code"] = rdf["コード"].astype(str).str.strip()
                rdf["コード"] = rdf["コード"].map(canonical_code_for_db)
                # P1-650: updated_at/発表日時ともJST-naiveへ統一。
                # 同一瞬間の「space区切りnaive」と「T+09:00」を別イベントにしない。
                rdf["_updated_sort"] = rdf.get("updated_at").map(_p1_608_jst_naive_ts)
                rdf["_event_sort"] = rdf["発表日時"].map(_p1_608_jst_naive_ts)
                # P3-19: future-dated eventをtail(8)へ混ぜない。parse不能は後続keyで保持するが、
                # formal reaction履歴には時点が検証できるeventだけを採用する。
                rdf = rdf[rdf["_event_sort"].notna() & (rdf["_event_sort"] <= _reaction_asof)].copy()
                rdf["_event_key"] = rdf["_event_sort"].map(
                    lambda _v: _v.isoformat() if pd.notna(_v) else ""
                )
                _event_key_missing = rdf["_event_key"].eq("")
                rdf.loc[_event_key_missing, "_event_key"] = rdf.loc[_event_key_missing, "発表日時"].fillna("").astype(str).str.strip()
                rdf["_canon_match"] = (
                    rdf["_raw_code"].str.upper() == rdf["コード"].astype(str).str.upper()
                ).astype(int)
                # P1-626: 同一logical code/発表日時はcurrent producer更新時刻→canonical表記→rowidで正本化。
                rdf = (rdf.sort_values(
                            ["コード", "_event_sort", "_event_key", "_updated_sort", "_canon_match", "_rowid"],
                            kind="stable", na_position="first")
                          .drop_duplicates(subset=["コード","_event_key"], keep="last"))
                rdf["引け後"] = pd.to_numeric(rdf["引け後"], errors="coerce")
                rdf["D1終値騰落率"] = pd.to_numeric(
                    rdf["D1終値騰落率"], errors="coerce"
                )
                rdf = rdf[(rdf["引け後"] == 1) & rdf["D1終値騰落率"].notna()].copy()
                rdf = rdf.drop(columns=["_raw_code", "_updated_sort", "_event_sort", "_event_key", "_canon_match", "updated_at"], errors="ignore")

                reaction_records = []
                for code, g in rdf.groupby("コード", sort=False):
                    g = g.tail(8)
                    rets = g["D1終値騰落率"].astype(float).tolist()
                    if not rets:
                        continue

                    _rm = _earnings_reaction_metrics(rets)
                    if _rm is None:
                        continue
                    reaction_records.append({
                        "コード": code,
                        "決算勝率": _rm["win_rate_pct"],
                        "決算期待値": _rm["expected_value"],
                        "過去決算D1期待値": _rm["expected_value"],
                        "決算リアクション件数": _rm["n"],
                        "決算リアクションスコア": (np.nan if _rm["score"] is None else _rm["score"]),
                        "決算リアクション履歴": _rm["spark"],
                    })

                if reaction_records:
                    react_df = pd.DataFrame(reaction_records)
                    df_cand = df_cand.drop(
                        columns=[c for c in reaction_cols if c in df_cand.columns],
                        errors="ignore"
                    )
                    df_cand = df_cand.merge(react_df, on="コード", how="left")

                    # 欠損は「0%」ではなく本当に欠損のまま。
                    # UI側で "-" 表示させる。
                    if "決算リアクション履歴" in df_cand.columns:
                        df_cand["決算リアクション履歴"] = df_cand["決算リアクション履歴"].fillna("")
                    for c in ["決算勝率", "決算期待値", "過去決算D1期待値", "決算リアクション件数", "決算リアクションスコア"]:
                        if c in df_cand.columns:
                            df_cand[c] = pd.to_numeric(df_cand[c], errors="coerce")

                    print(
                        f"[REACTION] earnings_reaction_labels直結: "
                        f"反応計算={len(reaction_records)}銘柄 / 元行={len(rdf)}"
                    )
                    return df_cand

        # ============================================================
        # 旧ルート fallback
        # ============================================================
        import json

        cur = conn.cursor()
        fn_cols = [r[1] for r in cur.execute("PRAGMA table_info(finance_notes)")]
        cur.close()

        if "past_earnings_dates" not in fn_cols:
            print("[REACTION][WARN] reaction_labelsなし / finance_notes.past_earnings_datesなし")
            return df_cand

        fn_df = pd.read_sql_query(
            "SELECT rowid AS _rowid, コード, updated_at, past_earnings_dates FROM finance_notes",
            conn,
        )
        if fn_df.empty:
            print("[REACTION][WARN] finance_notes が空です")
            return df_cand

        # P1-602: alias行の最新はrowidではなく実更新時刻で決める。
        fn_df = _latest_finance_notes_by_canonical(fn_df, "_key")
        dates_map = {}
        for _, rr in fn_df.iterrows():
            c = rr["_key"]
            raw = rr.get("past_earnings_dates")
            if raw is None or str(raw).strip() in ("", "[]", "null", "None"):
                continue
            try:
                dates = json.loads(raw) if isinstance(raw, str) else raw
                if isinstance(dates, list) and dates:
                    norm_dates = []
                    for v in dates:
                        dt = pd.to_datetime(v, errors="coerce")
                        if pd.notna(dt):
                            _dt_norm = pd.Timestamp(dt).normalize()
                            if _dt_norm <= _reaction_asof:
                                norm_dates.append(_dt_norm.date().isoformat())
                    norm_dates = sorted(set(norm_dates))[-8:]
                    if norm_dates:
                        dates_map[c] = norm_dates
            except Exception:
                pass

        if not dates_map:
            print("[REACTION][WARN] fallback側も過去決算日なし")
            return df_cand

        target_codes = tuple(dates_map.keys())
        query_codes = expand_code_query_variants(target_codes)
        qmarks = ",".join(["?"] * len(query_codes))
        ph_df = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, 日付, 終値 FROM price_history "
            f"WHERE CAST(コード AS TEXT) IN ({qmarks}) ORDER BY 日付, rowid",
            conn,
            params=query_codes,
        )
        ph_df = _dedupe_price_history_df(ph_df)
        if ph_df.empty:
            return df_cand

        ph_df["コード"] = ph_df["コード"].map(canonical_code_for_db)
        ph_df["日付"] = pd.to_datetime(ph_df["日付"], errors="coerce")
        ph_df["終値"] = pd.to_numeric(ph_df["終値"], errors="coerce")
        ph_df = ph_df.dropna(subset=["日付", "終値"])

        reaction_records = []
        for code, group in ph_df.groupby("コード", sort=False):
            dates = dates_map.get(code)
            if not dates:
                continue
            group = group.sort_values("日付").reset_index(drop=True)
            rets = []
            for d_str in dates[-8:]:
                d = pd.to_datetime(d_str, errors="coerce")
                if pd.isna(d):
                    continue
                idx_list = group.index[group["日付"] <= d].tolist()
                if not idx_list:
                    continue
                idx = idx_list[-1]
                if idx + 1 >= len(group):
                    continue
                prev_close = group.loc[idx, "終値"]
                next_close = group.loc[idx + 1, "終値"]
                if pd.notna(prev_close) and prev_close > 0 and pd.notna(next_close):
                    rets.append((next_close - prev_close) / prev_close * 100.0)

            rets = rets[-8:]
            if not rets:
                continue

            _rm = _earnings_reaction_metrics(rets)
            if _rm is None:
                continue
            reaction_records.append({
                "コード": code,
                "決算勝率": _rm["win_rate_pct"],
                "決算期待値": _rm["expected_value"],
                "過去決算D1期待値": _rm["expected_value"],
                "決算リアクション件数": _rm["n"],
                "決算リアクションスコア": (np.nan if _rm["score"] is None else _rm["score"]),
                "決算リアクション履歴": _rm["spark"],
            })

        if reaction_records:
            react_df = pd.DataFrame(reaction_records)
            df_cand = df_cand.drop(
                columns=[c for c in reaction_cols if c in df_cand.columns],
                errors="ignore"
            )
            df_cand = df_cand.merge(react_df, on="コード", how="left")
            if "決算リアクション履歴" in df_cand.columns:
                df_cand["決算リアクション履歴"] = df_cand["決算リアクション履歴"].fillna("")
            for c in ["決算勝率", "決算期待値", "過去決算D1期待値", "決算リアクション件数", "決算リアクションスコア"]:
                if c in df_cand.columns:
                    df_cand[c] = pd.to_numeric(df_cand[c], errors="coerce")
            print(f"[REACTION] fallback復元OK: {len(reaction_records)}銘柄")

    except Exception as e:
        print(f"[REACTION] 決算リアクションの計算エラー: {e}")

    return df_cand


# P2-57: 未使用だった旧kabunewsメモリcacheは削除。
# --- [INJECTED] kabunews 用の簡易ログ関数 ---
def _log(msg):
    """kabunews 周り専用の軽量ロガー"""
    try:
        # コンソールにそのまま出すだけ（フラッシュ付き）
        print(msg, flush=True)
    except Exception:
        # 最悪 print すら失敗しても無視
        pass
# --- [/INJECTED] ---





# === Bulk OR query support for Kabutan via Google News RSS ===
def _kabunews_make_multi_url(pairs):
    """
    pairs: list of tuples (code, name)
    Returns Google News RSS URL that ORs multiple symbols: site:kabutan.jp ((3350 OR "メタプラネット") OR (3905 OR "データセクション") ...)
    """
    parts = []
    for code, name in pairs:
        c = (str(code) or '').strip()
        n = (str(name) or '').strip().replace('"', '')  # avoid breaking quotes
        if not c and not n:
            continue
        if c and n:
            parts.append(f'({c} OR "{n}")')
        elif c:
            parts.append(f'({c})')
        else:
            parts.append(f'("{n}")')
    if not parts:
        parts = ['']
    q = f'site:kabutan.jp ' + '(' + ' OR '.join(parts) + ')'
    qs = urllib.parse.urlencode({
        "q": q,
        "hl": _KABUNEWS_CONF["lang"],
        "gl": _KABUNEWS_CONF["gl"],
        "ceid": _KABUNEWS_CONF["ceid"]
    })
    return f'{_KABUNEWS_CONF["rss_base"]}?{qs}'


_KABUNEWS_CACHE_LOCK = threading.Lock()
_KABUNEWS_CACHE_MEMORY = None


def _kabunews_normalize_cached_items(items):
    """JSON cacheからニュースtupleを安全に復元する。"""
    out = []
    if not isinstance(items, list):
        return out
    for item in items:
        if not isinstance(item, (list, tuple)) or len(item) < 3:
            continue
        out.append((str(item[0] or ""), str(item[1] or ""), str(item[2] or "")))
    return out


def _kabunews_load_cache():
    """P2-92: プロセス内では1回だけdisk cacheを読み、壊れたcacheは使わない。"""
    global _KABUNEWS_CACHE_MEMORY
    with _KABUNEWS_CACHE_LOCK:
        if isinstance(_KABUNEWS_CACHE_MEMORY, dict):
            return _KABUNEWS_CACHE_MEMORY
        expected_schema = int(_KABUNEWS_CONF.get("cache_schema_version", 1))
        cache = {"schema": expected_schema, "entries": {}}
        try:
            if KABUNEWS_CACHE_PATH.exists():
                raw = json.loads(KABUNEWS_CACHE_PATH.read_text(encoding="utf-8"))
                if (
                    isinstance(raw, dict)
                    and int(raw.get("schema", -1)) == expected_schema
                    and isinstance(raw.get("entries"), dict)
                ):
                    cache = raw
                else:
                    print("[kabunews][cache][WARN] schema/format mismatch; cache ignored")
        except Exception as e:
            print(f"[kabunews][cache][WARN] load failed; cache ignored: {e}")
        _KABUNEWS_CACHE_MEMORY = cache
        return cache


def _kabunews_cache_fresh_items(cache, code, name, now_epoch, ttl_seconds):
    entry = (cache.get("entries") or {}).get(code)
    if not isinstance(entry, dict):
        return None
    try:
        fetched_at = float(entry.get("fetched_at"))
    except Exception:
        return None
    age = float(now_epoch) - fetched_at
    # 壁時計の大幅な未来飛びもfreshとみなさない。
    if age < -300.0 or age > float(ttl_seconds):
        return None
    cached_name = str(entry.get("name") or "").strip()
    current_name = str(name or "").strip()
    if cached_name and current_name and cached_name != current_name:
        return None
    return _kabunews_normalize_cached_items(entry.get("items"))


def _kabunews_store_success_cache(cache, pairs, result_map, successful_codes, fetched_at):
    """HTTP 200 + XML parse成功の銘柄だけ更新。空結果も「取得成功・該当なし」として保存する。"""
    global _KABUNEWS_CACHE_MEMORY
    if not successful_codes:
        return
    name_map = {c: n for c, n in pairs}
    expected_schema = int(_KABUNEWS_CONF.get("cache_schema_version", 1))
    try:
        with _KABUNEWS_CACHE_LOCK:
            entries = cache.setdefault("entries", {})
            for code in successful_codes:
                entries[code] = {
                    "name": str(name_map.get(code) or "").strip(),
                    "fetched_at": float(fetched_at),
                    "items": [list(x) for x in (result_map.get(code) or [])],
                }
            # 長期運用で廃止銘柄がcacheに無制限残留しないよう、7日超を整理する。
            cutoff = float(fetched_at) - 7 * 86400.0
            stale_keys = []
            for code, entry in entries.items():
                try:
                    if float(entry.get("fetched_at")) < cutoff:
                        stale_keys.append(code)
                except Exception:
                    stale_keys.append(code)
            for code in stale_keys:
                entries.pop(code, None)
            cache["schema"] = expected_schema
            KABUNEWS_CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
            _atomic_write_text_file(
                KABUNEWS_CACHE_PATH,
                json.dumps(cache, ensure_ascii=False, separators=(",", ":")),
            )
            _KABUNEWS_CACHE_MEMORY = cache
    except Exception as e:
        # cacheは速度補助。保存失敗で取得済みニュースまで失わない。
        print(f"[kabunews][cache][WARN] save failed: {e}")

def kabutan_news_fetch_bulk(pairs, per_symbol=None):
    """
    Fetch Kabutan news for multiple symbols with a single (or few) Google News RSS requests using OR.
    pairs: list of (code, name)
    per_symbol: override max items per symbol (default from conf)
    Returns: dict {code_str4: [ (title, link, pubDate), ... ]}
    """

    per_symbol = per_symbol or int(_KABUNEWS_CONF.get("max_items_per_symbol", 3))
    # P1-155/P2-92: ニュース経路も7203.0/285Aを共通正規化し、
    # alias/重複銘柄でRSS queryとcacheを二重化しない。
    normalized_pairs = []
    seen_codes = set()
    for raw_code, raw_name in pairs or []:
        code = _normalize_jp_security_code(raw_code)
        if not code or code in seen_codes:
            continue
        seen_codes.add(code)
        normalized_pairs.append((code, str(raw_name or "").strip()))
    pairs = normalized_pairs
    out = {code: [] for code, _ in pairs}
    if not pairs:
        return out

    # P2-92: 同一候補の10分ごと再取得を避ける。新規候補はcache missで即時取得される。
    cache = _kabunews_load_cache()
    now_epoch = time.time()
    ttl_seconds = max(0.0, float(_KABUNEWS_CONF.get("cache_ttl_minutes", 30))) * 60.0
    fetch_pairs = []
    cache_hits = 0
    for code, name in pairs:
        cached_items = _kabunews_cache_fresh_items(cache, code, name, now_epoch, ttl_seconds)
        if cached_items is None:
            fetch_pairs.append((code, name))
        else:
            out[code] = cached_items[:per_symbol]
            cache_hits += 1

    # P2-86/P2-89/P2-90: 逐次RSS取得は初回54秒＋AI差分124秒を占めた。
    # P2-89で32銘柄queryを試した本番runは記事総数・ニュース有銘柄が約半減したため、
    # 検索coverageを優先して8銘柄queryへ戻す。実URLが上限を超えた時だけさらに半減し、
    # 有界6worker・429/5xxの1回retryで速度を得る。無制限並列化はしない。
    chunks = []
    i = 0
    _max_pairs = max(1, int(_KABUNEWS_CONF.get("max_pairs_per_query", 8)))
    _max_url = max(512, int(_KABUNEWS_CONF.get("max_url_length", 1800)))
    while i < len(fetch_pairs):
        width = min(_max_pairs, len(fetch_pairs) - i)
        chunk = fetch_pairs[i:i + width]
        url = _kabunews_make_multi_url(chunk)
        while len(url) > _max_url and width > 1:
            width = max(1, width // 2)
            chunk = fetch_pairs[i:i + width]
            url = _kabunews_make_multi_url(chunk)
        chunks.append((chunk, url))
        i += width

    def _fetch_one(item):
        chunk, url = item
        for attempt in range(2):
            try:
                resp = requests.get(
                    url,
                    headers={"User-Agent": _KABUNEWS_CONF["user_agent"]},
                    timeout=_KABUNEWS_CONF["http_timeout"],
                )
                if resp.status_code == 200:
                    root = ET.fromstring(resp.content)
                    return chunk, root.findall(".//item"), True
                if resp.status_code == 429 or 500 <= resp.status_code < 600:
                    if attempt == 0:
                        time.sleep(0.4)
                        continue
                _log(f'[kabunews][bulk][WARN] status={resp.status_code} url={url[:160]}')
                return chunk, [], False
            except Exception as e:
                if attempt == 0:
                    time.sleep(0.2)
                    continue
                _log(f'[kabunews][bulk][ERR] {e!r}')
                return chunk, [], False
        return chunk, [], False

    workers = max(1, min(int(_KABUNEWS_CONF.get("max_workers", 3)), len(chunks)))
    if chunks:
        with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="kabunews") as pool:
            fetched = list(pool.map(_fetch_one, chunks))
    else:
        fetched = []

    # mapは入力chunk順を保つため、逐次版と同じ決定的な優先順位でmergeできる。
    successful_codes = set()
    for chunk, items, fetch_ok in fetched:
        ck = [
            (_normalize_jp_security_code(c), (str(n) or '').strip())
            for c, n in chunk
        ]
        if fetch_ok:
            successful_codes.update(c4 for c4, _ in ck if c4)
        for it in items:
            title = (it.findtext("title") or "").strip()
            link = (it.findtext("link") or "").strip()
            pub = (
                it.findtext("{http://purl.org/dc/elements/1.1/}date")
                or it.findtext("pubDate")
                or ""
            ).strip()
            for c4, nm in ck:
                if len(out.get(c4, [])) >= per_symbol:
                    continue
                if (c4 in title) or (c4 in link) or (nm and nm in title):
                    out.setdefault(c4, []).append((title, link, pub))

    def _parse_pub_ts(s: str):
        """P1-156: aware/naive datetimeを混在比較せずUTC timestampへ統一。"""
        s = (s or "").strip()
        if not s:
            return None
        try:
            dt = _pdt(s)
        except Exception:
            try:
                dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            except Exception:
                return None
        try:
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt.timestamp()
        except Exception:
            return None

    for c4 in list(out.keys()):
        seen = set()
        deduped = []
        for title, link, pub in out[c4]:
            key = (title, link)
            if key in seen:
                continue
            seen.add(key)
            deduped.append((title, link, pub))
        deduped.sort(
            key=lambda tpl: (
                _parse_pub_ts(tpl[2])
                if _parse_pub_ts(tpl[2]) is not None
                else float("-inf")
            ),
            reverse=True,
        )
        out[c4] = deduped[:per_symbol]

    _kabunews_store_success_cache(cache, pairs, out, successful_codes, now_epoch)
    failed_codes = max(0, len(fetch_pairs) - len(successful_codes))
    _log(
        f"[kabunews][cache] hit={cache_hits} miss={len(fetch_pairs)} "
        f"fetched={len(successful_codes)} failed={failed_codes} ttl_min={ttl_seconds/60.0:g}"
    )
    return out

def kabutan_news_lines_bulk_for_dataframe(df, code_col="コード", name_col="銘柄名", per_symbol=None, return_fulltext=False):
    """
    df(コード・銘柄名入り) → 株探ニュース(3) 用の表示Seriesを返す。
    P1-9: return_fulltext=True の場合は (表示Series, 判定用全文Series) を返す。
    P1-11: 判定用全文Seriesは「YYYY-MM-DD\tタイトル」を改行区切りで保持し、
    材料の鮮度減衰に公開日を使えるようにする。表示は従来どおり24文字へ短縮する。
    """
    if df is None or getattr(df, "empty", True):
        empty = pd.Series([], dtype=object)
        return (empty, empty.copy()) if return_fulltext else empty

    per_symbol = per_symbol or int(_KABUNEWS_CONF.get("max_items_per_symbol", 2))
    pairs = []
    idx_map = []  # (index, code4)
    for idx, row in df[[code_col, name_col]].iterrows():
        c = str(row.get(code_col) or '')
        n = str(row.get(name_col) or '')
        c4 = _normalize_jp_security_code(c) if c else ''
        pairs.append((c4, n))
        idx_map.append((idx, c4))

    # OR一括でタイトル・リンク・日付を取る
    result_map = kabutan_news_fetch_bulk(pairs, per_symbol=per_symbol)


    # --- ポジティブ単語辞書で判定（軽量版） ---
    # bert_labels[code4][j] = "POS_DICT" のように、青●を付けたい位置だけマーキング
    bert_labels = {}

    # 設定から単語リストを取得（なければ空リスト）
    pos_words = tuple(_KABUNEWS_CONF.get("pos_keywords", []))

    if pos_words:
        pos_cnt = 0
        total = 0
        # P1-10: 材料語の文脈判定器は一括処理中に1回だけ生成する。
        material_detector = StockSurprisePredictor()
        material_words = {"自社株買い", "増配", "配当増額"}

        for code4, items in result_map.items():
            for j, (title, _, _pub) in enumerate(items or []):
                title = (title or "").strip()
                if not title:
                    continue
                total += 1

                # P1-10: 「自社株買い終了」「増配見送り」を青●にしない。
                # 材料系3語は文脈判定へ回し、それ以外の辞書語だけ従来どおりsubstring判定する。
                other_pos_hit = any((w not in material_words) and (w in title) for w in pos_words)
                material_flags = material_detector._extract_news_flags(title)
                material_hit = bool(material_flags.get("share_buyback_flag") or material_flags.get("dividend_up_flag"))
                if other_pos_hit or material_hit:
                    bert_labels.setdefault(code4, {})[j] = "POS_DICT"
                    pos_cnt += 1

        _log(
            f"[kabunews][pos-dict] total={total} "
            f"marked={pos_cnt} keywords={len(pos_words)}"
        )
    else:
        _log("[kabunews][pos-dict] no keywords configured; skip marking")



    # P1-11: 公開日を正規化。旧版は RFC 日付の strftime 呼び出しが誤っており、
    # RSS pubDate が表示/判定から落ちる経路があった。
    def _pub_date(pub: str):
        if not pub:
            return None
        s = str(pub).strip()
        try:
            dt = _pdt(s)
            if dt is not None:
                return dt.date()
        except Exception:
            pass
        try:
            dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
            return dt.date()
        except Exception:
            pass
        if len(s) >= 10 and s[0:4].isdigit() and s[4] in "-/" and s[7] in "-/":
            try:
                return datetime.strptime(s[:10].replace("/", "-"), "%Y-%m-%d").date()
            except Exception:
                pass
        return None

    def _fmt_pub(pub: str) -> str:
        d = _pub_date(pub)
        return d.strftime("%y/%m/%d") if d is not None else ""

    # 1銘柄分を HTML 1本に整形
    def fmt(code4, items):
        out = []
        for j, (title, link, pub) in enumerate(items or []):
            title = (title or "").strip()
            link  = (link or "").strip()
            date_str = _fmt_pub(pub)

            # タイトルはちょっと長めに（例：先頭 24 文字）
            short_title = title[:24]
            label = f"{date_str} {short_title}" if date_str else short_title

            # BERT が positive なら青丸を付与
            sentiment = bert_labels.get(code4, {}).get(j)
            dot = ""
            if sentiment:
                s_norm = str(sentiment).upper()
                if ("POS" in s_norm) or ("ポジ" in s_norm):
                    dot = '<span class="bert-pos-dot">●</span>'

            if link:
                html = f'{dot}<a href="{link}" target="_blank" rel="noopener noreferrer">{label}</a>'
            else:
                html = f'{dot}{label}'
            out.append(html)
        return " / ".join(out)

    data = []
    full_data = []
    for idx, code4 in idx_map:
        items = result_map.get(code4, [])
        data.append(fmt(code4, items))
        # P1-11: 予想器・材料判定向け。全文に公開日を付けて保持する。
        # 同じ材料が数週間たっても満額加点され続けるのを防ぐため、ISO日付を先頭に付与する。
        full_titles = []
        for title, _, _pub in (items or []):
            title = str(title or "").strip()
            if not title:
                continue
            d = _pub_date(_pub)
            if d is not None:
                full_titles.append(f"{d.isoformat()}\t{title}")
            else:
                # 日付不明の旧/異常データは互換性のためタイトルだけ残す。
                full_titles.append(title)
        full_data.append("\n".join(full_titles))

    display_ser = pd.Series(data, index=df.index, dtype=object)
    if return_fulltext:
        full_ser = pd.Series(full_data, index=df.index, dtype=object)
        return display_ser, full_ser
    return display_ser



# [PATCH] ensure 3 news columns exist on DataFrame
def ensure_news_cols(df):
    try:
        cols = getattr(df, 'columns', [])
        for c in ('株探ニュース(1)', '株探ニュース(2)', '株探ニュース(3)'):
            if c not in cols:
                df[c] = None
        return df
    except Exception:
        return df


# [PATCH] guard function to safely ensure news columns on both dfs



# === [THEME] Kabutan 人気テーマ + 株探ニュース から「関連テーマ」を付与するユーティリティ ===
def _ensure_theme_tables(conn: sqlite3.Connection):
    # theme_master / stock_theme_kabutan の2テーブルを保証する。
    cur = conn.cursor()
    _sp = "p2_64_theme_schema"
    try:
        cur.execute(f"SAVEPOINT {_sp}")
        cur.execute(
            "CREATE TABLE IF NOT EXISTS theme_master ("
            "  theme_id   INTEGER PRIMARY KEY AUTOINCREMENT,"
            "  theme_name TEXT NOT NULL UNIQUE"
            ")"
        )
        cur.execute(
            "CREATE TABLE IF NOT EXISTS stock_theme_kabutan ("
            "  コード     TEXT NOT NULL,"
            "  theme_id   INTEGER NOT NULL,"
            "  取得日     TEXT,"
            "  PRIMARY KEY (コード, theme_id)"
            ")"
        )
        cur.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception as e:
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            cur.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        try:
            print("[theme][ERROR] _ensure_theme_tables failed:", e)
        except Exception:
            pass
        # P1-503: 当日テーマsnapshotの前提schemaを作れなかった場合は成功扱いしない。
        raise RuntimeError("theme table schema ensure failed") from e
    finally:
        try:
            cur.close()
        except Exception:
            pass



def phase_refresh_theme_snapshot(conn: sqlite3.Connection):
    """P3-41: テーマWeb取得は外部日次producerが担当。本体はDB snapshotを読むだけ。"""
    _ensure_theme_tables(conn)
    row = conn.execute("SELECT MAX(取得日) FROM stock_theme_kabutan").fetchone()
    latest = row[0] if row else None
    print(f"[theme] external producer snapshot latest={latest or 'none'}")
    return latest


def _load_theme_maps_for_screener(conn: sqlite3.Connection):
    '''
    screener に『関連テーマ』列を付与するための補助データをロード。
    戻り値: (kabutan_map, theme_names)
      kabutan_map: { 'コード4桁': {'生成AI', '半導体', ...}, ... }
      theme_names: ['生成AI', '半導体', ...]
    '''
    kabutan_map = {}
    theme_names = []
    try:
        cur = conn.cursor()
        # P1-137: スコア側(P1-35/37)と同じく、関連テーマ表示も古い紐付けを永久保持しない。
        _rel_theme_today = date.fromisoformat(_today_jst())
        theme_cutoff = (_rel_theme_today - timedelta(days=30)).isoformat()
        theme_today = _rel_theme_today.isoformat()
        for code_raw, tname in cur.execute(
            "SELECT s.コード, m.theme_name "
            "FROM stock_theme_kabutan s "
            "JOIN theme_master m ON s.theme_id = m.theme_id "
            "WHERE date(s.取得日) >= date(?) AND date(s.取得日) <= date(?)",
            (theme_cutoff, theme_today)
        ):
            code4 = _normalize_jp_security_code(code_raw)
            if not code4 or not tname:
                continue
            # P1-132: 285A等の英数字コードを0285へ破壊しない。
            kabutan_map.setdefault(code4, set()).add(str(tname))
        for (tname,) in cur.execute(
            "SELECT theme_name FROM theme_master ORDER BY theme_name"
        ):
            if tname:
                theme_names.append(str(tname))
        cur.close()
    except Exception as e:
        print("[theme][WARN] _load_theme_maps_for_screener failed:", e)
        # P1-536: 空mapを正常結果として返すとscreenerの前回テーマ列が残る。
        raise
    return kabutan_map, theme_names

def _attach_related_themes(df, kabutan_map, theme_names):
    '''
    DataFrame(df) に '関連テーマ' 列を付与する。
    ・Kabutan 人気テーマ（stock_theme_kabutan）
    ・株探ニュース(1)〜(3) タイトルに含まれるテーマ名
    を両方マージして ' / ' 区切りで入れる。
    '''
    try:

        if df is None or getattr(df, "empty", True):
            return df
        cols = getattr(df, "columns", [])
        if "コード" not in cols:
            return df

        def _strip_tags(s: str) -> str:
            # aタグなどをざっくり除去
            return re.sub(r"<[^>]*>", "", s or "")

        def _themes_from_news(news_texts):
            if not theme_names:
                return set()
            joined = " ".join(
                _strip_tags(str(t)) for t in news_texts if t
            )
            if not joined:
                return set()
            hits = set()
            for name in theme_names:
                if name and name in joined:
                    hits.add(name)
            return hits

        # P1-154: 7203.0/285Aを共通正規化してテーマmapと突合。
        codes = df["コード"].map(_normalize_jp_security_code)
        n1 = df["株探ニュース(1)"] if "株探ニュース(1)" in cols else None
        n2 = df["株探ニュース(2)" ] if "株探ニュース(2)" in cols else None
        n3 = df["株探ニュース(3)"] if "株探ニュース(3)" in cols else None

        related = []
        for i, c4 in enumerate(codes):
            base = set(kabutan_map.get(str(c4), set()))
            news_texts = []
            if n1 is not None:
                news_texts.append(n1.iloc[i])
            if n2 is not None:
                news_texts.append(n2.iloc[i])
            if n3 is not None:
                news_texts.append(n3.iloc[i])
            from_news = _themes_from_news(news_texts)
            all_themes = sorted(base | from_news)
            related.append(" / ".join(all_themes) if all_themes else None)
        df["関連テーマ"] = related
    except Exception as e:
        print("[theme][WARN] _attach_related_themes failed:", e)
        # P1-536: export側へ失敗を伝え、前回テーマ表示をmaskさせる。
        raise
    return df

def _parse_theme_date_for_screener(date_text):
    if not date_text:
        return None
    
    s = str(date_text).strip()
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            # 【修正】datetime を使うことでインポート名の衝突によるクラッシュを完全に回避
            return datetime.strptime(s[:10], fmt).date()
        except Exception:
            continue
    return None


def _load_latest_theme_map_for_screener(conn: sqlite3.Connection):
    latest_theme_map = {}
    try:
        cur = conn.cursor()
        # P1-138: ROW_NUMBER(codeだけ)では最新日同着の複数テーマから1件を任意選択していた。
        # 最新取得日の全テーマを返し、後段のsorted(set(...))[0]で決定的に選ぶ。
        # P1-139: 「最新」といっても数か月前の最終記録を表示し続けない。30日以内のスナップショットだけ対象。
        _theme_today = date.fromisoformat(_today_jst())
        theme_cutoff = (_theme_today - timedelta(days=30)).isoformat()
        theme_today = _theme_today.isoformat()
        sql = """
            WITH fresh AS (
                SELECT * FROM stock_theme_kabutan
                WHERE date(取得日) >= date(?) AND date(取得日) <= date(?)
            ), latest AS (
                SELECT コード, MAX(取得日) AS max_date
                FROM fresh
                GROUP BY コード
            )
            SELECT s.コード AS c4, s.取得日 AS date_text, m.theme_name
            FROM fresh s
            JOIN latest l ON s.コード = l.コード AND s.取得日 = l.max_date
            JOIN theme_master m ON s.theme_id = m.theme_id
            ORDER BY s.コード, m.theme_name
        """
        for code_raw, raw_date, theme_name in cur.execute(sql, (theme_cutoff, theme_today)):
            if not code_raw or not raw_date or not theme_name:
                continue
            
            # P1-154: 最新テーマmapも英数字/float文字列を共通正規化。
            c4 = _normalize_jp_security_code(code_raw)
            
            date_obj = _parse_theme_date_for_screener(raw_date)
            if date_obj is None:
                continue
            
            latest_theme_map.setdefault(c4, {}).setdefault(date_obj, []).append(str(theme_name))
            
        cur.close()
    except Exception as e:
        print("[theme][WARN] _load_latest_theme_map_for_screener failed:", e)
        # P1-536: stale latest-theme fallbackを防ぐためexport側へ伝播。
        raise
        
    return latest_theme_map


def _attach_latest_theme(df, latest_theme_map):
    try:
        if df is None or getattr(df, "empty", True):
            return df
        if "コード" not in df.columns:
            return df

        def _fmt_date(dt):
            # 【修正】datetime.strftime ではなく dt.strftime が正しい呼び出し方（TypeError回避）
            return dt.strftime("%Y/%m/%d") if dt else ""

        latest_values = []
        # P1-154: DataFrame側も同じ正規化で突合。
        for code in df["コード"].map(_normalize_jp_security_code):
            entries = latest_theme_map.get(code)
            if not entries:
                latest_values.append(None)
                continue
            
            latest_date = max(entries.keys())
            theme_name = sorted(set(entries[latest_date]))[0]
            latest_values.append(f"{_fmt_date(latest_date)} {theme_name}")
            
        df["最新テーマ"] = latest_values
    except Exception as e:
        # エラーが起きた場合は必ずコンソールに吐き出す
        print(f"[theme][WARN] _attach_latest_theme failed: {e}")
        # P1-536: silent partial attachにせずexport側で列全体をmaskする。
        raise
        
    return df
    

def calculate_robust_theme_ranking(conn: sqlite3.Connection, df_cand: pd.DataFrame) -> list:
    """
    静的ノイズをクレンジングしたテーマランキングを計算。平均騰落率を内包させる。
    """
    try:
        # P1-122: SQLite date('now') はUTC基準。JST 0:00-8:59に30日窓が1日ずれるため、
        # Python側のJST市場日付からcutoffを明示して渡す。
        _theme_today = date.fromisoformat(_today_jst())
        theme_cutoff = (_theme_today - timedelta(days=30)).isoformat()
        theme_today = _theme_today.isoformat()
        query = """
            -- P1-38: 同一銘柄×テーマの複数取得日を重複カウントしない。
            SELECT DISTINCT t.コード AS コード, m.theme_name AS テーマ
            FROM stock_theme_kabutan t
            JOIN theme_master m ON t.theme_id = m.theme_id
            -- P1-37/P1-122: 銘柄×テーマ紐付け自身をJST基準の30日窓で採用。
            WHERE date(t.取得日) >= date(?) AND date(t.取得日) <= date(?)
        """
        df_theme = pd.read_sql_query(query, conn, params=(theme_cutoff, theme_today))
        if not df_theme.empty:
            # P1-133: テーマランキングの英数字証券コードを保持。
            df_theme["コード"] = df_theme["コード"].map(_normalize_jp_security_code)
            df_theme = df_theme[df_theme["コード"] != ""]
            # P1-468: SQL DISTINCTはrawコードに対して効くため、legacyの7203/7203.0や
            # 1234/1234.Nが併存するとcanonical化後に同一銘柄×テーマが二重化する。
            # active_stocks/turnover/signaled_countを水増ししないようlogical keyで再度1本化。
            df_theme = df_theme.drop_duplicates(["コード", "テーマ"], keep="last").reset_index(drop=True)
        
        if df_cand is None or df_cand.empty or "コード" not in df_cand.columns:
            return []
            
        turnover_col = '売買代金億' if '売買代金億' in df_cand.columns else '売買代金(億)'
        if turnover_col not in df_cand.columns:
            return []

        # ★ 修正: DBのカラム名である '前日終値比率' を使用する
        for c in ['初動フラグ', '右肩早期フラグ', '右肩上がりフラグ', '前日終値比率']:
            if c not in df_cand.columns:
                df_cand[c] = ""
                
        df_s = df_cand[['コード', turnover_col, '初動フラグ', '右肩早期フラグ', '右肩上がりフラグ', '前日終値比率']].copy()
        df_s['_raw_code'] = df_s['コード'].astype(str)
        df_s['コード'] = df_s['コード'].map(_normalize_jp_security_code)
        # P3-2: df_cand側にもlegacy aliasが混在してもテーマ人数/売買代金を二重計上しない。
        df_s['_canonical_row'] = [
            str(raw).strip().upper() == str(code).strip().upper()
            for raw, code in zip(df_s['_raw_code'], df_s['コード'])
        ]
        df_s = (
            df_s[df_s['コード'].astype(bool)]
            .sort_values(['_canonical_row'], ascending=False, kind='stable')
            .drop_duplicates('コード', keep='first')
            .drop(columns=['_raw_code', '_canonical_row'], errors='ignore')
        )

        # P1-588: テーマランキングの active_stocks / signal_density も、今回runの価格snapshotへ
        # 到達した銘柄だけで集計する。旧実装はstale銘柄の売買代金/騰落率自体はNULLでも、
        # active_stocksの分母には残るため、テーマのシグナル密度を古い休止銘柄で希釈できた。
        _theme_rank_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        _theme_rank_fresh = set()
        try:
            _tr_rows = conn.execute(
                "SELECT コード, 終値 FROM price_history WHERE date(日付)=date(?)",
                (_theme_rank_asof,),
            ).fetchall()
            # P1-679: 日付行の存在だけでfreshにせず、有限な正の終値が実在するlogical codeだけ採用。
            _theme_rank_fresh = set()
            for _r in _tr_rows:
                if not _r:
                    continue
                _ck = _normalize_jp_security_code(_r[0])
                _cv = ffloat(_r[1], None)
                if _ck and _cv is not None and math.isfinite(float(_cv)) and float(_cv) > 0:
                    _theme_rank_fresh.add(_ck)
        except Exception as _e:
            raise RuntimeError(f"theme ranking freshness lookup failed: {_e}") from _e
        df_s = df_s[df_s['コード'].isin(_theme_rank_fresh)].copy()
        if df_s.empty:
            return []

        # P1-42: 売買代金/騰落率の欠損を実測0へ変換してテーマ統計を薄めない。
        df_s['売買代金(億)'] = pd.to_numeric(df_s[turnover_col], errors='coerce')
        df_s['前日終値比率'] = pd.to_numeric(
            df_s['前日終値比率'].astype(str).str.replace(r'[^\d\.\-]', '', regex=True),
            errors='coerce'
        )
        
        df_s['is_signaled'] = np.where(
            df_s['初動フラグ'].astype(str).str.contains('候補') | 
            df_s['右肩早期フラグ'].astype(str).str.contains('候補') |
            df_s['右肩上がりフラグ'].astype(str).str.contains('候補'), 
            1, 0
        )
        
        df_mrg = pd.merge(df_theme, df_s, on='コード', how='inner')
        if df_mrg.empty:
            return []
            
        stats = df_mrg.groupby('テーマ').agg(
            total_turnover=('売買代金(億)', 'sum'),
            median_turnover=('売買代金(億)', 'median'),
            turnover_obs=('売買代金(億)', 'count'),
            active_stocks=('コード', 'count'),
            signaled_count=('is_signaled', 'sum'),
            avg_return=('前日終値比率', 'mean')
        ).reset_index()

        # P1-46: active_stocksが3でも、売買代金が1銘柄しか取れていないテーマを
        # その1点だけでランキングしない。最低3銘柄の有効売買代金を要求する。
        stats = stats[(stats['active_stocks'] >= 3) & (stats['turnover_obs'] >= 3)].copy()
        if stats.empty:
            return []
        
        stats['signal_density'] = stats['signaled_count'] / stats['active_stocks']
        stats['true_flow_score'] = _theme_true_flow_score(stats['median_turnover'], stats['signal_density']).round(1)
        # 売買代金が全件欠損のテーマは強度を計算不能としてランキング対象外。
        stats = stats[stats['median_turnover'].notna()].copy()

        stats = stats.sort_values(by='true_flow_score', ascending=False).head(30)
        return stats.to_dict(orient='records')
        
    except Exception as e:
        # P2-67: DB/schema障害まで「該当テーマ0件」に変換すると、正常な空ランキングと区別できない。
        # optional表示として空に落とす判断は呼出側に一本化し、ここでは原因を保持して伝播する。
        raise RuntimeError(f"robust theme ranking calculation failed: {e}") from e

# === [/THEME] ===========================================================



###############################################
# === 相対強度RS計算ユーティリティ ====================
###############################################


# --- 【追加】制限値幅（ストップ高幅）を返す関数 ---
def get_limit_price_width(price):
    """JPX内国株の通常制限値幅。臨時の値幅拡大は別途イベント情報が必要。"""
    # P1-504: 旧実装は3万円以上を一律1万円としており、高価格株のS高判定を誤る。
    p = float(price)
    bands = (
        (100, 30), (200, 50), (500, 80), (700, 100),
        (1_000, 150), (1_500, 300), (2_000, 400), (3_000, 500),
        (5_000, 700), (7_000, 1_000), (10_000, 1_500), (15_000, 3_000),
        (20_000, 4_000), (30_000, 5_000), (50_000, 7_000), (70_000, 10_000),
        (100_000, 15_000), (150_000, 30_000), (200_000, 40_000),
        (300_000, 50_000), (500_000, 70_000), (700_000, 100_000),
        (1_000_000, 150_000), (1_500_000, 300_000), (2_000_000, 400_000),
        (3_000_000, 500_000), (5_000_000, 700_000), (7_000_000, 1_000_000),
        (10_000_000, 1_500_000), (15_000_000, 3_000_000),
        (20_000_000, 4_000_000), (30_000_000, 5_000_000),
        (50_000_000, 7_000_000),
    )
    for ceiling, width in bands:
        if p < ceiling:
            return width
    return 10_000_000

# --- 【修正版】ストップ高の状態を判定する関数 ---
def analyze_stop_high_status(hist_df):
    if hist_df is None or len(hist_df) < 5:
        return "" # データ不足

    # 【修正1】カラム名を日本語('終値')に合わせる
    col_close = '終値' if '終値' in hist_df.columns else 'close'
    if col_close not in hist_df.columns:
        return ""

    # 終値リスト
    closes = hist_df[col_close].values
    
    # 【修正2】日付は index ではなく '日付' カラムにある
    if '日付' in hist_df.columns:
        dates = hist_df['日付'].values
    else:
        dates = hist_df.index

    # 日付フォーマット用
    def fmt_date(dt):
        # numpy.datetime64 などの対応
        s = str(dt)
        return s[:10]

    # --- 1. 今日(最新)がストップ高か判定 ---
    today_price = closes[-1]
    prev_price  = closes[-2]
    limit_w     = get_limit_price_width(prev_price)
    
    # 誤差対策で -1円 以上の余裕を見る
    is_today_sh = (today_price >= prev_price + limit_w - 0.5)

    if is_today_sh:
        # 連続記録を遡る
        start_date = fmt_date(dates[-1])
        for i in range(2, len(closes)):
            p_curr = closes[-i]
            p_prev = closes[-i-1]
            lim    = get_limit_price_width(p_prev)
            
            if p_curr >= p_prev + lim - 0.5:
                start_date = fmt_date(dates[-i])
            else:
                break
        return f"〇 {start_date}〜"

    # --- 2. 今日は違うが、昨日がストップ高だった場合 ---
    prev_prev_price = closes[-3]
    limit_w_prev    = get_limit_price_width(prev_prev_price)
    is_prev_sh      = (prev_price >= prev_prev_price + limit_w_prev - 0.5)

    if is_prev_sh:
        return "ストップ高停止"

    return ""

class MarketEventCalendar:
    """マクロ需給イベントを事前計算し、特定日のアラート状態を判定するクラス"""
    
    # 祝日データのキャッシュ（年をキーに保持）
    _holiday_cache = {}
    
    def __init__(self, year: int):
        self.year = year

        # P1-590: マクロ需給イベントの「営業日」も中央JPXカレンダーへ統一する。
        # 旧実装はjpholidayだけなので、12/31・1/2・1/3や追加休場日を営業日として数え、
        # 特に1月SQの「5営業日前」などがずれ得た。前年/翌年境界まで含めて構築する。
        _extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
        _cache_key = (year, tuple(sorted(d.isoformat() for d in _extra_closed)))
        if _cache_key not in self._holiday_cache:
            _jp_closed = set()
            for _yy in (year - 1, year, year + 1):
                _d = date(_yy, 1, 1)
                _end = date(_yy, 12, 31)
                while _d <= _end:
                    if _d.weekday() < 5 and is_jp_market_holiday(_d, _extra_closed):
                        _jp_closed.add(_d)
                    _d += timedelta(days=1)
            self._holiday_cache[_cache_key] = _jp_closed
        self.holidays = self._holiday_cache[_cache_key]

        self.jbd = pd.offsets.CustomBusinessDay(holidays=list(self.holidays))
        self.events = self._precompute_events()

    def _get_last_business_day(self, month: int) -> pd.Timestamp:
        """指定月の最終営業日を算出（前倒し）"""
        if month == 12:
            first_day_next = pd.Timestamp(date(self.year + 1, 1, 1))
        else:
            first_day_next = pd.Timestamp(date(self.year, month + 1, 1))
        return first_day_next - self.jbd

    def _get_nth_dow(self, month: int, weekday: int, n: int) -> pd.Timestamp:
        """第n曜日を算出（weekday: 月=0, 金=4）"""
        first_day = pd.Timestamp(date(self.year, month, 1))
        offset = (weekday - first_day.weekday()) % 7
        target_date = first_day + pd.Timedelta(days=offset + (n - 1) * 7)
        # 祝日の場合は前倒し
        while target_date.date() in self.holidays:
            target_date -= pd.Timedelta(days=1)
        return target_date

    def _precompute_events(self):
        events = []

        # 1. SQ (毎月第2金曜、アラートは5営業日前から)
        for month in range(1, 13):
            sq_date = self._get_nth_dow(month, 4, 2)
            alert_start = sq_date - (5 * self.jbd)
            name = f"{'メジャー' if month in [3,6,9,12] else 'マイナー'}SQ"
            events.append({"name": name, "start": alert_start, "end": sq_date, "type": "警戒"})

        # 2. MSCIリバランス (2,5,8,11月末)
        for month in [2, 5, 8, 11]:
            msci_date = self._get_last_business_day(month)
            events.append({"name": "MSCIリバランス", "start": msci_date - (15 * self.jbd), "end": msci_date, "type": "入替"})

        # 3. ETF分配金捻出売り (7月上旬)
        etf_base = pd.Timestamp(date(self.year, 7, 10))
        while etf_base.weekday() >= 5 or etf_base.date() in self.holidays:
            etf_base += pd.Timedelta(days=1)
        peak_sell = etf_base - (2 * self.jbd)
        july_first = pd.Timestamp(date(self.year, 7, 1))
        events.append({"name": "ETF分配金換金売り", "start": july_first, "end": peak_sell, "type": "下落注意"})

        return events

    def get_alerts_for_date(self, target_date) -> list:
        """指定日に該当するアラートを取得"""
        t = pd.Timestamp(target_date)
        active_alerts = []
        for ev in self.events:
            if ev["start"] <= t <= ev["end"]:
                days_left = (ev["end"] - t).days
                active_alerts.append(f"【{ev['type']}】{ev['name']} (目安:あと{days_left}日)")
        return active_alerts

# === [追加] セクター・マスタ統合とランキング計算ロジック ===
def sync_sector_data(conn: sqlite3.Connection):
    """JPX公式業種データを用いてscreenerセクターを同期。P1-452: atomic/fail-visible。"""
    url = "https://www.jpx.co.jp/markets/statistics-equities/misc/tvdivq0000001vg2-att/data_j.xls"
    try:
        # P1-461: pd.read_excel(URL)任せだとHTTP timeoutを制御できず、
        # JPX応答停止時にHTML生成全体が無期限hangし得る。明示timeoutでbytes取得。
        from io import BytesIO
        _sector_resp = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=30)
        _sector_resp.raise_for_status()
        df = pd.read_excel(BytesIO(_sector_resp.content))
        df = df.rename(columns={'33業種区分': 'セクター'})
        if 'コード' not in df.columns or 'セクター' not in df.columns:
            raise RuntimeError("JPX sector file missing required columns")
        df['コード'] = df['コード'].map(canonical_code_for_db)
        df = df[df['コード'].astype(str).str.len() > 0]
        if df.empty:
            raise RuntimeError("JPX sector file parsed zero rows")
        # P2-16: duplicate-column判定はschemaで行い、I/O/lock等のOperationalErrorは表面化させる。
        _sector_schema_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
        if "セクター" not in _sector_schema_cols:
            conn.execute('ALTER TABLE screener ADD COLUMN "セクター" TEXT')
        _sector_map = {canonical_code_for_db(r.get('コード')): r.get('セクター')
                       for _, r in df.iterrows() if canonical_code_for_db(r.get('コード'))}
        _sector_updates = []
        for (_raw_code,) in conn.execute("SELECT コード FROM screener").fetchall():
            _sector_updates.append((_sector_map.get(canonical_code_for_db(_raw_code)), _raw_code))
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_452_sector_sync")
            if _sector_updates:
                cur.executemany("UPDATE screener SET セクター=? WHERE コード=?", _sector_updates)
            cur.execute("RELEASE SAVEPOINT p1_452_sector_sync")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_452_sector_sync")
                cur.execute("RELEASE SAVEPOINT p1_452_sector_sync")
            except Exception:
                pass
            raise
        finally:
            cur.close()
    except Exception as e:
        raise RuntimeError(f"Sector sync failed: {e}") from e


def prepare_sector_ranking_view(conn: sqlite3.Connection):
    """P3-21: logical codeで重複除去したセクターランキングDataFrameを返す。

    旧実装はraw screener行をそのままGROUP BYしたため、7203/7203.0等のaliasが
    売買代金・active_stocks・平均騰落率を二重計上した。また表示専用VIEW作成で
    呼出元transactionをcommitしていたため、永続DDLを作らずPython集計へ一本化する。
    """
    sdf = pd.read_sql_query(
        "SELECT rowid AS _rowid, コード, セクター, 売買代金億, 前日終値比率 FROM screener",
        conn,
    )
    if sdf.empty:
        return pd.DataFrame(columns=["セクター", "total_turnover", "active_stocks", "avg_return"])
    sdf["_code_key"] = sdf["コード"].map(canonical_code_for_db)
    sdf["_canonical_row"] = [
        str(raw).strip().upper() == str(key).strip().upper()
        for raw, key in zip(sdf["コード"], sdf["_code_key"])
    ]
    sdf = (
        sdf[sdf["_code_key"].astype(bool)]
        .sort_values(["_canonical_row", "_rowid"], ascending=[False, False], kind="stable")
        .drop_duplicates("_code_key", keep="first")
        .copy()
    )
    sdf["売買代金億"] = pd.to_numeric(sdf["売買代金億"], errors="coerce")
    sdf["前日終値比率"] = pd.to_numeric(sdf["前日終値比率"], errors="coerce")
    _sector_text = sdf["セクター"].astype(str).str.strip()
    sdf = sdf[(sdf["売買代金億"] > 0) & sdf["セクター"].notna() & (~_sector_text.str.lower().isin(["", "nan", "none"]))].copy()
    if sdf.empty:
        return pd.DataFrame(columns=["セクター", "total_turnover", "active_stocks", "avg_return"])
    out = (
        sdf.groupby("セクター", dropna=False)
        .agg(
            total_turnover=("売買代金億", "sum"),
            active_stocks=("_code_key", "nunique"),
            avg_return=("前日終値比率", "mean"),
        )
        .reset_index()
        .sort_values("total_turnover", ascending=False, kind="stable")
        .reset_index(drop=True)
    )
    return out
# ==============================================================================
# ★ 株価上昇幅・インパクト予測エンジン (StockSurprisePredictor) 【売上・営業益の伸び率比較版】
# ==============================================================================
class StockSurprisePredictor:
    def __init__(self, weights: dict = None):
        _defaults = {
            "yoy_sales": 0.05,
            "yoy_op": 0.15,        # 営業利益の伸びを重視
            "buyback": 30.0,       # 自社株買い
            "dividend": 15.0,      # 増配
            # P3-14: 現行の過去D1反応は事前consensusではないため既定では無効化。
            "earnings_exp": 0.0,
        }
        # P3-35: custom設定は「全keyを再定義」ではなく部分overrideにする。
        # typo/未知keyは黙って無視せず設定ミスとして即時可視化する。
        self.weights = dict(_defaults)
        if weights is not None:
            if not isinstance(weights, dict):
                raise TypeError("StockSurprisePredictor weights must be a dict or None")
            unknown = sorted(set(weights) - set(_defaults))
            if unknown:
                raise ValueError(f"unknown StockSurprisePredictor weights: {unknown}")
            for _k, _v in weights.items():
                try:
                    _fv = float(_v)
                except Exception as _e:
                    raise ValueError(f"invalid weight {_k}={_v!r}") from _e
                if not math.isfinite(_fv):
                    raise ValueError(f"non-finite weight {_k}={_v!r}")
                self.weights[_k] = _fv

    def _resolve_news_as_of(self, data: dict):
        """ニュース鮮度の基準日を決める。バックテスト時はシグナル更新日を優先。"""
        for key in ("ニュース基準日", "シグナル更新日", "日付"):
            v = data.get(key) if isinstance(data, dict) else None
            if v in (None, ""):
                continue
            try:
                return pd.to_datetime(v, errors="raise").date()
            except Exception:
                pass
        return date.fromisoformat(_today_jst())

    @staticmethod
    def _news_freshness_weight(pub_date, as_of=None) -> float:
        """P1-11: 短期カタリストとしてのニュース鮮度を0〜1で返す。

        公開後2日までは満額、その後は7日半減で指数減衰し、45日超は0。
        これは統計学習済み係数ではなく、古い発表を毎回満額加点しないための
        保守的な暫定ルール。
        """
        # P1-611: 短期材料は公開日を検証できた記事だけ加点する。
        # Google News RSSのdate/pubDateが欠損・parse不能でもタイトル自体は残り得るため、
        # 日付不明を満額扱いすると古い自社株買い/増配が永久にcurrent材料化する。
        if pub_date is None:
            return 0.0
        try:
            pd_date = pd.to_datetime(pub_date).date()
            ref = pd.to_datetime(as_of).date() if as_of is not None else date.fromisoformat(_today_jst())
            age = (ref - pd_date).days
        except Exception:
            return 0.0
        if age < 0:
            return 0.0  # 未来ニュースを先読みしない
        if age <= 2:
            return 1.0
        if age > 45:
            return 0.0
        effective_age = age - 2
        return float(0.5 ** (effective_age / 7.0))

    def _extract_news_flags(self, news_text: str, as_of=None) -> dict:
        """P1-10/11: 文脈 + 鮮度付きでニュース材料を判定する。

        新形式は 1行ごとに ``YYYY-MM-DD\tタイトル``。旧形式（タイトルだけ）も読める。
        複数記事が同じ材料を報じても二重加点せず、最も新鮮な1件の強度を採用する。
        また、より新しい「終了・中止・撤回」があれば、それ以前のプラス材料は失効させる。
        """
        text = str(news_text or "")
        raw_items = [x.strip() for x in re.split(r"[\r\n]+|\s+(?:/|｜)\s+", text) if x and x.strip()]
        if not raw_items and text.strip():
            raw_items = [text.strip()]

        items = []
        for rank, raw in enumerate(raw_items):
            pub_date = None
            title = raw
            m = re.match(r"^(\d{4}-\d{2}-\d{2})\t(.*)$", raw)
            if m:
                try:
                    pub_date = datetime.strptime(m.group(1), "%Y-%m-%d").date()
                except Exception:
                    pub_date = None
                title = m.group(2).strip()
            items.append((title, pub_date, rank))

        buyback_strength = 0.0
        dividend_strength = 0.0
        buyback_age_days = None
        dividend_age_days = None
        buyback_pos_date = None
        dividend_pos_date = None
        buyback_pos_rank = None
        dividend_pos_rank = None
        buyback_neg_date = None
        dividend_neg_date = None
        buyback_neg_rank = None
        dividend_neg_rank = None

        buyback_expr = r"(?:自社株買い|自社株取得|自己株式(?:の)?取得)"
        buyback_neg = r"(?:終了|完了|中止|見送り|取りやめ|取止め|打ち切り|停止|撤回)"
        buyback_new = r"(?:新たな|新規|追加|再開)"

        dividend_expr = r"(?:増配|配当増額|配当(?:予想)?(?:を|の)?増額|配当(?:予想)?(?:を|の)?引き上げ)"
        dividend_neg = r"(?:見送り|中止|取りやめ|取止め|撤回|なし|せず|困難|後退)"

        ref_date = pd.to_datetime(as_of).date() if as_of is not None else None

        def _is_newer(event_date, event_rank, base_date, base_rank):
            if event_date is not None and base_date is not None:
                return event_date >= base_date
            # Google News取得順は新しい順。日付欠損時は小さいrankを新しいものとして扱う。
            if event_rank is not None and base_rank is not None:
                return event_rank <= base_rank
            return False

        for title, pub_date, rank in items:
            # バックテスト時に未来ニュースを先読みしない。
            if pub_date is not None and ref_date is not None and pub_date > ref_date:
                continue

            norm = re.sub(r"\s+", "", str(title))
            freshness = self._news_freshness_weight(pub_date, as_of=as_of)
            age_days = None
            if pub_date is not None and ref_date is not None:
                age_days = (ref_date - pub_date).days

            # --- 自社株買い ---
            if re.search(buyback_expr, norm):
                neg_ctx = bool(
                    re.search(buyback_expr + r".{0,14}" + buyback_neg, norm)
                    or re.search(buyback_neg + r".{0,14}" + buyback_expr, norm)
                    or ("材料出尽くし" in norm and re.search(buyback_expr, norm))
                )
                new_ctx = bool(
                    re.search(buyback_new + r".{0,18}" + buyback_expr, norm)
                    or re.search(buyback_expr + r".{0,18}" + buyback_new, norm)
                )
                if neg_ctx and not new_ctx:
                    if buyback_neg_rank is None or _is_newer(pub_date, rank, buyback_neg_date, buyback_neg_rank):
                        buyback_neg_date, buyback_neg_rank = pub_date, rank
                elif freshness > buyback_strength:
                    buyback_strength = freshness
                    buyback_age_days = age_days
                    buyback_pos_date, buyback_pos_rank = pub_date, rank

            # --- 増配 ---
            if re.search(dividend_expr, norm):
                neg_ctx = bool(
                    re.search(dividend_expr + r".{0,14}" + dividend_neg, norm)
                    or re.search(dividend_neg + r".{0,14}" + dividend_expr, norm)
                )
                if neg_ctx:
                    if dividend_neg_rank is None or _is_newer(pub_date, rank, dividend_neg_date, dividend_neg_rank):
                        dividend_neg_date, dividend_neg_rank = pub_date, rank
                elif freshness > dividend_strength:
                    dividend_strength = freshness
                    dividend_age_days = age_days
                    dividend_pos_date, dividend_pos_rank = pub_date, rank

        # より新しい終了/撤回は過去の発表材料を失効させる。
        if (buyback_neg_rank is not None and buyback_pos_rank is not None and
                _is_newer(buyback_neg_date, buyback_neg_rank, buyback_pos_date, buyback_pos_rank)):
            buyback_strength = 0.0
            buyback_age_days = None
        if (dividend_neg_rank is not None and dividend_pos_rank is not None and
                _is_newer(dividend_neg_date, dividend_neg_rank, dividend_pos_date, dividend_pos_rank)):
            dividend_strength = 0.0
            dividend_age_days = None

        return {
            "share_buyback_flag": 1 if buyback_strength > 0 else 0,
            "dividend_up_flag": 1 if dividend_strength > 0 else 0,
            "share_buyback_strength": float(buyback_strength),
            "dividend_up_strength": float(dividend_strength),
            "share_buyback_age_days": buyback_age_days,
            "dividend_up_age_days": dividend_age_days,
        }

    def calculate_surprise_score(self, data: dict) -> float:
        """Step 1: サプライズスコアの計算（売上YoYと営業益YoYの質的比較）"""
        # P1-9: 表示用の24文字短縮タイトルではなく、取得時に保持した全文タイトルを優先して材料判定する。
        # 旧DB/旧経路との互換性のため、全文列が無い場合だけ株探ニュース(3)へフォールバック。
        news_for_model = data.get("株探ニュース判定用", "") or data.get("株探ニュース(3)", "")
        news_as_of = self._resolve_news_as_of(data)
        flags = self._extract_news_flags(news_for_model, as_of=news_as_of)
        
        # P1-84: YoY欠損を実測0%へ変換しない。欠損値から加速ボーナス/偽加速ペナルティを作らない。
        def _finite_yoy(v):
            try:
                if v in (None, ""):
                    return None
                x = float(v)
                return x if math.isfinite(x) else None
            except Exception:
                return None

        sales_yoy = _finite_yoy(data.get("直近売上YoY"))
        op_yoy = _finite_yoy(data.get("直近営業益YoY"))
        sales_term = sales_yoy if sales_yoy is not None else 0.0
        op_term = op_yoy if op_yoy is not None else 0.0

        # P3-4: 「増益」と「利益加速」を分離。営業益YoY>0だけでは加速扱いしない。
        # authoritativeな利益加速フラグが立った時だけ、売上YoYとの比較で品質を確認する。
        accel_quality = _profit_acceleration_quality(data)
        if accel_quality == "quality":
            accel_bonus = 15.0
        elif accel_quality == "margin_lag":
            accel_bonus = -25.0
        else:
            # inactive / unknown / YoY不足は加速由来の加減点なし。YoY本体は別項で評価済み。
            accel_bonus = 0.0
        
        # 過去決算D1リアクション。P3-14では既定weight=0。
        # 真の事前期待/consensusとは別物なので、将来別featureが入るまでhurdleとしては使わない。
        # custom weightsで明示利用する場合に備え、値の安全なparseだけ維持する。
        _earnings_d1_supplied = False
        try:
            # P3-32: 明示名を正本にし、旧名は過去DB/外部呼出し互換だけに残す。
            _earnings_d1_raw = data.get("過去決算D1期待値")
            if _earnings_d1_raw not in (None, ""):
                _earnings_d1_supplied = True
            else:
                _earnings_d1_raw = data.get("決算期待値", 0)
                _earnings_d1_supplied = _earnings_d1_raw not in (None, "")
            earnings_exp = float(_earnings_d1_raw or 0)
            if not math.isfinite(earnings_exp):
                earnings_exp = 0.0
                _earnings_d1_supplied = False
        except Exception:
            earnings_exp = 0.0
            _earnings_d1_supplied = False

        # P3-36: 「材料が無い」を「材料はあるが中立=0点」と同一視しない。
        # 0%という実測YoYは有効証拠なので、値の大きさではなく存在で判定する。
        _has_surprise_evidence = bool(
            sales_yoy is not None
            or op_yoy is not None
            or flags["share_buyback_strength"] > 0
            or flags["dividend_up_strength"] > 0
            or (_earnings_d1_supplied and self.weights.get("earnings_exp", 0.0) != 0.0)
        )
        if not _has_surprise_evidence:
            return None

        score = (
            (sales_term * self.weights["yoy_sales"]) +
            (op_term * self.weights["yoy_op"]) +
            (flags["share_buyback_strength"] * self.weights["buyback"]) +
            (flags["dividend_up_strength"] * self.weights["dividend"]) +
            (earnings_exp * self.weights["earnings_exp"]) + 
            accel_bonus
        )
        return max(score, -20.0)
        
    def calculate_fuel_factor(self, data: dict):
        """Step 2: 需給・踏み上げ係数の計算（燃料 ÷ 抵抗）。必要母数不足時はNone。"""
        # P1-55: 出来高欠損を1株、需給OH欠損を1日と捏造すると、
        # 少量の空売りでもFuelが暴騰し得る。必要母数が無ければ踏み上げ増幅は計算不能とする。
        def _finite(v):
            try:
                if v in (None, ""):
                    return None
                x = float(v)
                return x if math.isfinite(x) else None
            except Exception:
                return None

        volume = _finite(data.get("出来高"))
        buy_oh = _finite(data.get("需給OH"))
        if volume is None or volume <= 0 or buy_oh is None or buy_oh < 0:
            return None

        inst_short = _finite(data.get("機関空売り合計株数"))
        margin_short = _finite(data.get("売り残"))
        if inst_short is None and margin_short is None:
            return None
        inst_short = max(inst_short or 0.0, 0.0)
        margin_short = max(margin_short or 0.0, 0.0)
        total_short_days = (inst_short + margin_short) / volume

        try:
            raw_margin_ratio = data.get("信用倍率", None)
            margin_ratio = float(raw_margin_ratio) if raw_margin_ratio not in (None, "") else None
        except Exception:
            margin_ratio = None

        if margin_ratio is not None and math.isfinite(margin_ratio) and margin_ratio > 30.0:
            buy_oh = max(buy_oh * 1.5, 5.0)

        buy_wall = max(buy_oh, 0.1)
        return total_short_days / buy_wall

    def calculate_elasticity(self, data: dict):
        """Step 3: 株価の軽さ（跳ねやすさ）の計算。必要データ欠損時はNone。"""
        # P1-51: 時価総額/売買代金欠損を100億/1億という架空値へ置換しない。
        try:
            mcap_raw = data.get("時価総額億円")
            # P1-631: current-runの正本は screener.売買代金億。
            # 表示用legacy alias「売買代金(億)」がDBに残っていても、
            # 正本列が存在する環境ではNULLを含めて正本を優先し、旧run値を復活させない。
            turnover_raw = data.get("売買代金億") if "売買代金億" in data else data.get("売買代金(億)")
            if mcap_raw is None or turnover_raw is None:
                return None
            mcap = float(mcap_raw)
            turnover = float(turnover_raw)
            if (not math.isfinite(mcap)) or (not math.isfinite(turnover)) or mcap <= 0 or turnover <= 0:
                return None
        except Exception:
            return None

        # P3-9: 極端な薄商いだけでelasticityが無限に大きくなるのを防ぐ。
        # Fair Valueの需給日数でも既に使っている0.5億円を共通の最小流動性単位として採用する。
        effective_turnover = max(turnover, 0.5)
        elasticity_score = 1.0 / math.sqrt(mcap * effective_turnover)
        return elasticity_score * 1000

    def predict_price_increase(self, data: dict) -> dict:
        """統合計算パイプライン"""
        s_score = self.calculate_surprise_score(data)
        f_factor = self.calculate_fuel_factor(data)
        elasticity = self.calculate_elasticity(data)

        # P3-36: 材料ゼロは「0%予想」ではなく予測不能。
        if s_score is None:
            return {
                "expected_return_pct": float("nan"),
                "target_price": float("nan"),
                "debug": {
                    "S_score": None,
                    "Fuel": None if f_factor is None else round(f_factor, 2),
                    "Elasticity": None if elasticity is None else round(elasticity, 2),
                    "reason": "サプライズ材料不足",
                },
            }

        # P1-51: 跳ねやすさの母数が無ければ予測値を捏造せず、当該行だけ計算不能にする。
        if elasticity is None:
            return {
                "expected_return_pct": float("nan"),
                "target_price": float("nan"),
                "debug": {
                    "S_score": round(s_score, 2),
                    "Fuel": None if f_factor is None else round(f_factor, 2),
                    "Elasticity": None,
                    "reason": "時価総額または売買代金が不足"
                }
            }

        # P1-586: 地合い欠損もnp.nanのままではmacro_coef以降を全NaN化する。
        # 未判定時は従来意図どおり中立0（係数1.0）として扱う。
        try:
            market_flag = float(data.get("地合いフラグ", 0) or 0)
            if not math.isfinite(market_flag):
                market_flag = 0.0
        except Exception:
            market_flag = 0.0
        macro_coef = 1.0 + (market_flag * 0.2)

        # P1-1: ファンダメンタルズ由来の「基礎反応」と、空売り由来の「踏み上げ増幅」を分離する。
        # 旧式は surprise × fuel だったため、空売りが0なら好決算でも必ず0%になっていた。
        # Fuel は上昇の必要条件ではなく、正の基礎反応を増幅する補助係数としてのみ使う。
        base_response_multiplier = 0.05
        base_response_pct = s_score * elasticity * base_response_multiplier * macro_coef
        base_response_pct = min(max(base_response_pct, -15.0), 20.0)

        # Fuel は極端値の暴走を防ぐため1.5で上限。1.0なら基礎反応を最大60%上乗せ。
        # P1-55: Fuel計算不能は「Fuel=0という実測」ではなく、踏み上げ増幅だけを無効化。
        squeeze_strength = 0.0 if f_factor is None else min(max(f_factor, 0.0), 1.5)
        squeeze_bonus_pct = max(base_response_pct, 0.0) * squeeze_strength * 0.60

        expected_return_pct = base_response_pct + squeeze_bonus_pct

        # 最終レンジは従来と同じ。下落-15%、上昇+30%でキャップ。
        expected_return_pct = min(max(expected_return_pct, -15.0), 30.0)

        # P1-56: 現在値欠損を0円として目標株価0円にしない。騰落率は返し、価格だけNaN。
        try:
            price_raw = data.get("現在値")
            price_current = float(price_raw) if price_raw not in (None, "") else None
            if price_current is not None and (not math.isfinite(price_current) or price_current <= 0):
                price_current = None
        except Exception:
            price_current = None
        target_price = (price_current * (1.0 + expected_return_pct / 100.0)) if price_current is not None else float("nan")

        return {
            "expected_return_pct": round(expected_return_pct, 2),
            "target_price": round(target_price, 2) if math.isfinite(target_price) else float("nan"),
            "debug": {
                "S_score": round(s_score, 2),
                "Fuel": None if f_factor is None else round(f_factor, 2),
                "Elasticity": round(elasticity, 2),
                "BaseImpact": round(base_response_pct, 2),
                "SqueezeStrength": round(squeeze_strength, 2),
                "SqueezeBonus": round(squeeze_bonus_pct, 2)
            }
        }

# === [/INJECTED] ===========================================================

def _supplement_ai_positive_news_and_refresh_predictor(df_cand, rows, prefetched_codes=None):
    """P1-8 / P1-542: AI陽性銘柄のニュースを差分取得し、予想インパクトを整合的に再計算。

    P1-542:
    - ニュース列を先に実DataFrameへ書き込んでから予測器を回す旧実装では、途中1銘柄の
      predictor例外で「前半だけ新予測・後半は旧予測」という半端なin-memory状態になれた。
    - まず一時dictへニュースを注入して全対象を個別計算し、結果を集めてから一括反映する。
    - 個別predictor失敗は他銘柄を巻き込まない。ただしその銘柄だけは、ニュース取得後なのに
      旧/no-news予測をcurrentとして残さないよう予想インパクト/ターゲットをNaNへ明示無効化する。
    - ニュースbulk取得そのものが失敗した場合は呼出側へ例外を返し、半端な反映を一切行わない。

    Returns: (df_cand, rows, stats)
    """
    stats = {
        "ai_positive": 0,
        "supplement_targets": 0,
        "news_nonempty": 0,
        "predictor_refreshed": 0,
        "predictor_failed": 0,
    }
    if df_cand is None or getattr(df_cand, "empty", True) or not rows:
        return df_cand, rows, stats
    if "コード" not in df_cand.columns or "銘柄名" not in df_cand.columns:
        return df_cand, rows, stats

    def _c4(v):
        return canonical_code_for_db(v)

    prefetched = {_c4(c) for c in (prefetched_codes or set()) if _c4(c)}
    ai_codes = {
        _c4(r.get("コード"))
        for r in rows
        if _c4(r.get("コード")) and str(r.get("AI判定") or "").strip().startswith("★")
    }
    stats["ai_positive"] = len(ai_codes)

    target_codes = sorted(ai_codes - prefetched)
    stats["supplement_targets"] = len(target_codes)
    if not target_codes:
        return df_cand, rows, stats

    code_norm = df_cand["コード"].map(_c4)
    df_focus = df_cand.loc[code_norm.isin(target_codes), ["コード", "銘柄名"]].copy()
    if df_focus.empty:
        return df_cand, rows, stats

    # ここが失敗した場合は実データへ一切触らず例外伝播。
    news_ser, news_full_ser = kabutan_news_lines_bulk_for_dataframe(
        df_focus, code_col="コード", name_col="銘柄名", return_fulltext=True
    )
    df_focus["株探ニュース(3)"] = news_ser.fillna("") if hasattr(news_ser, "fillna") else news_ser
    df_focus["株探ニュース判定用"] = news_full_ser.fillna("") if hasattr(news_full_ser, "fillna") else news_full_ser
    news_map = {
        _c4(c): str(n or "")
        for c, n in zip(df_focus["コード"], df_focus["株探ニュース(3)"])
    }
    news_full_map = {
        _c4(c): str(n or "")
        for c, n in zip(df_focus["コード"], df_focus["株探ニュース判定用"])
    }
    stats["news_nonempty"] = sum(1 for v in news_map.values() if str(v).strip())

    # 先に一時rowで全対象を計算する。実df/rowsはまだ変更しない。
    predictor = StockSurprisePredictor()
    pred_map = {}
    pred_fail_codes = set()
    for idx in df_cand.index:
        c4 = _c4(df_cand.at[idx, "コード"])
        if c4 not in target_codes:
            continue
        row_for_pred = df_cand.loc[idx].to_dict()
        row_for_pred["株探ニュース(3)"] = news_map.get(c4, "")
        row_for_pred["株探ニュース判定用"] = news_full_map.get(c4, "")
        try:
            res = predictor.predict_price_increase(row_for_pred)
            pred_map[c4] = (res["expected_return_pct"], res["target_price"])
        except Exception as e:
            pred_fail_codes.add(c4)
            print(f"[AI-news][WARN] predictor refresh failed code={c4}: {e}")

    stats["predictor_refreshed"] = len(pred_map)
    stats["predictor_failed"] = len(pred_fail_codes)

    # 全計算が終わってからcurrentニュースと予測結果を反映。
    if "株探ニュース(3)" not in df_cand.columns:
        df_cand["株探ニュース(3)"] = ""
    if "株探ニュース判定用" not in df_cand.columns:
        df_cand["株探ニュース判定用"] = ""
    if "予想インパクト_pct" not in df_cand.columns:
        df_cand["予想インパクト_pct"] = float("nan")
    if "予測ターゲット価格" not in df_cand.columns:
        df_cand["予測ターゲット価格"] = float("nan")

    for idx in df_cand.index:
        c4 = _c4(df_cand.at[idx, "コード"])
        if c4 not in news_map:
            continue
        df_cand.at[idx, "株探ニュース(3)"] = news_map[c4]
        df_cand.at[idx, "株探ニュース判定用"] = news_full_map.get(c4, "")
        if c4 in pred_map:
            df_cand.at[idx, "予想インパクト_pct"] = pred_map[c4][0]
            df_cand.at[idx, "予測ターゲット価格"] = pred_map[c4][1]
        elif c4 in pred_fail_codes:
            df_cand.at[idx, "予想インパクト_pct"] = float("nan")
            df_cand.at[idx, "予測ターゲット価格"] = float("nan")

    for r in rows:
        c4 = _c4(r.get("コード"))
        if c4 in news_map:
            r["株探ニュース(3)"] = news_map[c4]
            r["株探ニュース判定用"] = news_full_map.get(c4, "")
        if c4 in pred_map:
            r["予想インパクト_pct"] = pred_map[c4][0]
            r["予測ターゲット価格"] = pred_map[c4][1]
        elif c4 in pred_fail_codes:
            r["予想インパクト_pct"] = float("nan")
            r["予測ターゲット価格"] = float("nan")

    return df_cand, rows, stats


def _load_institution_short_summary(conn: sqlite3.Connection) -> pd.DataFrame:
    """
    P1-20: 機関空売りを「銘柄の最新日に更新した機関だけ」ではなく、
    code × institution_name ごとの最新報告残高を持ち越して集計する。

    重要:
    空売り公開データには「報告義務消失（0.5%未満）」の最終行が残ることがある。
    その行を永久に現在残高へ持ち越さないよう、以下の優先順位で現存判定する。
      1) 備考系カラムに「報告義務消失」等があれば除外
      2) 残高割合系カラムがあれば 0.5% 以上だけを集計
      3) screener.発行済株式数 があれば shares / 発行済株式数 >= 0.5% だけを集計
      4) 上記が無ければ shares > 0 を暫定採用（DB情報不足時の後方互換）

    - 機関空売り合計株数: 各機関の「現存と判定できる」最新報告残高の合計
    - 空売り更新日:      その銘柄で最も新しい報告日
    - 本日の増減合計株数: 上記の最新報告日に更新された機関の shares_change だけを合計
      （最新日に shares_change が全件NULLならNULLを保持）
    - 主要機関の動き:    現存機関の最新残高・最新報告日を表示用に連結
    """
    import numpy as np
    import pandas as pd

    # 必須列 + DBに存在すれば利用する補助列を検出。
    table_info = conn.execute("PRAGMA table_info(institution_short_sales)").fetchall()
    cols = {str(r[1]) for r in table_info}
    required = {"code", "calc_date", "institution_name", "shares", "shares_change"}
    if not required.issubset(cols):
        missing = sorted(required - cols)
        raise RuntimeError(f"institution_short_sales missing columns: {missing}")

    ratio_candidates = ["ratio", "short_ratio", "position_ratio", "残高割合", "残高比率"]
    note_candidates = ["note", "remarks", "remark", "備考"]
    ratio_col = next((c for c in ratio_candidates if c in cols), None)
    note_col = next((c for c in note_candidates if c in cols), None)

    select_cols = ["code", "calc_date", "institution_name", "shares", "shares_change"]
    if ratio_col:
        select_cols.append(f'"{ratio_col}" AS _short_ratio')
    if note_col:
        select_cols.append(f'"{note_col}" AS _short_note')
    raw = pd.read_sql_query(
        "SELECT rowid AS _rowid, " + ", ".join(select_cols) + " FROM institution_short_sales",
        conn,
    )
    if raw.empty:
        return pd.DataFrame(columns=[
            "code", "空売り更新日", "機関空売り合計株数",
            "本日の増減合計株数", "主要機関の動き"
        ])

    # P1-187: 機関空売りのコードを英数字対応の共通キーへ。
    raw["_raw_code"] = raw["code"].astype(str)
    raw["code"] = raw["code"].map(canonical_code_for_db)
    raw["calc_date"] = raw["calc_date"].astype(str)
    raw["_calc_date_dt"] = pd.to_datetime(raw["calc_date"], errors="coerce")
    # P1-477: 公開空売り残高の報告日は実績日。解析不能/未来日を現在残高へ採用せず、
    # 比較・表示用文字列もISOへ統一して表記ゆれのlexical maxを排除する。
    _short_today = pd.Timestamp(date.fromisoformat(_today_jst()))
    raw = raw[raw["_calc_date_dt"].notna() & (raw["_calc_date_dt"].dt.normalize() <= _short_today)].copy()
    raw["calc_date"] = raw["_calc_date_dt"].dt.strftime("%Y-%m-%d")
    raw["shares"] = pd.to_numeric(raw["shares"], errors="coerce")
    raw["shares_change"] = pd.to_numeric(raw["shares_change"], errors="coerce")

    # P1-464: alias統合後に同一機関・同日が複数行残る場合、SQLの偶然の順序で
    # 現在残高を選ばない。shares有効→canonical表記→新rowidを優先し、
    # その上で機関ごとの最新有効報告日を採用する。
    raw["_quality"] = raw[["shares", "shares_change"]].notna().sum(axis=1)
    raw["_canon_match"] = (raw["_raw_code"].str.strip() == raw["code"]).astype(int)
    raw = (raw[raw["code"].astype(str).str.len() > 0]
           .sort_values(["code", "institution_name", "_calc_date_dt", "_quality", "_canon_match", "_rowid"],
                        kind="mergesort", na_position="first")
           .drop_duplicates(["code", "institution_name", "_calc_date_dt"], keep="last"))
    latest = (raw.sort_values(["code", "institution_name", "_calc_date_dt", "_rowid"],
                              kind="mergesort", na_position="first")
                 .groupby(["code", "institution_name"], dropna=False, as_index=False)
                 .tail(1).copy())

    # 発行済株式数を取得（存在しない/未取得ならNaN）。
    outstanding_map = {}
    try:
        sc_cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
        if {"コード", "発行済株式数"}.issubset(sc_cols):
            sc = pd.read_sql_query('SELECT rowid AS _rowid, コード, 発行済株式数 FROM screener', conn)
            if not sc.empty:
                # P1-188: 発行済株式数とのJOINも共通キーへ。
                sc["_raw_code"] = sc["コード"].astype(str)
                sc["code"] = sc["コード"].map(canonical_code_for_db)
                sc["発行済株式数"] = pd.to_numeric(sc["発行済株式数"], errors="coerce")
                # P1-470: legacy screener aliasが残っていてもSELECTの偶然の順序で
                # 発行済株式数を選ばない。有効値→canonical表記→新rowidを優先する。
                sc["_has_issued"] = sc["発行済株式数"].notna().astype(int)
                sc["_canon_match"] = (sc["_raw_code"].str.strip() == sc["code"]).astype(int)
                sc = (sc[sc["code"].astype(str).str.len() > 0]
                      .sort_values(["code", "_has_issued", "_canon_match", "_rowid"], kind="stable")
                      .drop_duplicates("code", keep="last"))
                outstanding_map = sc.set_index("code")["発行済株式数"].to_dict()
    except Exception:
        outstanding_map = {}
    latest["_issued"] = latest["code"].map(outstanding_map)

    active = latest["shares"].notna() & (latest["shares"] > 0)

    # 明示的な報告義務消失/解消を最優先で除外。
    if "_short_note" in latest.columns:
        note = latest["_short_note"].fillna("").astype(str)
        inactive_note = note.str.contains(r"報告義務消失|義務消失|報告義務.*解消|ポジション解消", regex=True)
        active &= ~inactive_note

    # 残高割合があれば0.5%閾値で判定。DBによって「0.50」型と「0.005」型があるためスケール推定。
    if "_short_ratio" in latest.columns:
        ratio = pd.to_numeric(latest["_short_ratio"], errors="coerce")
        finite = ratio[np.isfinite(ratio)]
        ratio_threshold = 0.005 if (not finite.empty and finite.abs().quantile(0.90) <= 0.20) else 0.5
        known_ratio = ratio.notna()
        active = active.where(~known_ratio, active & (ratio >= ratio_threshold))
    else:
        # 残高割合が無ければ発行済株式数から0.5%を判定。
        issued = pd.to_numeric(latest["_issued"], errors="coerce")
        known_issued = issued.notna() & (issued > 0)
        ratio_from_shares = latest["shares"] / issued
        active = active.where(~known_issued, active & (ratio_from_shares >= 0.005))

    latest["_active_public_position"] = active.fillna(False)

    rows = []
    for code, g in latest.groupby("code", sort=False):
        g = g.copy()
        latest_date = g["calc_date"].max()
        active_g = g[g["_active_public_position"]]
        total_shares = float(active_g["shares"].fillna(0).sum()) if not active_g.empty else 0.0

        day_g = g[g["calc_date"] == latest_date]
        day_changes = day_g["shares_change"].dropna()
        day_change_total = float(day_changes.sum()) if not day_changes.empty else np.nan

        movement_parts = []
        for _, r in active_g.sort_values(["shares", "institution_name"], ascending=[False, True]).iterrows():
            shares = int(round(float(r["shares"]))) if pd.notna(r["shares"]) else 0
            part = f"{r['institution_name']}({shares}株, {r['calc_date']}"
            if r["calc_date"] == latest_date and pd.notna(r["shares_change"]):
                chg = int(round(float(r["shares_change"])))
                part += f", {chg:+d}株"
            part += ")"
            movement_parts.append(part)

        # 最新日に報告義務消失等が起きた機関は、当日変化の説明用にだけ表示する。
        inactive_day = day_g[~day_g["_active_public_position"]]
        for _, r in inactive_day.iterrows():
            if pd.notna(r["shares_change"]):
                chg = int(round(float(r["shares_change"])))
                movement_parts.append(f"{r['institution_name']}(対象外, {r['calc_date']}, {chg:+d}株)")

        rows.append({
            "code": code,
            "空売り更新日": latest_date,
            "機関空売り合計株数": total_shares,
            "本日の増減合計株数": day_change_total,
            "主要機関の動き": " / ".join(movement_parts) if movement_parts else "-",
        })

    return pd.DataFrame(rows)


def _load_institution_short_snapshot_status(
    conn: sqlite3.Connection,
    snapshot_date: str | None = None,
) -> pd.DataFrame:
    """P3-45: 当日crawlの成否を公開残高集計と分けて読む。

    institution_short_snapshot.has_shortはproducerがページ上の明細または
    明示的な「空売りなし」を確認できたかの取得契約。機関ごとの最新報告から
    算出する現在の公開残高は _load_institution_short_summary() の責務である。
    この2種類を1つの「空売りあり/なし」に潰さない。
    """
    import numpy as np
    import pandas as pd

    out_cols = [
        "code", "機関空売りsnapshot日", "機関空売り取得状態",
        "機関空売りsnapshot_has_short", "機関空売りsnapshot明細数",
        "機関空売り確認時刻",
    ]
    tables = {
        str(r[0])
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }
    if "institution_short_snapshot" not in tables:
        return pd.DataFrame(columns=out_cols)

    cols = {
        str(r[1])
        for r in conn.execute("PRAGMA table_info(institution_short_snapshot)").fetchall()
    }
    required = {
        "code", "snapshot_date", "crawl_success", "has_short", "detail_count", "checked_at"
    }
    if not required.issubset(cols):
        missing = sorted(required - cols)
        raise RuntimeError(f"institution_short_snapshot missing columns: {missing}")

    target_date = str(snapshot_date or _today_jst())[:10]
    raw = pd.read_sql_query(
        """
        SELECT rowid AS _rowid, code, snapshot_date, crawl_success,
               has_short, detail_count, checked_at
        FROM institution_short_snapshot
        WHERE snapshot_date=?
        """,
        conn,
        params=[target_date],
    )
    if raw.empty:
        return pd.DataFrame(columns=out_cols)

    raw["_raw_code"] = raw["code"].astype(str)
    raw["code"] = raw["code"].map(canonical_code_for_db)
    raw["_checked_sort"] = pd.to_datetime(raw["checked_at"], errors="coerce")
    raw["_canonical"] = (raw["_raw_code"].str.strip() == raw["code"]).astype(int)
    raw = (
        raw[raw["code"].astype(str).str.len() > 0]
        .sort_values(
            ["code", "_checked_sort", "_canonical", "_rowid"],
            kind="mergesort",
            na_position="first",
        )
        .drop_duplicates("code", keep="last")
    )
    success = pd.to_numeric(raw["crawl_success"], errors="coerce").eq(1)
    raw["機関空売りsnapshot日"] = raw["snapshot_date"].astype(str)
    raw["機関空売り取得状態"] = np.where(success, "成功", "失敗")
    raw["機関空売りsnapshot_has_short"] = pd.to_numeric(
        raw["has_short"], errors="coerce"
    )
    raw["機関空売りsnapshot明細数"] = pd.to_numeric(
        raw["detail_count"], errors="coerce"
    )
    raw["機関空売り確認時刻"] = raw["checked_at"].astype(str)
    return raw[out_cols].reset_index(drop=True)


def _derive_institution_short_semantics(df: pd.DataFrame) -> pd.DataFrame:
    """P3-45: 履歴・現在公開残高・取得状態を明示列にする。"""
    import numpy as np
    import pandas as pd

    if df is None:
        return df
    out = df.copy()
    idx = out.index

    acquisition = (
        out["機関空売り取得状態"].fillna("未取得").astype(str).str.strip()
        if "機関空売り取得状態" in out.columns
        else pd.Series("未取得", index=idx, dtype=object)
    )
    acquisition = acquisition.where(acquisition.isin(["成功", "失敗"]), "未取得")
    out["機関空売り取得状態"] = acquisition

    report_date = (
        out["空売り更新日"].fillna("").astype(str).str.strip().str.lower()
        if "空売り更新日" in out.columns
        else pd.Series("", index=idx, dtype=object)
    )
    history_known = ~report_date.isin(["", "-", "nan", "none", "n/a", "na"])
    out["機関空売り履歴状態"] = np.where(history_known, "あり", "なし")

    total = pd.to_numeric(
        out["機関空売り合計株数"]
        if "機関空売り合計株数" in out.columns
        else pd.Series(np.nan, index=idx),
        errors="coerce",
    )
    snap_has_short = pd.to_numeric(
        out["機関空売りsnapshot_has_short"]
        if "機関空売りsnapshot_has_short" in out.columns
        else pd.Series(np.nan, index=idx),
        errors="coerce",
    )
    success = acquisition.eq("成功")
    explicit_no_history = success & snap_has_short.eq(0)
    # 明示「ページに空売り明細なし」は現在公開残高0株として扱える。
    total = total.mask(explicit_no_history & total.isna(), 0.0)
    out["機関空売り合計株数"] = total

    current = pd.Series("不明", index=idx, dtype=object)
    current.loc[success & total.gt(0)] = "あり"
    current.loc[success & total.le(0)] = "なし"
    out["機関空売り現在状態"] = current

    def _display(i):
        cur = current.at[i]
        hist = out.at[i, "機関空売り履歴状態"]
        acq = acquisition.at[i]
        if acq == "成功":
            return f"現在:{cur} / 履歴:{hist} / 取得:成功"
        return f"現在:不明 / 履歴:{hist} / 取得:{acq}"

    out["機関空売り状態"] = [_display(i) for i in idx]
    return out


def _overlay_latest_prices_canonical(df_cand: pd.DataFrame, conn: sqlite3.Connection) -> pd.DataFrame:
    """P1-298: latest_prices由来列をcanonical codeの最新1行から付与する。"""
    if df_cand is None or getattr(df_cand, "empty", True) or "コード" not in df_cand.columns:
        return df_cand
    try:
        # P1-580: export overlayもrun mode基準日まで。PREOPENは前営業日。
        _overlay_cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
        # P1-422: latest_pricesに旧future/weekend行が残っていてもHTMLへ採用しない。
        _lp_overlay = pd.read_sql_query(
            "SELECT rowid AS _rowid, * FROM latest_prices WHERE 日付 IS NULL OR date(日付)<=date(?)",
            conn, params=[_overlay_cutoff])
        if _lp_overlay.empty or "コード" not in _lp_overlay.columns:
            return df_cand
        # P1-457: cutoff内でも土日/祝日legacy rowはHTMLへ採用しない。
        if "日付" in _lp_overlay.columns:
            _lp_extra = _load_extra_closed(EXTRA_CLOSED_PATH)
            _lp_dt = pd.to_datetime(_lp_overlay["日付"], errors="coerce")
            _lp_valid_map = {}
            for _d in pd.Series(_lp_dt.dropna().dt.date.unique()).tolist():
                _lp_valid_map[_d] = not is_jp_market_holiday(_d, _lp_extra)
            _lp_keep = _lp_dt.isna() | _lp_dt.dt.date.map(lambda _d: _lp_valid_map.get(_d, False))
            _lp_overlay = _lp_overlay[_lp_keep].copy()
        if _lp_overlay.empty:
            return df_cand
        _lp_overlay["_key"] = _lp_overlay["コード"].map(canonical_code_for_db)
        if "日付" in _lp_overlay.columns:
            _lp_overlay["_date_sort"] = pd.to_datetime(_lp_overlay["日付"], errors="coerce")
        else:
            _lp_overlay["_date_sort"] = pd.NaT
        _lp_overlay = (_lp_overlay[_lp_overlay["_key"] != ""]
                       .sort_values(["_key", "_date_sort", "_rowid"], kind="stable")
                       .drop_duplicates("_key", keep="last")
                       .set_index("_key"))
        _df_keys = df_cand["コード"].map(canonical_code_for_db)
        _lp_fields = {
            "S_High_Status":"S高", "Res_HH":"直近高値90日", "Res_Zone":"抵抗帯中心",
            "Res_Zone_Touches":"抵抗タッチ数", "Res_Zone_Last":"抵抗最終日", "Res_Round":"抵抗節目",
            "Res_Round_Step":"抵抗節目刻み", "Res_Round_Near":"抵抗節目近", "Res_Line_Today":"抵抗線今日",
            "Res_Line_R2":"抵抗R2", "Res_Nearest":"最寄り抵抗", "Sup_LL":"直近安値90日",
            "Sup_Zone":"支持帯中心", "Sup_Zone_Touches":"支持タッチ数", "Sup_Zone_Last":"支持最終日",
            "Sup_Round":"支持節目", "Sup_Round_Step":"支持節目刻み", "Sup_Round_Near":"支持節目近",
            "Sup_Line_Today":"支持線今日", "Sup_Line_R2":"支持R2", "Sup_Nearest":"最寄り支持",
            # P1-601: P1-298でraw latest_prices JOINをcanonical overlayへ置換した際に
            # 自前計算ATR_14だけoverlay対象から落ちていた。Algo_VolTargetはATR14を読むため、
            # stale時NULL化済みのcurrent ATRを正規名へ明示転送する。
            "ATR_14":"ATR14",
        }
        out = df_cand.copy()
        for _src, _dst in _lp_fields.items():
            if _src in _lp_overlay.columns:
                out[_dst] = _df_keys.map(_lp_overlay[_src].to_dict())
        return out
    except Exception as _e:
        print(f"[export][ERROR] latest_prices canonical overlay failed: {_e}")
        # P1-422: 支持抵抗等を欠落させたまま「正常な最新HTML」として公開しない。
        raise RuntimeError("latest_prices dashboard overlay failed") from _e


# P1-525/P1-526: 外部shindenが当日full snapshotを完成できなかった場合、
# DBに残る前日以前のスコアを「今日の判定」としてHTML/LLMへ出さない。
_SHINDEN_SNAPSHOT_COLS = [
    "シンデン総合スコア", "シンデン判定", "予想ギャップスコア", "予想信頼性スコア",
    "予想根拠スコア", "予想可視性スコア", "未織り込みスコア",
    "予想達成履歴信頼度", "予想達成履歴期数",
    "営業益予想ギャップ_pct", "EPS予想ギャップ_pct",
    "予想平均達成率_pct", "予想最低達成率_pct",
    "シンデン判定理由", "シンデン需給注釈",
    "次期転換期待スコア", "次期転換判定", "反動余地スコア", "履歴土台スコア",
    "根拠可視性スコア", "次期未織込スコア", "回復兆候スコア", "次期転換理由",
]

def _shinden_snapshot_is_current(conn) -> bool:
    try:
        row = conn.execute(
            "SELECT value FROM shinden_runtime_state WHERE key='last_full_day' LIMIT 1"
        ).fetchone()
        db_current = bool(row and str(row[0] or "").strip() == _today_jst())
        if not db_current:
            return False
        if not EXTERNAL_JOBS_REQUIRED:
            return True

        # P3-41: 「今日fullした」だけでは朝の値を夕方までfresh扱いできる。
        # 場中/EODは直近LIVEのshinden full成功も要求し、失敗時は専門列だけmaskする。
        mode = _auto_run_mode()
        jobs = ["live_shinden_full"] if mode in ("MIDDAY", "EOD") else ["morning_shinden_full", "live_shinden_full"]
        _fresh_dts = []
        for job in jobs:
            st = _external_job_state(job)
            if str(st.get("status") or "") != "success":
                continue
            ts = st.get("last_finished_at") or st.get("last_success_at")
            if not ts:
                continue
            try:
                _fresh_dts.append(datetime.fromisoformat(str(ts)))
            except Exception:
                continue
        if not _fresh_dts:
            return False
        dt = max(_fresh_dts)
        max_age = 45 if mode == "MIDDAY" else (180 if mode == "EOD" else 12 * 60)
        age_min = (datetime.now() - dt).total_seconds() / 60.0
        # PREOPENは08:00のMORNING/LIVEどちらが先に成功しても同じ当日fullを採用する。
        if mode == "PREOPEN" and dt.date() != datetime.now().date():
            return False
        return 0 <= age_min <= max_age
    except Exception:
        return False


def _mask_stale_shinden_snapshot(df: pd.DataFrame, conn) -> bool:
    current = _shinden_snapshot_is_current(conn)
    if current or df is None or df.empty:
        return current
    for _col in _SHINDEN_SNAPSHOT_COLS:
        if _col in df.columns:
            df[_col] = None
    return False


def _attach_shinden_score_semantics(df: pd.DataFrame) -> pd.DataFrame:
    """P3-47: シンデンの正式点と履歴不足時の参考点を最終出力で分離する。

    外部producerは履歴0〜1期の行にも比較材料として数値を保存し、判定を
    ``参考：...`` にしている。互換列 ``シンデン総合スコア`` を上書きすると
    shinden_logicや既存DBとの契約を壊すため、正式/参考の派生列を追加するだけにする。

    決算直後は履歴0期でもproducerが正式ラベルを付ける場合があるため、履歴期数ではなく
    authoritativeな ``シンデン判定`` の「参考」接頭辞で区分する。ラベルまたは点数が
    欠ける（stale maskを含む）行は、弱い0点ではなく ``不明`` とする。
    """
    if df is None:
        return df

    out = df.copy()
    total = (
        pd.to_numeric(out["シンデン総合スコア"], errors="coerce")
        if "シンデン総合スコア" in out.columns
        else pd.Series(np.nan, index=out.index, dtype=float)
    ).replace([np.inf, -np.inf], np.nan)
    label = (
        out["シンデン判定"].fillna("").astype(str).str.strip()
        if "シンデン判定" in out.columns
        else pd.Series("", index=out.index, dtype=object)
    )

    has_score = total.notna()
    is_reference = has_score & label.str.startswith("参考")
    is_formal = has_score & label.ne("") & label.ne("-") & ~label.str.startswith("参考")

    out["シンデン評価区分"] = np.select(
        [is_formal, is_reference],
        ["正式", "参考"],
        default="不明",
    )
    out["シンデン正式スコア"] = total.where(is_formal, np.nan)
    out["シンデン参考スコア"] = total.where(is_reference, np.nan)

    # ダッシュボードの降順では正式点を必ず参考点より先にする。
    # 不明はNaNのままにし、汎用sorterの「欠損は末尾」を利用する。
    out["シンデンソート値"] = pd.Series(
        np.where(is_formal, 1000.0 + total, np.where(is_reference, total, np.nan)),
        index=out.index,
        dtype=float,
    )
    return out

def _dedupe_final_snapshot_logical(df: pd.DataFrame) -> pd.DataFrame:
    """P3-33: display/LLM snapshotをlogical code 1銘柄1行へ正規化する。

    DB自体は変更しない。aliasが併存した場合は canonical表記そのもののraw行
    （例: 7203 を 7203.0 より優先）を選び、それでも複数なら元の表示順を維持する。
    code欠損/空欄は勝手に同一銘柄扱いせず各行を保持する。
    """
    if df is None or getattr(df, "empty", True) or "コード" not in df.columns:
        return df

    out = df.copy()
    _order = pd.Series(np.arange(len(out)), index=out.index, dtype=int)
    _raw = out["コード"]
    _logical = _raw.map(canonical_code_for_db)

    def _raw_text(v):
        if v is None:
            return ""
        try:
            if pd.isna(v):
                return ""
        except Exception:
            pass
        return str(v).strip().upper()

    _raw_norm = _raw.map(_raw_text)
    _logical_norm = _logical.map(_raw_text)
    _valid = _logical_norm.ne("")
    if not bool(_valid.any()):
        return out

    # 空codeは各行固有キーにして保持する。
    _key = _logical_norm.astype(object).copy()
    for _pos, _idx in enumerate(out.index):
        if not bool(_valid.loc[_idx]):
            _key.loc[_idx] = f"__MISSING_CODE_ROW_{_pos}__"

    _work = pd.DataFrame({
        "_idx": list(out.index),
        "_key": _key.to_numpy(),
        "_canonical_exact": (_raw_norm == _logical_norm).astype(int).to_numpy(),
        "_order": _order.to_numpy(),
    })
    _picked = (
        _work.sort_values(["_key", "_canonical_exact", "_order"], ascending=[True, False, True], kind="stable")
             .drop_duplicates("_key", keep="first")
             .sort_values("_order", kind="stable")
    )
    if len(_picked) == len(out):
        return out
    return out.loc[_picked["_idx"].tolist()].copy()


def phase_export_html_dashboard_offline(conn, html_path, template_dir="templates",
                                        include_log: bool=False, log_limit: int=2000):
    """HTMLダッシュボード出力（1ファイル完結版：二重管理を廃止した高速一本化モデル）"""

    try:
        from zoneinfo import ZoneInfo
    except Exception:
        ZoneInfo = None

    def _p(msg): print(f"[exportL] {msg}")

    def dumps_json_clean(obj, **kw):
        # P1-381: export内の同名ローカル関数がグローバル安全版をshadowしていた。
        # NaN/Infinityを含むネスト値でも、必ず共通sanitizerを通す。
        kw.setdefault("ensure_ascii", False)
        kw.setdefault("separators", (",", ":"))
        kw.setdefault("default", str)
        return globals()["dumps_json_clean"](obj, **kw)

    # 0) 事前
    _p("enter: phase_export_html_dashboard_offline")

    def _safe_jsonable(v):
        if v is None: return None
        try:
            if isinstance(v, (date, datetime)): return str(v)
        except Exception: pass
        try:
            if isinstance(v, float) and (np.isnan(v) or np.isinf(v)): return None
        except Exception: pass
        return v

    def _records_safe(df: pd.DataFrame):
        if df is None or df.empty: return []
        out = []
        for rec in df.to_dict("records"):
            out.append({k: _safe_jsonable(v) for k, v in rec.items()})
        return out

    # 1) latest_prices のコード型を安全化
    _ensure_latest_prices_code_col(conn)
    _p("done: _ensure_latest_prices_code_col")

    # 1.5) P1-451: テーマsnapshotは初動スコア計算前の日次フェーズで更新済み。
    # export後追い更新で「表示テーマは今日・スコアは昨日」の時間差を作らない。
    _ensure_theme_tables(conn)
    _p("theme: tables ready; snapshot refreshed before scoring")

    # 2) V5は直前のphase_resistance_updateで一括計算・保存済み。
    # 旧実装はここで約4500銘柄へ個別SQLを再発行し、同じ計算を重複していた。
    # v5_rowsは後段で未使用のため、export内の再計算・JSON化を完全に廃止する。
    _v5_current_run_ok = True
    _p("V5: reuse phase_resistance_update snapshot (duplicate collect skipped)")

    # 3) メインデータの取得（★ df_cand に一本化）
    t = time.perf_counter()
    _p("SQL: df_cand start")

    # P1-647: 推奨ヒステリシスは「前回runのraw比率」を読む設計だが、
    # 旧実装は推奨比率_rawをDBへ保存せず、legacy列が存在する環境では何日も前の値を
    # 前回値として使い続け、新規DBではヒステリシス自体が働かなかった。
    # 専用更新時刻を持つ永続状態へ明示化し、更新時刻の無いlegacy raw値は無効扱いにする。
    _recommendation_state_cols_changed = False
    _rec_schema_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    for _col, _ddl in (("推奨比率_raw", "REAL"), ("推奨比率更新日時", "TEXT")):
        if _col not in _rec_schema_cols:
            conn.execute(f'ALTER TABLE screener ADD COLUMN "{_col}" {_ddl}')
            _rec_schema_cols.add(_col)
            _recommendation_state_cols_changed = True
    if _recommendation_state_cols_changed:
        conn.commit()

    # P1-298: latest_pricesとのraw SQL JOINを廃止。7203/7203.0の不一致や
    # legacy重複による候補行の複製を避け、下段のcanonical overlayへ一本化する。
    df_cand = pd.read_sql_query("""
      SELECT s.*
      FROM screener s
      ORDER BY COALESCE(時価総額億円,0) DESC, COALESCE(出来高,0) DESC, コード
    """, conn)

    # P1-647: markerの無いraw値はこの実装が保存した「前回公開snapshot」ではない。
    # legacy DBの偶然残った推奨比率_rawをヒステリシスへ混ぜない。
    if "推奨比率_raw" in df_cand.columns:
        _rec_state_marker = (
            df_cand["推奨比率更新日時"].fillna("").astype(str).str.strip()
            if "推奨比率更新日時" in df_cand.columns
            else pd.Series("", index=df_cand.index, dtype=object)
        )
        df_cand.loc[_rec_state_marker.eq(""), "推奨比率_raw"] = np.nan

    # P1-298: latest_pricesはraw SQL JOINせず、canonical overlayで付与する。
    df_cand = _overlay_latest_prices_canonical(df_cand, conn)

    # P1-632: current-runで再計算しているATR正本は latest_prices.ATR_14 -> ATR14（価格単位）。
    # legacyの ATR14_PCT / ATR14% はこのファイル内でcurrent更新されないため、存在していても
    # 3Algo/三角/仕込み計算で優先すると旧runのボラを今回値として使える。
    # current ATR14とcurrent価格から%を再生成し、ATR14がNULLなら%もNULLへ戻す。
    if "ATR14" in df_cand.columns:
        _atr_abs_current = pd.to_numeric(df_cand["ATR14"], errors="coerce")
        _atr_px_current = (
            pd.to_numeric(df_cand["現在値"], errors="coerce")
            if "現在値" in df_cand.columns
            else pd.Series(np.nan, index=df_cand.index, dtype=float)
        )
        _atr_pct_current = pd.Series(
            np.where(
                (_atr_px_current > 0) & _atr_abs_current.notna() & np.isfinite(_atr_abs_current),
                (_atr_abs_current / _atr_px_current) * 100.0,
                np.nan,
            ),
            index=df_cand.index,
            dtype=float,
        )
        df_cand["ATR14_PCT"] = _atr_pct_current
        df_cand["ATR14%"] = _atr_pct_current

    # P1-633: 営利対時価は営業利益が同じでも時価総額が場中に動けばcurrent比率が変わる。
    # MIDDAYは時価総額をquoteで更新する一方、EOD日次の保存済み営利対時価をそのまま読むと
    # Algo_Factorだけ前日比率になれる。旧DBの営利対時価_pctも同じcurrent比率へ統一する。
    if "営業利益" in df_cand.columns and "時価総額億円" in df_cand.columns:
        _op_income_current = pd.to_numeric(df_cand["営業利益"], errors="coerce")
        _mcap_current = pd.to_numeric(df_cand["時価総額億円"], errors="coerce")
        _op_ratio_current = pd.Series(
            np.where(
                _op_income_current.notna() & np.isfinite(_op_income_current) &
                _mcap_current.notna() & np.isfinite(_mcap_current) & (_mcap_current > 0),
                (_op_income_current / _mcap_current) * 100.0,
                np.nan,
            ),
            index=df_cand.index,
            dtype=float,
        )
        df_cand["営利対時価"] = _op_ratio_current
        # legacy schemaに存在する場合だけでなく、3Algo入力名を一意化するため同値で作成する。
        df_cand["営利対時価_pct"] = _op_ratio_current

    # P1-646: YahooのpriceToBookは財務batchの最大7日cache値だが、PBRは株価連動指標。
    # current価格とlast-known公表BPSから毎export再生成し、今日の株価に古いPBRを併記しない。
    if "現在値" in df_cand.columns and "BPS" in df_cand.columns:
        _pbr_px = pd.to_numeric(df_cand["現在値"], errors="coerce")
        _pbr_bps = pd.to_numeric(df_cand["BPS"], errors="coerce")
        df_cand["PBR"] = pd.Series(
            np.where(
                _pbr_px.notna() & np.isfinite(_pbr_px) & (_pbr_px > 0) &
                _pbr_bps.notna() & np.isfinite(_pbr_bps) & (_pbr_bps > 0),
                _pbr_px / _pbr_bps,
                np.nan,
            ),
            index=df_cand.index,
            dtype=float,
        )

    # P1-645: 増資リスク/増資スコア/増資理由は現行プログラム内にcurrent writerが無い。
    # 元祖版のflags_rows書込もSQL 4値に対し3値tupleで成立しておらず、legacy DBの非NULL値を
    # current三角Safetyへ使うと根拠のない旧スコアが残留する。今回snapshotでは明示的に無効化し、
    # 実際の希薄化イベント警告は offerings_events -> offer_codes のauthoritative経路へ一本化する。
    for _legacy_dil_col in ("増資リスク", "増資スコア", "増資理由"):
        if _legacy_dil_col in df_cand.columns:
            df_cand[_legacy_dil_col] = None

    # P1-534: 今回runでV5支持抵抗の再計算に失敗した場合、latest_pricesに残る
    # 前回正常値を「今回の支持抵抗」として再表示しない。DBのlast-good値は保持し、
    # このexport snapshotだけ不明(NULL)として扱う。
    if not _v5_current_run_ok:
        for _v5_dst in (
            "直近高値90日", "抵抗帯中心", "抵抗タッチ数", "抵抗最終日",
            "抵抗節目", "抵抗節目刻み", "抵抗節目近", "抵抗線今日", "抵抗R2", "最寄り抵抗",
            "直近安値90日", "支持帯中心", "支持タッチ数", "支持最終日",
            "支持節目", "支持節目刻み", "支持節目近", "支持線今日", "支持R2", "最寄り支持",
        ):
            if _v5_dst in df_cand.columns:
                df_cand[_v5_dst] = None
        _p("V5: current calculation unavailable -> stale support/resistance masked")

    # P3-33: DBに7203/7203.0等のaliasが併存していても、display/LLM/model snapshotは
    # 1 logical code=1行に固定する。canonical表記raw行を優先してからコード値自体を正規化する。
    _before_logical_dedupe = len(df_cand)
    df_cand = _dedupe_final_snapshot_logical(df_cand)
    if len(df_cand) != _before_logical_dedupe:
        _p(f"snapshot: logical alias dedupe {_before_logical_dedupe}->{len(df_cand)}")

    # P1-189: HTML出力の全下流JOINが同じキーを使うよう入口でcanonical化。
    if "コード" in df_cand.columns:
        df_cand["コード"] = df_cand["コード"].map(canonical_code_for_db)

    # P1-525: shinden外部処理が当日のfull更新を完了していなければ、
    # 前日以前の保存済みスコアを今回snapshotから明示的に隠す。DB自体は保持する。
    if not _mask_stale_shinden_snapshot(df_cand, conn):
        _p("shinden: current full snapshot unavailable -> stale score columns masked")
    
    if "raw_fin_json" in df_cand.columns:
        df_cand.drop(columns=["raw_fin_json"], inplace=True)

    df_cand = ensure_news_cols(df_cand)
    _p(f"SQL: df_cand done shape={df_cand.shape} dt={( time.perf_counter()-t):.2f}s")

    # ==============================================================================
    # P1-20: institution_short_sales は「銘柄の最新日だけ」ではなく、
    # 各機関の最新報告残高を持ち越して現在の公開残高合計を作る。
    # ==============================================================================
    try:
        df_karauri = _load_institution_short_summary(conn)
        df_karauri_status = _load_institution_short_snapshot_status(conn)
        # P1-616: df_candはSELECT s.*なので、旧screenerに空売り集計列が残っている場合がある。
        # そのままcurrent summaryをmergeすると同名列が _x/_y 化して直後の参照が壊れ、
        # summaryが0件なら前回runの機関空売り値をそのままcurrent risk入力へ持ち越してしまう。
        # current集計をauthoritative snapshotとし、旧表示列を必ず捨ててから付け直す。
        _inst_snapshot_cols = [
            "空売り更新日", "機関空売り合計株数", "本日の増減合計株数", "主要機関の動き",
            "機関空売りsnapshot日", "機関空売り取得状態",
            "機関空売りsnapshot_has_short", "機関空売りsnapshot明細数",
            "機関空売り確認時刻", "機関空売り履歴状態",
            "機関空売り現在状態", "機関空売り状態",
        ]
        df_cand = df_cand.drop(
            columns=[_c for _c in _inst_snapshot_cols if _c in df_cand.columns],
            errors="ignore",
        )
        if "code" in df_karauri.columns:
            df_karauri["code"] = df_karauri["code"].map(canonical_code_for_db)
            df_karauri = df_karauri.rename(columns={"code": "コード"})
        if "code" in df_karauri_status.columns:
            df_karauri_status["code"] = df_karauri_status["code"].map(canonical_code_for_db)
            df_karauri_status = df_karauri_status.rename(columns={"code": "コード"})
        # 空summaryでもschema付きDataFrameをmergeし、全銘柄を今回「不明」へ戻す。
        df_cand = df_cand.merge(df_karauri, on="コード", how="left")
        df_cand = df_cand.merge(df_karauri_status, on="コード", how="left")
        for _c in _inst_snapshot_cols:
            if _c not in df_cand.columns:
                df_cand[_c] = np.nan
        df_cand["空売り更新日"] = df_cand["空売り更新日"].fillna("N/A")
        # P1-19: 機関空売りデータが無い銘柄を「0株」と捏造しない。
        # 数値列は NaN のまま保持し、apply_risk_factors_labels() 側で
        # 「実際に0株」と「データなし」を分離する。
        df_cand["機関空売り合計株数"] = pd.to_numeric(df_cand["機関空売り合計株数"], errors="coerce")
        df_cand["本日の増減合計株数"] = pd.to_numeric(df_cand["本日の増減合計株数"], errors="coerce")
        df_cand["主要機関の動き"] = df_cand["主要機関の動き"].fillna("-")
        df_cand = _derive_institution_short_semantics(df_cand)
        _acq_counts = df_cand["機関空売り取得状態"].value_counts(dropna=False).to_dict()
        _cur_counts = df_cand["機関空売り現在状態"].value_counts(dropna=False).to_dict()
        _p(
            "karauri: history/current/acquisition separated "
            f"acquisition={_acq_counts} current={_cur_counts} (P3-45)"
        )
    except Exception as e:
        # P1-495: 機関空売りは需給リスク判定へ使う。読込/merge障害を
        # 「機関空売り情報なし」と同じ見た目で公開しない。
        print(f"[karauri][FATAL] 集計データのマージに失敗しました: {e}")
        raise RuntimeError("institution short enrichment failed; refusing incomplete risk snapshot") from e
    # ==============================================================================
    # --- テーマ付与 ---
    latest_theme_map = {}
    try:
        kabutan_map, theme_names = _load_theme_maps_for_screener(conn)
        latest_theme_map = _load_latest_theme_map_for_screener(conn)
        # P1-536: 当日fresh mapが空でも、前回screener列を残さず今回snapshotを明示する。
        _attach_related_themes(df_cand, kabutan_map, theme_names)
        _p("theme: attached '関連テーマ'")
        _attach_latest_theme(df_cand, latest_theme_map)
        _p("theme: attached '最新テーマ'")
    except Exception as _e:
        print("[theme][WARN] attach to DataFrame failed; stale theme columns masked:", _e)
        latest_theme_map = {}
        for _tc in ("関連テーマ", "最新テーマ"):
            if _tc in df_cand.columns:
                df_cand[_tc] = None
    try:
        _p("shinyo & safety_metrics: attached")
    except Exception as _e:
        print(f"[shinyo][WARN] attach to DataFrame failed: {_e}")

    # 4) 軽い整形
    _p("rename/round: start")
    t_rename = time.perf_counter()
    try:
        ren = {"推奨": "推奨アクション", "推奨比率%": "推奨比率"}
        present = {k: v for k, v in ren.items() if k in df_cand.columns}
        if present:
            df_cand.rename(columns=present, inplace=True)
        # P1-54: 右肩上がりフラグはシグナル種別であり、検証結果の「判定」ではない。
        # 判定列が無い場合は未検証として空欄にし、当たり/外れを捏造しない。
        if "判定" not in df_cand.columns:
            df_cand["判定"] = ""
        if "判定理由" not in df_cand.columns:
            df_cand["判定理由"] = None
    except Exception as _e:
        print("[rename][WARN]", _e)

    def _round2_inplace(df):
        if df is None or df.empty: return
        percent = ["前日終値比率","前日終値比率（％）","フォロー高値pct","最大逆行pct","リターン終値pct","推奨比率","ATR14%","進捗率","初動騰落率","初動終値位置","初動事前5日騰落率","初動事前20日騰落率"]
        money   = ["売買代金(億)","売買代金億","売買代金20日平均億","RVOL代金","時価総額億円","初動売買代金億","初動出来高倍率20","初動代金倍率20","初動レンジ拡大率"]
        price   = ["現在値","前日終値","前日円差","始値","高値","安値","終値"]
        score   = [
            "右肩早期スコア","合成スコア","スコア","INITIAL_MOMENTUM_SCORE",
            "シンデン総合スコア","シンデン正式スコア","シンデン参考スコア","シンデンソート値",
            "予想ギャップスコア","予想信頼性スコア",
            "予想根拠スコア","予想可視性スコア","未織り込みスコア",
            "営業益予想ギャップ_pct","EPS予想ギャップ_pct",
            "予想平均達成率_pct","予想最低達成率_pct",
            "次期転換期待スコア","反動余地スコア","履歴土台スコア",
            "根拠可視性スコア","次期未織込スコア","回復兆候スコア"
        ]
        for c in percent + money + price + score:
            if c in df.columns:
                df[c] = pd.to_numeric(df[c], errors="coerce").round(2)

    _round2_inplace(df_cand)

    # === 決算跨ぎワンクリックUI用 必須列ガード ===
    # shinden_logic.py が screener に保存した列をHTMLへ渡す。
    # 万一列が欠けてもtemplate側JSが落ちないよう空列を保証する。
    _earnings_bridge_cols = [
        "決算発表予定日",
        "シンデン総合スコア",
        "シンデン判定",
        "シンデン判定理由",
        "次期転換期待スコア",
        "次期転換判定",
        "次期転換理由",
    ]
    _earnings_bridge_missing = []
    for _c in _earnings_bridge_cols:
        if _c not in df_cand.columns:
            df_cand[_c] = None
            _earnings_bridge_missing.append(_c)
    if _earnings_bridge_missing:
        print(f"[earnings-bridge][WARN] 欠損列を空列で補完: {_earnings_bridge_missing}")
    else:
        _p("earnings-bridge: required columns OK")

    # P3-47: producerの互換列は保持しつつ、正式点と履歴不足の参考点を表示/監査用に分離。
    df_cand = _attach_shinden_score_semantics(df_cand)
    _sh_eval_counts = df_cand["シンデン評価区分"].value_counts(dropna=False).to_dict()
    _p(f"shinden: formal/reference semantics attached {_sh_eval_counts} (P3-47)")

    _p(f"rename/round: done dt={( time.perf_counter() - t_rename ): .2f}s")

    # --- 株探ニュース(3) ---
    # P1-8: この段階ではAI判定がまだ未計算なので、技術シグナルだけを先行取得する。
    # AI陽性銘柄は add_ai_analysis() 後に差分取得し、その銘柄だけ予想インパクトを再計算する。
    _news_prefetched_codes = set()
    try:
        _p("news: kabutan bulk start (technical signals)")
        t_news = time.perf_counter()
        if 'コード' in df_cand.columns and '銘柄名' in df_cand.columns:
            def _has_signal(col_name, keyword):
                if col_name in df_cand.columns:
                    return df_cand[col_name].astype(str).fillna('').str.contains(keyword)
                return pd.Series([False] * len(df_cand), index=df_cand.index)

            _im_focus = (
                pd.to_numeric(df_cand['INITIAL_MOMENTUM'], errors='coerce').fillna(0).eq(1)
                if 'INITIAL_MOMENTUM' in df_cand.columns
                else pd.Series(False, index=df_cand.index)
            )
            mask_focus = (
                _im_focus | _has_signal('初動フラグ', '候補') | _has_signal('底打ちフラグ', '候補') |
                _has_signal('右肩上がりフラグ', '候補') | _has_signal('右肩早期フラグ', '候補')
            )
            df_focus = df_cand[mask_focus].copy()
            if not df_focus.empty:
                # P1-191: 先行取得済み判定もcanonical code。
                _news_prefetched_codes = set(df_focus['コード'].map(canonical_code_for_db))
                news_ser_focus, news_full_ser_focus = kabutan_news_lines_bulk_for_dataframe(
                    df_focus, code_col='コード', name_col='銘柄名', return_fulltext=True
                )
                df_focus['株探ニュース(3)'] = news_ser_focus
                df_focus['株探ニュース判定用'] = news_full_ser_focus
                news_map = df_focus.set_index('コード')['株探ニュース(3)'].to_dict()
                news_full_map = df_focus.set_index('コード')['株探ニュース判定用'].to_dict()
                df_cand['株探ニュース(3)'] = df_cand['コード'].map(news_map).fillna("")
                df_cand['株探ニュース判定用'] = df_cand['コード'].map(news_full_map).fillna("")
            else:
                df_cand['株探ニュース(3)'] = ""
                df_cand['株探ニュース判定用'] = ""
        _p(f"news: kabutan bulk done dt={( time.perf_counter() - t_news ): .2f}s")
    except Exception as _e_news:
        print('[news][WARN] 株探ニュース列の付与に失敗:', _e_news)
        # P1-541: screenerに残る前回ニュースを今回の短期材料としてpredictorへ流さない。
        # 今回取得不能は「現在ニュース不明/なし」とし、AI後の差分補完は改めて試行できる。
        df_cand['株探ニュース(3)'] = ""
        df_cand['株探ニュース判定用'] = ""
        _news_prefetched_codes = set()

    

    # 6) フラグ装飾
    t_flags = time.perf_counter()
    def _mini(text): return "" if not text else f"&nbsp;<span class='mini'>{text}</span>"
    def _flag_with_since(flag, since):
        flag = (flag or "").strip()
        return f"{flag}{_mini(f'{since}〜')}" if flag=="候補" and since else flag
    def _early_kind_mini(since_kind, prev_kind):
        extras = []
        if since_kind: extras.append(f"{since_kind}〜")
        if prev_kind:  extras.append(f"prev: {prev_kind}")
        return _mini(" / ".join(extras)) if extras else ""

    if df_cand is not None and not df_cand.empty:
        df_cand["初動フラグ"]       = df_cand.apply(lambda r: _flag_with_since(r.get("初動フラグ"), r.get("初動開始日")), axis=1)
        df_cand["底打ちフラグ"]     = df_cand.apply(lambda r: _flag_with_since(r.get("底打ちフラグ"), r.get("底打ち開始日")), axis=1)
        df_cand["右肩上がりフラグ"] = df_cand.apply(lambda r: _flag_with_since(r.get("右肩上がりフラグ"), r.get("右肩開始日")), axis=1)
        # P1-660: phase_update_since_dates() が毎run更新/NULL化する正本列だけを使う。
        # writerの無い旧「右肩早期...」列へ行単位fallbackすると、current正本がNULLになった後も
        # 過去シグナルの開始日/前回種別が表示へ復活し得るため廃止する。
        df_cand["右肩早期フラグ"] = df_cand.apply(
            lambda r: _flag_with_since(
                r.get("右肩早期フラグ"), r.get("早期開始日")
            ), axis=1
        )
        df_cand["右肩早期種別_mini"] = df_cand.apply(
            lambda r: _early_kind_mini(
                r.get("早期種別開始日"), r.get("早期前回種別")
            ) if (r.get("右肩早期種別") or "").strip() else "", axis=1
        )
        for c in [
            "初動開始日", "底打ち開始日", "右肩開始日",
            "早期開始日", "早期種別開始日", "早期前回種別",
            "右肩早期開始日", "右肩早期種別開始日", "右肩早期前回種別",
        ]:
            if c in df_cand.columns:
                df_cand.drop(columns=[c], inplace=True)
    _p(f"flags: decorate done dt={( time.perf_counter() - t_flags ): .2f}s")

    # 7) レコード化（★ 二重管理を排除したストレート処理）
    _t0 = time.time()
    def _tick(msg): print(f"[timer] {msg}: {time.time()-_t0:.2f}s", flush=True)
    _p("records_safe: start")
    _tick("enter")

    df_cand = _vectorize_minimum_fields(df_cand)
    if latest_theme_map:
        _attach_latest_theme(df_cand, latest_theme_map)
    df_cand = apply_3algo_labels(df_cand) # ★追加: 3大アルゴリズム列の生成
    df_cand = apply_volume_quality_labels(df_cand) # ★追加: 短期需給(出来高の質)判定
    df_cand = apply_risk_factors_labels(df_cand) # ← ★変更

    # P3-14: 過去D1決算リアクションはSurpriseの「事前期待ハードル」には使わない。
    # ただしHTML/LLMへ同一snapshotを一度だけ付与するため、この地点でauthoritativeに計算して後段で再利用する。
    df_cand = enrich_earnings_reaction(conn, df_cand)
    _tick("earnings_reaction_pre_predictor")
    _tick("vectorize_minimum_fields")
    
    
    # ==============================================================================
    # ★【追加】株価上昇幅・インパクト予測エンジンの実行とマージ
    # ==============================================================================
    try:
        predictor = StockSurprisePredictor()
        pred_returns = []
        target_prices = []
        predictor_errors = 0

        # P1-29: 1銘柄の不正値/例外で全銘柄の予測を0へ潰さない。
        # 銘柄単位で隔離し、失敗銘柄だけNaN、他銘柄は正常値を保持する。
        for _, row_series in df_cand.iterrows():
            row_dict = row_series.to_dict()
            try:
                res = predictor.predict_price_increase(row_dict)
                pred_returns.append(res["expected_return_pct"])
                target_prices.append(res["target_price"])
            except Exception as row_e:
                predictor_errors += 1
                code_err = str(row_dict.get("コード") or "")
                print(f"[predictor][WARN] code={code_err} 個別予測失敗: {row_e}")
                pred_returns.append(np.nan)
                target_prices.append(np.nan)

        df_cand["予想インパクト_pct"] = pred_returns
        df_cand["予測ターゲット価格"] = target_prices
        _p(f"predictor: expected return calculated; row_errors={predictor_errors}")
    except Exception as e:
        print(f"[predictor][WARN] 予測器初期化/全体処理に失敗しました: {e}")
        df_cand["予想インパクト_pct"] = np.nan
        df_cand["予測ターゲット価格"] = np.nan
    _tick("surprise_predictor_enrich")
    
    # ============================================================
    # 季節調整進捗を実行して、更新後のDB値をdf_candへ戻す
    # （旧版は関数を定義しただけで呼んでいなかった）
    # ============================================================
    try:
        update_seasonal_progress(conn)
        season_df = pd.read_sql_query(
            """
            SELECT コード, 過去平均進捗率, 季節調整済進捗差分
            FROM screener
            """,
            conn,
        )
        if not season_df.empty:
            # P1-212: HTML季節進捗マージもcanonical code。
            season_df["コード"] = season_df["コード"].map(canonical_code_for_db)
            df_cand["コード"] = df_cand["コード"].map(canonical_code_for_db)
            df_cand = df_cand.drop(
                columns=[c for c in ["過去平均進捗率", "季節調整済進捗差分"] if c in df_cand.columns],
                errors="ignore",
            )
            df_cand = df_cand.merge(season_df, on="コード", how="left")
            print(
                f"[seasonality] df_cand反映: "
                f"過去平均あり={int(df_cand['過去平均進捗率'].notna().sum())} / "
                f"差分あり={int(df_cand['季節調整済進捗差分'].notna().sum())}"
            )
    except Exception as e:
        # P1-460: 失敗時にdf_candへ読み込み済みの前回季節値を
        # 「今回の値」として公開しない。seasonalityは下値安全/決算評価にも使うためfatal。
        print(f"[seasonality][FATAL] 実行/反映失敗: {e}")
        raise RuntimeError("seasonality refresh failed; refusing stale seasonal metrics") from e

    # ここで生成される rows には、自動的に予測値が含まれるようになります
    # 全銘柄の行整形は1回だけ実行する
    rows = _prepare_rows(df_cand, conn)

    # P1-647: DBへの保存はHTML atomic write成功後に行う。ここでは今回値だけ保持する。
    # 失敗runの推奨を「前回公開値」にしないため、export途中ではDBを進めない。
    _recommendation_state_updates = []
    for _rr in rows:
        _rc = canonical_code_for_db(_rr.get("コード"))
        if not _rc:
            continue
        _rv = _to_float(_rr.get("推奨比率_raw"))
        if _rv is not None and not np.isfinite(_rv):
            _rv = None
        _recommendation_state_updates.append((None if _rv is None else float(_rv), _rc))

    # 🎯 90日スイング・トレンドフォロー算出モデル
    # P0-6: 実データ列名とATR単位を統一。
    # - 直近高値90日 / 直近安値90日 を優先（current Res_HH / Sup_LLを互換fallback）
    # P1-667: writerless旧「直近高値60日/直近安値60日」はcurrent売買水準へ使わない。
    # - ATR14_PCT / ATR14% は株価比(%)なので円ATRへ変換
    # - P1-662: 円ATRの正式正本はcurrent ATR14のみ（writerless旧ATR20へfallbackしない）
    for r in rows:
        try:
            # P1-661: 仕込み/利確/損切りもcurrent「現在値」だけを価格正本とする。
            # stale clear後に旧「終値」から売買水準を復活させない。
            _cur_px = _to_float(r.get("現在値"))
            _res_hh = _to_float(r.get("直近高値90日"))
            if _res_hh is None:
                _res_hh = _to_float(r.get("Res_HH"))
            _sup_ll = _to_float(r.get("直近安値90日"))
            if _sup_ll is None:
                _sup_ll = _to_float(r.get("Sup_LL"))
            _res_hh = _res_hh if _res_hh is not None and np.isfinite(_res_hh) else 0.0
            _sup_ll = _sup_ll if _sup_ll is not None and np.isfinite(_sup_ll) else 0.0

            _atr_val = None
            _atr_pct = _to_float(r.get("ATR14_PCT"))
            if _atr_pct is None:
                _atr_pct = _to_float(r.get("ATR14%"))

            if (
                _atr_pct is not None and np.isfinite(_atr_pct) and _atr_pct > 0
                and _cur_px is not None and np.isfinite(_cur_px) and _cur_px > 0
            ):
                _atr_val = _cur_px * (_atr_pct / 100.0)
            else:
                _atr_abs = _to_float(r.get("ATR14"))
                if _atr_abs is not None and np.isfinite(_atr_abs) and _atr_abs > 0:
                    _atr_val = _atr_abs

            # P1-668: ATR欠損を「株価の2.5%」という架空値で補完して、
            # 精密そうな仕込み/利確/損切りを生成しない。current ATRが無ければ計算不能。
            if not (
                _cur_px is not None and np.isfinite(_cur_px) and _cur_px > 0
                and _atr_val is not None and np.isfinite(_atr_val) and _atr_val > 0
            ):
                r["shikomi_txt"] = r["rikaku_txt"] = r["songiri_txt"] = "-"
                continue

            if _cur_px > 0:
                if _sup_ll > 0 and _sup_ll < _cur_px:
                    _shikomi = _sup_ll + (_atr_val * 0.2)
                else:
                    _shikomi = _cur_px * 0.96 
                    
                if _res_hh > _cur_px and (_res_hh / _cur_px) >= 1.03:
                    _rikaku = _res_hh * 0.99
                else:
                    _rikaku = _cur_px + (5 * _atr_val)
                    
                if _sup_ll > 0:
                    _songiri = _sup_ll - (_atr_val * 0.2)
                else:
                    _songiri = _shikomi - (2.5 * _atr_val)
                    
                r["shikomi_txt"] = f"{int(round(_shikomi)):,}"
                r["rikaku_txt"]  = f"{int(round(_rikaku)):,}"
                r["songiri_txt"] = f"{int(round(_songiri)):,}"
            else:
                r["shikomi_txt"] = r["rikaku_txt"] = r["songiri_txt"] = "-"
        except Exception:
            r["shikomi_txt"] = r["rikaku_txt"] = r["songiri_txt"] = "-"

    
    _tick("prepare_rows")

    # 高値/安値/MA(5/25/75) を一括付与
    def _code4(x): return canonical_code_for_db(x)
    codes = sorted({ _code4(r.get("コード")) for r in rows if r.get("コード") })
    summary_map = preload_price_summaries(conn, codes)
    rows = enrich_rows_with_price_summary(rows, summary_map)
    _tick("price_summary_enrich")

    # P3-43: 前夜PTSはPREOPENだけ許可し、場中/EODは当日取得分以外をmask。
    _mask_stale_pts_for_run(rows, _auto_run_mode())

    # chart / 移動平均 / ボリバン / GC / 三役
    enhance_with_chart_flags(conn, rows)
    _tick("chart_flags_enhance")
    
    # P1-587: 決算リアクションは予測器より前でcurrent-run値へ確定済み。
    # ここでは再計算せず、同じsnapshotをrowsへ同期するだけにする。
    # P3-20: df_cand行検索をrows件数ぶん繰り返さずcanonical mapを1回作る。
    _reaction_map = {}
    if "コード" in df_cand.columns:
        for _, _er in df_cand.iterrows():
            _ek = canonical_code_for_db(_er.get("コード"))
            if _ek and _ek not in _reaction_map:
                _reaction_map[_ek] = {
                    "決算勝率": _er.get("決算勝率"),
                    "決算期待値": _er.get("決算期待値"),
                    "過去決算D1期待値": _er.get("過去決算D1期待値"),
                    "決算リアクション件数": _er.get("決算リアクション件数"),
                    "決算リアクションスコア": _er.get("決算リアクションスコア"),
                    "決算リアクション履歴": _er.get("決算リアクション履歴"),
                }
    for r in rows:
        _ev = _reaction_map.get(canonical_code_for_db(r.get("コード", "")))
        if _ev is not None:
            r.update(_ev)
    _tick("earnings_reaction_enrich")
    
   
    
    # TOBデータの注入処理
    try:
        tob_map = load_tob_titles_map(180, conn=conn)
        for r in rows:
            c4 = _code4(r.get("コード") or r.get("code"))
            r["tob_titles"] = tob_map.get(c4, [])
    except Exception as e:
        # P1-493: TOB取得障害を「TOBなし」として公開しない。
        print(f"[TOB][FATAL] inject failed: {e}")
        raise RuntimeError("TOB event enrichment failed; refusing incomplete event dashboard") from e

    # 8) 軽量メタ等
    meta = {"base_day": None, "next_business_day": None, "market_alerts": []}
    
    # === ★追加: 需給イベントのアラート判定 ===
    try:
        # 強制的に「イベント発生日」の日付を指定
        # 例: 2026年7月8日（ETF分配金換金売りの期間中）
        #test_today = date(2026, 7, 8) 
        
        #cal = MarketEventCalendar(test_today.year)
        #meta["market_alerts"] = cal.get_alerts_for_date(test_today)
        
        today_val = date.fromisoformat(_today_jst())
        cal = MarketEventCalendar(today_val.year)
        meta["market_alerts"] = cal.get_alerts_for_date(today_val)
    except Exception as e:
        print(f"[EventAlert] 計算エラー: {e}")
    # ==========================================

    try:
        def _to_date(s):
            if not s: return None
            try: return datetime.strptime(str(s)[:10], "%Y-%m-%d").date()
            except: return None
        if rows:
            dates = [d for d in (_to_date(r.get("シグナル更新日")) for r in rows) if d]
            if dates:
                base = max(dates)
                # P1-303: HTMLの「次営業日」もJPX固定休場日/追加休場日込みで算出。
                _meta_extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
                meta["base_day"] = base.strftime("%Y-%m-%d")
                meta["next_business_day"] = next_business_day_jp(base, _meta_extra_closed).strftime("%Y-%m-%d")
    except Exception as e:
        # P1-423: 次営業日メタだけ誤ったまま黙って公開しない。
        raise RuntimeError(f"dashboard business-day metadata failed: {e}") from e
    _tick("base_day next_business_day done")

    # ★AI判定を実行
    print("[AI] 詳細分析を実行しています...")
    try:
        rows = add_ai_analysis(conn, rows)
    except Exception as e:
        logging.error(f"[AI] 分析に失敗しました", exc_info=True)
    # P2-91: AI履歴短縮と後続RSS補完を個別計測し、ネットワーク変動と混同しない。
    _tick("ai_model_analysis")

    # P1-8: AI判定が確定した後、★銘柄で先行ニュース対象外だったものだけ差分取得。
    # ニュースはStockSurprisePredictorの自社株買い/増配判定に使うため、
    # 補完した銘柄の予想インパクトもその場で再計算してからfair valueへ渡す。
    try:
        df_cand, rows, _ai_news_stats = _supplement_ai_positive_news_and_refresh_predictor(
            df_cand, rows, _news_prefetched_codes
        )
        print(
            "[news][AI補完] "
            f"AI陽性={_ai_news_stats['ai_positive']} "
            f"追加対象={_ai_news_stats['supplement_targets']} "
            f"ニュース有={_ai_news_stats['news_nonempty']} "
            f"予想再計算={_ai_news_stats['predictor_refreshed']}"
        )
        _tick("ai_positive_news_supplement")
    except Exception as e:
        print(f"[news][WARN] AI陽性ニュース補完に失敗: {e}")

    # P1-5: ここが適正/期待株価の正しい再計算タイミング。
    # 予想インパクトは上段で、AIスコアは直前で最新化済み。
    # 最新2値をscreenerへ同期してからfair valueを再計算し、表示用rowsへ戻す。
    try:
        rows = _sync_latest_model_outputs_and_refresh_fair_value(conn, rows)

        # P1-571: Algo_Factor/Algo_総合は割安度を入力に使うため、fair value再計算前に
        # 一度作ったAlgo列をそのまま出すと前回runの割安度が混ざる。今回の適正株価/割安度を
        # df_candへ戻して3Algoを再計算し、HTML rowsとLLM用df_candを同一snapshotへ揃える。
        _fv_now = pd.read_sql_query(
            "SELECT コード, 適正株価, 割安度, 期待株価 FROM screener",
            conn,
        )
        _fv_map_now = {}
        if not _fv_now.empty:
            for _, _fr in _fv_now.iterrows():
                _fk = canonical_code_for_db(_fr.get("コード"))
                if _fk:
                    _fv_map_now[_fk] = {
                        "適正株価": _fr.get("適正株価"),
                        "割安度": _fr.get("割安度"),
                        "期待株価": _fr.get("期待株価"),
                    }
        if "コード" in df_cand.columns:
            _cand_keys_now = df_cand["コード"].map(canonical_code_for_db)
            for _fc in ("適正株価", "割安度", "期待株価"):
                df_cand[_fc] = _cand_keys_now.map(
                    lambda _k, _c=_fc: (_fv_map_now.get(_k) or {}).get(_c)
                )
        df_cand = apply_3algo_labels(df_cand)
        _algo_cols_now = ["Algo_Momentum", "Algo_VolTarget", "Algo_Factor", "Algo_総合判定"]
        _algo_map_now = {}
        if "コード" in df_cand.columns:
            for _, _ar in df_cand.iterrows():
                _ak = canonical_code_for_db(_ar.get("コード"))
                if _ak:
                    _algo_map_now[_ak] = {c: _ar.get(c) for c in _algo_cols_now}
        for _rr in rows:
            _rk = canonical_code_for_db(_rr.get("コード"))
            _av = _algo_map_now.get(_rk) or {}
            for _ac in _algo_cols_now:
                _rr[_ac] = _av.get(_ac)

        _tick("fair_value_refresh_after_ai + algo_resync")
        _log_model_quality_snapshot(rows)
    except Exception as e:
        # P1-424/P1-571: AI/impact・fair value・Algoのどれかだけ新しいpartial snapshotを公開しない。
        print(f"[fair-value][FATAL] 最新AI/インパクト・適正株価・Algo再同期に失敗: {e}")
        raise RuntimeError("fair-value/algo refresh failed; refusing inconsistent dashboard") from e

    # === ★追加: セクターランキングの集計 ===
    try:
        sync_sector_data(conn)
        sector_ranking_df = prepare_sector_ranking_view(conn)
        sector_ranking_list = sector_ranking_df.to_dict(orient='records')
        _p("sector ranking: calculated")
    except Exception as e:
        print(f"[sector][WARN] ranking calculation failed: {e}")
        sector_ranking_list = []
    # ==========================================

    # === ★追加: テーマランキングの集計（ロバスト化版） ===
    try:
        theme_ranking_list = calculate_robust_theme_ranking(conn, df_cand)
        _p("theme ranking: calculated robustly")
    except Exception as e:
        print(f"[theme][WARN] ranking calculation failed: {e}")
        theme_ranking_list = []
    # ==========================================

    # P3-41: publish-quality。欠損を0点に偽装せず、空洞化も画面上で検知する。
    def _coverage_pct(col):
        if not rows:
            return None
        ok = 0
        for _r in rows:
            _v = _r.get(col)
            if _v is None or _v == "":
                continue
            try:
                if isinstance(_v, (float, np.floating)) and not math.isfinite(float(_v)):
                    continue
                if pd.isna(_v):
                    continue
            except Exception:
                pass
            ok += 1
        return round(ok * 100.0 / len(rows), 1)

    _quality_cols = [
        "現在値", "出来高", "売買代金(億)", "RVOL代金", "ATR14%",
        "初動スコア", "三角スコア", "AIスコア", "適正株価", "信用倍率",
        "決算リアクションスコア",
    ]
    _coverage = {c: _coverage_pct(c) for c in _quality_cols if any(c in _r for _r in rows)}
    _warnings = []
    for _c in ("現在値", "出来高", "RVOL代金", "ATR14%"):
        _pct = _coverage.get(_c)
        if _pct is not None and _pct < 80.0:
            _warnings.append(f"{_c} coverage={_pct:.1f}%")
    meta["quality"] = {
        "rows": len(rows),
        "coverage": _coverage,
        "warnings": _warnings,
        "run_mode": _auto_run_mode(),
        "external_live": _external_job_state("live_materials"),
        "shinden_job": _external_job_state("live_shinden_full"),
    }

    # 9) data オブジェクトに組み込む
    offering_code_set = _load_offering_codes_from_db(conn, days=3650)
    data_obj = {
        "cand": rows,
        "meta": meta,
        "offer_codes":  sorted(offering_code_set),
        "sector_ranking": sector_ranking_list,
        "theme_ranking": theme_ranking_list # ★追加
    }
    data_json = dumps_json_clean(data_obj)
    _tick("json clean done")

    # P1-649: dashboard_data.json は「最新ダッシュボード用」のlive snapshot。
    # index.htmlより先に本番名へ置換すると、後続HTML失敗時にJSONだけ新runへ進む。
    # ここではパスだけ準備し、完成JSONのstage/commitはHTML最終書込み直前〜直後に行う。
    json_export_path = os.path.join(OUTPUT_DIR, "dashboard_data.json")

    # 10) テンプレ描画
    _ensure_template_file(template_dir, overwrite=True)
    env = Environment(loader=FileSystemLoader(template_dir, encoding="utf-8"),
                      autoescape=select_autoescape(["html"]))
    env.filters["fmt_cell"] = _fmt_cell

    try:
        _tz = ZoneInfo("Asia/Tokyo") if ZoneInfo else None
        # P1-208: ZoneInfo取得不能時もOSローカルへ戻さずJST共通時計を使う。
        build_id = datetime.now(_tz).strftime("%Y-%m-%d %H:%M:%S") if _tz else _now_jst().strftime("%Y-%m-%d %H:%M:%S")
    except Exception:
        build_id = _now_jst().strftime("%Y-%m-%d %H:%M:%S")

    tpl = env.get_template("dashboard.html")
    html_output = tpl.render(
        include_log=include_log,
        data_json="{}", # テンプレート側には空を渡す
        generated_at=build_id,
        build_id=build_id
    )
    _tick("template done")

    # 11) __DATA__ を安全にインライン注入
    t_inject = time.perf_counter()
    try:
        # ★修正1：標準の json.dumps だと NaN がそのまま出力されてブラウザが死ぬため、
        # 9) のステップで安全に生成済みの「data_json」を再利用します。
        data_json_str = data_json
        
        # ★修正2：HTMLの崩れを防ぐエスケープ ＋ 万が一残ったNaNをnullに強制置換する徹底ガード
        data_json_str = data_json_str.replace("<", "\\u003c").replace(">", "\\u003e")
        import re
        data_json_str = re.sub(r':\s*NaN', ': null', data_json_str)
        data_json_str = re.sub(r':\s*Infinity', ': null', data_json_str)
        data_json_str = re.sub(r':\s*-Infinity', ': null', data_json_str)
        
        # 埋め込んだJSONをダッシュボードに読み込ませる「JavaScriptの紐付け処理」
        json_script = f"""
<script id="__DATA__" type="application/json">
{data_json_str}
</script>
<script id="data_inline">
    try {{
        var el = document.getElementById('__DATA__');
        if (el) {{
            // 埋め込まれたテキストを読み込み、ダッシュボードのシステム(window.DATA)に渡す
            window.__DATA__ = JSON.parse(el.textContent);
            window.DATA = window.__DATA__;
        }}
    }} catch(e) {{
        console.error("データの読み込みに失敗しました:", e);
    }}
</script>
"""
        
        # 既存の古いタグを掃除
        import re
        html_output = re.sub(r'<script\s+id="__DATA__"[^>]*>.*?</script>', '', html_output, flags=re.DOTALL | re.IGNORECASE)
        html_output = re.sub(r'<script\s+id="(data_inline|inline-data)"[^>]*>.*?</script>', '', html_output, flags=re.DOTALL | re.IGNORECASE)
        
        # </body> の直前に挿入
        if "</body>" in html_output:
            html_output = html_output.replace("</body>", json_script + "\n</body>")
        else:
            html_output += json_script
            
    except Exception as e:
        # P1-319: テンプレートへは data_json="{}" を渡しているため、埋め込み失敗を
        # 成功扱いすると中身が空のダッシュボードを公開してしまう。必ず上位へ失敗を返す。
        logging.error(f"[HTML-EXPORT][ERROR] JSONデータの埋め込みに失敗しました", exc_info=True)
        raise RuntimeError("dashboard JSON inline injection failed") from e
        
    _p(f"inject_inline_data_json: dt={( time.perf_counter() - t_inject ): .2f}s")
    
    # 12) 書き出し
    t_write = time.perf_counter()
    os.makedirs(os.path.dirname(html_path), exist_ok=True)
    
    # P1-649: live dashboard JSONを本番名へ出す前に隠しstageへ完成させる。
    # stage作成失敗ならHTMLも進めない。
    _json_stage_path = f"{json_export_path}.stage.{os.getpid()}.{time.time_ns()}"
    try:
        _atomic_write_text_file(_json_stage_path, data_json)
    except Exception as e:
        logging.error("[JSON-EXPORT][ERROR] live dashboard JSON staging failed", exc_info=True)
        raise RuntimeError("dashboard live JSON staging failed") from e

    # 書き込み
    try:
        # P1-321: index.html は直接truncateせず、完成した一時ファイルをatomic replaceする。
        _atomic_write_text_file(html_path, html_output)
    except Exception as e:
        try:
            if os.path.exists(_json_stage_path):
                os.unlink(_json_stage_path)
        except Exception:
            pass
        # P1-320: 書込失敗を握りつぶすと、この後Git同期が古いindex.htmlを再公開し得る。
        logging.error(f"[HTML-EXPORT][ERROR] ファイルの書き出しに失敗しました", exc_info=True)
        raise RuntimeError(f"dashboard HTML write failed: {html_path}") from e

    # P1-649: HTML成功後にだけlive dashboard_data.jsonを同一snapshotへ進める。
    # commitに失敗した場合は旧JSONを残す方が危険なのでinvalidateして上位へ失敗を返す。
    try:
        os.replace(_json_stage_path, json_export_path)
    except Exception as e:
        try:
            if os.path.exists(_json_stage_path):
                os.unlink(_json_stage_path)
        except Exception:
            pass
        try:
            if os.path.exists(json_export_path):
                os.unlink(json_export_path)
                print(f"[JSON-EXPORT][INVALIDATE] stale live JSON removed: {json_export_path}")
        except Exception as _rm_e:
            print(f"[JSON-EXPORT][WARN] stale live JSON removal failed: {_rm_e}")
        logging.error("[JSON-EXPORT][ERROR] live dashboard JSON commit failed", exc_info=True)
        raise RuntimeError("dashboard live JSON commit failed after HTML write") from e

    # 履歴JSONはlive参照ではない。HTML＋current JSONの成功snapshotだけを記録する。
    # P1-199/P1-384: JST＋マイクロ秒名でrun単位に一意化。
    timestamp = _now_jst().strftime("%Y%m%d_%H%M%S_%f")
    json_history_path = os.path.join(OUTPUT_DIR, f"dashboard_data_{timestamp}.json")
    try:
        _atomic_write_text_file(json_history_path, data_json)
    except Exception as _hist_e:
        # live dashboardは既に同一snapshotで確定済み。archive欠落だけでliveを失敗扱いにしない。
        print(f"[JSON-HISTORY][WARN] dashboard history write failed: {_hist_e}")
    else:
        # P4-2: 履歴JSONは容量肥大の主因だったため、成功snapshotの最新N個だけ保持。
        # dashboard_data.json (live/current) は対象外。
        _prune_dashboard_history(DASHBOARD_HISTORY_KEEP)

    # P1-648: monitor/LLMをindex.htmlより先に置換すると、最後のHTML writeだけ失敗したrunで
    # realtime monitor/LLMは新snapshot、dashboardは旧snapshotという分裂が起きる。
    # まずHTML atomic writeを成功させ、その後に連携ファイルを更新する。
    # 連携側失敗時は各export関数が旧ファイルをinvalidateするため、新HTML＋古い連携状態も残さない。
    _monitor_ok = export_monitor_list(pd.DataFrame(rows))
    if _monitor_ok is False:
        print("[monitor][WARN] realtime monitor list export failed")
    _llm_ok = export_llm_dataset(pd.DataFrame(rows))
    if _llm_ok is False:
        print("[llm][WARN] LLM dataset export failed")

    # P4-LIVE: HTML/dashboard_data.jsonと同じ最終rowsから正式Candidate Contractを生成。
    # HTMLを読み戻さず、失敗時は前回正常feedを保持して既存dashboard処理を壊さない。
    _live_ok = export_live_candidate_feed(conn, rows)
    if _live_ok is False:
        print("[live-feed][WARN] candidate contract was not advanced for this run")

    # P1-647: HTMLのatomic writeが成功したsnapshotだけを次回ヒステリシスの前回値にする。
    # raw比率が今回計算不能ならNULLもauthoritativeに保存し、古い前回値を残さない。
    if _recommendation_state_updates:
        _rec_ts = _now_jst().isoformat(timespec="seconds")
        _rec_rows = [(_rv, _rec_ts, _rc) for _rv, _rc in _recommendation_state_updates]
        _rec_cur = conn.cursor()
        try:
            _rec_cur.execute("SAVEPOINT p1_647_recommendation_state")
            _rec_cur.executemany(
                'UPDATE screener SET "推奨比率_raw"=?, "推奨比率更新日時"=? WHERE コード=?',
                _rec_rows,
            )
            _rec_cur.execute("RELEASE SAVEPOINT p1_647_recommendation_state")
        except Exception:
            try:
                _rec_cur.execute("ROLLBACK TO SAVEPOINT p1_647_recommendation_state")
                _rec_cur.execute("RELEASE SAVEPOINT p1_647_recommendation_state")
            except Exception:
                pass
            raise
        finally:
            _rec_cur.close()

    _p(f"write+planA: dt={( time.perf_counter() - t_write ): .2f}s")
    
    # --- Git同期 ---
    REPO_ROOT = r"C:\Users\sasit\Documents\GitHub\sc"
    try:
        # P1-357: Git同期の成否を呼出側でも明示。失敗時に成功風の無反応で終わらない。
        if not sync_to_github_pages(REPO_ROOT, html_path):
            print("[git][WARN] local HTML was generated, but GitHub Pages sync did not complete")
    except Exception as e:
        print(f"[git] sync failed: {e}")
    
    print(f"[export] HTML書き出し: {html_path} (logs={'ON' if include_log else 'OFF'}) | build: {build_id}")

def export_monitor_list(df_cand, filename="rss_monitor_list.json"):
    """
    スクリプト実行時点で「何らかのシグナル」が出ている銘柄をすべて抽出し、
    OUTPUT_DIR に JSONファイルとして保存する。（エラー回避・全部入り版）
    """
    # P1-529: 失敗時に前回の監視対象JSONを残して別プロセスが古い銘柄を監視し続けない。
    out_dir = OUTPUT_DIR if "OUTPUT_DIR" in globals() else "."
    full_path = os.path.join(out_dir, filename)

    try:
        # 1. 抽出対象のコードを集める（重複なし）
        target_codes = set()
        
        # A) 「推奨アクション」があれば優先的に拾う
        if "推奨アクション" in df_cand.columns:
            s = df_cand["推奨アクション"].fillna("").astype(str)
            # P1-557: 75%バンドの「中強度」を監視対象から落とさない。
            # 旧実装は100%/50%を拾う一方で、より強い75%推奨だけ漏れていた。
            codes = df_cand[s.isin(['エントリー有力', '中強度', '小口提案', '押し目買い'])]['コード']
            target_codes.update(codes.tolist())

        # B) 「右肩早期種別」（ブレイク、ポケット、20MAリバ等）が出ている銘柄を拾う
        if "右肩早期種別" in df_cand.columns:
            s = df_cand["右肩早期種別"].fillna("").astype(str)
            # 空文字でないものをすべて抽出
            codes = df_cand[s.str.strip() != ""]['コード']
            target_codes.update(codes.tolist())

        # C) P4-1の独立INITIAL_MOMENTUM候補を拾う
        if "INITIAL_MOMENTUM" in df_cand.columns:
            _im = pd.to_numeric(df_cand["INITIAL_MOMENTUM"], errors="coerce").fillna(0)
            target_codes.update(df_cand[_im.eq(1)]["コード"].tolist())

        # D) その他の主要フラグ（右肩上がり、旧初動、底打ち）が「候補」の銘柄を拾う
        for col in ["右肩上がりフラグ", "初動フラグ", "底打ちフラグ"]:
            if col in df_cand.columns:
                s = df_cand[col].fillna("").astype(str)
                # P1-559: export前に「候補 + 開始日mini HTML」へ装飾済みなので完全一致では漏れる。
                codes = df_cand[s.str.contains("候補", regex=False, na=False)]['コード']
                target_codes.update(codes.tolist())

        # リスト化してソート（None排除）
        # P1-193: 外部監視へ7203.0等を流さずcanonical codeで重複排除。
        _monitor_codes = {canonical_code_for_db(c) for c in target_codes if canonical_code_for_db(c)}
        # P1-563: screenerには地合い計算用の^TOPX等も存在するが、rss_monitor_listは
        # 個別株監視consumer向け。指数へ右肩/初動フラグが立っても外部監視へ流さない。
        final_list = sorted(c for c in _monitor_codes if re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", str(c or "")))

        # 2. 保存パスの生成
        os.makedirs(out_dir, exist_ok=True)

        # 3. JSON保存
        output_data = {
            # P1-194: 監視JSONの更新時刻もJST固定。
            "updated_at": _now_jst().strftime("%Y-%m-%d %H:%M:%S"),
            "target_codes": final_list
        }
        
        # P1-339: 監視プロセスが読むJSONを直接truncateせずatomic replace。
        _atomic_write_text_file(
            full_path,
            json.dumps(output_data, indent=4, ensure_ascii=False)
        )
            
        print(f"\n[連携] リアルタイム監視リストを更新しました: {len(final_list)} 銘柄")
        print(f"       保存先: {full_path}", flush=True)
        return True

    except Exception as e:
        try:
            if os.path.exists(full_path):
                os.unlink(full_path)
                print(f"[連携][INVALIDATE] stale monitor list removed: {full_path}", flush=True)
        except Exception as _rm_e:
            print(f"[連携][WARN] stale monitor list removal failed: {_rm_e}", flush=True)
        print(f"[連携][ERROR] 監視リストの出力に失敗しました: {e}", flush=True)
        return False


def export_llm_dataset(df_cand, filename="llm_dataset.csv"):
    """
    LLM（Gemini等）に丸投げして分析させるための厳選データCSVを出力する。
    売買代金5億円以上で足切りし、トークン節約のため不要な列を削ぎ落とす。
    """
    # P1-530: 今回export失敗時に前回CSVを残し、下流LLMが旧snapshotを再利用しない。
    out_dir = OUTPUT_DIR if "OUTPUT_DIR" in globals() else "."
    full_path = os.path.join(out_dir, filename)
    try:
        df = pd.DataFrame() if df_cand is None else df_cand.copy()
        turnover_col = '売買代金億' if '売買代金億' in df.columns else ('売買代金(億)' if '売買代金(億)' in df.columns else None)

        target_cols = [
            "コード", "銘柄名", "市場", "最新テーマ",
            "前日終値比率", "売買代金(億)", "RVOL代金", "RS_5",
            "需給OH", "信用倍率", "信用買い残_浮動株比率", "信用買い残増減率_20d", "信用需給負荷スコア",
            "利益加速フラグ", "決算リアクション件数", "決算勝率", "過去決算D1期待値", "決算リアクションスコア", "進捗率", "季節調整済進捗差分",
            "シンデン総合スコア", "シンデン評価区分", "シンデン正式スコア", "シンデン参考スコア", "シンデン判定",
            "予想ギャップスコア", "予想信頼性スコア",
            "予想根拠スコア", "予想可視性スコア", "未織り込みスコア",
            "予想達成履歴信頼度", "予想達成履歴期数",
            "営業益予想ギャップ_pct", "EPS予想ギャップ_pct",
            "シンデン判定理由", "シンデン需給注釈",
            "次期転換期待スコア", "次期転換判定",
            "反動余地スコア", "履歴土台スコア", "根拠可視性スコア",
            "次期未織込スコア", "回復兆候スコア", "次期転換理由",
            "右肩早期種別", "初動フラグ",
            "INITIAL_MOMENTUM", "INITIAL_MOMENTUM_SCORE", "INITIAL_MOMENTUM_GRADE", "INITIAL_MOMENTUM_REASON",
            "初動出来高倍率20", "初動代金倍率20", "初動騰落率", "初動終値位置", "初動レンジ拡大率",
            "初動20日高値更新", "初動20日終値ブレイク", "初動事前5日騰落率", "初動事前20日騰落率",
            "初動売買代金億", "初動低位株タグ", "Algo_総合判定",
            "イナゴ過熱判定", "信用需給判定", "機関売り判定"
        ]

        # P1-359: 有効候補0件/売買代金列欠損でも旧CSVを残さず、
        # authoritativeな空スナップショットを書いて下流のstale利用を防ぐ。
        if df.empty or turnover_col is None:
            df_llm = pd.DataFrame(columns=target_cols)
        else:
            df[turnover_col] = pd.to_numeric(df[turnover_col], errors="coerce").fillna(0)
            df_filtered = df[df[turnover_col] >= 5.0].copy()
            exist_cols = [c for c in target_cols if c in df_filtered.columns or (c == "売買代金(億)" and turnover_col in df_filtered.columns)]
            actual_cols = [turnover_col if c == "売買代金(億)" else c for c in exist_cols]
            df_llm = df_filtered[actual_cols].copy()
            if turnover_col != "売買代金(億)" and turnover_col in df_llm.columns:
                df_llm = df_llm.rename(columns={turnover_col: "売買代金(億)"})

        os.makedirs(out_dir, exist_ok=True)

        # P1-358: LLM CSVも直接truncateせず、同一ディレクトリtmp→atomic replace。
        csv_text = df_llm.to_csv(index=False, lineterminator="\n")
        _atomic_write_text_file(full_path, "\ufeff" + csv_text, encoding="utf-8")

        print(f"\n[LLM] 分析用データを抽出しました: {len(df_llm)} 銘柄 (売買代金5億円以上)")
        print(f"       保存先: {full_path}", flush=True)
        return True

    except Exception as e:
        try:
            if os.path.exists(full_path):
                os.unlink(full_path)
                print(f"[LLM][INVALIDATE] stale dataset removed: {full_path}", flush=True)
        except Exception as _rm_e:
            print(f"[LLM][WARN] stale dataset removal failed: {_rm_e}", flush=True)
        print(f"[LLM][ERROR] 分析用データの出力に失敗しました: {e}", flush=True)
        return False


# ========= 営業利益 
def update_operating_income_and_ratio(conn: sqlite3.Connection, batch_size: int = 300, max_workers: int = 12, use_quarterly: bool = False) -> None:
    """
    予想営業利益(Forward)を最優先とし、欠損する場合は直近4Q実績(TTM)で補完する。
    営利対時価（小数2桁・単位=%）を算出して screener を更新。
    """
    # 1. 会社予想 (Forward) の取得 (finance_notes テーブルから)
    try:
        df_fwd = pd.read_sql_query("""
            SELECT rowid AS _rowid, コード, updated_at, forecast_op AS 営業利益_fwd
            FROM finance_notes
        """, conn)
        # P1-602/P1-622: canonical alias重複のcurrent snapshotを先に確定してから
        # forecast_opの有効性を判定する。WHERE forecast_op IS NOT NULL を先に掛けると、
        # 最新正本がNULLでも古いaliasの非NULL予想を復活させ得る。
        if not df_fwd.empty:
            df_fwd = _latest_finance_notes_by_canonical(df_fwd, "コード")
            df_fwd["営業利益_fwd"] = pd.to_numeric(df_fwd["営業利益_fwd"], errors="coerce")
            df_fwd = df_fwd[df_fwd["営業利益_fwd"].notna()].copy()
            df_fwd = df_fwd.drop(columns=["_rowid", "updated_at"], errors="ignore")
        # P1-608: finance更新後に新しい決算イベントが来た銘柄は、
        # 旧Forwardだけでなく旧TTM fallbackもcurrent営業利益として公開しない。
        _stale_finance_codes = _finance_codes_stale_after_latest_earnings(conn)
        if _stale_finance_codes and not df_fwd.empty:
            df_fwd = df_fwd[~df_fwd["コード"].map(canonical_code_for_db).isin(_stale_finance_codes)].copy()
    except Exception as e:
        # P1-443: source query障害を「予想値なし」と同一視して旧営業利益を公開しない。
        raise RuntimeError(f"finance_notes forecast_op read failed: {e}") from e

    # 2. 直近4Q実績 (TTM) の取得 (既存の pl_quarter から)
    # pandas 2.x/将来版で groupby.apply の仕様が変わるため apply は使わない。
    try:
        # P2-5: pl_quarter producerは外部/世代差があるためschemaを決め打ちしない。
        # updated_atが存在する将来schemaではそれを正本選択へ使い、旧schemaはrowidで互換維持する。
        _pl_cols = {r[1] for r in conn.execute("PRAGMA table_info(pl_quarter)").fetchall()}
        _pl_updated_sel = 'updated_at' if 'updated_at' in _pl_cols else 'NULL AS updated_at'
        pl = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, 決算期, 四半期, 営業利益, {_pl_updated_sel} FROM pl_quarter",
            conn,
        )

        if pl.empty:
            df_ttm = pd.DataFrame(columns=["コード", "営業利益_ttm"])
        else:
            pl["コード"] = pl["コード"].map(canonical_code_for_db)
            pl["決算期_ord"] = pd.to_datetime(pl["決算期"], errors="coerce")
            pl["営業利益"] = pd.to_numeric(pl["営業利益"], errors="coerce")
            # P2-5: updated_atは有効時刻だけを順位化。欠損/旧schemaは最古扱いにし、rowidで決着する。
            pl["_updated_ord"] = pd.to_datetime(pl.get("updated_at"), errors="coerce", utc=True)

            # 四半期を安定ソートする補助列
            def _quarter_ord(v):
                if v is None or (isinstance(v, float) and np.isnan(v)):
                    return 99
                s = str(v).strip().upper().replace(" ", "")
                import re as _re

                m = _re.search(r"([1-4])\s*Q", s)
                if m:
                    return int(m.group(1))

                m = _re.search(r"Q\s*([1-4])", s)
                if m:
                    return int(m.group(1))

                m = _re.search(r"第?\s*([1-4])\s*四半期", s)
                if m:
                    return int(m.group(1))

                # P1-689: 「通期 / 本決算 / FULL / FY」は4Qではない。
                # TTM fallback は3ヵ月単独営業利益を1Q〜4Qの4本合算する前提なので、
                # 年間値をQ4として混ぜると営業利益を過大計上し得る。
                # 4Qと明示されたラベルだけをQ4として採用し、通期ラベルは対象外にする。
                if any(k in s for k in ("通期", "本決算", "FULL", "FY")):
                    return 99

                return 99

            pl["四半期_ord"] = pl["四半期"].map(_quarter_ord)

            # P1-224: 同じ年度/Qの重複行を二重加算しない。
            # P1-223: Q番号を特定できる1Q〜4QだけをTTM候補にする。
            # P1-688: 同一logical code / 決算期 / Qの正本を、営業利益の有無より先に確定する。
            # 先に営業利益NULLをdropすると、最新rowがNULLでも古い重複rowの非NULL値が復活し、
            # 無効化/未取得になったQをTTMへ再混入できる。
            pl = (
                pl.dropna(subset=["コード", "決算期_ord"])
                .loc[lambda x: x["四半期_ord"].between(1, 4)]
                .sort_values(
                    ["コード", "決算期_ord", "四半期_ord", "_updated_ord", "_rowid"],
                    kind="stable",
                    na_position="first",
                )
                .drop_duplicates(
                    subset=["コード", "決算期_ord", "四半期_ord"],
                    keep="last",
                )
            )
            pl = pl[pl["営業利益"].notna() & np.isfinite(pl["営業利益"])].copy()

            if pl.empty:
                df_ttm = pd.DataFrame(columns=["コード", "営業利益_ttm"])
            else:
                # P1-471: 「4行ある」だけではTTMにしない。Q2欠落などを飛び越えた
                # 4観測を年換算すると誤るため、直近4行のQ番号が 4→1→2→3 等を含む
                # modulo 4 の連続系列であることを要求する。
                _ttm_rows = []
                for _code, _g in pl.groupby("コード", sort=False):
                    _g = _g.sort_values(["決算期_ord", "四半期_ord", "_updated_ord", "_rowid"], kind="stable", na_position="first").tail(4)
                    if len(_g) != 4:
                        continue
                    _qs = [int(x) for x in _g["四半期_ord"].tolist()]
                    _is_consecutive = all(_qs[i] == ((_qs[i-1] % 4) + 1) for i in range(1, 4))
                    # P1-687: Q番号だけが4→1→2→3等で連続していても、
                    # 年度が丸ごと欠落した古いQ4＋数年後Q1〜Q3をTTMへ合算しない。
                    # 決算期が「年度末ラベル」なら同年度内は差0日・年度跨ぎは約1年、
                    # 「各四半期末」なら約3か月なので、どちらの形式でも隣接Qは400日以内に収まる。
                    # 日付が逆行/欠損している系列もTTMとして採用しない。
                    _periods = pd.to_datetime(_g["決算期_ord"], errors="coerce").tolist()
                    _period_contiguous = len(_periods) == 4 and all(
                        pd.notna(_periods[i-1]) and pd.notna(_periods[i])
                        and 0 <= (pd.Timestamp(_periods[i]) - pd.Timestamp(_periods[i-1])).days <= 400
                        for i in range(1, 4)
                    )
                    if not (_is_consecutive and _period_contiguous):
                        continue
                    _vals = pd.to_numeric(_g["営業利益"], errors="coerce")
                    if _vals.notna().sum() != 4:
                        continue
                    _ttm_rows.append({"コード": _code, "営業利益_ttm": float(_vals.sum())})
                df_ttm = pd.DataFrame(_ttm_rows, columns=["コード", "営業利益_ttm"])

        print(f"[oper] TTM算出 rows={len(df_ttm)} (groupby.apply不使用)")

    except Exception as e:
        # P1-443: TTM source query障害もfatal。空テーブルは正常にemptyとして扱う。
        raise RuntimeError(f"pl_quarter TTM read failed: {e}") from e

    # 3. データ結合と優先適用 (Forward > TTM)
    # P1-195: Forward/TTMのJOINキーもcanonical code。
    df_fwd["コード"] = df_fwd["コード"].map(canonical_code_for_db)
    df_fwd["営業利益_fwd"] = pd.to_numeric(df_fwd["営業利益_fwd"], errors="coerce")
    df_ttm["コード"] = df_ttm["コード"].map(canonical_code_for_db)
    if _stale_finance_codes and not df_ttm.empty:
        df_ttm = df_ttm[~df_ttm["コード"].isin(_stale_finance_codes)].copy()
    
    df_oper = pd.merge(df_fwd, df_ttm, on="コード", how="outer")
    
    # 会社予想があれば採用、なければ実績で補完
    df_oper["採用営業利益_百万円"] = df_oper["営業利益_fwd"].combine_first(df_oper["営業利益_ttm"])
    df_oper = df_oper.dropna(subset=["採用営業利益_百万円"])
    
    # 百万円 → 億円換算（1億円 = 100百万円）
    # finance_notes.forecast_op / pl_quarter.営業利益 は百万円単位として扱う。
    DIVISOR_TO_OKU = 100.0
    # P1-225: 億円整数へ丸めてから比率計算しない。小型株の0.3億→0億化を防ぐ。
    df_oper["営業利益_億"] = pd.to_numeric(df_oper["採用営業利益_百万円"], errors="coerce") / DIVISOR_TO_OKU

    # 4. 時価総額と結合して営利対時価を計算
    try:
        sc = pd.read_sql_query("SELECT コード, 時価総額億円 FROM screener", conn)
        # P1-241: 計算JOINはcanonical、UPDATEはscreenerの実rawコードへ戻す。
        sc["_screener_code_raw"] = sc["コード"]
        sc["コード"] = sc["コード"].map(canonical_code_for_db)
    except Exception as e:
        raise RuntimeError(f"operating-income screener read failed: {e}") from e

    df = df_oper.merge(sc, on="コード", how="inner")

    def _calc_ratio(row: pd.Series) -> Optional[float]:
        op_oku = row.get("営業利益_億")
        mc_oku = row.get("時価総額億円")
        if pd.isna(op_oku) or pd.isna(mc_oku) or float(mc_oku) <= 0: 
            return None
        return round(float(op_oku) * 100.0 / float(mc_oku), 2)

    df["営利対時価_pct"] = df.apply(_calc_ratio, axis=1)

    # 5. UPDATE実行
    updates = [(round(float(r["営業利益_億"]), 4), float(r["営利対時価_pct"]) if pd.notna(r["営利対時価_pct"]) else None, str(r["_screener_code_raw"]))
               for _, r in df.iterrows() if pd.notna(r["営業利益_億"])]

    # P1-443: 旧値clearと今回値再付与を原子的にする。計算/読込失敗では旧snapshotを保持。
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_443_operating_income")
        cur.execute("UPDATE screener SET 営業利益=NULL, 営利対時価=NULL")
        if updates:
            cur.executemany("""
                UPDATE screener
                SET 営業利益 = ?, 営利対時価 = ?
                WHERE コード = ?
            """, updates)
        cur.execute("RELEASE SAVEPOINT p1_443_operating_income")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_443_operating_income")
            cur.execute("RELEASE SAVEPOINT p1_443_operating_income")
        except Exception:
            pass
        raise
    finally:
        cur.close()

    if not updates:
        print("[oper] 更新対象レコードなし（旧値はクリア）")
        return
    print(f"[oper] 予想優先で営業利益・営利対時価を更新しました rows={len(updates)}")

# ==========================================
# ★ 追加: 季節調整済み進捗率の計算
# ==========================================
def update_seasonal_progress(conn: sqlite3.Connection) -> None:
    """
    季節調整済み進捗差分 v13
    quarterly_actual_history + tdnet_xbrl_metrics 実DB準拠版

    現在進捗率:
        screener.進捗率
        （finance_notes.progress_percent 由来の既存値を利用）

    現在Q:
        quarterly_actual_history の銘柄別・最新 fiscal_key / quarter_no

    過去同Q進捗率:
        quarterly_actual_history の3ヵ月単独 operating_profit を
        同一 fiscal_key 内で quarter_no 順に累積し、
        その年度の最終通期営業利益 actual_op で割る。

    通期営業利益 actual_op:
        1) tdnet_xbrl_metrics.actual_op を最優先
        2) forecast_achievement_history.actual_op を補完
        3) quarterly_actual_history が1Q〜4Q揃う年度は4Q合計を最終fallback

    過去平均:
        現在 fiscal_key を除外した同じ quarter_no の過去最大5年度。
        原則2年度以上で採用。

    季節調整済進捗差分:
        今回進捗率 - 過去平均進捗率
    """
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
    }

    required_tables = {"screener", "quarterly_actual_history"}
    missing_tables = required_tables - tables
    if missing_tables:
        raise RuntimeError(
            "季節調整に必要なテーブルがありません: "
            + ", ".join(sorted(missing_tables))
        )

    screener_cols = {
        r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()
    }
    qah_cols = {
        r[1]
        for r in conn.execute(
            "PRAGMA table_info(quarterly_actual_history)"
        ).fetchall()
    }

    required_sc = {"コード", "進捗率"}
    required_qah = {
        "コード", "fiscal_key", "quarter_no",
        "operating_profit", "announcement_date"
    }

    miss_sc = required_sc - screener_cols
    miss_qah = required_qah - qah_cols
    if miss_sc:
        raise RuntimeError(
            "screener に季節調整用の必須列がありません: "
            + ", ".join(sorted(miss_sc))
        )
    if miss_qah:
        raise RuntimeError(
            "quarterly_actual_history に必須列がありません: "
            + ", ".join(sorted(miss_qah))
        )

    if "過去平均進捗率" not in screener_cols:
        conn.execute("ALTER TABLE screener ADD COLUMN 過去平均進捗率 REAL")
    if "季節調整済進捗差分" not in screener_cols:
        conn.execute("ALTER TABLE screener ADD COLUMN 季節調整済進捗差分 REAL")
    conn.commit()

    # P1-444: 計算途中では旧snapshotを触らない。意味的に「計算結果なし」まで
    # 正常判定できた時、または新updates完成後だけclear+writeを一括確定する。
    def _seasonality_commit(_updates=None):
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_444_seasonality")
            cur.execute("UPDATE screener SET 過去平均進捗率=NULL, 季節調整済進捗差分=NULL")
            if _updates:
                cur.executemany(
                    """
                    UPDATE screener
                    SET 過去平均進捗率=?,
                        季節調整済進捗差分=?
                    WHERE コード=?
                    """,
                    _updates,
                )
            cur.execute("RELEASE SAVEPOINT p1_444_seasonality")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_444_seasonality")
                cur.execute("RELEASE SAVEPOINT p1_444_seasonality")
            except Exception:
                pass
            raise
        finally:
            cur.close()

    # --------------------------------------------------------
    # 1. 現在進捗率
    # --------------------------------------------------------
    current = pd.read_sql_query(
        """
        SELECT コード, 進捗率 AS 今回進捗率
        FROM screener
        WHERE 進捗率 IS NOT NULL
        """,
        conn,
    )
    if current.empty:
        print("[seasonality] screener.進捗率 が空のため更新なし")
        _seasonality_commit()
        return

    # P1-211: 季節進捗の全データソースをcanonical codeで結合。
    # P1-242: 計算JOINはcanonical、最終UPDATEはscreenerの実rawコードを使う。
    current["_screener_code_raw"] = current["コード"]
    current["コード"] = current["コード"].map(canonical_code_for_db)
    current["今回進捗率"] = pd.to_numeric(
        current["今回進捗率"], errors="coerce"
    )
    # -1/-2等のステータス値は除外
    current = current[
        current["今回進捗率"].notna()
        & (current["今回進捗率"] >= 0)
        & np.isfinite(current["今回進捗率"])
    ].copy()

    if current.empty:
        print("[seasonality] 有効な現在進捗率なし")
        _seasonality_commit()
        return

    # P1-222: 旧値クリアは全銘柄に対して上で完了済み。

    # --------------------------------------------------------
    # 2. 四半期単独実績
    # --------------------------------------------------------
    # P1-625: legacy aliasが共存する場合、値の非NULL条件で先に絞ると
    # 最新正本Qのoperating_profitがNULLでも古いaliasの非NULL値を復活できる。
    # またUPSERT更新ではrowidが進まないため、updated_atを主に正本Qを先に確定する。
    _qhist_cols = {
        r[1] for r in conn.execute(
            "PRAGMA table_info(quarterly_actual_history)"
        ).fetchall()
    }
    _qhist_updated_expr = "updated_at" if "updated_at" in _qhist_cols else "NULL AS updated_at"
    q = pd.read_sql_query(
        f"""
        SELECT
            rowid AS _rowid,
            コード,
            fiscal_key,
            quarter_no,
            quarter_label,
            announcement_date,
            operating_profit,
            {_qhist_updated_expr}
        FROM quarterly_actual_history
        WHERE fiscal_key IS NOT NULL
          AND quarter_no IS NOT NULL
        """,
        conn,
    )
    if q.empty:
        print("[seasonality] quarterly_actual_history が空のため更新なし")
        _seasonality_commit()
        return

    q["_raw_code"] = q["コード"].astype(str).str.strip()
    q["コード"] = q["コード"].map(canonical_code_for_db)
    q["fiscal_key"] = q["fiscal_key"].astype(str).str.strip()
    q["quarter_no"] = pd.to_numeric(q["quarter_no"], errors="coerce")
    q["operating_profit"] = pd.to_numeric(
        q["operating_profit"], errors="coerce"
    )
    # P1-650: 発表/更新時刻はtimezone表現が混在しても同じJST壁時計へ正規化。
    q["announcement_date"] = q["announcement_date"].map(_p1_608_jst_naive_ts)
    q["_updated_sort"] = q.get("updated_at").map(_p1_608_jst_naive_ts)
    q["_canon_match"] = (
        q["_raw_code"].str.upper() == q["コード"].astype(str).str.upper()
    ).astype(int)

    # P1-537: quarterly_actual_historyは実績履歴。legacy誤データの未来発表日を
    # 「現在Q」として採用し、季節進捗を未来年度へ飛ばさない。
    _season_today = pd.Timestamp(date.fromisoformat(_today_jst()))
    q = q[
        q["quarter_no"].isin([1, 2, 3, 4])
        & (q["announcement_date"].isna() | (q["announcement_date"].dt.normalize() <= _season_today))
    ].copy()
    if q.empty:
        print("[seasonality] 有効な四半期履歴なし")
        _seasonality_commit()
        return

    q["quarter_no"] = q["quarter_no"].astype(int)

    # P1-625: 同一logical code / fiscal_key / Qの正本を、値の有無より先に確定。
    # current producerのUPSERT更新時刻を主、canonical raw表記をtie-breakにする。
    q = (
        q.sort_values(
            ["コード", "fiscal_key", "quarter_no", "_updated_sort",
             "_canon_match", "announcement_date", "_rowid"],
            kind="stable", na_position="first"
        )
        .drop_duplicates(
            subset=["コード", "fiscal_key", "quarter_no"],
            keep="last",
        )
        .reset_index(drop=True)
    )
    q = q[q["operating_profit"].notna()].copy()
    if q.empty:
        print("[seasonality] 正本Qに有効な四半期営業利益なし")
        _seasonality_commit()
        return
    q = q.drop(columns=["_raw_code", "_updated_sort", "_canon_match", "updated_at"], errors="ignore")

    # 3ヵ月単独値を年度内で累積
    q = q.sort_values(
        ["コード", "fiscal_key", "quarter_no"]
    ).reset_index(drop=True)
    q["累積営業利益"] = (
        q.groupby(["コード", "fiscal_key"], sort=False)["operating_profit"]
        .cumsum()
    )

    # --------------------------------------------------------
    # 3. 通期営業利益 actual_op
    # --------------------------------------------------------
    actual_frames = []

    # 3-A. tdnet_xbrl_metrics を最優先
    if "tdnet_xbrl_metrics" in tables:
        xcols = {
            r[1]
            for r in conn.execute(
                "PRAGMA table_info(tdnet_xbrl_metrics)"
            ).fetchall()
        }
        if {
            "コード", "提出時刻", "actual_fiscal_key", "actual_op"
        }.issubset(xcols):
            # P1-628: 同一TDnet開示がlegacy aliasとcanonicalの両方で残る場合、
            # actual_op IS NOT NULL を先に掛けると、再解析でNULLへ無効化された正本より
            # 古いaliasの誤抽出値を復活させ得る。まず同一開示イベントの正本を確定する。
            _x_updated_expr = "updated_at" if "updated_at" in xcols else "NULL AS updated_at"
            _x_title_expr = 'タイトル' if "タイトル" in xcols else "'' AS タイトル"
            x = pd.read_sql_query(
                f"""
                SELECT
                    rowid AS _rowid,
                    コード,
                    提出時刻,
                    {_x_title_expr},
                    actual_fiscal_key AS fiscal_key,
                    actual_op,
                    {_x_updated_expr}
                FROM tdnet_xbrl_metrics
                WHERE actual_fiscal_key IS NOT NULL
                """,
                conn,
            )
            if not x.empty:
                x["_raw_code"] = x["コード"].astype(str).str.strip()
                x["コード"] = x["コード"].map(canonical_code_for_db)
                x["fiscal_key"] = x["fiscal_key"].astype(str).str.strip()
                x["actual_op"] = pd.to_numeric(
                    x["actual_op"], errors="coerce"
                )
                # P1-650: TDNET提出時刻/更新時刻もJST-naive正規化。
                x["_submit_sort"] = x["提出時刻"].map(_p1_608_jst_naive_ts)
                x["_submit_key"] = x["_submit_sort"].map(
                    lambda _v: _v.isoformat() if pd.notna(_v) else ""
                )
                _submit_key_missing = x["_submit_key"].eq("")
                x.loc[_submit_key_missing, "_submit_key"] = x.loc[_submit_key_missing, "提出時刻"].fillna("").astype(str).str.strip()
                x["_updated_sort"] = x.get("updated_at").map(_p1_608_jst_naive_ts)
                x["_canon_match"] = (
                    x["_raw_code"].str.upper() == x["コード"].astype(str).str.upper()
                ).astype(int)
                x["タイトル"] = x.get("タイトル", "").fillna("").astype(str)

                # 1) 同一logical event（code + 提出時刻 + title）のauthoritative rowを先に選択。
                x = (
                    x.sort_values(
                        ["コード", "_submit_sort", "_submit_key", "タイトル", "_updated_sort",
                         "_canon_match", "_rowid"],
                        kind="stable", na_position="first"
                    )
                    .drop_duplicates(
                        ["コード", "_submit_key", "タイトル"], keep="last"
                    )
                    .reset_index(drop=True)
                )
                # 2) 正本イベントで有効なactualだけ残し、年度内では最新開示を採用。
                x = x[x["actual_op"].notna() & (x["actual_op"] > 0)].copy()
                if not x.empty:
                    x = (
                        x.sort_values(
                            ["コード", "fiscal_key", "_submit_sort", "_updated_sort",
                             "_canon_match", "_rowid"],
                            kind="stable", na_position="first"
                        )
                        .drop_duplicates(["コード", "fiscal_key"], keep="last")
                    )
                    x = x[["コード", "fiscal_key", "actual_op"]]
                    x["actual_source"] = "tdnet_xbrl"
                    actual_frames.append(x)

    # 3-B. forecast_achievement_history を補完
    if "forecast_achievement_history" in tables:
        ah_cols = {
            r[1]
            for r in conn.execute(
                "PRAGMA table_info(forecast_achievement_history)"
            ).fetchall()
        }
        if {"コード", "fiscal_key", "actual_op"}.issubset(ah_cols):
            # P1-627: forecast_achievement_history は同じ raw code / fiscal_key を
            # UPSERT更新するため、rowidは更新の新しさを表さない。legacy aliasが共存すると
            # 古いaliasの大きいrowidが、後から更新されたcanonical正本を負かし得る。
            _ah_updated_expr = "updated_at" if "updated_at" in ah_cols else "NULL AS updated_at"
            _ah_actual_date_expr = "actual_date" if "actual_date" in ah_cols else "NULL AS actual_date"
            ah = pd.read_sql_query(
                f"""
                SELECT rowid AS _rowid, コード, fiscal_key, actual_op,
                       {_ah_updated_expr}, {_ah_actual_date_expr}
                FROM forecast_achievement_history
                WHERE fiscal_key IS NOT NULL
                """,
                conn,
            )
            if not ah.empty:
                ah["_raw_code"] = ah["コード"].astype(str).str.strip()
                ah["コード"] = ah["コード"].map(canonical_code_for_db)
                ah["fiscal_key"] = ah["fiscal_key"].astype(str).str.strip()
                ah["actual_op"] = pd.to_numeric(
                    ah["actual_op"], errors="coerce"
                )
                # P1-650: achievement更新/実績日もJST-naiveへ統一してaware/naive混在を排除。
                ah["_updated_sort"] = ah.get("updated_at").map(_p1_608_jst_naive_ts)
                ah["_actual_date_sort"] = ah.get("actual_date").map(_p1_608_jst_naive_ts)
                ah["_canon_match"] = (
                    ah["_raw_code"].str.upper() == ah["コード"].astype(str).str.upper()
                ).astype(int)
                # 値の有無で古いaliasを先に残さず、年度のauthoritative rowを先に確定する。
                ah = (
                    ah.sort_values(
                        ["コード", "fiscal_key", "_updated_sort", "_canon_match",
                         "_actual_date_sort", "_rowid"],
                        kind="stable", na_position="first"
                    )
                    .drop_duplicates(["コード", "fiscal_key"], keep="last")
                    .reset_index(drop=True)
                )
                ah = ah[ah["actual_op"].notna() & (ah["actual_op"] > 0)].copy()
                if not ah.empty:
                    ah = ah[["コード", "fiscal_key", "actual_op"]]
                    ah["actual_source"] = "achievement"
                    actual_frames.append(ah)

    # 3-C. 1Q〜4Qが揃う年度は四半期合計を fallback
    q_year = (
        q.groupby(["コード", "fiscal_key"], as_index=False)
        .agg(
            q_count=("quarter_no", "nunique"),
            actual_op=("operating_profit", "sum"),
        )
    )
    q_year = q_year[
        (q_year["q_count"] == 4)
        & q_year["actual_op"].notna()
        & (q_year["actual_op"] > 0)
    ].copy()
    if not q_year.empty:
        q_year = q_year[["コード", "fiscal_key", "actual_op"]]
        q_year["actual_source"] = "quarter_sum"
        actual_frames.append(q_year)

    if not actual_frames:
        print("[seasonality] 通期営業利益 actual_op を作れず更新なし")
        _seasonality_commit()
        return

    actual = pd.concat(actual_frames, ignore_index=True)

    # 優先順位: XBRL > achievement > quarter_sum
    actual["_pri"] = actual["actual_source"].map(
        {"tdnet_xbrl": 0, "achievement": 1, "quarter_sum": 2}
    ).fillna(9)

    actual = (
        actual.sort_values(["コード", "fiscal_key", "_pri"])
        .drop_duplicates(["コード", "fiscal_key"], keep="first")
        .drop(columns=["_pri"])
        .reset_index(drop=True)
    )

    # --------------------------------------------------------
    # 4. 過去各Qの進捗率
    # --------------------------------------------------------
    hist = q.merge(
        actual,
        on=["コード", "fiscal_key"],
        how="inner",
    )
    if hist.empty:
        print("[seasonality] 四半期実績と通期実績の年度一致なし")
        _seasonality_commit()
        return

    hist["過去進捗率"] = (
        hist["累積営業利益"] / hist["actual_op"] * 100.0
    )

    # 赤字通期は既に除外。極端値だけ安全のため除外
    hist = hist[
        hist["過去進捗率"].notna()
        & np.isfinite(hist["過去進捗率"])
        & (hist["過去進捗率"] >= -100.0)
        & (hist["過去進捗率"] <= 300.0)
    ].copy()

    if hist.empty:
        print("[seasonality] 有効な過去進捗率なし")
        _seasonality_commit()
        return

    # --------------------------------------------------------
    # 5. 現在Q / 現在fiscal_key
    # --------------------------------------------------------
    # 最新announcement_dateを優先し、同日ならquarter_no大を採用。
    # P1-537: 発表日不明の古い履歴は過去平均には使えるが「現在Q」には昇格させない。
    _q_current = q[q["announcement_date"].notna()].copy()
    latest_q = (
        _q_current.sort_values(
            ["コード", "announcement_date", "fiscal_key", "quarter_no", "_rowid"],
            kind="stable", na_position="first"
        )
        .groupby("コード", sort=False)
        .tail(1)[
            ["コード", "fiscal_key", "quarter_no", "announcement_date"]
        ]
        .rename(
            columns={
                "fiscal_key": "現在fiscal_key",
                "quarter_no": "現在Q",
                "announcement_date": "現在Q発表日",
            }
        )
    )

    current = current.merge(latest_q, on="コード", how="inner")
    if current.empty:
        print("[seasonality] 現在Qを特定できる銘柄なし")
        _seasonality_commit()
        return

    # --------------------------------------------------------
    # 6. 同Qの過去最大5年度から平均
    # --------------------------------------------------------
    hist_for_avg = hist.merge(
        latest_q[["コード", "現在fiscal_key", "現在Q"]],
        left_on=["コード", "quarter_no"],
        right_on=["コード", "現在Q"],
        how="inner",
    )

    # 現在年度を教師に混ぜない
    hist_for_avg = hist_for_avg[
        hist_for_avg["fiscal_key"] != hist_for_avg["現在fiscal_key"]
    ].copy()

    if hist_for_avg.empty:
        print("[seasonality] 過去同Q履歴なし")
        _seasonality_commit()
        return

    # fiscal_key YYYY-MM を時系列化し、直近5年度のみ
    hist_for_avg["_fy_sort"] = pd.to_datetime(
        hist_for_avg["fiscal_key"] + "-01",
        errors="coerce",
    )
    hist_for_avg = (
        hist_for_avg.sort_values(
            ["コード", "quarter_no", "_fy_sort"],
            ascending=[True, True, False],
        )
        .groupby(["コード", "quarter_no"], group_keys=False)
        .head(5)
        .copy()
    )

    avg = (
        hist_for_avg.groupby(
            ["コード", "quarter_no"], as_index=False
        )
        .agg(
            過去平均進捗率=("過去進捗率", "mean"),
            過去進捗標準偏差=("過去進捗率", "std"),
            過去同Q件数=("過去進捗率", "count"),
        )
        .rename(columns={"quarter_no": "現在Q"})
    )

    # v13:
    # 2年度以上は正式採用、1年度のみは参考値としてfallback採用。
    # 表示列は増やさず、履歴数はログで監査する。
    avg = avg[avg["過去同Q件数"] >= 1].copy()
    if avg.empty:
        print("[seasonality] 過去同Q履歴がある銘柄なし")
        _seasonality_commit()
        return

    merged = current.merge(
        avg,
        on=["コード", "現在Q"],
        how="inner",
    )
    if merged.empty:
        print("[seasonality] 現在Qと過去同Q平均の一致なし")
        _seasonality_commit()
        return

    merged["季節調整済進捗差分"] = (
        merged["今回進捗率"] - merged["過去平均進捗率"]
    )

    # --------------------------------------------------------
    # 7. DB更新
    # --------------------------------------------------------
    updates = []
    for _, r in merged.iterrows():
        if (
            pd.notna(r["過去平均進捗率"])
            and pd.notna(r["季節調整済進捗差分"])
        ):
            updates.append(
                (
                    round(float(r["過去平均進捗率"]), 2),
                    round(float(r["季節調整済進捗差分"]), 2),
                    str(r["_screener_code_raw"]),
                )
            )

    if not updates:
        print("[seasonality] 計算可能な銘柄なし")
        _seasonality_commit()
        return

    _seasonality_commit(updates)

    q_counts = (
        merged["現在Q"].value_counts().sort_index().to_dict()
    )
    hist_median = float(merged["過去同Q件数"].median())
    source_counts = actual["actual_source"].value_counts().to_dict()
    hist_tiers = {
        "1年参考": int((merged["過去同Q件数"] == 1).sum()),
        "2年以上": int((merged["過去同Q件数"] >= 2).sum()),
    }

    print(
        f"[seasonality] v13 更新={len(updates)}銘柄 "
        f"Q内訳={q_counts} "
        f"過去同Q件数中央値={hist_median:.1f} "
        f"履歴内訳={hist_tiers} "
        f"actual_source={source_counts}"
    )

def _to_raw(v):
    if isinstance(v, dict):
        return v.get("raw", v.get("fmt"))
    return v

# ==== _to_raw が無ければ保険で定義（既にあれば不要）====
try:
    _to_raw
except NameError:
    def _to_raw(val):
        """yahooqueryが返す {'raw':x,'fmt':y} や文字列などを数値に寄せる."""
        if isinstance(val, dict):
            val = val.get("raw", val.get("fmt"))
        if val is None:
            return None
        try:
            return float(str(val).replace(",", ""))
        except Exception:
            return None


def _load_prev_close_map_for_trade_date(conn: sqlite3.Connection, codes, trade_date) -> dict[str, float]:
    """P2-31: MIDDAY用の前営業日終値を複数銘柄まとめて返す。"""
    try:
        td = pd.Timestamp(trade_date).date()
    except Exception:
        td = _expected_jpx_asof_date()
    extra = _load_extra_closed(EXTRA_CLOSED_PATH)
    expected_prev = prev_business_day_jp(td, extra).isoformat()
    keys = list(dict.fromkeys(canonical_code_for_db(c) for c in (codes or []) if canonical_code_for_db(c)))
    out = {}
    for i in range(0, len(keys), 200):
        part = keys[i:i + 200]
        qvars = expand_code_query_variants(part)
        if not qvars:
            continue
        ph = ",".join("?" * len(qvars))
        df = pd.read_sql_query(
            f"SELECT rowid AS _rowid, コード, 日付, 終値 FROM price_history "
            f"WHERE CAST(コード AS TEXT) IN ({ph}) AND date(日付)=date(?) "
            f"ORDER BY rowid ASC",
            conn, params=[*qvars, expected_prev]
        )
        if df.empty:
            continue
        df = _dedupe_price_history_df(df)
        for _, r in df.iterrows():
            ck = canonical_code_for_db(r.get("コード"))
            v = pd.to_numeric(pd.Series([r.get("終値")]), errors="coerce").iloc[0]
            if ck and pd.notna(v) and np.isfinite(float(v)) and float(v) != 0.0:
                out[ck] = float(v)
    return out


# ===== RVOL/売買代金 自動更新ユーティリティ =====

def _jp_session_progress(dt: datetime | None = None) -> float:
    """P1-292: 東証現行時間 9:00–11:30 / 12:30–15:30 を330分=1.0で換算。"""
    if dt is None:
        dt = _now_jst()
    m = dt.hour * 60 + dt.minute
    s1, e1, s2, e2 = 9*60, 11*60+30, 12*60+30, 15*60+30
    total = 330.0
    if m < s1: return 0.0
    if s1 <= m <= e1: return (m - s1) / total
    if e1 < m < s2:  return 150.0 / total
    if s2 <= m <= e2: return (150.0 + (m - s2)) / total
    return 1.0

def apply_auto_metrics_midday(conn: sqlite3.Connection,
                              use_time_progress: bool = True,
                              denom_floor: float = 1.0,
                              progress_floor: float = 0.33):
    """MIDDAY売買代金/RVOLを原子的に更新。P1-445。"""
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_445_auto_metrics_midday")
        cur.execute("""
            UPDATE screener
            SET 売買代金億 =
              CASE
                WHEN 現在値 IS NOT NULL AND 出来高 IS NOT NULL AND 出来高 > 0
                THEN ROUND((現在値 * 出来高) / 100000000.0, 2)
              END
        """)
        if use_time_progress:
            f = max(_jp_session_progress(), progress_floor)
            cur.execute("""
                WITH p AS (SELECT ? AS f, ? AS dmin)
                UPDATE screener
                SET RVOL代金 =
                  CASE
                    WHEN 売買代金億 IS NOT NULL AND 売買代金20日平均億 IS NOT NULL
                    THEN ROUND(
                      売買代金億 /
                      ((CASE WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                             THEN (SELECT dmin FROM p) ELSE 売買代金20日平均億 END) * (SELECT f FROM p)), 2)
                  END
            """, (f, denom_floor))
        else:
            cur.execute("""
                WITH p AS (SELECT ? AS dmin)
                UPDATE screener
                SET RVOL代金 =
                  CASE
                    WHEN 売買代金億 IS NOT NULL AND 売買代金20日平均億 IS NOT NULL
                    THEN ROUND(
                      売買代金億 /
                      (CASE WHEN 売買代金20日平均億 < (SELECT dmin FROM p)
                            THEN (SELECT dmin FROM p) ELSE 売買代金20日平均億 END), 2)
                  END
            """, (denom_floor,))
        cur.execute("RELEASE SAVEPOINT p1_445_auto_metrics_midday")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_445_auto_metrics_midday")
            cur.execute("RELEASE SAVEPOINT p1_445_auto_metrics_midday")
        except Exception:
            pass
        raise
    finally:
        cur.close()


def apply_auto_metrics_eod(conn: sqlite3.Connection, denom_floor: float = 1.0):
    """P1-286: EOD売買代金/RVOLを論理銘柄の異なる営業日で再計算する。"""
    # P1-665: current売買代金/RVOLの基準日はDB内の最大日ではなく、
    # このrunで到達すべきJPX snapshot日を正本とする。
    # PREOPENでは前営業日、EODでは当日（休場日は直近営業日）。
    # 全対象がsentinel/API未到達でも、古いDB最大日をcurrentとして自己認定しない。
    _eod_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    hist = pd.read_sql_query("""
        SELECT rowid AS _rowid, コード, 日付, 終値, 出来高
        FROM price_history
        WHERE date(日付) >= date(?, '-140 day') AND date(日付)<=date(?)
        ORDER BY 日付, rowid
    """, conn, params=[_eod_asof, _eod_asof])
    hist = _dedupe_price_history_df(hist)
    if not hist.empty:
        hist["終値"] = pd.to_numeric(hist["終値"], errors="coerce")
        hist["出来高"] = pd.to_numeric(hist["出来高"], errors="coerce")
    raw_map = {}
    for (_raw,) in conn.execute("SELECT コード FROM screener").fetchall():
        k = canonical_code_for_db(_raw)
        if k and k not in raw_map:
            raw_map[k] = _raw
    by_code = {k:g.sort_values("日付") for k,g in hist.groupby("コード", sort=False)} if not hist.empty else {}
    updates=[]
    for k, raw in raw_map.items():
        g=by_code.get(k)
        latest_turn = avg20 = rvol = None
        if g is not None and not g.empty:
            valid = g[g["終値"].notna() & g["出来高"].notna() & (g["終値"]>0) & (g["出来高"]>=0)].copy()
            if not valid.empty:
                turns = valid["終値"].astype(float) * valid["出来高"].astype(float) / 1e8
                avg20 = round(float(turns.tail(20).mean()), 2) if not turns.tail(20).empty else None
                # P1-565: 個別最終足が市場EOD as-ofに到達していない銘柄へ、
                # 前営業日の売買代金/RVOLを「今日の値」として書き戻さない。
                # 20日平均は履歴統計として保持できるが、current turnover/RVOLはNULLにする。
                try:
                    _valid_last_date = pd.to_datetime(valid["日付"], errors="coerce").dropna().max().date().isoformat()
                except Exception:
                    _valid_last_date = None
                if _valid_last_date == str(_eod_asof)[:10]:
                    latest_turn = round(float(turns.iloc[-1]), 2)
                    if avg20 is not None:
                        denom=max(float(avg20), float(denom_floor))
                        rvol=round(float(latest_turn)/denom, 2) if denom>0 else None
        updates.append((latest_turn, avg20, rvol, raw))
    cur=conn.cursor()
    _sp = f"sp_auto_eod_metrics_{time.time_ns()}"
    conn.execute(f"SAVEPOINT {_sp}")
    try:
        cur.executemany("UPDATE screener SET 売買代金億=?, 売買代金20日平均億=?, RVOL代金=? WHERE コード=?", updates)
        conn.execute(f"RELEASE SAVEPOINT {_sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {_sp}")
            conn.execute(f"RELEASE SAVEPOINT {_sp}")
        except Exception:
            pass
        raise
    finally:
        cur.close()
    # P1-428: EOD売買代金/RVOLの多行書戻しを原子的に。

# ===== /RVOL/売買代金 自動更新ユーティリティ =====

def open_html_locally(html_path: str, wait_sec: float = 0.0, cool_min: int = 0, force: bool = False) -> bool:
    """
    ローカルHTMLを既定ブラウザで開く（Windowsは os.startfile を優先）。
    - cool_min > 0 なら、直近オープンからその分(分)は再オープンしない
    - force=True でクールダウン無視
    戻り値: 開けたら True
    """
    p = Path(html_path).resolve()
    if not p.exists():
        raise FileNotFoundError(f"not found: {p}")

    stamp = p.with_suffix(p.suffix + ".opened")
    if not force and cool_min > 0 and stamp.exists():
        if time.time() - stamp.stat().st_mtime < cool_min * 60:
            return False  # クールダウン中

    if wait_sec > 0:
        # P1-364: dt_timeはdatetime.timeクラスでsleepを持たない。
        time.sleep(wait_sec)

    opened = False

    # 1) Windowsは最も確実な ShellExecute 相当で開く
    if os.name == "nt":
        try:
            os.startfile(str(p))  # 既定ブラウザで開く
            opened = True
        except Exception:
            opened = False

    # 2) うまくいかなければ webbrowser にフォールバック
    if not opened:
        url = p.as_uri()  # file:///H:/... に変換
        try:
            opened = webbrowser.open(url) or webbrowser.open_new_tab(url)
        except Exception:
            opened = False

    # 3) 成功したらスタンプ作成
    if opened:
        try:
            stamp.touch()
        except Exception:
            pass

    return opened

def _percentile_rank_01_neutral(series: pd.Series, *, inverse: bool = False) -> pd.Series:
    """P3-1: 有効値を0..1へ順位化。最下位0/最上位1、単一値・全同値は中立0.5。

    pandas ``rank(pct=True)`` は最下位でも1/N、全同値では(N+1)/(2N)、
    N=1では1.0になる。スコア配点へ直接使うと、母集団サイズや同値数だけで
    加点量が変わるため、端点を明示した順位へ正規化する。欠損はNaNのまま保持。
    ``inverse=True`` は小さい値ほど高評価（過熱抑制など）。
    """
    out = pd.Series(np.nan, index=series.index, dtype=float)
    vals = pd.to_numeric(series, errors="coerce")
    valid = vals.notna() & np.isfinite(vals)
    n = int(valid.sum())
    if n == 0:
        return out
    vv = vals.loc[valid]
    if n == 1 or vv.nunique(dropna=True) <= 1:
        out.loc[valid] = 0.5
        return out
    ranks = vv.rank(method="average", ascending=not inverse)
    out.loc[valid] = (ranks - 1.0) / float(n - 1)
    return out.clip(0.0, 1.0)


def apply_composite_score(conn: sqlite3.Connection,
                          w_rate=0.4, w_rvol=0.4, w_turn=0.2):
    """
    合成スコア = 前日騰落 / RVOL / 売買代金 のpercentile合成。
    P1-106: 欠損成分を0点として固定配点に入れず、利用可能2成分以上で重みを再正規化。
    1成分以下は信頼不足としてNULL。指数もNULL。
    """
    df = pd.read_sql_query("""
        SELECT コード, 前日終値比率, RVOL代金, 売買代金億, 市場
        FROM screener
    """, conn)
    if df.empty: return
    df['is_index'] = df['市場'].astype(str).str.contains('指数') | df['コード'].astype(str).str.startswith('^')
    for c in ["前日終値比率", "RVOL代金", "売買代金億"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")

    # P1-567: 正式なpercentile母集団は、今回の市場as-of日に価格行が存在する個別株だけ。
    # 個別休止/取得漏れで前営業日のscreener値が残っても、古い騰落率/RVOL/売買代金を
    # 今日の合成スコアへ再ランクしない。stale銘柄の合成スコアはNULLへ戻す。
    # P1-677: current score母集団の基準日はDB内の最新有効日に自己決定させず、
    # このrunが要求するJPX snapshot日へ固定する。clear-only MIDDAY等で当日有効足が0件でも
    # 前営業日をfresh扱いして旧合成スコアを再生成しない。
    _score_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    _fresh_score_codes = set()
    if _score_asof:
        try:
            _fresh_rows = conn.execute(
                "SELECT コード, 終値 FROM price_history WHERE date(日付)=date(?)",
                (_score_asof,),
            ).fetchall()
            # P1-679: formal score fresh集合も有限な正の終値があるlogical codeだけ。
            _fresh_score_codes = set()
            for _r in _fresh_rows:
                if not _r:
                    continue
                _ck = canonical_code_for_db(_r[0])
                _cv = ffloat(_r[1], None)
                if _ck and _cv is not None and math.isfinite(float(_cv)) and float(_cv) > 0:
                    _fresh_score_codes.add(_ck)
        except Exception as _e:
            raise RuntimeError(f"composite score freshness lookup failed: {_e}") from _e
    df['_code_key'] = df['コード'].map(canonical_code_for_db)
    df['is_fresh'] = df['_code_key'].isin(_fresh_score_codes)
    # P3-2: percentile母集団はlogical codeごとに1銘柄1票。
    # legacy aliasが残っていても7203/7203.0等を2票として順位分布へ混ぜない。
    df['_canonical_row'] = [
        str(raw).strip().upper() == str(key).strip().upper()
        for raw, key in zip(df['コード'], df['_code_key'])
    ]
    ds = (
        df[(~df['is_index']) & df['is_fresh'] & df['_code_key'].astype(bool)]
        .sort_values(['_canonical_row'], ascending=False, kind='stable')
        .drop_duplicates('_code_key', keep='first')
        .copy()
    )

    def _pct_valid(s):
        return _percentile_rank_01_neutral(s) * 100.0
    r_rate=_pct_valid(ds['前日終値比率']); r_rvol=_pct_valid(ds['RVOL代金']); r_turn=_pct_valid(ds['売買代金億'])
    v_rate=ds['前日終値比率'].notna(); v_rvol=ds['RVOL代金'].notna(); v_turn=ds['売買代金億'].notna()
    valid_count=v_rate.astype(int)+v_rvol.astype(int)+v_turn.astype(int)
    weight_sum=w_rate*v_rate.astype(float)+w_rvol*v_rvol.astype(float)+w_turn*v_turn.astype(float)
    weighted=(r_rate.fillna(0)*w_rate + r_rvol.fillna(0)*w_rvol + r_turn.fillna(0)*w_turn)
    ds['合成スコア']=(weighted/weight_sum.replace(0,np.nan)).where(valid_count>=2).round(1)

    score_map=ds.set_index('_code_key')['合成スコア'].to_dict()
    updates=[]
    for _, r in df.iterrows():
        _rk = r['_code_key']
        val=None if (r['is_index'] or not bool(r['is_fresh'])) else score_map.get(_rk)
        if val is not None and pd.isna(val): val=None
        updates.append((None if val is None else float(val), str(r['コード'])))
    # P1-446: 全銘柄のpercentile合成は一括snapshotで反映。
    cur=conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_446_composite_score")
        cur.executemany("UPDATE screener SET 合成スコア=? WHERE コード=?", updates)
        cur.execute("RELEASE SAVEPOINT p1_446_composite_score")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_446_composite_score")
            cur.execute("RELEASE SAVEPOINT p1_446_composite_score")
        except Exception:
            pass
        raise
    finally:
        cur.close()

def _theme_true_flow_score(median_turnover, signal_density):
    """P3-12: テーマ強度の共通定義。売買代金中央値×(1+シグナル密度)。"""
    mt = pd.to_numeric(median_turnover, errors="coerce")
    sd = pd.to_numeric(signal_density, errors="coerce").clip(lower=0.0, upper=1.0)
    return mt * (1.0 + sd)


def apply_shodou_score(conn: sqlite3.Connection):
    """
    「テーマ全体の強さ」を加味した新・初動スコア（95点設計）を計算。

    配点内訳:
      - テーマ強度順位: 30点
      - 有効テーマ一致: 20点
      - RVOL代金順位:   20点
      - 前日騰落順位:   15点
      - RS_5過熱抑制:    10点
      合計:              95点

    ※ 旧来の「100点満点」表記は配点合計と不一致だったため、P1-14で表記のみ訂正。
    ※ P1-15: テーマ強度順位のpercentile母集団は、有効テーマを持つ銘柄だけに限定。
       無テーマ銘柄の件数によるテーマ順位の水増しを防ぐ。
    """
    # P2-16: 初動スコア列の追加失敗をduplicate-column以外まで無視しない。
    _shodou_schema_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    if "初動スコア" not in _shodou_schema_cols:
        conn.execute('ALTER TABLE screener ADD COLUMN "初動スコア" REAL')

    # P1-123: 初動テーマ側もSQLite UTCのdate('now')を使わず、JST市場日付の30日窓に統一。
    _shodou_theme_today = date.fromisoformat(_today_jst())
    theme_cutoff = (_shodou_theme_today - timedelta(days=30)).isoformat()
    theme_today = _shodou_theme_today.isoformat()
    df_theme = pd.read_sql_query("""
        -- P1-36: 同一銘柄×テーマの日次スナップショット重複を1件へ畳み、active_stocksを実銘柄数にする。
        SELECT DISTINCT t.コード AS コード, m.theme_name AS テーマ
        FROM stock_theme_kabutan t
        JOIN theme_master m ON t.theme_id = m.theme_id
        -- P1-35/P1-123: 各銘柄×テーマ紐付け自身をJST基準の30日窓で絞る。
        WHERE date(t.取得日) >= date(?) AND date(t.取得日) <= date(?)
    """, conn, params=(theme_cutoff, theme_today))
    if not df_theme.empty:
        # P1-134: 初動テーマ側も285A等を数値CASTせず保持。
        df_theme["コード"] = df_theme["コード"].map(_normalize_jp_security_code)
        df_theme = df_theme[df_theme["コード"] != ""]
        # P1-482: raw DISTINCT後に7203/7203.0等が同じlogical codeへ畳まれるため、
        # active_stocks/中央値を水増ししないよう銘柄×テーマを再dedupe。
        df_theme = df_theme.drop_duplicates(["コード", "テーマ"], keep="last").reset_index(drop=True)
    
    df_s_all = pd.read_sql_query("SELECT コード, 前日終値比率, RVOL代金, RS_5, 売買代金億, 初動フラグ, 右肩早期フラグ, 右肩上がりフラグ FROM screener", conn)
    # P1-265/P3-2: テーマ結合はcanonical。順位母集団はlogical codeごと1行だけにし、
    # 最終UPDATEだけ全raw screener rowへ同じlogical scoreを戻す。
    df_s_all['_screener_code_raw'] = df_s_all['コード'].astype(str)
    df_s_all['コード'] = df_s_all['コード'].map(_normalize_jp_security_code)
    df_s_all['_canonical_row'] = [
        str(raw).strip().upper() == str(code).strip().upper()
        for raw, code in zip(df_s_all['_screener_code_raw'], df_s_all['コード'])
    ]
    df_s = (
        df_s_all[df_s_all['コード'].astype(bool)]
        .sort_values(['_canonical_row'], ascending=False, kind='stable')
        .drop_duplicates('コード', keep='first')
        .copy()
    )
    # P1-43: 売買代金欠損を0億円としてテーマ中央値へ混ぜない。
    df_s['売買代金(億)'] = pd.to_numeric(df_s['売買代金億'], errors='coerce')
    # P3-12: 表示側テーマランキングと同じsignal densityを初動テーマ強度にも使う。
    df_s['is_signaled'] = np.where(
        df_s['初動フラグ'].astype(str).str.contains('候補') |
        df_s['右肩早期フラグ'].astype(str).str.contains('候補') |
        df_s['右肩上がりフラグ'].astype(str).str.contains('候補'),
        1, 0
    )

    # P1-568: 初動スコアも今回の市場as-of日に価格行がある個別株だけを正式母集団にする。
    # stale銘柄の前日騰落/RVOL/売買代金をテーマflow中央値や技術percentileへ混ぜず、
    # その銘柄自身の初動スコアもNULLへ戻す。
    # P1-677: 初動スコアもrun期待snapshot日をfresh基準に固定。
    # DB最新有効日fallbackで前営業日のRVOL/騰落率/テーマflowをcurrent扱いしない。
    _shodou_asof = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()
    _fresh_shodou_codes = set()
    if _shodou_asof:
        try:
            _fresh_rows = conn.execute(
                "SELECT コード, 終値 FROM price_history WHERE date(日付)=date(?)",
                (_shodou_asof,),
            ).fetchall()
            # P1-679: 初動スコアfresh集合も有限な正の終値があるlogical codeだけ。
            _fresh_shodou_codes = set()
            for _r in _fresh_rows:
                if not _r:
                    continue
                _ck = _normalize_jp_security_code(_r[0])
                _cv = ffloat(_r[1], None)
                if _ck and _cv is not None and math.isfinite(float(_cv)) and float(_cv) > 0:
                    _fresh_shodou_codes.add(_ck)
        except Exception as _e:
            raise RuntimeError(f"shodou score freshness lookup failed: {_e}") from _e
    df_s['_price_fresh'] = df_s['コード'].isin(_fresh_shodou_codes)

    # テーマ強度そのものもfresh銘柄だけで測る。
    df_mrg = pd.merge(df_theme, df_s[df_s['_price_fresh']].copy(), on='コード', how='inner')
    theme_stats = df_mrg.groupby('テーマ').agg(
        median_turnover=('売買代金(億)', 'median'),
        turnover_obs=('売買代金(億)', 'count'),
        active_stocks=('コード', 'count'),
        signaled_count=('is_signaled', 'sum')
    ).reset_index()
    # P1-47: テーマ一致3銘柄だけでなく、有効な売買代金も最低3銘柄必要。
    theme_stats = theme_stats[(theme_stats['active_stocks'] >= 3) & (theme_stats['turnover_obs'] >= 3)].copy()
    theme_stats['signal_density'] = theme_stats['signaled_count'] / theme_stats['active_stocks']
    theme_stats['true_flow_score'] = _theme_true_flow_score(
        theme_stats['median_turnover'], theme_stats['signal_density']
    )
    
    df_theme_score = pd.merge(df_theme, theme_stats[['テーマ', 'true_flow_score']], on='テーマ', how='left')
    stock_theme_max = df_theme_score.groupby('コード')['true_flow_score'].max().reset_index()
    
    df = pd.merge(df_s, stock_theme_max, on='コード', how='left')
    df['true_flow_score'] = pd.to_numeric(df['true_flow_score'], errors='coerce')
    has_active_theme = (
        df['true_flow_score'].notna()
        & (df['true_flow_score'] > 0)
        & df['_price_fresh'].fillna(False).astype(bool)
    )
    df['true_flow_score'] = df['true_flow_score'].fillna(0)

    # P1-27: RVOL/騰落率/RS5の欠損を0という実測値に変換して順位母集団へ混ぜない。
    # 欠損銘柄はその成分0点、percentileは実データ保有銘柄だけで算出する。
    for c in ['前日終値比率', 'RVOL代金', 'RS_5']:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    # P1-568: stale値は順位母集団からも完全除外する。
    _stale_mask = ~df['_price_fresh'].fillna(False).astype(bool)
    df.loc[_stale_mask, ['前日終値比率', 'RVOL代金', 'RS_5']] = np.nan

    def _rank_valid(s):
        ranked = _percentile_rank_01_neutral(s)
        return ranked.fillna(0.0)

    # P3-1: テーマ/技術順位はworst=0,best=1。単一/全同値は中立0.5。
    # 旧rank(pct=True)ではN=1や全同値でも最大寄りに加点され、RS逆順位はN=1で0点になっていた。
    rank_theme = pd.Series(0.0, index=df.index, dtype=float)
    if has_active_theme.any():
        rank_theme.loc[has_active_theme] = _percentile_rank_01_neutral(
            df.loc[has_active_theme, 'true_flow_score']
        )
    rank_rvol = _rank_valid(df['RVOL代金'])
    rank_mom = _rank_valid(df['前日終値比率'])
    rank_heat = pd.Series(0.0, index=df.index, dtype=float)
    valid_rs = df['RS_5'].notna()
    if valid_rs.any():
        # abs(RS5)が小さいほど「過熱していない」ため高得点。
        rank_heat.loc[valid_rs] = _percentile_rank_01_neutral(
            df.loc[valid_rs, 'RS_5'].abs(), inverse=True
        )

    # P1-107: RVOL/前日騰落/RS5の欠損を「弱い=0点」と固定配点へ入れない。
    # 技術3要素のうち2つ以上が実測できた時だけ、利用可能配点を45点へ再正規化する。
    valid_rvol = df['RVOL代金'].notna()
    valid_mom = df['前日終値比率'].notna()
    tech_count = valid_rvol.astype(int) + valid_mom.astype(int) + valid_rs.astype(int)
    tech_weight = 20.0*valid_rvol.astype(float) + 15.0*valid_mom.astype(float) + 10.0*valid_rs.astype(float)
    tech_raw = (rank_rvol*20.0*valid_rvol.astype(float) + rank_mom*15.0*valid_mom.astype(float) + rank_heat*10.0*valid_rs.astype(float))
    tech_score = (tech_raw * (45.0 / tech_weight.replace(0, np.nan))).where(tech_count >= 2)

    score_theme_match = pd.Series(np.where(has_active_theme, 20.0, 0.0), index=df.index)
    theme_score = (rank_theme * 30.0) + score_theme_match
    raw_score = theme_score + tech_score
    score_out = pd.Series(np.nan, index=df.index, dtype=float)
    valid_score = tech_score.notna() & df['_price_fresh'].fillna(False).astype(bool)
    score_out.loc[valid_score] = np.where(
        rank_theme.loc[valid_score] < 0.4,
        np.clip(raw_score.loc[valid_score], 0, 60),
        raw_score.loc[valid_score]
    )
    df['初動スコア'] = score_out.round(1)
    
    _shodou_map = df.set_index('コード')['初動スコア'].to_dict()
    updates = [
        (None if pd.isna(_shodou_map.get(code, np.nan)) else float(_shodou_map.get(code)), raw_c)
        for code, raw_c in zip(df_s_all['コード'], df_s_all['_screener_code_raw'])
    ]
    # P1-437: 初動スコアを全銘柄同一snapshotで反映。
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_437_shodou_score")
        cur.executemany("UPDATE screener SET 初動スコア=? WHERE コード=?", updates)
        cur.execute("RELEASE SAVEPOINT p1_437_shodou_score")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_437_shodou_score")
            cur.execute("RELEASE SAVEPOINT p1_437_shodou_score")
        except Exception:
            pass
        raise
    finally:
        cur.close()
    print(f"[shodou_score] テーマ強度を加味した初動スコア（95点設計）を更新しました。")


# P2-2: screener.EPSは出所が「予想EPS」と保証できないため、Fair Valueには既定で流用しない。
# 旧DB互換が必要な場合だけ明示的にTrueへ変更する。
ALLOW_LEGACY_SCREENER_EPS_FALLBACK = False


def _ensure_fair_value_schema(conn: sqlite3.Connection):
    """Fair Valueと、そのcurrent入力を保存する列を不足時だけ追加する。"""
    required_columns = (
        ("適正株価", "REAL"),
        ("割安度", "REAL"),
        ("期待株価", "REAL"),
        ("AIスコア", "REAL"),
        ("予想インパクト_pct", "REAL"),
    )
    schema_cols = {
        r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()
    }
    added = []
    for col_name, ddl in required_columns:
        if col_name not in schema_cols:
            conn.execute(f'ALTER TABLE screener ADD COLUMN "{col_name}" {ddl}')
            schema_cols.add(col_name)
            added.append(col_name)
    if added:
        print(f"[schema] screener Fair Value関連列を追加: {', '.join(added)}")


def _read_fair_value_screener_inputs(conn: sqlite3.Connection) -> pd.DataFrame:
    """DB世代差のあるFair Value入力列を、欠損時はNULLとして同じ論理schemaで読む。"""
    input_columns = (
        "コード", "現在値", "EPS", "需給OH", "売買代金億", "売り残", "買い残",
        "機関空売り合計株数", "RS_20", "AIスコア", "予想インパクト_pct",
        "利益加速フラグ", "直近売上YoY", "直近営業益YoY",
    )
    schema_cols = {
        r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()
    }
    missing_required = [c for c in ("コード", "現在値") if c not in schema_cols]
    if missing_required:
        raise RuntimeError(
            "screener Fair Value必須列がありません: " + ", ".join(missing_required)
        )
    missing_optional = [
        c for c in input_columns if c not in schema_cols and c not in missing_required
    ]
    if missing_optional:
        print(
            "[fair-value] screener任意入力列なし→NULL扱い: "
            + ", ".join(missing_optional)
        )
    select_exprs = [
        f'"{c}"' if c in schema_cols else f'NULL AS "{c}"'
        for c in input_columns
    ]
    return pd.read_sql_query(
        "SELECT " + ", ".join(select_exprs) + " FROM screener",
        conn,
    )


def apply_fair_value_metrics(conn: sqlite3.Connection):
    """
    【機関投資家風・最高峰 動的適正株価＆期待株価算出エンジン】
    【高成長・PER主軸・PBR完全撤廃版】

    ■ 基本思想
    - 適正株価はPER法（予想EPS × 動的許容PER）のみで算出
    - PBR / ROE / BPS、およびテーマ補正・ブレンドは完全撤廃
    - 営業利益成長、EPS成長、利益加速、総合財務評価、信用需給で許容PERを動的算出
    - 3ヶ月期待株価は企業価値50% ＋ 市場モメンタム（RS20・予想インパクト・AI・空売り踏み上げ）50%
    """
    # P2-9/P3-43: 出力列に加え、HTML内で確定するcurrentモデル入力もschema保証する。
    # 旧DBではAIスコア/予想インパクト_pctが未作成で、最終同期時に落ちていた。
    _ensure_fair_value_schema(conn)

    # P1-227/P3-44: SQLの生コード完全一致JOINをやめ、canonical結合する。
    # 機関空売り等のDB世代差がある任意列は固定SELECTで落とさず、NULLとして読んだ後に
    # current authoritative sourceをoverlayする。
    df = _read_fair_value_screener_inputs(conn)
    # P1-603: finance_notesはproducer/DB世代によって列構成が異なる。
    # 現行の株探ファンダは prev_eps を保存する一方、旧DBには previous_eps があり得る。
    # optional列を固定SELECTすると、1列でも無いDBで適正株価フェーズ全体がSQLエラーになるため、
    # 実在列だけを読み、欠損列はNULLとして同じ論理schemaへ正規化する。
    _fin_cols = {r[1] for r in conn.execute("PRAGMA table_info(finance_notes)").fetchall()}
    _fin_logical = [
        ("forecast_eps", ("forecast_eps",)),
        ("previous_eps", ("prev_eps", "previous_eps")),
        # P1-654: YoYは現行FUND_SCRIPTがscreenerへ直接current更新する。
        # finance_notesの旧世代列は現行producerが更新しないためFair Valueへoverlayしない。
        ("overall_alpha", ("overall_alpha",)),
    ]
    if "コード" in _fin_cols:
        _sel = ['rowid AS _rowid', 'コード']
        _sel.append('updated_at' if "updated_at" in _fin_cols else 'NULL AS updated_at')
        for _out, _candidates in _fin_logical:
            _srcs = [_c for _c in _candidates if _c in _fin_cols]
            # P1-658: 現行FUND_SCRIPTが更新するprev_eps列が存在するDBでは、
            # current prev_eps=NULLを旧previous_epsで行単位fallbackしない。
            # 旧schemaでprev_eps列自体が無い場合だけprevious_epsを互換利用する。
            if _out == "previous_eps" and "prev_eps" in _fin_cols:
                _srcs = ["prev_eps"]
            if not _srcs:
                _sel.append(f'NULL AS "{_out}"')
            elif len(_srcs) == 1:
                _src = _srcs[0]
                if _src == _out:
                    _sel.append(f'"{_src}"')
                else:
                    _sel.append(f'"{_src}" AS "{_out}"')
            else:
                # 複数の現役candidateを持つ論理列用。P1-658のprevious_epsは上で1列へ固定済み。
                _sel.append('COALESCE(' + ', '.join(f'"{_c}"' for _c in _srcs) + f') AS "{_out}"')
        fin = pd.read_sql_query("SELECT " + ", ".join(_sel) + " FROM finance_notes", conn)
    else:
        fin = pd.DataFrame(columns=["_rowid", "コード", "updated_at"] + [_x[0] for _x in _fin_logical])
    if not df.empty:
        df["_code_key"] = df["コード"].map(canonical_code_for_db)

        # P1-617: Fair Valueだけscreener保存済みの機関空売り合計を読むと、
        # HTML/risk側がinstitution_short_salesから再集計したcurrent snapshotと時点が分裂する。
        # 踏み上げボーナスへ直接効くため、ここでもcurrent authoritative集計をoverlayする。
        try:
            _inst_now = _load_institution_short_summary(conn)
        except Exception as _e:
            raise RuntimeError(f"current institution short summary read failed for fair value: {_e}") from _e
        _inst_now_map = {}
        if _inst_now is not None and not _inst_now.empty and {"code", "機関空売り合計株数"}.issubset(_inst_now.columns):
            for _, _ir in _inst_now.iterrows():
                _ik = canonical_code_for_db(_ir.get("code"))
                if _ik:
                    _iv = pd.to_numeric(pd.Series([_ir.get("機関空売り合計株数")]), errors="coerce").iloc[0]
                    _inst_now_map[_ik] = _iv if pd.notna(_iv) else np.nan
        # summaryに銘柄行が無い = current残高を確認できない。旧screener値へfallbackしない。
        df["機関空売り合計株数"] = df["_code_key"].map(_inst_now_map)
    _stale_finance_codes = _finance_codes_stale_after_latest_earnings(conn)
    if not fin.empty:
        fin = _latest_finance_notes_by_canonical(fin, "_code_key")
        # P1-670: current finance正本の「行が存在する」ことと forecast_eps の値有無を分離する。
        # 正本行があるのにforecast_eps=NULLなら旧screener.EPSへ行単位fallbackしない。
        fin["_finance_row_present"] = 1
        fin = fin.drop(columns=["コード", "_rowid", "updated_at"], errors="ignore")
        df = df.merge(fin, on="_code_key", how="left")
    else:
        for _c in ("forecast_eps", "previous_eps", "overall_alpha"):
            df[_c] = np.nan
        df["_finance_row_present"] = 0

    if df.empty:
        return

    # P1-608: 新決算がfinance_notesより新しい銘柄は、screener.EPSへのfallbackも含め
    # 旧財務を使った適正株価を出さない。詳細財務refresh完了まで一旦NULLにする。
    df["_finance_stale_after_event"] = df["_code_key"].isin(_stale_finance_codes)

    numeric_columns = [
        "現在値", "EPS", "forecast_eps", "previous_eps",
        "直近営業益YoY", "需給OH",
        "売買代金億", "売り残", "買い残", "機関空売り合計株数", "RS_20",
        "AIスコア", "予想インパクト_pct", "利益加速フラグ", "直近売上YoY"
    ]

    for c in numeric_columns:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    # P2-10/P2-11: 「計算結果が空」の原因を運用ログから即座に判別できるよう、
    # 生forecast_eps件数ではなく、stale maskとlegacy fallback設定まで反映した実効入力を数える。
    _finance_present_mask = pd.to_numeric(df.get("_finance_row_present"), errors="coerce").fillna(0).eq(1)
    _forecast_raw_valid_mask = df["forecast_eps"].notna() & (df["forecast_eps"] > 0)
    if ALLOW_LEGACY_SCREENER_EPS_FALLBACK:
        _legacy_eps_valid_mask = (~_finance_present_mask) & df["EPS"].notna() & (df["EPS"] > 0)
    else:
        _legacy_eps_valid_mask = pd.Series(False, index=df.index)
    _effective_eps_mask = (_finance_present_mask & _forecast_raw_valid_mask) | _legacy_eps_valid_mask
    _stale_mask = df["_finance_stale_after_event"].fillna(False).astype(bool)
    _forecast_raw_valid = int(_forecast_raw_valid_mask.sum())
    _forecast_usable = int((_effective_eps_mask & ~_stale_mask).sum())
    _finance_rows = int(_finance_present_mask.sum())
    _stale_rows = int(_stale_mask.sum())
    print(
        f"[quant][P2-11] forecast_eps_raw={_forecast_raw_valid}/{len(df)} "
        f"usable={_forecast_usable}/{len(df)} finance_rows={_finance_rows} stale_rows={_stale_rows} "
        f"legacy_eps_fallback={ALLOW_LEGACY_SCREENER_EPS_FALLBACK}",
        flush=True,
    )
    if len(df) and _forecast_usable == 0:
        print("[quant][WARN] no usable forecast EPS for current earnings snapshot; Fair Value will remain NULL", flush=True)

    def calc_dynamic_fair_value(row):
        if bool(row.get("_finance_stale_after_event", False)):
            return np.nan, np.nan
        price = row["現在値"]
        
        # 1. 予想EPSの確定
        # P2-2: forecast EPSの出所を厳密化。current finance正本がある場合はforecast_epsだけを使う。
        # finance_notes正本自体が無いlegacy環境でも、screener.EPSは予想値と保証できないため既定では使わない。
        # 互換運用が必要な場合だけ ALLOW_LEGACY_SCREENER_EPS_FALLBACK=True で明示opt-inする。
        _has_finance_row = bool(pd.notna(row.get("_finance_row_present")) and float(row.get("_finance_row_present")) == 1.0)
        forecast_eps = (
            row["forecast_eps"]
            if _has_finance_row
            else (row["EPS"] if ALLOW_LEGACY_SCREENER_EPS_FALLBACK else np.nan)
        )
        if pd.isna(forecast_eps) or forecast_eps <= 0:
            return np.nan, np.nan

        # 2. 特徴量の抽出
        op_yoy = row["直近営業益YoY"] if pd.notna(row["直近営業益YoY"]) else 0.0
        
        # current finance正本があるならprevious_epsもその正本だけを信頼する。
        # 欠損時に同じscreener.EPSを前期EPSとして流用し、成長率を捏造しない。
        prev_eps = (
            row["previous_eps"]
            if _has_finance_row
            else (row["EPS"] if ALLOW_LEGACY_SCREENER_EPS_FALLBACK else np.nan)
        )
        eps_growth = 0.0
        if pd.notna(prev_eps) and prev_eps > 0:
            eps_growth = ((forecast_eps / prev_eps) - 1.0) * 100.0

        # P1-30: 赤字脱却・V字回復判定。旧式は prev_eps>0 が必須だったため、
        # 本当の赤字(prev_eps<=0)→黒字(forecast_eps>0)を「赤字脱却」と判定できなかった。
        turnaround_flag = bool(pd.notna(prev_eps) and forecast_eps > 0 and prev_eps <= 0)
        near_zero_recovery = bool(pd.notna(prev_eps) and prev_eps > 0 and prev_eps < forecast_eps * 0.1)
        if turnaround_flag:
            # 赤字からの成長率は分母が成立しないためEPS成長加点は0。
            # 代わりに後段のPER上限25倍を適用し、過大評価を防ぐ。
            eps_growth_effect = 0.0
        elif near_zero_recovery:
            turnaround_flag = True
            eps_growth_effect = eps_growth * 0.3
        else:
            eps_growth_effect = eps_growth

        rs20 = np.clip(row["RS_20"] if pd.notna(row["RS_20"]) else 0.0, -1.0, 1.0)

        ai_score = row["AIスコア"] if pd.notna(row["AIスコア"]) else 50.0

        # P1-25: 信用データ欠損を「需給OH=2日 / 売買代金5億 / 残高0株」と捏造しない。
        # 欠損時に後段の「需給良好 +1PER」が発動していたため、Noneのまま評価不能とする。
        demand_days = row["需給OH"] if pd.notna(row["需給OH"]) else None
        turnover_oku = row["売買代金億"] if pd.notna(row["売買代金億"]) and row["売買代金億"] > 0 else None
        sell_margin = row["売り残"] if pd.notna(row["売り残"]) else None
        buy_margin = row["買い残"] if pd.notna(row["買い残"]) else None

        # P1-26: 期待株価の踏み上げ成分にもP1-20以降で正しく集計した機関空売り残高を含める。
        inst_short = row["機関空売り合計株数"] if pd.notna(row["機関空売り合計株数"]) else None
        short_margin_days = None
        buy_margin_days = None
        if pd.notna(price) and price > 0 and turnover_oku is not None:
            known_short_shares = sum(v for v in (sell_margin, inst_short) if v is not None and v > 0)
            if sell_margin is not None or inst_short is not None:
                short_margin_days = (known_short_shares * price / 1e8) / max(turnover_oku, 0.5)
            if buy_margin is not None:
                buy_margin_days = (max(buy_margin, 0.0) * price / 1e8) / max(turnover_oku, 0.5)

        expected_impact = row["予想インパクト_pct"] if pd.notna(row["予想インパクト_pct"]) else 0.0
        # P3-16: flag単独ではなく、P3-4と同じauthoritativeな加速品質を使う。
        accel_quality = _profit_acceleration_quality(row)
        alpha_rank = str(row["overall_alpha"]).strip() if pd.notna(row["overall_alpha"]) else ""

        # 3. 動的許容PERの算出（PER法100%）
        target_per = 12.0

        if op_yoy > 0:
            # P3-10: tiny-base由来の数千%YoYだけでPERが上限へ張り付かないよう、
            # EPS成長加点と同じ7倍相当を上限にする。
            target_per += min(math.sqrt(max(op_yoy, 0)) * 0.22, 7.0)

        if eps_growth_effect > 0:
            target_per += min(math.sqrt(max(eps_growth_effect, 0)) * 0.35, 7.0)

        if accel_quality == "quality":
            target_per += 1.0

        if alpha_rank in ["S++", "S"]:
            target_per += 1.5
        elif alpha_rank in ["A+", "A"]:
            target_per += 0.8

        # P1-25: 取得できた需給指標だけでPER補正する。欠損は良好/悪化どちらにも寄せない。
        overhang_high = (buy_margin_days is not None and buy_margin_days > 20.0) or (demand_days is not None and demand_days > 30.0)
        overhang_mid = (buy_margin_days is not None and buy_margin_days > 10.0) or (demand_days is not None and demand_days > 15.0)
        overhang_low = (buy_margin_days is not None and buy_margin_days < 3.0) or (demand_days is not None and 0 < demand_days < 5.0)
        if overhang_high:
            target_per -= 3.0
        elif overhang_mid:
            target_per -= 1.5
        elif overhang_low:
            target_per += 1.0

        max_per = 36.0
        if turnaround_flag:
            max_per = min(max_per, 25.0)

        target_per = np.clip(target_per, 8.0, max_per)

        # 4. 適正株価 = 予想EPS × 許容PER
        fair_per = forecast_eps * target_per
        target_price = fair_per

        # 5. 3ヶ月期待株価の算出（企業価値50% ＋ 市場モメンタム50%）
        short_squeeze_bonus = (
            min(math.sqrt(max(short_margin_days, 0.0)) * 0.015, 0.12)
            if short_margin_days is not None else 0.0
        )
        
        # P3-11: AIスコアは50を中立点として左右対称に使う。旧式は50未満を
        # すべて50相当へ切り上げ、AIが悪材料を示しても期待株価へ反映されなかった。
        ai_score_clamped = float(np.clip(ai_score, 0.0, 100.0))
        raw_momentum_multiplier = (
            1.0 
            + (rs20 * 0.20) 
            + (expected_impact * 0.005) 
            + ((ai_score_clamped - 50.0) * 0.001)
            + short_squeeze_bonus
        )
        
        clamped_momentum_mult = np.clip(raw_momentum_multiplier, 0.75, 1.55)
        expected_3m_price = (target_price * 0.5) + ((price * clamped_momentum_mult) * 0.5)

        return float(target_price), float(expected_3m_price)

    results = df.apply(calc_dynamic_fair_value, axis=1)
    df["適正株価"] = [r[0] for r in results]
    df["期待株価"] = [r[1] for r in results]

    df["割安度"] = np.where(
        (df["適正株価"] > 0) & (df["現在値"] > 0),
        ((df["適正株価"] - df["現在値"]) / df["現在値"]) * 100.0,
        np.nan
    )

    # P1-228: 今回計算不能になった銘柄もNULLで更新し、前回の適正/期待株価を残さない。
    updates = []
    for _, r in df.iterrows():
        updates.append((
            round(r["適正株価"], 1) if pd.notna(r["適正株価"]) else None,
            round(r["割安度"], 2) if pd.notna(r["割安度"]) else None,
            round(r["期待株価"], 1) if pd.notna(r["期待株価"]) else None,
            str(r["コード"])
        ))

    if updates:
        # P1-447: 適正/割安/期待株価を全銘柄同一snapshotで反映。
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_447_fair_value")
            cur.executemany("""
                UPDATE screener
                SET 適正株価=?, 割安度=?, 期待株価=?
                WHERE コード=?
            """, updates)
            cur.execute("RELEASE SAVEPOINT p1_447_fair_value")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_447_fair_value")
                cur.execute("RELEASE SAVEPOINT p1_447_fair_value")
            except Exception:
                pass
            raise
        finally:
            cur.close()
        print(f"[quant] 高成長・PER主軸・PBR完全撤廃型エンジンによる更新完了: {len(updates)} 銘柄")


def _sync_latest_model_outputs_and_refresh_fair_value(conn: sqlite3.Connection, rows):
    """
    P1-5: HTML出力時に最新化された AIスコア / 予想インパクト_pct をDBへ同期し、
    その直後に適正株価・期待株価を再計算してrowsへ戻す。

    旧処理では apply_fair_value_metrics() が19時バッチの途中で先に実行され、
    その後HTML出力内でAI/予想インパクトが更新されていたため、期待株価だけ
    前回値またはNULLの入力を使う時間的不整合があった。
    """
    if rows is None:
        return rows

    # P3-43: UPDATEがapply_fair_value_metrics()より先なので、ここでも事前保証が必要。
    _ensure_fair_value_schema(conn)

    def _finite_float(v):
        try:
            if v is None or v == "":
                return None
            if isinstance(v, str) and v.strip() in ("", "-", "N/A", "None", "nan", "NaN"):
                return None
            x = float(str(v).replace(",", ""))
            return x if math.isfinite(x) else None
        except Exception:
            return None

    # P1-261: canonical計算キーからscreener実在rawキーへ戻してUPDATEする。
    _sync_raw = {}
    # P2-18: AI/Fair Value同期の更新先map取得失敗をsilent fallbackしない。
    for (_rc,) in conn.execute("SELECT コード FROM screener").fetchall():
        _rk = canonical_code_for_db(_rc)
        if _rk and _rk not in _sync_raw:
            _sync_raw[_rk] = _rc
    updates = []
    for r in rows:
        # P1-213: 適正/期待株価同期のコードもcanonical化。
        code = canonical_code_for_db(r.get("コード"))
        if not code:
            continue
        # 最新AIが判定不能ならNULLへ明示的に戻す。古いAI値を残さない。
        ai = _finite_float(r.get("AIスコア"))
        impact = _finite_float(r.get("予想インパクト_pct"))
        updates.append((ai, impact, _sync_raw.get(code, code)))

    # P1-448: 最新AI/予想インパクトと、それを入力にした適正株価を同じトランザクションで確定。
    cur = conn.cursor()
    try:
        cur.execute("SAVEPOINT p1_448_model_and_fair_value")
        if updates:
            cur.executemany(
                """
                UPDATE screener
                   SET AIスコア = ?,
                       予想インパクト_pct = ?
                 WHERE コード = ?
                """,
                updates,
            )
        apply_fair_value_metrics(conn)
        cur.execute("RELEASE SAVEPOINT p1_448_model_and_fair_value")
    except Exception:
        try:
            cur.execute("ROLLBACK TO SAVEPOINT p1_448_model_and_fair_value")
            cur.execute("RELEASE SAVEPOINT p1_448_model_and_fair_value")
        except Exception:
            pass
        raise
    finally:
        cur.close()
    if updates:
        print(f"[fair-value] 最新AI/予想インパクトをDB同期: {len(updates)}銘柄")

    fv = pd.read_sql_query(
        "SELECT コード, 適正株価, 割安度, 期待株価 FROM screener",
        conn,
    )
    if fv.empty:
        return rows

    fv_map = {}
    for _, rr in fv.iterrows():
        code = canonical_code_for_db(rr.get("コード"))
        fv_map[code] = {
            "適正株価": rr.get("適正株価"),
            "割安度": rr.get("割安度"),
            "期待株価": rr.get("期待株価"),
        }

    for r in rows:
        code = canonical_code_for_db(r.get("コード"))
        vals = fv_map.get(code)
        if vals:
            r.update(vals)

    return rows


def _parse_early_tag(detail: str) -> str | None:
    if detail is None:
        return None
    try:
        # 例: "ブレイク | score=..." → "ブレイク"
        head = str(detail).split("|", 1)[0].strip()
        return head if head else None
    except Exception:
        return None

def _prev_business_day(d, extra_closed):
    # 既存の prev_business_day_jp / is_jp_market_holiday を想定
    return prev_business_day_jp(d, extra_closed)



def phase_update_since_dates(conn):
    """
    screener に以下を埋める:
      - 初動開始日 / 底打ち開始日 / 早期開始日 / 右肩開始日
      - 早期種別開始日 / 早期前回種別
    """
    extra_closed = _load_extra_closed(EXTRA_CLOSED_PATH)
    cur = conn.cursor()
    cur.execute("""
        SELECT コード, 初動フラグ, 底打ちフラグ, 右肩上がりフラグ, 右肩早期フラグ, 右肩早期種別
        FROM screener
    """)
    rows = cur.fetchall()
    cur.close()

    # P2-22: 候補銘柄ごとに最大5回signals_logをSELECTしていたN+1を廃止。
    # 対象4種の履歴をchunk bulk preloadし、営業日連続区間/早期タグ推移をメモリ上で再現する。
    _signal_kinds = ("初動", "底打ち", "右肩上がり-持続", "右肩上がり-早期")
    _candidate_codes = []
    for _code, _sh, _bt, _ru, _er, _etype in rows:
        if (_sh == "候補" or _bt == "候補" or _ru == "候補" or _er == "候補"
                or (_etype is not None and str(_etype).strip() != "")):
            _ck = canonical_code_for_db(_code)
            if _ck:
                _candidate_codes.append(_ck)
    _candidate_codes = list(dict.fromkeys(_candidate_codes))

    _signal_cache = {}
    for _i in range(0, len(_candidate_codes), 250):
        _part = _candidate_codes[_i:_i + 250]
        _qvars = expand_code_query_variants(_part)
        if not _qvars:
            continue
        _code_ph = ",".join("?" * len(_qvars))
        _kind_ph = ",".join("?" * len(_signal_kinds))
        _cur = conn.cursor()
        try:
            _cur.execute(
                f"SELECT rowid, コード, 日時, 種別, 詳細 FROM signals_log "
                f"WHERE CAST(コード AS TEXT) IN ({_code_ph}) AND 種別 IN ({_kind_ph}) "
                f"ORDER BY 日時 DESC, rowid DESC",
                tuple(_qvars) + _signal_kinds,
            )
            _log_rows = _cur.fetchall()
        finally:
            _cur.close()
        for _rowid, _raw_code, _dt_s, _kind, _detail in _log_rows:
            _ck = canonical_code_for_db(_raw_code)
            if not _ck:
                continue
            _signal_cache.setdefault((_ck, _kind), []).append((_dt_s, _detail, _rowid))

    # chunk境界に依存せず、従来の「日時DESC」を確定させる。同時刻はrowid DESCで決定的にする。
    for _items in _signal_cache.values():
        _items.sort(key=lambda x: (str(x[0]), int(x[2] or 0)), reverse=True)

    def _streak_from_cache(_code, _kind):
        _ck = canonical_code_for_db(_code)
        _items = _signal_cache.get((_ck, _kind), []) if _ck else []
        if not _items:
            return None
        _dates = []
        for _dt_s, _, _ in _items:
            try:
                _dates.append(datetime.strptime(str(_dt_s)[:10], "%Y-%m-%d").date())
            except Exception:
                continue
        if not _dates:
            return None
        _latest = _dates[0]
        _have = {d.isoformat() for d in _dates}
        _run_start = _latest
        _cur_check = _latest
        while True:
            _prev_d = _prev_business_day(_cur_check, extra_closed)
            if _prev_d.isoformat() not in _have:
                break
            _run_start = _prev_d
            _cur_check = _prev_d
        return _run_start.isoformat()

    def _early_from_cache(_code):
        _ck = canonical_code_for_db(_code)
        _items = _signal_cache.get((_ck, "右肩上がり-早期"), []) if _ck else []
        if not _items:
            return None, None, None
        _daily = []
        _seen_days = set()
        for _dt_s, _detail, _ in _items:
            try:
                _d = datetime.strptime(str(_dt_s)[:10], "%Y-%m-%d").date()
            except Exception:
                continue
            _ds = _d.isoformat()
            if _ds in _seen_days:
                continue
            _seen_days.add(_ds)
            _daily.append((_d, _parse_early_tag(_detail)))
        if not _daily:
            return None, None, None
        _cur_tag = _daily[0][1]
        _run_start = _daily[0][0]
        _cur_check = _daily[0][0]
        for _d, _tag in _daily[1:]:
            _expected_prev = _prev_business_day(_cur_check, extra_closed)
            if _d == _expected_prev and _tag == _cur_tag:
                _run_start = _d
                _cur_check = _d
                continue
            break
        _prev_tag = None
        for _d, _tag in _daily[1:]:
            if _tag != _cur_tag:
                _prev_tag = _tag
                break
        return _run_start.isoformat(), _cur_tag, _prev_tag

    upd = []
    for code, sh, bt, ru, er, etype in rows:
        code = str(code)
        s_sh = _streak_from_cache(code, "初動") if (sh == "候補") else None
        s_bt = _streak_from_cache(code, "底打ち") if (bt == "候補") else None
        # P1-579: 持続右肩の開始日は別アルゴリズムのtob_scoreログではなく専用履歴から取る。
        s_ru = _streak_from_cache(code, "右肩上がり-持続") if (ru == "候補") else None
        s_er = _streak_from_cache(code, "右肩上がり-早期") if (er == "候補") else None

        etype_start = None
        prev_type = None
        if etype and str(etype).strip() != "":
            etype_start, _, prev_type = _early_from_cache(code)
            # P2-22: 旧コードの etype = cur_type はDBへ書き戻されないno-opだったため削除。
            # 現在の右肩早期種別そのものはsignal検出側を正本とし、ここでは開始日/前回種別だけ更新する。

        upd.append((s_sh, s_bt, s_er, s_ru, etype_start, prev_type, code))

    if upd:
        # P1-433: 開始日群は同一snapshotとして一括反映。途中失敗で銘柄ごとに新旧混在させない。
        cur = conn.cursor()
        try:
            cur.execute("SAVEPOINT p1_433_since_dates")
            cur.executemany("""
                UPDATE screener
                   SET 初動開始日=?,
                       底打ち開始日=?,
                       早期開始日=?,
                       右肩開始日=?,
                       早期種別開始日=?,
                       早期前回種別=?
                 WHERE コード=?
            """, upd)
            cur.execute("RELEASE SAVEPOINT p1_433_since_dates")
        except Exception:
            try:
                cur.execute("ROLLBACK TO SAVEPOINT p1_433_since_dates")
                cur.execute("RELEASE SAVEPOINT p1_433_since_dates")
            except Exception:
                pass
            raise
        finally:
            cur.close()

def relax_rejudge_signals(
    conn,
    lookahead_days: int = None,
    req_high_pct: float = None,
    max_adverse_pct: float = None,
):
    """
    signals_log で '外れ' になっているシグナルを、発生日から一定日数内の値動きで再評価。
    条件:
      ・lookahead_days 日以内に +req_high_pct% 到達
      ・かつ 最大逆行（安値基準）が -max_adverse_pct% 以内
    満たせば 判定='再評価OK' / 理由 を上書き。
    """

    L = lookahead_days or REJUDGE_LOOKAHEAD_DAYS
    UP = req_high_pct   or REJUDGE_REQ_HIGH_PCT
    DN = max_adverse_pct or REJUDGE_MAX_ADVERSE_PCT

    cur = conn.cursor()
    # P1-126: 直近30日窓もJST市場日付を基準に固定。SQLite UTC日付による境界ズレを防ぐ。
    cutoff_date = (date.fromisoformat(_today_jst()) - timedelta(days=30)).isoformat()
    cur.execute("""
      SELECT コード, 日時, 種別
        FROM signals_log
       WHERE 判定='外れ'
         AND date(日時) >= date(?)
    """, (cutoff_date,))
    rows = cur.fetchall()

    upd = 0
    _rejudge_updates = []
    # P1-597: PREOPENは前営業日snapshotが正本。旧バグ由来の当日price_history行が残っていても、
    # 寄り前の再評価で未来（当日）価格として採用しない。MIDDAY/EODは従来どおり当日が上限。
    _rejudge_cutoff = _expected_snapshot_date_for_run(_auto_run_mode()).isoformat()

    # P2-21: 外れシグナルごとのprice_history SELECTを廃止。対象銘柄・期間をbulk preloadする。
    _rejudge_hist = {}
    _parsed_signal_rows = []
    for code, ts, kind in rows:
        base_date = str(ts)[:10]
        try:
            _base_d = date.fromisoformat(base_date)
        except Exception:
            continue
        _ck = canonical_code_for_db(code)
        if not _ck:
            continue
        _parsed_signal_rows.append((code, ts, kind, base_date, _base_d, _ck))
    if _parsed_signal_rows:
        _min_base = min(r[4] for r in _parsed_signal_rows).isoformat()
        _max_end = min(
            max(r[4] + timedelta(days=int(L)) for r in _parsed_signal_rows),
            date.fromisoformat(_rejudge_cutoff),
        ).isoformat()
        _rejudge_codes = list(dict.fromkeys(r[5] for r in _parsed_signal_rows))
        for _i in range(0, len(_rejudge_codes), 250):
            _part = _rejudge_codes[_i:_i + 250]
            _qvars = expand_code_query_variants(_part)
            if not _qvars:
                continue
            _qmarks = ",".join("?" * len(_qvars))
            _bulk = pd.read_sql_query(
                f"SELECT rowid AS _rowid, コード, 日付, 終値, 高値, 安値 FROM price_history "
                f"WHERE CAST(コード AS TEXT) IN ({_qmarks}) "
                f"AND date(日付) BETWEEN date(?) AND date(?) ORDER BY 日付 ASC, rowid ASC",
                conn, params=[*_qvars, _min_base, _max_end]
            )
            if _bulk.empty:
                continue
            _bulk = _dedupe_price_history_df(_bulk)
            for _hcode, _hg in _bulk.groupby("コード", sort=False) if not _bulk.empty else []:
                _hk = canonical_code_for_db(_hcode)
                if _hk:
                    _rejudge_hist[_hk] = _hg.sort_values("日付").reset_index(drop=True)

    for code, ts, kind, base_date, _base_d, _ck in _parsed_signal_rows:
        _end_d = min(_base_d + timedelta(days=int(L)), date.fromisoformat(_rejudge_cutoff))
        _all_g = _rejudge_hist.get(_ck)
        if _all_g is None or _all_g.empty:
            continue
        _dates = pd.to_datetime(_all_g["日付"], errors="coerce").dt.date
        g = _all_g.loc[(_dates >= _base_d) & (_dates <= _end_d)].copy().reset_index(drop=True)
        if g.empty:
            continue
        # P1-309: 同日aliasのどちらをentryにするかをSQLの偶然の順序へ委ねない。
        g = _dedupe_price_history_df(g)
        if g.empty:
            continue

        # P1-70: シグナル当日の価格行が無いのに翌日以降の終値をエントリー価格へすり替えない。
        first_price_date = str(g.iloc[0]["日付"])[:10]
        if first_price_date != base_date:
            continue

        # P1-65: 基準終値/高安欠損でfloat(None)例外を起こし、再評価全体を止めない。
        entry = pd.to_numeric(pd.Series([g.iloc[0]["終値"]]), errors="coerce").iloc[0]
        highs = pd.to_numeric(g["高値"], errors="coerce")
        lows = pd.to_numeric(g["安値"], errors="coerce")
        if pd.isna(entry) or float(entry) <= 0 or highs.notna().sum() == 0 or lows.notna().sum() == 0:
            continue
        entry = float(entry)

        max_up = (float(highs.max()) / entry - 1.0) * 100.0
        max_dn = (float(lows.min()) / entry - 1.0) * 100.0  # 負の値（例: -6.3）

        if (max_up >= UP) and (max_dn >= -DN):
            _rejudge_updates.append((f"delayed hit: {L}D +{max_up:.1f}% / MAE {max_dn:.1f}%", code, ts, kind))
            upd += 1

    cur.close()
    if _rejudge_updates:
        cur2 = conn.cursor()
        try:
            cur2.execute("SAVEPOINT p1_453_rejudge")
            cur2.executemany("""
              UPDATE signals_log
                 SET 判定='再評価OK', 理由=?
               WHERE コード=? AND 日時=? AND 種別=?
            """, _rejudge_updates)
            cur2.execute("RELEASE SAVEPOINT p1_453_rejudge")
        except Exception:
            try:
                cur2.execute("ROLLBACK TO SAVEPOINT p1_453_rejudge")
                cur2.execute("RELEASE SAVEPOINT p1_453_rejudge")
            except Exception:
                pass
            raise
        finally:
            cur2.close()
    if upd:
        print(f"[rejudge] 再評価OK に更新: {upd} 件")
    else:
        print("[rejudge] 該当なし")

# === 追加：ロガー共通セットアップ（新規） ===

def setup_fin_logger(verbose: bool = False):
    """
    増資リスク系の処理で使う共通ロガー。
    - コンソール & ファイルに出力（ローテーション）
    - verbose=True で DEBUG、False で INFO
    """
    logger = logging.getLogger("dilution")
    # すでにハンドラ付いてたら再利用
    if logger.handlers:
        logger.setLevel(logging.DEBUG if verbose else logging.INFO)
        return logger

    logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # 出力先ディレクトリ（既存の OUTPUT_DIR を利用）
    try:
        base_dir = OUTPUT_DIR  # 既存の出力先を利用
    except NameError:
        base_dir = os.getcwd()

    log_dir = os.path.join(base_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    # P1-207: 日次希薄化ログのファイル日付もJST。
    log_path = os.path.join(log_dir, f"dilution_{_now_jst().strftime('%Y%m%d')}.log")

    fmt = logging.Formatter("[%(asctime)s] %(levelname)s %(message)s")

    # ファイル（1MBローテーション×3）
    fh = RotatingFileHandler(log_path, maxBytes=1_000_000, backupCount=3, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.DEBUG)

    # コンソール
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    ch.setLevel(logging.DEBUG if verbose else logging.INFO)

    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.propagate = False
    logger.info(f"Logger initialized. log_path={log_path}")
    return logger

# ===== 増資判定用



# --- BEGIN: batch_update_all_financials (貼り付け用) ---
# 依存: pip install yahooquery
# P2-51: 旧YQダミーと、この位置の未使用YQ_MAX_WORKERS再定義は削除。

# P1-547: 財務取得ロジックのcache世代。P1-516〜524で年次→四半期/TTMへ
# 意味が変わったため、旧raw_fin_jsonを7日間fresh扱いしない。
FINANCIAL_FETCH_SCHEMA_VERSION = 3  # P2-20: P2-15 symbol別payload正規化を既存7日cacheへ即反映するためversion bump

def _safe_num(v):
    try:
        if v is None: return None
        if isinstance(v, str):
            s = v.strip().replace(",", "").replace(" ", "")
            if s in ("", "-", "None", "nan", "NaN"): return None
            return float(s)
        if isinstance(v, (int, float)):
            if isinstance(v, float) and (v != v): return None
            return float(v)
    except Exception:
        return None


# === 置換：本体（yahooquery 取得→解析→DB反映） ===
def batch_update_all_financials(conn,
                                chunk_size: int = 200,
                                force_refresh: bool = False,
                                sleep_between_chunks: float = 0.1,
                                verbose: bool = False,
                                set_wal: bool = True):
    """
    yahooquery 一括取得 -> raw_fin_json キャッシュ -> 指標抽出 -> DB 一括更新
    (改修版: 既存ロジック完全維持 + PBR/現金/負債/大株主 追加)
    """
    # --------------------------
    # ロガー
    # --------------------------
    log = setup_fin_logger(verbose)
    if Ticker is None:
        raise RuntimeError("financial batch requires yahooquery; install yahooquery before running this phase")
    # ▼▼▼ 過去のゴミデータ（辞書文字列）をDBから一掃 ▼▼▼
    # P2-54: 深いhelperからconn.commit()せず、外側transactionがあればその原子性を維持する。
    _cleanup_cur = conn.cursor()
    try:
        _cleanup_cur.execute("SAVEPOINT p2_54_financial_legacy_cleanup")
        _cleanup_cur.execute("UPDATE screener SET 大株主 = NULL WHERE 大株主 LIKE '{%';")
        _cleanup_cur.execute("RELEASE SAVEPOINT p2_54_financial_legacy_cleanup")
    except Exception as _e:
        try:
            _cleanup_cur.execute("ROLLBACK TO SAVEPOINT p2_54_financial_legacy_cleanup")
            _cleanup_cur.execute("RELEASE SAVEPOINT p2_54_financial_legacy_cleanup")
        except Exception:
            pass
        # P1-491: DB cleanup失敗を握りつぶして財務更新成功扱いにしない。
        raise RuntimeError(f"financial cleanup for legacy 大株主 failed: {_e}") from _e
    finally:
        _cleanup_cur.close()
    # ▲▲▲ 追加ここまで ▲▲▲
    # --------------------------
    # 依存のフォールバック
    # --------------------------
    try:
        _safe_num  # noqa
    except NameError:
        def _safe_num(v):
            try:
                if v is None: return None
                if isinstance(v, str):
                    s = v.strip().replace(",", "").replace(" ", "")
                    if s in ("", "-", "None", "nan", "NaN"): return None
                    return float(s)
                if isinstance(v, (int, float)):
                    if isinstance(v, float) and (v != v): return None
                    return float(v)
            except Exception:
                return None

    # --------------------------
    # ユーティリティ（元のまま維持）
    # --------------------------
    def _fmt(x, nd=2):
        try:
            if x is None: return "NA"
            return f"{float(x):.{nd}f}"
        except Exception: return "NA"

    def _is_nonempty_df(x):
        try:
            return isinstance(x, pd.DataFrame) and (not x.empty)
        except Exception:
            return False

    def _df_values_recent_first(df, row_key):
        try:
            ser = df.loc[row_key]
            if isinstance(ser, pd.DataFrame):
                ser = ser.iloc[0]
            pairs = list(zip(list(ser.index), list(ser.values)))
            parsed = []
            any_date = False
            for pos, (k, v) in enumerate(pairs):
                d = pd.to_datetime(k, errors="coerce")
                if pd.notna(d):
                    any_date = True
                    parsed.append((pd.Timestamp(d).value, -pos, v))
                else:
                    parsed.append((-(10**30), -pos, v))
            if any_date:
                parsed.sort(reverse=True)
                return [v for _, _, v in parsed]
            return [v for _, v in pairs]
        except Exception:
            return []

    def _norm_fin_metric_name(x):
        # P1-518: yahooquery financial DataFrameは指標名がcolumns側。
        # 大文字小文字/空白/underscore差を吸収して現在・旧表記を同じキーとして扱う。
        return re.sub(r"[^a-z0-9]", "", str(x or "").lower())

    def _find_fin_metric_col(df, keys):
        try:
            cmap = {_norm_fin_metric_name(c): c for c in df.columns}
            for k in keys:
                hit = cmap.get(_norm_fin_metric_name(k))
                if hit is not None:
                    return hit
        except Exception:
            pass
        return None

    def _financial_rows_recent_first(df, period_types=None):
        try:
            if not _is_nonempty_df(df):
                return df
            work = df.copy()
            if period_types and "periodType" in work.columns:
                allowed = {str(x).upper() for x in period_types}
                work = work[work["periodType"].astype(str).str.upper().isin(allowed)].copy()
            if work.empty:
                return work
            if "asOfDate" in work.columns:
                work["__p1_asof"] = pd.to_datetime(work["asOfDate"], errors="coerce")
                work = work.sort_values("__p1_asof", ascending=False, kind="stable")
                work = work.drop(columns=["__p1_asof"], errors="ignore")
            return work
        except Exception:
            return df.iloc[0:0] if isinstance(df, pd.DataFrame) else df

    def _yf_pick_recent_from_df(df, keys, period_types=None):
        try:
            if not _is_nonempty_df(df): return None
            # P1-518: 現行yahooquery形（symbol index / metric columns）を先に読む。
            col = _find_fin_metric_col(df, keys)
            if col is not None:
                work = _financial_rows_recent_first(df, period_types=period_types)
                for v in pd.to_numeric(work[col], errors="coerce").tolist():
                    try:
                        if v is None or pd.isna(v) or not np.isfinite(float(v)): continue
                        return float(v)
                    except Exception:
                        continue
            # 旧/別形状（metric index / period columns）も後方互換で残す。
            for k in keys:
                if k in df.index:
                    for v in _df_values_recent_first(df, k):
                        try:
                            if v is None or pd.isna(v): continue
                            x = float(v)
                            if np.isfinite(x): return x
                        except Exception: continue
            return None
        except Exception: return None

    def _yf_sum_quarters_df(df, keys, n=4, require_n=False, missing_as_zero=False):
        try:
            if not _is_nonempty_df(df): return None
            # P1-519/P1-521: 3Mだけを日付降順で扱い、TTMを四半期の1本として二重加算しない。
            col = _find_fin_metric_col(df, keys)
            if col is not None:
                work = _financial_rows_recent_first(df, period_types=("3M",))
                if "asOfDate" in work.columns:
                    work = work.drop_duplicates(subset=["asOfDate"], keep="first")
                work = work.head(int(n))
                if len(work) < int(n) and require_n:
                    return None
                vals = pd.to_numeric(work[col], errors="coerce")
                if missing_as_zero:
                    if len(work) < int(n):
                        return None
                    vals = vals.fillna(0.0)
                    return float(vals.sum())
                good = vals[np.isfinite(vals)]
                if require_n and len(good) < int(n):
                    return None
                return float(good.iloc[:int(n)].sum()) if len(good) else None
            # 旧/別形状の後方互換。
            for k in keys:
                if k in df.index:
                    acc = 0.0; cnt = 0
                    for v in _df_values_recent_first(df, k):
                        try:
                            if v is None or pd.isna(v): continue
                            x = float(v)
                            if not np.isfinite(x): continue
                            acc += x; cnt += 1
                        except Exception:
                            continue
                        if cnt >= n: break
                    if require_n and cnt < n:
                        return None
                    return float(acc) if cnt else None
            return None
        except Exception: return None

    def _slice_yq_symbol_payload(obj, symbol):
        """P1-517: yahooquery複数銘柄レスポンスを1銘柄だけへ安全に切り出す。"""
        if obj is None:
            return None
        if isinstance(obj, dict):
            if symbol in obj:
                return obj.get(symbol)
            su = str(symbol).upper()
            for k, v in obj.items():
                if str(k).upper() == su:
                    return v
            return None
        if not isinstance(obj, pd.DataFrame):
            return obj
        if obj.empty:
            return obj.copy()
        su = str(symbol).upper()
        try:
            if isinstance(obj.index, pd.MultiIndex) and "symbol" in list(obj.index.names):
                level = list(obj.index.names).index("symbol")
                mask = pd.Index(obj.index.get_level_values(level)).astype(str).str.upper() == su
                part = obj.loc[mask].copy()
                # 配当履歴などsymbol/date MultiIndexはsymbol levelを落とし、dateをindexに残す。
                if not part.empty:
                    try:
                        part = part.droplevel("symbol")
                    except Exception:
                        pass
                return part
            idx = pd.Index(obj.index).astype(str).str.upper()
            if (idx == su).any():
                return obj.loc[idx == su].copy()
            if "symbol" in obj.columns:
                mask = obj["symbol"].astype(str).str.upper() == su
                return obj.loc[mask].copy()
        except Exception:
            return None
        # symbol識別子の無いDataFrameを複数銘柄へ使い回さない。
        return None

    def _sorted_period_items(obj):
        """period辞書を日付キーの新しい順に並べる。日付解釈不能なら元順を維持。"""
        items = list(obj.items()) if isinstance(obj, dict) else []
        parsed = []
        any_date = False
        for pos, (k, v) in enumerate(items):
            d = pd.to_datetime(k, errors="coerce")
            if pd.notna(d):
                any_date = True
                parsed.append((pd.Timestamp(d).value, -pos, k, v))
            else:
                parsed.append((-(10**30), -pos, k, v))
        if not any_date:
            return items
        parsed.sort(reverse=True)
        return [(k, v) for _, _, k, v in parsed]

    def _get_from_periods(obj, keys):
        if obj is None: return None
        if isinstance(obj, dict):
            for k in keys:
                if k in obj and obj[k] is not None:
                    try: return float(obj[k])
                    except Exception: pass
            for _per, fields in _sorted_period_items(obj):
                if isinstance(fields, dict):
                    for k in keys:
                        if k in fields and fields[k] is not None:
                            try: return float(fields[k])
                            except Exception: pass
        return None

    def _sum_recent(obj, keys, n=4):
        if obj is None: return None
        total = 0.0; cnt = 0
        if isinstance(obj, dict):
            for _per, fields in _sorted_period_items(obj):
                if isinstance(fields, dict):
                    v = None
                    for k in keys:
                        if k in fields and fields[k] is not None:
                            try: v = float(fields[k]); break
                            except Exception: v = None
                    # P1-93: 対象キーが無いperiodを「0の観測」と数えない。
                    if v is not None:
                        total += v
                        cnt += 1
                        if cnt >= n: break
        return total if cnt > 0 else None

    def _sum_dividends_1y(divs, one_year_ago):
        """P1-94: 日付が確認できる配当だけを直近1年で合計。期間不明データを「1年」と偽らない。"""
        if divs is None:
            return None
        cutoff = pd.Timestamp(one_year_ago).normalize()

        def _dt(v):
            try:
                x = pd.to_datetime(v, errors="coerce")
                if pd.isna(x):
                    return None
                if getattr(x, "tzinfo", None) is not None:
                    x = x.tz_localize(None)
                return pd.Timestamp(x).normalize()
            except Exception:
                return None

        def _amt(v):
            try:
                if v is None or isinstance(v, bool):
                    return None
                x = float(v)
                return x if np.isfinite(x) else None
            except Exception:
                return None

        records = []
        if isinstance(divs, dict):
            for k, v in divs.items():
                if isinstance(v, dict):
                    d = _dt(v.get("date") or v.get("exDate") or v.get("asOfDate") or k)
                    a = _amt(v.get("amount") if "amount" in v else v.get("dividend") if "dividend" in v else v.get("value"))
                else:
                    d = _dt(k)
                    a = _amt(v)
                if d is not None and a is not None:
                    records.append((d, a))
        elif isinstance(divs, (list, tuple)):
            for item in divs:
                if not isinstance(item, dict):
                    continue
                d = _dt(item.get("date") or item.get("exDate") or item.get("asOfDate"))
                a = _amt(item.get("amount") if "amount" in item else item.get("dividend") if "dividend" in item else item.get("value"))
                if d is not None and a is not None:
                    records.append((d, a))
        elif _is_nonempty_df(divs):
            try:
                # P1-522: dividend_historyのsymbol/date MultiIndexはP1-517でsymbolを落とし、
                # 残ったdate indexをそのまま直近1年判定へ使う。
                ddf = divs.copy()
                date_col = next((c for c in ("date", "exDate", "asOfDate") if c in ddf.columns), None)
                dates = pd.to_datetime(ddf[date_col], errors="coerce") if date_col else pd.to_datetime(ddf.index, errors="coerce")
                amount_col = next((c for c in ("amount", "dividend", "dividends", "value") if c in ddf.columns), None)
                if amount_col is not None:
                    amounts = pd.to_numeric(ddf[amount_col], errors="coerce")
                else:
                    num = ddf.select_dtypes(include=["number"])
                    amounts = pd.to_numeric(num.iloc[:, 0], errors="coerce") if not num.empty else pd.Series(np.nan, index=ddf.index)
                for d, a in zip(dates, amounts):
                    if pd.notna(d) and pd.notna(a):
                        dd = pd.Timestamp(d)
                        if dd.tzinfo is not None:
                            dd = dd.tz_localize(None)
                        records.append((dd.normalize(), float(a)))
            except Exception:
                records = []

        # 日付を確認できる観測が無いなら、全期間合計を1年合計として使わない。
        if not records:
            return None
        # P2-42: 「直近1年」は過去実績。将来exDate等がpayloadへ混じっても合算しない。
        upper = pd.Timestamp(date.fromisoformat(_today_jst())).normalize()
        return float(sum(a for d, a in records if cutoff <= d <= upper))

    # --------------------------
    # 前処理
    # --------------------------
    one_year_ago = date.fromisoformat(_today_jst()) - timedelta(days=365)
    
    if set_wal:
        try: conn.execute("PRAGMA journal_mode=WAL;")
        except: pass

    # カラム確保（不足があれば追加）
    _financial_cols_to_ensure = [
        ("raw_fin_json", "TEXT"), ("財務更新日", "TEXT"),
        ("自己資本比率", "REAL"), ("営業CF_直近", "REAL"), ("営業CF_4Q合計", "REAL"),
        ("配当1年合計", "REAL"), ("自社株買い4Q合計", "REAL"),
        ("増資リスク", "INTEGER"), ("増資スコア", "REAL"), ("増資理由", "TEXT"),
        ("PBR", "REAL"), ("現金同等物", "REAL"), ("有利子負債", "REAL"), ("大株主", "TEXT"),
        ("EPS", "REAL"), ("BPS", "REAL"), ("ROE", "REAL"),
        ("浮動株数", "REAL"), ("発行済株式数", "REAL")
    ]
    # P2-16: 既存列追加時のbroad exceptを廃止。DB lock/破損等は財務更新失敗として止める。
    _financial_schema_cols = {r[1] for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    for name, decl in _financial_cols_to_ensure:
        if name not in _financial_schema_cols:
            conn.execute(f'ALTER TABLE screener ADD COLUMN "{name}" {decl}')
            _financial_schema_cols.add(name)

    cur = conn.cursor()
    cur.execute('SELECT rowid, コード, raw_fin_json, 財務更新日 FROM screener ORDER BY rowid DESC')
    _rows_all = cur.fetchall()
    cur.close()

    # P2-60: cache値とUPDATE先を別々のdict comprehension/先勝ちmapで決めない。
    # canonical raw行をaliasより優先し、同じ優先度なら最新rowidを正本にする。
    _financial_snapshot = {}
    for _rid, _rc, _raw_json, _fin_date in _rows_all:
        _rk = canonical_code_for_db(_rc)
        if not _rk:
            continue
        _is_canonical_raw = (str(_rc).strip().upper() == str(_rk).upper())
        _prev = _financial_snapshot.get(_rk)
        if _prev is None or (_is_canonical_raw and not _prev[0]):
            _financial_snapshot[_rk] = (_is_canonical_raw, _rid, _rc, _raw_json, _fin_date)

    # P1-216/P2-60: universe/cache/update-keyを同じsnapshotから揃える。
    codes = list(_financial_snapshot.keys())
    # P1-549: 財務諸表/配当等も企業情報。指数は価格系だけに残し、財務batchから除外。
    codes = list(dict.fromkeys(filter_yahoo_company_codes(conn, codes)))
    raw_map = {k: v[3] for k, v in _financial_snapshot.items()}
    fin_date_map = {k: v[4] for k, v in _financial_snapshot.items()}

    # P3-42: 固定7日cacheだけでは新決算当日にYahoo財務を取り直せない。
    # 最新TDnet決算日が財務更新日と同日以降なら再取得対象にし、同日Yahoo未反映でも翌日もう一度試す。
    _latest_earnings_date_map = {}
    try:
        _et = conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='earnings_events'").fetchone()
        if _et:
            _ecols = {r[1] for r in conn.execute("PRAGMA table_info(earnings_events)").fetchall()}
            _code_col = next((c for c in ("コード","code","銘柄コード") if c in _ecols), None)
            _time_col = next((c for c in ("提出時刻","発表日時","time","pubdate") if c in _ecols), None)
            if _code_col and _time_col:
                _q = f'SELECT "{_code_col}", MAX(substr("{_time_col}",1,10)) FROM earnings_events GROUP BY "{_code_col}"'
                for _c,_d in conn.execute(_q).fetchall():
                    _ck = canonical_code_for_db(_c)
                    if _ck and _d:
                        _latest_earnings_date_map[_ck] = str(_d)[:10]
    except Exception as _e:
        raise RuntimeError(f"latest earnings date load failed for financial cache invalidation: {_e}") from _e

    # P1-262: API/計算はcanonicalでも最終UPDATEはscreener実在rawキーへ。
    screener_raw_map = {k: v[2] for k, v in _financial_snapshot.items()}

    total = len(codes)
    processed = 0; updated_rows = 0; errors = 0
    log.info(f"[batch.start] total={total} chunk={chunk_size} force={force_refresh} +NewCols")

    # --------------------------
    # DB登録用関数 (★ここを拡張)
    # --------------------------
    def commit_batch(metrics_rows):
        nonlocal updated_rows
        if metrics_rows:
            # P1-432: 1チャンク内の財務更新を原子的にする。
            cur = conn.cursor()
            try:
                cur.execute("SAVEPOINT p1_432_financial_chunk")
                cur.executemany("""
                    UPDATE screener SET
                      -- P1-523: fresh snapshotで欠損になった値へ旧値をCOALESCEで残さない。
                      -- 「今回取れない」を「今回も有効」と見せないため、fresh解析行はNULLも反映する。
                      "自己資本比率"      = ?,
                      "営業CF_直近"       = ?,
                      "営業CF_4Q合計"     = ?,
                      "配当1年合計"       = ?,
                      "自社株買い4Q合計"  = ?,
                      "財務更新日"        = ?,
                      "raw_fin_json"      = ?,
                      "PBR"               = ?,
                      "現金同等物"        = ?,
                      "有利子負債"        = ?,
                      "大株主"            = ?,
                      "EPS"               = ?,
                      "BPS"               = ?,
                      "ROE"               = ?
                    WHERE "コード" = ?
                """, metrics_rows)
                _affected = cur.rowcount
                if _affected != len(metrics_rows):
                    raise RuntimeError(
                        f"financial UPDATE rowcount mismatch: expected={len(metrics_rows)} affected={_affected}"
                    )
                cur.execute("RELEASE SAVEPOINT p1_432_financial_chunk")
            except Exception:
                try:
                    cur.execute("ROLLBACK TO SAVEPOINT p1_432_financial_chunk")
                    cur.execute("RELEASE SAVEPOINT p1_432_financial_chunk")
                except Exception:
                    pass
                raise
            finally:
                cur.close()
            updated_rows += len(metrics_rows)
            log.info(f"[commit.metrics] rows={len(metrics_rows)} total_updated={updated_rows}")

    # --------------------------
    # メインループ
    # --------------------------
    # P2-46: 財務も全対象symbolを先に解決し、別chunkの設定衝突を見逃さない。
    _financial_symbols_all = resolve_yahoo_symbols_bulk(codes, conn) if codes else []

    for i in range(0, total, chunk_size):
        chunk = codes[i:i+chunk_size]
        syms = _financial_symbols_all[i:i+chunk_size]
        
        # 取得要否判定
        to_fetch = []
        for c, s in zip(chunk, syms):
            if force_refresh:
                to_fetch.append(s); continue
            raw = raw_map.get(c)
            if not raw:
                to_fetch.append(s); continue
            # P1-547: 旧版raw cacheには取得方式versionが無い。
            # P1-516以降のquarterly/TTMロジックへ移行した最初の1回は必ず再取得する。
            try:
                _raw_meta = json.loads(raw) if isinstance(raw, str) else raw
                _cache_ver = int((_raw_meta or {}).get("__financial_fetch_schema_version__", 0))
            except Exception:
                _cache_ver = 0
            if _cache_ver != int(FINANCIAL_FETCH_SCHEMA_VERSION):
                to_fetch.append(s); continue
            fin_d = fin_date_map.get(c)
            if not fin_d:
                to_fetch.append(s); continue
            try:
                fd = datetime.strptime(str(fin_d)[:10], "%Y-%m-%d").date()
                _earn_s = _latest_earnings_date_map.get(c)
                _earn_d = datetime.strptime(_earn_s, "%Y-%m-%d").date() if _earn_s else None
                # 新決算当日のYahoo反映遅延に備え、earnings日==財務更新日も次generationで再試行。
                if _earn_d is not None and _earn_d >= fd:
                    to_fetch.append(s)
                    continue
                if (date.fromisoformat(_today_jst()) - fd).days >= 7:
                    to_fetch.append(s)
            except Exception:
                to_fetch.append(s)
        
        log.info(f"[batch.chunk] {i}-{i+len(chunk)-1} need_fetch={len(to_fetch)}")

        # --------------------------
        # データ取得 (★追加取得)
        # --------------------------
        fetched_raw = {}
        _fetch_exception = False
        if to_fetch:
            try:
                tk = Ticker(to_fetch, max_workers=8) # ワーカー8推奨
                
                # P2-4: 財務batchではquotesを使わないため取得自体を削除。
                # marketCapは専用の時価総額フェーズがauthoritative。
                ks = getattr(tk, "key_stats", {}) or {}
                
                try: fd_data = tk.financial_data  # ←追加: ROE用
                except: fd_data = {}
                
                # P1-516: 年次defaultではなく最新四半期のBSを取得。
                # P1-524: core財務moduleの呼出失敗をNoneへ潰して「一部だけfresh」にしない。
                try:
                    bs = tk.balance_sheet(frequency="q")
                except Exception as _bs_err:
                    raise RuntimeError(f"yahooquery balance_sheet(q) failed: {_bs_err}") from _bs_err
                # P1-519: 4Q合計は3M×4またはTTMを明示的に使うため、q + trailingを取得。
                try:
                    cf = tk.cash_flow(frequency="q", trailing=True)
                except Exception as _cf_err:
                    raise RuntimeError(f"yahooquery cash_flow(q) failed: {_cf_err}") from _cf_err
                if bs is None or isinstance(bs, str):
                    raise RuntimeError(f"yahooquery balance_sheet(q) returned unusable payload: {bs!r}")
                if cf is None or isinstance(cf, str):
                    raise RuntimeError(f"yahooquery cash_flow(q) returned unusable payload: {cf!r}")
                # P1-520: 現行yahooqueryは dividend_history(start=...) が履歴API。
                try:
                    if hasattr(tk, "dividend_history"):
                        divs = tk.dividend_history(start=one_year_ago.isoformat())
                    else:
                        _legacy_div = getattr(tk, "dividends", None)
                        divs = _legacy_div() if callable(_legacy_div) else None
                except Exception:
                    divs = None
                try: mh = tk.major_holders
                except: mh = None

                def _record_payload(obj, symbol):
                    """P2-15: key_stats/financial_dataをsymbol単位の1レコードdictへ正規化。"""
                    part = _slice_yq_symbol_payload(obj, symbol)
                    if isinstance(part, dict):
                        return part
                    if isinstance(part, pd.Series):
                        return part.to_dict()
                    if isinstance(part, pd.DataFrame):
                        if part.empty:
                            return {}
                        # symbolで切出した後に複数行ある場合は最新/先頭1行のみをrecordとして扱う。
                        return part.iloc[0].to_dict()
                    return {}

                for s in to_fetch:
                    # P2-15: yahooqueryはmoduleによってdict/DataFrame/MultiIndex DataFrameが混在し得る。
                    # BS/CF/dividendだけでなくkey_stats/financial_data/major_holdersもsymbol単位へ切り出す。
                    k = _record_payload(ks, s)
                    f_dat = _record_payload(fd_data, s)
                    b = _slice_yq_symbol_payload(bs, s)
                    cflow = _slice_yq_symbol_payload(cf, s)
                    d = _slice_yq_symbol_payload(divs, s)
                    m = _slice_yq_symbol_payload(mh, s)
                    
                    fetched_raw[s] = {
                        "key_stats": k, 
                        "financial_data": f_dat,  
                        "balance_sheet": b, 
                        "cashflow": cflow, 
                        "dividends": d,
                        "major_holders": m
                    }
                log.info(f"[fetch.done] symbols={len(to_fetch)}")
            except Exception as e:
                _fetch_exception = True
                log.exception(f"[fetch.error] {e}")
                errors += len(to_fetch)

        # --------------------------
        # 解析 & 行作成
        # --------------------------
        metrics_rows = []
        for c, s in zip(chunk, syms):
            processed += 1
            # P1-91: API取得を省略した/失敗した銘柄を、欠損値で再UPDATEしない。
            # 既存DB値はそのまま保持し、fresh取得に成功した銘柄だけ解析・更新する。
            if s not in fetched_raw:
                continue
            raw_text = None; sym_raw = fetched_raw[s]
            # P2-50: 直前で fetched_raw 未収載symbolはcontinue済み。
            # 旧raw-cacheをこのloopで再解析する到達不能分岐を廃止し、fresh payloadだけを扱う。
            if s in fetched_raw:
                # P1-92: symbolキーは返っていても全moduleが空/エラーなら取得成功とはみなさない。
                _payload_vals = [sym_raw.get(k) for k in (
                    "key_stats", "financial_data", "balance_sheet",
                    "cashflow", "dividends", "major_holders"
                )] if isinstance(sym_raw, dict) else []
                # P1-233: quoteだけ返ったレスポンスで「財務更新成功」にしない。
                _financial_payload_vals = [sym_raw.get(k) for k in (
                    "key_stats", "financial_data", "balance_sheet", "cashflow"
                )] if isinstance(sym_raw, dict) else []
                def _fresh_payload(v):
                    if v is None:
                        return False
                    if isinstance(v, dict):
                        return bool(v)
                    if isinstance(v, (list, tuple)):
                        return len(v) > 0
                    if isinstance(v, str):
                        return bool(v.strip()) and not v.lower().startswith(("error", "http"))
                    try:
                        return bool(_is_nonempty_df(v))
                    except Exception:
                        return True
                if not any(_fresh_payload(v) for v in _payload_vals):
                    continue
                # P1-252: 財務moduleの単なる非空文字列を成功データとみなさない。
                def _financial_payload_fresh(v):
                    if isinstance(v, dict):
                        return bool(v)
                    if isinstance(v, (list, tuple)):
                        return len(v) > 0
                    return _is_nonempty_df(v)
                if not any(_financial_payload_fresh(v) for v in _financial_payload_vals):
                    continue
                try:
                    # JSON保存時は重い項目(major_holdersなど)を除外して軽くする
                    dump_data = {k:v for k,v in sym_raw.items() if k not in ("major_holders","balance_sheet","cashflow")}
                    dump_data["__financial_fetch_schema_version__"] = int(FINANCIAL_FETCH_SCHEMA_VERSION)
                    raw_text = json.dumps(dump_data, default=str, ensure_ascii=False)
                except: raw_text = None

            equity_ratio = None; ocf_recent_val = None; ocf_4q_val = None
            div_1y = None; buyback_4q = None
            # ★追加項目
            pbr = None; cash = None; debt = None; holders_str = None
            # P1-232: parse途中で例外になっても未束縛変数でbatch全体を落とさない。
            eps = None; bps = None; roe = None

            _parse_failed = False
            try:
                if sym_raw is not None:
                    # --- key_stats & financial_data (PBR, EPS, BPS, ROE) ---
                    k = sym_raw.get("key_stats") or {}
                    f_dat = sym_raw.get("financial_data") or {}
                    
                    pbr = _safe_num(k.get("priceToBook"))
                    
                    # ▼ ここから追加・修正 ▼
                    eps = _safe_num(k.get("trailingEps"))
                    if eps is None: eps = _safe_num(k.get("forwardEps"))
                    bps = _safe_num(k.get("bookValue"))
                    
                    roe_raw = _safe_num(f_dat.get("returnOnEquity"))
                    roe = roe_raw * 100.0 if roe_raw is not None else None
                    
                    # ※「浮動株数」「発行済株式数」の取得はここで行わず、
                    # 別タスクとして「浮動.py」で独立して取得・DB保存を行う運用に変更しました。
                    # ▲ ここまで追加・修正 ▲

                    # --- balance_sheet ---
                    bsobj = sym_raw.get("balance_sheet")
                    if bsobj is not None:
                        # 既存ロジック
                        assets = _get_from_periods(bsobj, ["TotalAssets","totalAssets","Total Assets","total_assets"])
                        equity = _get_from_periods(bsobj, ["StockholdersEquity","TotalStockholderEquity","totalStockholderEquity",
                                                          "Total Equity Gross Minority Interest","TotalEquityGrossMinorityInterest",
                                                          "CommonStockEquity","Total Stockholder Equity","total_equity"])
                        if (assets is None or equity is None) and _is_nonempty_df(bsobj):
                            # P1-235: 有効な0を欠損扱いして別期間値へ差し替えない。
                            if assets is None:
                                assets = _yf_pick_recent_from_df(bsobj, ["TotalAssets","totalAssets","Total Assets","total_assets"], period_types=("3M",))
                            if equity is None:
                                equity = _yf_pick_recent_from_df(bsobj, ["StockholdersEquity","TotalStockholderEquity","totalStockholderEquity",
                                                                         "TotalEquityGrossMinorityInterest","CommonStockEquity",
                                                                         "Total Stockholder Equity","total_equity"], period_types=("3M",))
                        # P1-234: 自己資本0は0%という有効値。truthy判定で欠損化しない。
                        if assets is not None and equity is not None and float(assets) != 0:
                            try: equity_ratio = float(equity) / float(assets) * 100.0
                            except: equity_ratio = None
                        
                        # ★追加: 現金・負債
                        # 辞書アクセス
                        cash = _get_from_periods(bsobj, ["CashAndCashEquivalents", "CashCashEquivalentsAndShortTermInvestments"])
                        debt = _get_from_periods(bsobj, ["TotalDebt", "TotalLiabilitiesNetMinorityInterest"])
                        # DataFrameフォールバック
                        if (cash is None or debt is None) and _is_nonempty_df(bsobj):
                             if cash is None:
                                 cash = _yf_pick_recent_from_df(bsobj, ["CashAndCashEquivalents", "CashCashEquivalentsAndShortTermInvestments"], period_types=("3M",))
                             if debt is None:
                                 debt = _yf_pick_recent_from_df(bsobj, ["TotalDebt", "TotalLiabilitiesNetMinorityInterest"], period_types=("3M",))

                    # --- cash_flow ---
                    cfobj = sym_raw.get("cashflow")
                    if cfobj is not None:
                        _ocf_keys = ["OperatingCashFlow","operatingCashflow","Operating Cash Flow","operatingCashFlow",
                                     "TotalCashFromOperatingActivities"]
                        _buy_keys = ["RepurchaseOfCapitalStock","repurchaseOfCapitalStock","repurchaseOfStock",
                                     "Repurchase Of Stock","RepurchaseOfStock"]
                        # P1-519: 直近は3Mのみ。TTMを「直近四半期」と誤認しない。
                        ocf_recent = _get_from_periods(cfobj, _ocf_keys)
                        ocf_recent_val = _safe_num(ocf_recent)
                        if _is_nonempty_df(cfobj):
                            ocf_recent_val = _yf_pick_recent_from_df(cfobj, _ocf_keys, period_types=("3M",))

                            # P1-521: 4Q合計はYahooのTTMを第一候補。無ければ3Mが4本揃った時だけ合算。
                            ocf_4q_val = _yf_pick_recent_from_df(cfobj, _ocf_keys, period_types=("TTM",))
                            if ocf_4q_val is None:
                                ocf_4q_val = _yf_sum_quarters_df(cfobj, _ocf_keys, 4, require_n=True)

                            buyback_4q = _yf_pick_recent_from_df(cfobj, _buy_keys, period_types=("TTM",))
                            if buyback_4q is None:
                                # 4四半期の行自体が存在する場合、line itemのNaNはその四半期0として合算。
                                buyback_4q = _yf_sum_quarters_df(cfobj, _buy_keys, 4, require_n=True, missing_as_zero=True)
                        else:
                            # dict型の旧/例外応答は従来互換。ただし「4Q」の名称と矛盾する部分合計は避ける。
                            ocf_4q_val = _sum_recent(cfobj, _ocf_keys, 4)
                            buy = _sum_recent(cfobj, _buy_keys, 4)
                            buyback_4q = float(buy) if buy is not None else None

                    # --- dividends ---
                    divobj = sym_raw.get("dividends")
                    div_1y = _sum_dividends_1y(divobj, one_year_ago)

                    # --- ★追加: 大株主 (修正版) ---
                    mhobj = sym_raw.get("major_holders")
                    if mhobj is not None:
                        try:
                            if hasattr(mhobj, "to_string") and "Holder" in getattr(mhobj, "columns", []):
                                # 米国株などで DataFrame (Holder列) が取れた場合
                                top = mhobj["Holder"].head(3).astype(str).tolist()
                                holders_str = ", ".join(top)
                            elif isinstance(mhobj, dict):
                                # dict化されたDataFrameの場合（Holderキーが存在）
                                if "Holder" in mhobj and isinstance(mhobj["Holder"], dict):
                                    h_dict = mhobj["Holder"]
                                    s_keys = sorted(h_dict.keys(), key=lambda x: int(x) if str(x).isdigit() else 999)
                                    holders_str = ", ".join([str(h_dict[k]) for k in s_keys if h_dict[k]][:3])
                                else:
                                    # 日本株特有の統計データ(insidersPercentHeld等)の場合
                                    insiders = mhobj.get("insidersPercentHeld")
                                    institutions = mhobj.get("institutionsPercentHeld")
                                    count = mhobj.get("institutionsCount")
                                    
                                    parts = []
                                    if insiders is not None:
                                        parts.append(f"内部者:{insiders*100:.1f}%")
                                    if institutions is not None:
                                        parts.append(f"機関:{institutions*100:.1f}%")
                                    if count is not None:
                                        parts.append(f"({int(count)}社)")
                                        
                                    if parts:
                                        holders_str = " ".join(parts)
                                    else:
                                        # 完全に未知の構造の場合は短くカット
                                        holders_str = ""
                        except Exception: 
                            pass

            except Exception as e:
                # P2-49: 途中まで値が取れていても、1つのpayload解析が例外になった銘柄を
                # fresh成功として保存しない。更新日を進めず次回再取得させる。
                _parse_failed = True
                log.warning(f"[parse.warn] {c} parse error; financial row not refreshed: {e}")

            if _parse_failed:
                continue

            # P1-264: 財務moduleが非空でも、有効な財務値を1つも解釈できなければ
            # 「財務更新成功」にせず、財務更新日だけ進めて7日間 stale を固定しない。
            _parsed_financial_values = [
                equity_ratio, ocf_recent_val, ocf_4q_val, div_1y, buyback_4q,
                pbr, cash, debt, holders_str, eps, bps, roe
            ]
            if not any(v is not None and (not isinstance(v, str) or bool(v.strip())) for v in _parsed_financial_values):
                continue

            # P2-4: 旧「財務4条件フラグ」はDBへ一度も書かれておらず、flags_rowsもcommit_batchで未使用だった。
            # その専用判定（時価総額/自己資本/CF/還元）は削除し、実際に保存する財務値だけを組み立てる。
            # 行データ作成
            today_iso = _today_jst()
            metrics_rows.append((
                float(equity_ratio) if (equity_ratio is not None) else None,
                float(ocf_recent_val) if (ocf_recent_val is not None) else None,
                float(ocf_4q_val) if (ocf_4q_val is not None) else None,
                float(div_1y) if (div_1y is not None) else None,
                float(buyback_4q) if (buyback_4q is not None) else None,
                today_iso,
                raw_text,
                # ★追加データ
                float(pbr) if pbr is not None else None,
                float(cash) if cash is not None else None,
                float(debt) if debt is not None else None,
                str(holders_str) if holders_str else None,
                # ▼ 以下追加
                float(eps) if eps is not None else None,
                float(bps) if bps is not None else None,
                float(roe) if roe is not None else None,
                # ※ float_shares, shares_out は「浮動.py」で更新するため削除
                # WHERE句
                screener_raw_map.get(c, c)
            ))

        # P1-432: API呼出自体が成功しても、要求した財務銘柄の一部が空/解釈不能なら
        # 「完全成功」にしない。財務更新日を進めなかった銘柄は次回再取得対象として残る。
        if to_fetch and not _fetch_exception:
            _missing_fin = max(0, len(to_fetch) - len(metrics_rows))
            if _missing_fin:
                errors += _missing_fin
                log.error(f"[fetch.partial] requested={len(to_fetch)} parsed={len(metrics_rows)} missing={_missing_fin}")

        # commit
        try:
            commit_batch(metrics_rows)
        except Exception as e:
            log.exception(f"[DB.commit.error] chunk {i}: {e}")
            errors += 1

        time.sleep(sleep_between_chunks)

    summary = {"total": total, "processed": processed, "updated_rows": updated_rows, "errors": errors}
    log.info(f"[batch.done] {summary}")
    # P1-432: 19時バッチ側が戻り値を検査しなくても、部分失敗を成功扱いさせない。
    if errors:
        raise RuntimeError(f"batch_update_all_financials incomplete: {summary}")
    return summary
    
# --- END: batch_update_all_financials ---


def yj_board(code: str, name: str):
    # P1-153: 掲示板URLも共通コード正規化。
    c = _normalize_jp_security_code(code)
    return f'<a href="https://finance.yahoo.co.jp/quote/{c}.T/bbs" target="_blank" rel="noopener">{name} <span class="code">({c})</span></a>'    


def _yahoo_quote_url(code: str, market: str | None = None, conn: sqlite3.Connection | None = None) -> str:
    """
    Yahoo!ファイナンス（日本）の銘柄ページURLを安全に生成する（クリーン実装）
    - 1引数呼び出し: _yahoo_quote_url('3350')
    - 3引数呼び出し: _yahoo_quote_url('3350', '東P', conn)
    どちらも受け付け、常に "https://finance.yahoo.co.jp/quote/XXXX.SUF" を返す。
    優先度: DB明示override/resolve_yahoo_symbol() → market推定 → '.T' フォールバック
    """
    try:
        raw = ("" if code is None else str(code)).strip()
        if not raw:
            return ""
        # P1-152: 7203.0→7203 / 285A維持。
        c4 = _normalize_jp_security_code(raw)

        # 市場からサフィックス推定
        def _suffix_from_market(m: str | None) -> str | None:
            if not m: 
                return None
            m = str(m)
            if "名" in m or "NSE" in m or "名証" in m or "NAGOYA" in m: return ".N"
            if "札" in m or "SSE" in m or "札証" in m or "SAPPORO" in m: return ".S"
            if "福" in m or "FSE" in m or "福証" in m or "FUKUOKA" in m: return ".F"
            if "東" in m or "TSE" in m or "東証" in m or "TOKYO" in m: return ".T"
            return None

        # P2-40: DB明示overrideを市場推定より優先する。特殊symbolはそのままURLへ使う。
        if conn is not None:
            ys = resolve_yahoo_symbol(raw, conn)
            if isinstance(ys, str) and ys.strip():
                return f"https://finance.yahoo.co.jp/quote/{ys.strip()}"

        suf = _suffix_from_market(market)
        if not suf:
            try:
                ys = resolve_yahoo_symbol(raw, None)
                if isinstance(ys, str) and "." in ys:
                    suf = ys[ys.find("."):]
            except Exception:
                suf = None

        if not suf:
            suf = ".T"  # 最終フォールバック（東証）

        return f"https://finance.yahoo.co.jp/quote/{c4}{suf}"
    except Exception:
        return ""    # conn の判定（後方互換：パスでもOK）


def load_earnings_events_from_db(db_or_conn, days: int, limit: int):
    """P1-300: earnings_events をDB/DBパスから読み出す。

    旧ソースではこの本体が _yahoo_quote_url() の return 後ろに誤って
    インデントされ、完全な到達不能コードになっていた。
    """
    owns_conn = False
    if isinstance(db_or_conn, sqlite3.Connection):
        conn = db_or_conn
    else:
        db_path = str(db_or_conn)
        if not os.path.exists(db_path):
            print(f"[earnings][WARN] DB not found: {db_path} → []")
            return []
        conn = sqlite3.connect(db_path, timeout=30.0, isolation_level=None)
        owns_conn = True

    try:
        # テーブル存在チェック（row_factory をいじらない）
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name='earnings_events'")
            if not cur.fetchone():
                print("[earnings][WARN] table earnings_events not found → []")
                return []
        finally:
            cur.close()

        # 期間
        now_jst = datetime.now(ZoneInfo("Asia/Tokyo"))
        # P1-300: datetimeはクラスimport。datetime.timedeltaは存在しないため、import済みtimedeltaを使う。
        since   = (now_jst - timedelta(days=int(days))).strftime("%Y-%m-%d 00:00:00")

        # P1-624: P1-621のfreshness helperだけでなく通常イベント読込もtimestamp schemaを統一。
        # SQLiteのCOALESCEは空文字をNULL扱いしないため、発表日時='' / 提出時刻=有効値だと
        # recent eventを期間外へ落とす。また旧DBで片方の列しか無い場合は固定SQL自体が失敗する。
        _ecols = {r[1] for r in conn.execute("PRAGMA table_info(earnings_events)").fetchall()}
        if "発表日時" in _ecols and "提出時刻" in _ecols:
            _ts_expr = "COALESCE(NULLIF(TRIM(発表日時),''), NULLIF(TRIM(提出時刻),''))"
        elif "発表日時" in _ecols:
            _ts_expr = "NULLIF(TRIM(発表日時),'')"
        elif "提出時刻" in _ecols:
            _ts_expr = "NULLIF(TRIM(提出時刻),'')"
        else:
            print("[earnings][WARN] earnings_events timestamp column missing → []")
            return []

        # 取得
        sql = f"""
            SELECT
              コード,
              銘柄名,
              タイトル,
              リンク,
              {_ts_expr} AS ts,
              要約,
              判定,
              判定スコア,
              理由JSON,
              指標JSON,
              進捗率,
              センチメント,
              素点
            FROM earnings_events
            WHERE {_ts_expr} >= ?
            ORDER BY {_ts_expr} DESC
            LIMIT ?
        """
        df = pd.read_sql_query(sql, conn, params=[since, int(limit)])

        if df.empty:
            return []

        # JSON列のデコード
        def _loads_or(default):
            def _f(x):
                if isinstance(x, (dict, list)): return x
                if isinstance(x, str):
                    try: return json.loads(x)
                    except Exception: return default
                return default
            return _f

        df["理由JSON"] = df.get("理由JSON", []).apply(_loads_or([])) if "理由JSON" in df.columns else []
        df["指標JSON"] = df.get("指標JSON", []).apply(_loads_or({})) if "指標JSON" in df.columns else {}

        # 出力整形
        out = []
        for rec in df.to_dict("records"):
            out.append({
                # P1-200: 決算イベントのtickerも英数字/floatコード共通正規化。
                "ticker":     canonical_code_for_db(rec.get("コード")),
                "name":       rec.get("銘柄名") or "",
                "title":      rec.get("タイトル") or "",
                "link":       rec.get("リンク") or "",
                "time":       rec.get("ts") or "",
                "summary":    rec.get("要約") or "",
                "verdict":    rec.get("判定") or "",
                "score_judge": int(rec.get("判定スコア") or 0),
                "reasons":    rec.get("理由JSON") or [],
                "metrics":    rec.get("指標JSON") or {},
                "progress":   rec.get("進捗率"),
                "sentiment":  rec.get("センチメント") or "",
                "score":      int(rec.get("素点") or 0),
            })
        return out

    finally:
        # ここで開いたときだけ閉じる（共有接続は閉じない）
        if owns_conn:
            try: conn.close()
            except Exception: pass


# ===== 予測タブの付加情報（列）を作るユーティリティ =====

    
def phase_sync_finance_comments(conn):
    """
    finance_notes → screener へ 財務コメント/スコア/進捗率/overall_alpha を同期。
    - finance_notes に overall_alpha が無ければ追加し、コメントから抽出（無ければ score で暫定）
    - 空文字は既存値を潰さない（NULLIFで無効化）
    - 必要カラムを screener に追加
    - 可能な限り1トランザクションで実行
    """

    cur = conn.cursor()
    cur.execute("PRAGMA busy_timeout=8000")

    SYNC_HTML = False  # html_path/updated_at も同期したい場合は True

    # ---- 軽量インデックス ----
    # P2-68: indexは性能最適化なので失敗しても同期自体は継続可能。ただし完全silentにはしない。
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_finance_notes_code ON finance_notes(コード);")
    except Exception as _e:
        print(f"[sync][WARN] finance_notes index ensure failed: {_e}")
    try:
        cur.execute("CREATE INDEX IF NOT EXISTS idx_screener_code ON screener(コード);")
    except Exception as _e:
        print(f"[sync][WARN] screener index ensure failed: {_e}")

    # ---- screener 必要カラム追加 ----
    cur.execute("PRAGMA table_info(screener)")
    sc_cols = {r[1] for r in cur.fetchall()}
    need_cols = [
        ("財務コメント","TEXT"),
        ("スコア","INTEGER"),
        ("進捗率","REAL"),
        ("overall_alpha","TEXT"),
        # P1-654: 現行FUND_SCRIPT（株探ファンダ.py）がscreenerへ直接current値を書き込む。
        # schema保証だけここでも維持するが、finance_notesからは上書きしない。
        ("直近売上YoY","REAL"),
        ("直近営業益YoY","REAL"),
        ("利益加速フラグ","INTEGER"),
    ]
    for name, ddl in need_cols:
        if name not in sc_cols:
            cur.execute(f"ALTER TABLE screener ADD COLUMN {name} {ddl};")
            sc_cols.add(name)

    # ---- finance_notes: overall_alpha 追加＆初期埋め ----
    cur.execute("PRAGMA table_info(finance_notes)")
    fn_cols = {r[1] for r in cur.fetchall()}
    if "overall_alpha" not in fn_cols:
        cur.execute("ALTER TABLE finance_notes ADD COLUMN overall_alpha TEXT;")
        
    # ★ ここを追加（カラム不在によるエラーを防止）
    if "forecast_eps" not in fn_cols:
        cur.execute("ALTER TABLE finance_notes ADD COLUMN forecast_eps REAL;")

    # まとめて実行
    # P2-69: BEGIN IMMEDIATE/conn.rollback() は、呼出元が既にtransaction中だと失敗/全rollbackし得る。
    # 同期DMLだけをSAVEPOINTで囲み、外側transactionの境界は変更しない。
    _sync_sp = f"sp_finance_comments_{time.time_ns()}"
    cur.execute(f"SAVEPOINT {_sync_sp}")
    try:
        # P1-237: カラム追加時だけでなく、後から追加されたNULL/空overall_alphaも毎回補完。
        if True:
            # コメントから抽出 → 失敗時は score から暫定
            # 例: 「【総合評価】超優良 (S++)」「【総合評価】優良（A+）」などを拾う
            _re_label = re.compile(r"【総合評価】[^（\(\n]*?(?:（\s*([^)）]+)\s*）|\(([^)]+)\))")

            # 1) コメントから抽出
            # P1-474: finance_notesは履歴テーブル。コード単位UPDATEだと、同一銘柄の
            # 過去コメントまで最後に処理した評価で塗り替わるためrowid単位で補完する。
            cur.execute("SELECT rowid, 財務コメント FROM finance_notes WHERE overall_alpha IS NULL OR TRIM(overall_alpha)=''")
            rows = cur.fetchall()

            def _normalize_alpha(s):
                if not s: return None
                t = str(s).strip()
                # 全角括弧や全角＋を吸収
                t = (t.replace('（','(').replace('）',')')
                       .replace('＋','+').replace('＋＋','++')
                       .replace('＋ +','++'))
                # 余計な空白を除去
                t = t.replace(' ', '')
                # 想定外はそのまま返す（例: "S+", "A", "S++" など）
                return t or None

            updates = []
            for _rowid, comment in rows:
                alpha = None
                if comment:
                    m = _re_label.search(str(comment))
                    if m:
                        alpha = _normalize_alpha(m.group(1) or m.group(2))
                updates.append((alpha, _rowid))
            if updates:
                cur.executemany("UPDATE finance_notes SET overall_alpha = ? WHERE rowid = ?;", updates)

            # 2) まだNULLの所は score から暫定推定
            #    しきい値: >=8:S++ / >=5:A+ / >=2:A / >=0:C / 負評価:D / score=NULLはNULL
            cur.execute("""
                UPDATE finance_notes
                   SET overall_alpha =
                       CASE
                         WHEN score IS NULL THEN NULL
                         WHEN score >= 8 THEN 'S++'
                         WHEN score >= 5 THEN 'A+'
                         WHEN score >= 2 THEN 'A'
                         WHEN score >= 0 THEN 'C'
                         ELSE 'D'
                       END
                 WHERE overall_alpha IS NULL OR TRIM(overall_alpha)='';
            """)

        # ---- screener へ同期（空文字は上書きしない）----
        # P1-238: 生コード相関サブクエリをやめ、canonical codeでfinance_notesを対応付ける。
        # P1-654: YoY/利益加速は現行FUND_SCRIPTがscreenerへ直接authoritative更新する。
        # finance_notesに旧世代の同名列が残っていてもcurrent producerは更新しないため、ここでは読まない。
        _fn = pd.read_sql_query("""
            SELECT rowid AS _rowid, コード, updated_at, 財務コメント, score, progress_percent, overall_alpha, forecast_eps
            FROM finance_notes
        """, conn)
        _sc_codes = pd.read_sql_query("SELECT コード FROM screener", conn)
        _sync_rows = []
        _nmap = {}
        if not _fn.empty:
            _fn = _latest_finance_notes_by_canonical(_fn, "_key")
            _nmap = {r["_key"]: r for _, r in _fn.iterrows()}
        _stale_finance_codes = _finance_codes_stale_after_latest_earnings(conn)
        if _stale_finance_codes:
            print(f"[sync][P1-608] newer earnings event -> stale finance mask: {len(_stale_finance_codes)} codes", flush=True)
        if not _sc_codes.empty:
            for _raw_code in _sc_codes["コード"].tolist():
                _key = canonical_code_for_db(_raw_code)
                _n = _nmap.get(_key)
                if _key in _stale_finance_codes:
                    # P1-608: finance正本が無い場合も含め、fetch_allが新決算を先に認識したら
                    # 古い評価値を「今回の決算評価」として同期しない。EPS自体はlast-known
                    # 公表値として保持するが、コメントは再取得待ちを明示して旧評価文の誤読を防ぐ。
                    _sync_rows.append(("⚠️ 新しい決算を検知。財務再取得待ち", None, None, None, _raw_code))
                    continue
                if _n is None:
                    # P1-589: 今回finance_notesに正本行が存在しない銘柄も、最新決算由来の
                    # score/progress/overall_alphaだけはauthoritativeにNULLへ戻す。finance_notes全体が
                    # 空の場合も同じ。旧実装はcontinue/外側条件で前回評価を永久保持できた。
                    # コメントはlast-known表示として保持する。
                    _sync_rows.append((None, None, None, None, _raw_code))
                    continue
                # P1-654: YoY/利益加速はFUND_SCRIPTがscreenerへ直接current更新済み。
                # finance_notesのlegacy列で上書きせず、そのcurrent値を保持する。
                _sync_rows.append((
                    _n.get("財務コメント"), _n.get("score"), _n.get("progress_percent"),
                    _n.get("overall_alpha"), _raw_code
                ))
        if _sync_rows:
            cur.executemany("""
                UPDATE screener
                   SET 財務コメント = COALESCE(NULLIF(?, ''), 財務コメント),
                       -- P1-562: score/progress/overall_alpha は「今回の最新決算評価」。
                       -- 最新finance_notesで欠損した時に前回決算値をCOALESCEで残すと、
                       -- 季節進捗・Factor/適正株価へ古い評価をfresh値として持ち越すため直接置換する。
                       スコア        = ?,
                       進捗率        = ?,
                       overall_alpha= NULLIF(?, '')
                       -- P1-670: screener.EPS と finance_notes.forecast_eps を同じ列へ混在させない。
                       -- EPSはYahoo財務等のlast-known EPS、予想EPSはfinance_notes側で別管理する。
                       -- current forecast_eps=NULL時に前回予想EPSをCOALESCEで復活させない。
                       -- P1-654: 直近YoY/利益加速はFUND_SCRIPTがscreenerへ直接更新するため、
                       -- finance_notes同期では触らない。stale-after-event時だけ下段でNULL化する。
                 WHERE コード = ?
            """, _sync_rows)
            if cur.rowcount != len(_sync_rows):
                raise RuntimeError(
                    f"finance sync UPDATE rowcount mismatch: expected={len(_sync_rows)} affected={cur.rowcount}"
                )

        # P1-608: update_operating_income_and_ratioはEOD日次1回のため、その実行後に
        # 新決算が出ると次runではmarkerでskipされる。さらに直近YoY/利益加速も
        # predictor・三角安全スコアへ入るため、stale銘柄は毎runここでcurrent派生値を無効化する。
        if _stale_finance_codes and not _sc_codes.empty:
            _stale_raw_codes = [
                _rc for _rc in _sc_codes["コード"].tolist()
                if canonical_code_for_db(_rc) in _stale_finance_codes
            ]
            _stale_current_fin_cols = [
                _c for _c in ("営業利益", "営利対時価", "直近売上YoY", "直近営業益YoY", "利益加速フラグ")
                if _c in sc_cols
            ]
            if _stale_raw_codes and _stale_current_fin_cols:
                _set_null = ", ".join(f'"{_c}"=NULL' for _c in _stale_current_fin_cols)
                _stale_params = [(_rc,) for _rc in _stale_raw_codes]
                cur.executemany(
                    f"UPDATE screener SET {_set_null} WHERE コード=?",
                    _stale_params,
                )
                if cur.rowcount != len(_stale_params):
                    raise RuntimeError(
                        f"stale finance clear rowcount mismatch: expected={len(_stale_params)} affected={cur.rowcount}"
                    )

        if SYNC_HTML:
            # screener に html_path/updated_at カラムが無ければ追加
            cur.execute("PRAGMA table_info(screener)")
            sc_cols = {r[1] for r in cur.fetchall()}
            if "html_path"  not in sc_cols: cur.execute("ALTER TABLE screener ADD COLUMN html_path TEXT;")
            if "updated_at" not in sc_cols: cur.execute("ALTER TABLE screener ADD COLUMN updated_at TEXT;")
            # P1-297/P1-602: rawコード完全一致を廃止し、canonical alias間は
            # updated_at主で実際に最後に更新されたcurrent snapshotを採用する。
            _fh = pd.read_sql_query(
                "SELECT rowid AS _rowid, コード, html_path, updated_at FROM finance_notes",
                conn,
            )
            if not _fh.empty:
                _fh = _latest_finance_notes_by_canonical(_fh, "_key")
                _hmap = {r["_key"]: r for _, r in _fh.iterrows()}
                _html_rows = []
                for (_raw_code,) in cur.execute("SELECT コード FROM screener").fetchall():
                    _n = _hmap.get(canonical_code_for_db(_raw_code))
                    if _n is None:
                        continue
                    _html_rows.append((_n.get("html_path"), _n.get("updated_at"), _raw_code))
                if _html_rows:
                    # P1-630: finance_notes writerはout_html欠損時もhtml_path=''をauthoritativeに
                    # 上書きする。同期側だけCOALESCEすると前回レポートリンクが残るため、
                    # current正本の値（空文字/NULLを含む）をそのままmirrorする。
                    cur.executemany("""
                        UPDATE screener
                           SET html_path  = ?,
                               updated_at = ?
                         WHERE コード = ?
                    """, _html_rows)
                    if cur.rowcount != len(_html_rows):
                        raise RuntimeError(
                            f"finance HTML sync rowcount mismatch: expected={len(_html_rows)} affected={cur.rowcount}"
                        )

        cur.execute(f"RELEASE SAVEPOINT {_sync_sp}")
        print("[sync] screener <- finance_notes 同期完了")

    except Exception as e:
        try:
            cur.execute(f"ROLLBACK TO SAVEPOINT {_sync_sp}")
            cur.execute(f"RELEASE SAVEPOINT {_sync_sp}")
        except Exception:
            pass
        print(f"[sync][ERROR] {type(e).__name__}: {e}")
        raise
    finally:
        try: cur.close()
        except Exception: pass
        
def _calc_inago_heat_score(rs5, pct, rvol):
    """
    P1-18: 短期イナゴ過熱を 0-40 点で連続評価する。

    旧式は pct<0 / RVOL<1.2 / RVOL>2.0 で倍率が段差変化し、
    ごく小さな入力差で 15/30 点の表示閾値を飛び越えることがあった。
    新式は独立した3成分の加算にして不連続点を除去する。

      RS_5 相対強度 : 0-20点（+10%で上限）
      当日上昇率    : 0-12点（+5%で上限、下落日は0点）
      RVOL代金      : 0- 8点（2.0倍で上限）

    合計最大40点。配点 20/12/8 は暫定の設計値であり、統計学習済み係数ではない。
    """
    import numpy as np

    rs_arr = np.asarray(rs5, dtype=float)
    pct_arr = np.asarray(pct, dtype=float)
    rvol_arr = np.asarray(rvol, dtype=float)

    rs_heat = np.clip(rs_arr / 0.10, 0.0, 1.0) * 20.0
    day_heat = np.clip(pct_arr / 5.0, 0.0, 1.0) * 12.0
    vol_heat = np.clip(rvol_arr / 2.0, 0.0, 1.0) * 8.0

    return np.rint(np.clip(rs_heat + day_heat + vol_heat, 0.0, 40.0)).astype(int)


def _calc_institution_short_base_score(inst_short, avg_volume, float_shares, outstanding_shares):
    """
    P1-21: 機関空売りの「残高がある/ない」だけでなく量を0-10点で評価する。

    - 残高÷流通株式ベース（浮動株数優先、無ければ発行済株式数）
      1.5%で10点上限。
    - 残高÷平常出来高（20日平均代金から推定、無ければ当日出来高をfallback）
      2.0日分で10点上限。
    - 2軸の大きい方を採用。

    1.5% / 2.0日という閾値は暫定設計値で、統計学習済み係数ではない。
    比較分母が一切取れないが残高>0の場合は、旧式10点を捏造せず中立寄りの5点とする。
    """
    import numpy as np

    short = np.asarray(inst_short, dtype=float)
    avgv = np.asarray(avg_volume, dtype=float)
    flt = np.asarray(float_shares, dtype=float)
    out = np.asarray(outstanding_shares, dtype=float)

    share_base = np.where(flt > 0, flt, np.where(out > 0, out, np.nan))
    short_pct = np.where(share_base > 0, short / share_base * 100.0, np.nan)
    share_score = np.where(np.isfinite(short_pct), np.clip(short_pct / 1.5, 0.0, 1.0) * 10.0, np.nan)

    days = np.where(avgv > 0, short / avgv, np.nan)
    days_score = np.where(np.isfinite(days), np.clip(days / 2.0, 0.0, 1.0) * 10.0, np.nan)

    both_nan = ~np.isfinite(share_score) & ~np.isfinite(days_score)
    base = np.fmax(share_score, days_score)
    base = np.where(both_nan & (short > 0), 5.0, np.where(short <= 0, 0.0, base))
    return np.clip(base, 0.0, 10.0)


def _calc_institution_short_change_adjustment(inst_change, avg_volume, float_shares, outstanding_shares):
    """
    P1-22: 機関空売りの最新報告日の増減を量で評価する。

    売り増し: 0〜+15点
      - 増加株数が平常出来高の25%で上限
      - または流通株式ベースの0.25%で上限
    買い戻し: 0〜-5点
      - 同じ強度基準で最大5点の緩和

    25% / 0.25% は暫定設計値で、統計学習済み係数ではない。
    比較分母が取れない場合は、増減の符号だけで旧式+15/-5を再現せず0点とする。
    """
    import numpy as np

    chg = np.asarray(inst_change, dtype=float)
    avgv = np.asarray(avg_volume, dtype=float)
    flt = np.asarray(float_shares, dtype=float)
    out = np.asarray(outstanding_shares, dtype=float)

    share_base = np.where(flt > 0, flt, np.where(out > 0, out, np.nan))
    abs_chg = np.abs(chg)
    vol_intensity = np.where(avgv > 0, abs_chg / avgv / 0.25, np.nan)
    share_intensity = np.where(share_base > 0, (abs_chg / share_base * 100.0) / 0.25, np.nan)

    both_nan = ~np.isfinite(vol_intensity) & ~np.isfinite(share_intensity)
    intensity = np.fmax(vol_intensity, share_intensity)
    intensity = np.where(both_nan, 0.0, np.clip(intensity, 0.0, 1.0))

    positive = np.where(chg > 0, intensity * 15.0, 0.0)
    relief = np.where(chg < 0, intensity * 5.0, 0.0)
    return positive - relief


def _institution_change_freshness_weight(inst_date, ref_date):
    """
    P1-23: 機関空売り「増減」の鮮度係数。残高そのものには適用しない。

    - 報告後2日までは100%
    - 以降7日半減
    - 30日超は0
    - 日付不明/未来日は0

    2日・7日半減・30日打切りは暫定設計値であり、統計学習済みではない。
    """
    import numpy as np
    import pandas as pd

    inst = pd.to_datetime(inst_date, errors="coerce").dt.normalize()
    ref = pd.to_datetime(ref_date, errors="coerce").dt.normalize()
    age = (ref - inst).dt.days.astype(float)

    w = pd.Series(0.0, index=inst.index, dtype=float)
    valid = inst.notna() & ref.notna() & age.ge(0)
    w.loc[valid & age.le(2)] = 1.0
    mid = valid & age.gt(2) & age.le(30)
    w.loc[mid] = np.power(0.5, (age.loc[mid] - 2.0) / 7.0)
    return w.clip(0.0, 1.0)


def apply_risk_factors_labels(df: pd.DataFrame) -> pd.DataFrame:
    """過熱と需給リスクを3階層（イナゴ過熱、信用需給、機関売り）に分解して独立判定する"""
    if df is None or getattr(df, "empty", True):
        return df

    import numpy as np
    import pandas as pd

    # P1-550: 過熱3要素の欠損を0点=安全へ変換して「🟢静」と表示しない。
    # 信用/機関と同じく、入力が揃わない場合は評価不能として扱う。
    rs5_raw = pd.to_numeric(df["RS_5"], errors="coerce") if "RS_5" in df.columns else pd.Series(np.nan, index=df.index, dtype=float)
    pct_raw = pd.Series(np.nan, index=df.index, dtype=float)
    for _hc in ["前日終値比率_raw", "前日終値比率"]:
        if _hc in df.columns:
            pct_raw = pct_raw.combine_first(pd.to_numeric(df[_hc], errors="coerce"))
    rvol_raw = pd.Series(np.nan, index=df.index, dtype=float)
    # P1-657: writerless旧RVOL_売買代金はcurrent過熱判定へ混ぜない。
    for _hc in ["RVOL代金"]:
        if _hc in df.columns:
            rvol_raw = rvol_raw.combine_first(pd.to_numeric(df[_hc], errors="coerce"))
    heat_complete = rs5_raw.notna() & pct_raw.notna() & rvol_raw.notna()
    rs5 = rs5_raw.fillna(0.0)
    pct = pct_raw.fillna(0.0)
    rvol = rvol_raw.fillna(0.0)

    # P1-16: 信用倍率の欠損を「1倍=安全」と捏造しない。
    # 信用倍率0倍は（買い残0などで）実値として成立し得るため有効値として扱う。
    if "信用倍率" in df.columns:
        margin_ratio_raw = pd.to_numeric(df["信用倍率"], errors="coerce")
    else:
        margin_ratio_raw = pd.Series(np.nan, index=df.index, dtype=float)
    has_margin_ratio = margin_ratio_raw.notna() & (margin_ratio_raw >= 0)
    margin_ratio = margin_ratio_raw.where(has_margin_ratio)

    # P1-19: 「機関空売り0株」と「機関データなし」を分離する。
    if "機関空売り合計株数" in df.columns:
        inst_short_raw = pd.to_numeric(df["機関空売り合計株数"], errors="coerce")
    else:
        inst_short_raw = pd.Series(np.nan, index=df.index, dtype=float)
    if "本日の増減合計株数" in df.columns:
        inst_change_raw = pd.to_numeric(df["本日の増減合計株数"], errors="coerce")
    else:
        inst_change_raw = pd.Series(np.nan, index=df.index, dtype=float)

    # P3-45: 公開残高集計値がDBに残っていても、当日crawl失敗/未取得を
    # 「現在判定済み」にしない。当日成功+現在状態あり/なしだけが正式入力。
    if "機関空売り取得状態" in df.columns:
        inst_acquisition = df["機関空売り取得状態"].fillna("").astype(str).str.strip()
        inst_snapshot_success = inst_acquisition.eq("成功")
    else:
        # 互換経路: P3-45付与前の単体テストは数値列自体を証拠とする。
        inst_snapshot_success = pd.Series(True, index=df.index)
    if "機関空売り現在状態" in df.columns:
        inst_current_state = df["機関空売り現在状態"].fillna("不明").astype(str).str.strip()
        inst_current_known = inst_current_state.isin(["あり", "なし"])
        explicit_snapshot_none = inst_snapshot_success & inst_current_state.eq("なし")
        # 明示「現在なし」の時は過去報告日の増減を現在売圧に再利用しない。
        inst_short_raw = inst_short_raw.mask(explicit_snapshot_none, 0.0)
        inst_change_raw = inst_change_raw.mask(explicit_snapshot_none, 0.0)
    else:
        inst_current_known = pd.Series(True, index=df.index)

    # 更新日が存在する、または数値のどちらかが実在すれば「機関データあり」。
    # 再IN等では増減株数が空欄になることがあるため、両数値の完備は要求しない。
    if "空売り更新日" in df.columns:
        inst_date_text = df["空売り更新日"].astype(str).str.strip()
        has_inst_date = ~inst_date_text.str.lower().isin(["", "nan", "none", "n/a", "na", "-"])
    else:
        has_inst_date = pd.Series(False, index=df.index)
    inst_current_usable = inst_snapshot_success & inst_current_known
    has_inst_balance = inst_short_raw.notna() & inst_current_usable
    has_inst_change = inst_change_raw.notna() & inst_current_usable
    has_inst_record = has_inst_date | has_inst_balance | has_inst_change
    # P3-3: 日付文字列だけでは「残高0株」の証拠にならない。
    # 数値が1つも無い行はscore/labelとも不明。残高が無く増減だけある場合は部分情報として扱う。
    has_inst_quant = has_inst_balance | has_inst_change

    inst_short = inst_short_raw.fillna(0.0)
    inst_change = inst_change_raw.fillna(0.0)

    # ① 短期イナゴ過熱 (Max 40点)
    # P1-18: RS5・当日上昇率・RVOLを独立した連続成分として加算。
    # pct=0 / RVOL=1.2 / RVOL=2.0 の段差倍率を廃止し、微小入力差での判定ジャンプを防ぐ。
    score_heat = pd.Series(_calc_inago_heat_score(rs5, pct, rvol), index=df.index, dtype=float)
    score_heat = score_heat.where(heat_complete, np.nan)

    # ② 信用需給リスク (Max 35点)
    # 欠損は0点（安全）ではなく NaN（評価不能）を保持する。
    score_margin = pd.Series(
        np.clip(((margin_ratio - 3.0) / 17.0) * 35, 0, 35),
        index=df.index, dtype=float
    ).round()
    score_margin = score_margin.where(has_margin_ratio, np.nan)

    # ③ 機関売り圧力 (Max 25点)
    # P1-21: 残高あり=一律10点を廃止し、残高量を流通株比・平常出来高比で0-10点評価。
    current_price = pd.to_numeric(
        df["現在値"] if "現在値" in df.columns else pd.Series(np.nan, index=df.index),
        errors="coerce"
    )
    avg_turnover_oku = pd.to_numeric(
        df["売買代金20日平均億"] if "売買代金20日平均億" in df.columns else pd.Series(np.nan, index=df.index),
        errors="coerce"
    )
    current_volume = pd.to_numeric(
        df["出来高"] if "出来高" in df.columns else pd.Series(np.nan, index=df.index),
        errors="coerce"
    )
    avg_volume_est = (avg_turnover_oku * 100_000_000.0 / current_price).where(
        (avg_turnover_oku > 0) & (current_price > 0)
    )
    avg_volume = avg_volume_est.where(avg_volume_est > 0, current_volume)
    float_shares = pd.to_numeric(
        df["浮動株数"] if "浮動株数" in df.columns else pd.Series(np.nan, index=df.index),
        errors="coerce"
    )
    outstanding_shares = pd.to_numeric(
        df["発行済株式数"] if "発行済株式数" in df.columns else pd.Series(np.nan, index=df.index),
        errors="coerce"
    )
    base_inst = _calc_institution_short_base_score(
        inst_short, avg_volume, float_shares, outstanding_shares
    )
    # P1-22: 増減あり=一律+15/-5を廃止し、最新報告日の増減量を連続評価。
    add_inst_raw = _calc_institution_short_change_adjustment(
        inst_change, avg_volume, float_shares, outstanding_shares
    )

    # P1-23: 現在残高は持ち越すが、「最新報告日の売り増し/買い戻しの勢い」は鮮度減衰させる。
    # バックテスト時はシグナル更新日を優先し、無ければ日付/更新日、最後に実行日を使う。
    ref_date = pd.Series(pd.NaT, index=df.index, dtype="datetime64[ns]")
    _ref_resolved = pd.Series(False, index=df.index)
    for _ref_col in ("シグナル更新日", "日付", "更新日"):
        if _ref_col in df.columns:
            _cand_ref = pd.to_datetime(df[_ref_col], errors="coerce")
            _use = _cand_ref.notna() & (~_ref_resolved)
            ref_date.loc[_use] = _cand_ref.loc[_use].dt.normalize()
            _ref_resolved.loc[_use] = True
    ref_date = ref_date.fillna(pd.Timestamp(_today_jst()))
    inst_report_date = (
        df["空売り更新日"] if "空売り更新日" in df.columns
        else pd.Series(pd.NaT, index=df.index)
    )
    inst_change_freshness = _institution_change_freshness_weight(inst_report_date, ref_date)
    add_inst = add_inst_raw * inst_change_freshness.to_numpy(dtype=float)
    score_inst = pd.Series(
        np.clip(base_inst + add_inst, 0, 25),
        index=df.index, dtype=float
    ).round()
    # P1-19: 機関データ行そのものが無い銘柄は0点ではなく評価不能。
    score_inst = score_inst.where(has_inst_quant, np.nan)
    # P1-21補足: 現存ポジションが実在するのに丸めで0点になる矛盾を防ぐ。
    # ごく小さい残高は最低1点（低リスクだが「なし」ではない）。
    _has_positive_inst_short = has_inst_balance & (inst_short > 0)
    score_inst = score_inst.where(~_has_positive_inst_short | (score_inst >= 1), 1.0)

    # 生スコアの保存
    df["イナゴ過熱スコア"] = score_heat
    df["信用需給スコア"] = score_margin
    df["機関売りスコア"] = score_inst

    # ラベル付け
    lbl_heat = np.select(
        [~heat_complete, score_heat >= 30, score_heat >= 15],
        ["⚪不明", "🔥過熱", "🟡微熱"],
        default="🟢静"
    )
    lbl_margin = np.select(
        [~has_margin_ratio, score_margin >= 25, score_margin >= 15],
        ["⚪不明", "🚨需給悪", "🟡重め"],
        default="🟢正常"
    )
    has_current_inst_short = has_inst_balance & (inst_short > 0)
    # 残高が明示0なら「なし」。残高不明でも正の売増scoreがあればリスクあり、
    # 日付だけ/買戻しだけ/増減0だけで現在残高が分からない場合は「不明」。
    explicit_no_current_short = has_inst_balance & (inst_short <= 0)
    inst_risk_visible = has_current_inst_short | (score_inst > 0)
    lbl_inst = np.select(
        [~has_inst_quant, score_inst >= 20, inst_risk_visible, explicit_no_current_short],
        ["⚪不明", "🚨売増", "🟡空売有", "🟢なし"],
        default="⚪不明"
    )

    # 表示用テキスト生成
    heat_label_s = pd.Series(lbl_heat, index=df.index).astype(str)
    heat_score_text = df["イナゴ過熱スコア"].map(lambda x: f"{int(x)}点" if pd.notna(x) else "")
    df["イナゴ過熱判定"] = np.where(
        heat_complete,
        heat_label_s + " (" + heat_score_text + ")",
        "⚪不明"
    )
    margin_label_s = pd.Series(lbl_margin, index=df.index).astype(str)
    margin_score_text = df["信用需給スコア"].map(lambda x: f"{int(x)}点" if pd.notna(x) else "")
    df["信用需給判定"] = np.where(
        has_margin_ratio,
        margin_label_s + " (" + margin_score_text + ")",
        "⚪不明"
    )
    inst_label_s = pd.Series(lbl_inst, index=df.index).astype(str)
    inst_score_text = df["機関売りスコア"].map(lambda x: f"{int(x)}点" if pd.notna(x) else "")
    _inst_display_known = has_inst_quant & (explicit_no_current_short | inst_risk_visible)
    df["機関売り判定"] = np.where(
        _inst_display_known,
        inst_label_s + " (" + inst_score_text + ")",
        "⚪不明"
    )

    return df

def _log_model_quality_snapshot(rows_or_df) -> None:
    """P3-7: 最終スクリーニングsnapshotのモデル品質をログ監査する。

    判定値は変更しない。実運用DBで初めて見える欠損集中・定数化・cap張り付き・
    スコア同士のほぼ完全な重複を毎run可視化し、次の係数学習/整理に使う。
    """
    try:
        if rows_or_df is None:
            return
        dfq = rows_or_df.copy() if isinstance(rows_or_df, pd.DataFrame) else pd.DataFrame(list(rows_or_df))
        if dfq.empty:
            print("[model-quality] empty snapshot", flush=True)
            return

        def _num(col):
            if col not in dfq.columns:
                return pd.Series(np.nan, index=dfq.index, dtype=float)
            x = dfq[col]
            if x.dtype == object:
                x = x.astype(str).str.replace(',', '', regex=False).str.replace('%', '', regex=False).str.replace('％', '', regex=False)
            return pd.to_numeric(x, errors='coerce').replace([np.inf, -np.inf], np.nan)

        total = len(dfq)
        audit_cols = [
            '合成スコア', '初動スコア', 'INITIAL_MOMENTUM_SCORE', 'AIスコア', '予想インパクト_pct', '割安度',
            'tri_growth', 'tri_safety', 'tri_vol', 'イナゴ過熱スコア', '信用需給スコア', '機関売りスコア',
            '信用需給負荷スコア', '踏み上げ期待スコア', '上昇余地スコア', '右肩上がりスコア', '右肩早期スコア',
            '決算リアクション件数', '決算リアクションスコア', 'シンデン正式スコア', 'シンデン参考スコア', '次期転換期待スコア',
            '予想ギャップスコア', '未織り込みスコア',
            'RVOL代金', 'RS_5', 'RS_20', '直近売上YoY', '直近営業益YoY', '信用倍率', '機関空売り合計株数',
        ]
        print(f"[model-quality] rows={total}", flush=True)
        # P3-23: output行自体にlogical aliasが残れば、各順位側でdedupeしていても表示/LLM件数が水増しされる。
        if 'コード' in dfq.columns:
            _code_keys = dfq['コード'].map(canonical_code_for_db)
            _dup_mask = _code_keys.astype(bool) & _code_keys.duplicated(keep=False)
            if _dup_mask.any():
                _dups = sorted(set(_code_keys[_dup_mask].astype(str)))
                print(f"[model-quality][WARN] logical-code duplicates in final snapshot: count={len(_dups)} sample={_dups[:10]}", flush=True)
        numeric_cache = {}
        for col in audit_cols:
            if col not in dfq.columns:
                continue
            x = _num(col); numeric_cache[col] = x
            v = x.dropna(); n = len(v)
            if n == 0:
                print(f"[model-quality][MISS] {col}: valid=0/{total}", flush=True)
                continue
            qs = v.quantile([0.05, 0.5, 0.95])
            uniq = int(v.nunique(dropna=True))
            print(
                f"[model-quality] {col}: valid={n}/{total} ({n/total:.1%}) unique={uniq} "
                f"p05={qs.loc[0.05]:.4g} p50={qs.loc[0.5]:.4g} p95={qs.loc[0.95]:.4g}",
                flush=True,
            )
            if n >= 20 and uniq <= 2:
                print(f"[model-quality][WARN] {col}: near-constant (unique={uniq})", flush=True)

        # P3-47: 互換列の数値coverageを「正式評価coverage」と誤認しない。
        if 'シンデン評価区分' in dfq.columns:
            _sh_eval = dfq['シンデン評価区分'].fillna('不明').astype(str).str.strip().replace('', '不明')
            _sh_counts = {k: int((_sh_eval == k).sum()) for k in ('正式', '参考', '不明')}
            print(
                "[model-quality][shinden] "
                f"formal={_sh_counts['正式']}/{total} ({_sh_counts['正式']/total:.1%}) "
                f"reference={_sh_counts['参考']}/{total} ({_sh_counts['参考']/total:.1%}) "
                f"unknown={_sh_counts['不明']}/{total} ({_sh_counts['不明']/total:.1%})",
                flush=True,
            )
            _sh_formal = numeric_cache.get('シンデン正式スコア', pd.Series(np.nan, index=dfq.index))
            _sh_reference = numeric_cache.get('シンデン参考スコア', pd.Series(np.nan, index=dfq.index))
            _sh_bad = (
                ((_sh_eval == '正式') & _sh_formal.isna())
                | ((_sh_eval == '参考') & _sh_reference.isna())
                | ((_sh_eval == '正式') & _sh_reference.notna())
                | ((_sh_eval == '参考') & _sh_formal.notna())
            )
            if _sh_bad.any():
                print(
                    f"[model-quality][WARN] shinden formal/reference mismatch: count={int(_sh_bad.sum())}",
                    flush=True,
                )

        # P3-34: 主要な正式判定がほとんど計算不能なら、分布以前にproducer/入力coverageを直すべき。
        # optionalな決算反応等は対象外。閾値はモデル係数ではなく運用診断用。
        _core_coverage_cols = [
            '合成スコア', '初動スコア', 'AIスコア', '予想インパクト_pct',
            'tri_growth', 'tri_safety', 'tri_vol', '上昇余地スコア',
        ]
        for _cc in _core_coverage_cols:
            _cx = numeric_cache.get(_cc)
            if _cx is None:
                continue
            _valid_n = int(_cx.notna().sum())
            _rate = (_valid_n / total) if total else 0.0
            if _rate < 0.50:
                print(
                    f"[model-quality][SPARSE] {_cc}: valid={_valid_n}/{total} ({_rate:.1%}) < 50%",
                    flush=True,
                )

        # 明示capへ張り付く率。高率ならスコアが順位情報を失っている可能性が高い。
        caps = {
            '初動スコア': (95.0,), 'INITIAL_MOMENTUM_SCORE': (100.0,), '予想インパクト_pct': (-15.0, 30.0),
            'tri_growth': (100.0,), 'tri_safety': (0.0, 100.0), 'tri_vol': (100.0,),
            'イナゴ過熱スコア': (40.0,), '信用需給スコア': (35.0,), '機関売りスコア': (25.0,),
            '信用需給負荷スコア': (0.0, 100.0), '踏み上げ期待スコア': (0.0, 100.0), '上昇余地スコア': (0.0, 100.0),
            '右肩上がりスコア': (0.0, 100.0), '右肩早期スコア': (0.0, 100.0),
            '決算リアクションスコア': (0.0, 100.0),
        }
        for col, edges in caps.items():
            x = numeric_cache.get(col)
            if x is None:
                continue
            v = x.dropna()
            if len(v) < 20:
                continue
            for edge in edges:
                rate = float(np.isclose(v.to_numpy(dtype=float), edge, atol=1e-9).mean())
                if rate >= 0.20:
                    print(f"[model-quality][WARN] {col}: cap={edge:g} saturation={rate:.1%}", flush=True)

        # P3-24: 仕様レンジ外はcap率とは別にproducer/単位異常として即WARN。
        ranges = {
            '合成スコア': (0.0, 100.0), '初動スコア': (0.0, 95.0), 'INITIAL_MOMENTUM_SCORE': (0.0, 100.0), 'AIスコア': (0.0, 100.0),
            '予想インパクト_pct': (-15.0, 30.0), 'tri_growth': (0.0, 100.0),
            'tri_safety': (0.0, 100.0), 'tri_vol': (0.0, 100.0),
            'イナゴ過熱スコア': (0.0, 40.0), '信用需給スコア': (0.0, 35.0),
            '機関売りスコア': (0.0, 25.0), '信用需給負荷スコア': (0.0, 100.0),
            '踏み上げ期待スコア': (0.0, 100.0), '上昇余地スコア': (0.0, 100.0), '右肩上がりスコア': (0.0, 100.0),
            '右肩早期スコア': (0.0, 100.0), '決算リアクション件数': (0.0, 8.0),
            '決算リアクションスコア': (0.0, 100.0),
            'シンデン正式スコア': (0.0, 100.0), 'シンデン参考スコア': (0.0, 100.0),
        }
        for col, (lo, hi) in ranges.items():
            x = numeric_cache.get(col)
            if x is None:
                continue
            bad = x.notna() & ((x < lo) | (x > hi))
            if bad.any():
                vals = x[bad].head(5).tolist()
                print(f"[model-quality][WARN] {col}: out-of-range [{lo:g},{hi:g}] count={int(bad.sum())} sample={vals}", flush=True)

        # P3-30: 正式な決算反応スコアは最低3観測というproducer仕様とsnapshotを照合。
        if '決算リアクション件数' in numeric_cache and '決算リアクションスコア' in numeric_cache:
            _rn = numeric_cache['決算リアクション件数']
            _rs = numeric_cache['決算リアクションスコア']
            _bad_reaction = _rs.notna() & (_rn.isna() | (_rn < 3))
            if _bad_reaction.any():
                print(
                    f"[model-quality][WARN] 決算リアクションスコア with insufficient samples: count={int(_bad_reaction.sum())}",
                    flush=True,
                )

        # 主要スコア間のSpearmanが極端に高い場合、実質的な二重評価候補として可視化。
        corr_cols = [c for c in ['合成スコア','初動スコア','INITIAL_MOMENTUM_SCORE','AIスコア','予想インパクト_pct','tri_growth','上昇余地スコア','右肩上がりスコア','右肩早期スコア','決算リアクションスコア'] if c in numeric_cache]
        if len(corr_cols) >= 2:
            cm = pd.DataFrame({c: numeric_cache[c] for c in corr_cols}).corr(method='spearman', min_periods=20)
            for i, a in enumerate(corr_cols):
                for b in corr_cols[i+1:]:
                    try:
                        rho = float(cm.loc[a, b])
                    except Exception:
                        continue
                    if math.isfinite(rho) and abs(rho) >= 0.97:
                        print(f"[model-quality][WARN] high-correlation {a} vs {b}: rho={rho:.3f}", flush=True)
    except Exception as e:
        # 診断は本体の公開を止めない。ただし診断失敗自体は見えるようにする。
        print(f"[model-quality][WARN] audit failed: {type(e).__name__}: {e}", flush=True)


# P1-391: 後段の重複 _run_charts60 定義を削除（上段の正本だけを使用）。

def _fmt_hms(sec: float) -> str:
    """秒 → 'H時間M分S秒' 表記（Hが0でも0分を明示）"""
    try:
        total = int(round(sec))
    except Exception:
        try:
            total = int(sec)
        except Exception:
            total = 0
    h, rem = divmod(total, 3600)
    m, s = divmod(rem, 60)
    parts = []
    if h > 0:
        parts.append(f"{h}時間")
    parts.append(f"{m}分")
    parts.append(f"{s}秒")
    return "".join(parts)



def _timed(name, func, *args, **kwargs):
    """関数を実行し、正確な実行時間を計測してログに出力する"""
    print(f"[{name}] 実行開始...")
    
    # 【修正】最も精度が高く、システム時刻変更の影響を受けない perf_counter を使用
    t_start = time.perf_counter() 
    
    try:
        # 実際の処理を実行
        res = func(*args, **kwargs)
        return res
    finally:
        t_end = time.perf_counter()
        elapsed_sec = t_end - t_start
        
        # 見やすい形式に変換（例: 1分24秒）
        mins = int(elapsed_sec // 60)
        secs = int(elapsed_sec % 60)
        
        if mins > 0:
            print(f"[TIMER] {name}: {mins}分{secs}秒")
        else:
            print(f"[TIMER] {name}: {elapsed_sec:.2f}秒") # 1分未満は小数点まで出す

# ===== 実行モード判定ユーティリティ =====
def _is_trading_session_now(now_jst=None) -> bool:
    """現在がJPXの通常立会時間内かをJSTで判定する。

    P3-5: JPXカレンダー依存/設定障害を ``False``（立会外）へ変換しない。
    live/EOD経路の選択に効くため、判定不能は上位へ伝播させる。
    """
    now = _now_jst() if now_jst is None else now_jst
    if getattr(now, "tzinfo", None) is None:
        now = now.replace(tzinfo=_now_jst().tzinfo)
    if not _is_jp_business_day(now.date()):
        return False
    t = now.time().replace(tzinfo=None)
    return (dt_time(9, 0) <= t < dt_time(11, 30)) or (dt_time(12, 30) <= t < dt_time(15, 30))


def _auto_run_mode():
    """JPX営業日を PREOPEN / MIDDAY / EOD に分離する。

    P1-301: 15:25-15:30 はクロージング・オークション中なのでMIDDAY。
    P1-385: 営業日9:00前をEOD扱いすると、朝の実行で当日EOD日次markerを
    先に立て、引け後の本更新をスキップし得るためPREOPENへ分離。
    """
    if not AUTO_MODE:
        return RUN_SESSION.upper()

    now_jst = _now_jst()

    # 休場日は確定済みの直近履歴を扱うためEOD扱い。
    if not _is_jp_business_day(now_jst.date()):
        return "EOD"

    now_time = now_jst.time()
    if now_time < dt_time(9, 0):
        return "PREOPEN"
    if now_time < dt_time(15, 30):
        return "MIDDAY"
    return "EOD"


# ===== P3-41 external job / shared writer integration =====
def _external_job_state(job_name: str) -> dict:
    if not SYSTEM_JOB_STATE_DB.exists():
        return {}
    try:
        c = sqlite3.connect(str(SYSTEM_JOB_STATE_DB), timeout=3.0)
        try:
            row = c.execute("""
                SELECT last_started_at,last_finished_at,last_success_at,status,return_code,message
                FROM system_job_state WHERE job_name=?
            """, (job_name,)).fetchone()
        finally:
            c.close()
        if not row:
            return {}
        return dict(zip(["last_started_at","last_finished_at","last_success_at","status","return_code","message"], row))
    except Exception as e:
        print(f"[external-jobs][WARN] state read failed {job_name}: {e}")
        return {}


def _external_live_materials_ready(max_age_minutes: int = 45) -> tuple[bool, str]:
    st = _external_job_state("live_materials")
    status = str(st.get("status") or "")
    ts = st.get("last_finished_at") or st.get("last_success_at")
    if status not in {"success", "partial"} or not ts:
        return False, f"status={status or 'missing'}"
    try:
        dt = datetime.fromisoformat(str(ts))
        age = (datetime.now() - dt).total_seconds() / 60.0
    except Exception as e:
        return False, f"timestamp invalid: {e}"
    _mode_now = _auto_run_mode()
    _age_limit = 180 if _mode_now == "EOD" else max_age_minutes
    if age > _age_limit:
        return False, f"status={status} age={age:.1f}m limit={_age_limit}m"

    # P3-42: EODはlive_materialsがpartialでも通さない。Yahoo確定足のEOD専用job成功を要求。
    if _mode_now == "EOD":
        eod = _external_job_state("live_eod_finalize")
        if str(eod.get("status") or "") != "success":
            return False, f"EOD finalize status={eod.get('status') or 'missing'}"
        ets = eod.get("last_finished_at") or eod.get("last_success_at")
        try:
            edt = datetime.fromisoformat(str(ets))
        except Exception:
            return False, "EOD finalize timestamp missing/invalid"
        if edt.date() != datetime.now().date():
            return False, f"EOD finalize is not today: {edt.date()}"
    return True, f"status={status} age={age:.1f}m"


def _shared_writer_lock_set_child_pid(child_pid=None):
    try:
        if not SYSTEM_WRITER_LOCK.exists():
            return
        data = json.loads(SYSTEM_WRITER_LOCK.read_text(encoding="utf-8"))
        if int(data.get("pid") or -1) != os.getpid():
            return
        if child_pid is None:
            data.pop("child_pid", None); data.pop("child_started_at", None)
        else:
            data["child_pid"] = int(child_pid); data["child_started_at"] = datetime.now().isoformat(timespec="seconds")
        tmp = SYSTEM_WRITER_LOCK.with_suffix(SYSTEM_WRITER_LOCK.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
        os.replace(tmp, SYSTEM_WRITER_LOCK)
    except Exception:
        pass


def _acquire_shared_writer_lock(stale_hours: float = 6.0):
    SYSTEM_WRITER_LOCK.parent.mkdir(parents=True, exist_ok=True)
    try:
        fd = os.open(str(SYSTEM_WRITER_LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError:
        try:
            age = time.time() - SYSTEM_WRITER_LOCK.stat().st_mtime
        except Exception:
            age = 0.0
        if _json_lock_owner_is_stale(SYSTEM_WRITER_LOCK) or (not _json_lock_owner_is_alive(SYSTEM_WRITER_LOCK) and age > stale_hours * 3600):
            try: SYSTEM_WRITER_LOCK.unlink()
            except Exception: pass
            fd = os.open(str(SYSTEM_WRITER_LOCK), os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        else:
            return None
    payload = json.dumps({"owner":"auto_screening","pid":os.getpid(),"boot":_boot_token(),"at":datetime.now().isoformat(timespec="seconds")}, ensure_ascii=False)
    os.write(fd, payload.encode("utf-8")); os.close(fd)
    return str(SYSTEM_WRITER_LOCK)


def _shared_writer_lock_owner() -> str:
    """現在の共有lock所有者を返す。旧system_jobs lock（ownerなし）は外部producer扱い。"""
    try:
        data = json.loads(SYSTEM_WRITER_LOCK.read_text(encoding="utf-8"))
    except Exception:
        return ""
    return str(data.get("owner") or "").strip()


def _record_shared_writer_lock_event(event: str, detail: str = "") -> None:
    """P2-99: pythonw定期実行でも共有lockの判断だけを後から確認可能にする。"""
    try:
        SCREEN_LOCK_EVENT_LOG.parent.mkdir(parents=True, exist_ok=True)
        if SCREEN_LOCK_EVENT_LOG.exists() and SCREEN_LOCK_EVENT_LOG.stat().st_size >= 1_000_000:
            backup = SCREEN_LOCK_EVENT_LOG.with_suffix(SCREEN_LOCK_EVENT_LOG.suffix + ".1")
            try:
                if backup.exists():
                    backup.unlink()
                os.replace(SCREEN_LOCK_EVENT_LOG, backup)
            except FileNotFoundError:
                # 別processが同時にrotate済みなら、そのまま現行logへ追記する。
                pass
        suffix = f" {detail.strip()}" if detail and detail.strip() else ""
        line = f"{_now_jst().isoformat(timespec='seconds')} pid={os.getpid()} event={event}{suffix}\n"
        with SCREEN_LOCK_EVENT_LOG.open("a", encoding="utf-8") as fh:
            fh.write(line)
    except Exception as e:
        # 観測ログの失敗で本体や排他制御を止めない。
        print(f"[shared-writer-lock][WARN] event log failed: {e}")


def _acquire_shared_writer_lock_with_wait(
    max_wait_seconds: float = None,
    poll_seconds: float = 5.0,
):
    """P2-98: 外部producerとの同時刻起動だけを有界待機で吸収する。

    ``auto_screening`` が既に所有している場合は、手動二重起動や次triggerとの
    重複なので即skipする。ownerなしを含む外部producerは最大3分だけ待つ。
    待機中も既存の原子的O_EXCL取得とstale判定を繰り返し、lockを強制削除しない。
    """
    token = _acquire_shared_writer_lock()
    if token is not None:
        return token

    if _shared_writer_lock_owner() == "auto_screening":
        print("[shared-writer-lock] another screening run is active; duplicate run is skipped")
        _record_shared_writer_lock_event("duplicate_screening_skip")
        return None

    wait_seconds = SHARED_WRITER_WAIT_SECONDS if max_wait_seconds is None else max_wait_seconds
    try:
        wait_seconds = max(0.0, float(wait_seconds))
        poll_seconds = max(0.1, float(poll_seconds))
    except (TypeError, ValueError):
        wait_seconds = SHARED_WRITER_WAIT_SECONDS
        poll_seconds = 5.0

    if wait_seconds <= 0.0:
        print("[shared-writer-lock] external producer is writing; wait disabled, screening run is skipped")
        _record_shared_writer_lock_event("producer_wait_disabled_skip")
        return None

    started = time.monotonic()
    deadline = started + wait_seconds
    next_progress = 30.0
    print(f"[shared-writer-lock] external producer is writing; waiting up to {wait_seconds:.0f}s")
    _record_shared_writer_lock_event("producer_wait_start", f"max_wait={wait_seconds:.0f}s")

    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0.0:
            break
        time.sleep(min(poll_seconds, remaining))

        token = _acquire_shared_writer_lock()
        if token is not None:
            elapsed = time.monotonic() - started
            print(f"[shared-writer-lock] acquired after producer wait: elapsed={elapsed:.1f}s")
            _record_shared_writer_lock_event("producer_wait_acquired", f"elapsed={elapsed:.1f}s")
            return token

        # 待機中に別の本体が先に取得した場合、さらに待って同じ計算を重ねない。
        if _shared_writer_lock_owner() == "auto_screening":
            elapsed = time.monotonic() - started
            print(f"[shared-writer-lock] another screening run acquired the lock; duplicate run is skipped (elapsed={elapsed:.1f}s)")
            _record_shared_writer_lock_event("duplicate_screening_skip_after_wait", f"elapsed={elapsed:.1f}s")
            return None

        elapsed = time.monotonic() - started
        if elapsed >= next_progress:
            remaining_log = max(0.0, deadline - time.monotonic())
            print(f"[shared-writer-lock] still waiting: elapsed={elapsed:.0f}s remaining={remaining_log:.0f}s")
            next_progress += 30.0

    elapsed = time.monotonic() - started
    print(f"[shared-writer-lock] external producer still active after {elapsed:.0f}s; screening run is safely skipped")
    _record_shared_writer_lock_event("producer_wait_timeout_skip", f"elapsed={elapsed:.1f}s")
    return None


def _release_shared_writer_lock(token):
    if not token:
        return
    try:
        p=Path(token)
        if p.exists(): p.unlink()
    except Exception as e:
        print(f"[shared-writer-lock][WARN] release failed: {e}")


def _phase_preopen_price_rebuild(conn: sqlite3.Connection, codes) -> None:
    """P2-94: 前営業日の確定price snapshotから価格派生値を1回だけ再構築。

    PREOPEN中は価格入力が変化しない。このcomposite全体をdaily markerの
    成功単位にし、中途失敗で後続runがskipされることを防ぐ。
    """
    _timed("PREOPEN:_update_screener_from_history", _update_screener_from_history, conn, codes)
    _timed("PREOPEN:apply_auto_metrics_eod", apply_auto_metrics_eod, conn)
    _timed("PREOPEN:apply_composite_score", apply_composite_score, conn)
    _timed("PREOPEN:update_market_metrics", phase_update_market_metrics, conn)
    _timed("PREOPEN:update_margin_metrics", phase_update_margin_metrics, conn)
    pre_asof = _expected_snapshot_date_for_run("PREOPEN")
    pre_log_dt = datetime.combine(pre_asof, dt_time(15, 30), tzinfo=JST)
    _timed("PREOPEN:right_up_persistent", compute_right_up_persistent, conn, pre_asof, pre_log_dt, True)
    _timed("PREOPEN:right_up_early", compute_right_up_early_triggers, conn, pre_asof, pre_log_dt, True)
    _timed("PREOPEN:signal_detection", phase_signal_detection, conn)
    _timed("PREOPEN:snapshot_shodou_baseline", phase_snapshot_shodou_baseline, conn)
    _timed("PREOPEN:update_shodou_multipliers", phase_update_shodou_multipliers, conn)
    _timed("PREOPEN:apply_shodou_score", apply_shodou_score, conn)
    _timed("PREOPEN:update_since_dates", phase_update_since_dates, conn)


def _phase_common_price_derivatives(conn: sqlite3.Connection, label_prefix: str = "") -> None:
    """P2-94: 価格snapshotだけで決まる共通派生3phaseを一つの成功単位にする。"""
    prefix = f"{label_prefix}:" if label_prefix else ""
    _timed(f"{prefix}sync_latest_prices", phase_sync_latest_prices, conn)
    _timed(f"{prefix}shortterm_enhancements", phase_shortterm_enhancements, conn)
    _timed(f"{prefix}resistance_update", phase_resistance_update, conn)


# ==============================================================================
# P4-1 INITIAL_MOMENTUM Candidate Engine
# ==============================================================================
# 既存「初動」はテーマ等を含む複合モデルであり、baseline保存にも使われるため変更しない。
# このengineはユーザー原型の「出来高2倍 + 株価+2%以上」を独立Candidateとして保持する。
_P4_IM_VOL_GATE = 2.0
_P4_IM_RET_GATE_PCT = 2.0
_P4_IM_HISTORY_ROWS = 30
_P4_IM_LOW_PRICE_MAX = 300.0

_P4_IM_SCREEN_COLS = [
    ("INITIAL_MOMENTUM", "INTEGER"),
    ("INITIAL_MOMENTUM_SCORE", "REAL"),
    ("INITIAL_MOMENTUM_GRADE", "TEXT"),
    ("INITIAL_MOMENTUM_REASON", "TEXT"),
    ("INITIAL_MOMENTUM_SNAPSHOT", "TEXT"),
    ("初動出来高倍率20", "REAL"),
    ("初動代金倍率20", "REAL"),
    ("初動騰落率", "REAL"),
    ("初動終値位置", "REAL"),
    ("初動レンジ拡大率", "REAL"),
    ("初動20日高値更新", "INTEGER"),
    ("初動20日終値ブレイク", "INTEGER"),
    ("初動事前5日騰落率", "REAL"),
    ("初動事前20日騰落率", "REAL"),
    ("初動売買代金億", "REAL"),
    ("初動低位株タグ", "INTEGER"),
    ("初動データ状態", "TEXT"),
    ("初動計算基準日", "TEXT"),
]


def _p4_im_num(value):
    try:
        v = float(value)
        return v if math.isfinite(v) else None
    except Exception:
        return None


def _p4_im_pct(a, b):
    a = _p4_im_num(a); b = _p4_im_num(b)
    if a is None or b is None or b <= 0:
        return None
    return (a / b - 1.0) * 100.0


def _p4_im_is_non_stock(code, market, name) -> bool:
    c = canonical_code_for_db(code)
    if not re.fullmatch(r"(?:\d{4}|\d{3}[A-Z])", str(c or "")):
        return True
    text = f"{market or ''} {name or ''}".upper()
    blockers = ("ETF", "ETN", "上場投資信託", "投資信託", "インフラファンド", "REIT", "リート", "指数")
    return any(x.upper() in text for x in blockers)


def _p4_im_ensure_schema(conn: sqlite3.Connection) -> None:
    cols = {str(r[1]) for r in conn.execute("PRAGMA table_info(screener)").fetchall()}
    if "コード" not in cols:
        raise RuntimeError("P4-1 requires screener.コード")
    sp = f"p4_im_schema_{time.time_ns()}"
    try:
        conn.execute(f"SAVEPOINT {sp}")
        for col, dtype in _P4_IM_SCREEN_COLS:
            if col not in cols:
                conn.execute(f'ALTER TABLE screener ADD COLUMN "{col}" {dtype}')
                cols.add(col)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS candidate_signal_log (
                コード TEXT NOT NULL,
                シグナル日 TEXT NOT NULL,
                候補種別 TEXT NOT NULL,
                snapshot_kind TEXT NOT NULL,
                score REAL,
                grade TEXT,
                price REAL,
                ret1_pct REAL,
                volume_ratio20 REAL,
                turnover_ratio20 REAL,
                turnover_oku REAL,
                close_position_pct REAL,
                range_expansion REAL,
                breakout20 INTEGER,
                close_breakout20 INTEGER,
                pre5_ret_pct REAL,
                pre20_ret_pct REAL,
                low_price_tag INTEGER,
                reason TEXT,
                created_at TEXT NOT NULL,
                PRIMARY KEY (コード, シグナル日, 候補種別)
            )
        """)
        conn.execute("CREATE INDEX IF NOT EXISTS idx_candidate_signal_log_date ON candidate_signal_log(シグナル日, 候補種別)")
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        raise


def _p4_im_score(vol_ratio20, ret1_pct, close_pos, range_exp, breakout20,
                 close_breakout20, pre5, pre20, turnover_oku) -> float:
    """説明用暫定スコア。候補gateそのものには使わない。"""
    vr = max(0.0, float(vol_ratio20))
    rp = max(0.0, float(ret1_pct))
    cp = 50.0 if close_pos is None else max(0.0, min(100.0, float(close_pos)))
    rexp = 1.0 if range_exp is None else max(0.0, float(range_exp))
    p5 = 0.0 if pre5 is None else max(0.0, float(pre5))
    p20 = 0.0 if pre20 is None else max(0.0, float(pre20))
    turn = 0.0 if turnover_oku is None else max(0.0, float(turnover_oku))

    vol_pts = float(np.interp(min(max(vr, 1.0), 5.0), [1.0, 2.0, 5.0], [0.0, 15.0, 25.0]))
    px_pts = float(np.interp(min(max(rp, 0.0), 6.0), [0.0, 2.0, 6.0], [0.0, 10.0, 20.0]))
    close_pts = float(np.interp(min(max(cp, 50.0), 100.0), [50.0, 100.0], [0.0, 15.0]))
    range_pts = float(np.interp(min(max(rexp, 1.0), 2.0), [1.0, 2.0], [0.0, 10.0]))
    breakout_pts = 10.0 if close_breakout20 else (5.0 if breakout20 else 0.0)
    if p5 <= 8.0 and p20 <= 20.0:
        early_pts = 10.0
    elif p5 <= 15.0 and p20 <= 35.0:
        early_pts = 6.0
    elif p5 <= 25.0 and p20 <= 50.0:
        early_pts = 2.0
    else:
        early_pts = 0.0
    if turn >= 5.0:
        liq_pts = 10.0
    elif turn >= 1.0:
        liq_pts = 7.0
    elif turn >= 0.3:
        liq_pts = 4.0
    else:
        liq_pts = 0.0
    return round(float(np.clip(vol_pts + px_pts + close_pts + range_pts + breakout_pts + early_pts + liq_pts, 0.0, 100.0)), 1)


def _p4_im_grade(score):
    if score is None:
        return None
    if score >= 80: return "S"
    if score >= 70: return "A"
    if score >= 60: return "B"
    return "C"


def phase_initial_momentum_p4_1(conn: sqlite3.Connection):
    """P4-1: 独立INITIAL_MOMENTUMをcurrent screenerへ反映し、確定候補を履歴保存する。"""
    _p4_im_ensure_schema(conn)
    run_mode = str(_auto_run_mode() or "").upper()
    asof = _expected_snapshot_date_for_run(run_mode).isoformat()
    snapshot = "INTRADAY" if run_mode == "MIDDAY" else "EOD"

    screener = pd.read_sql_query(
        'SELECT rowid AS _srowid, コード AS _raw_code, 銘柄名 AS _name, 市場 AS _market FROM screener',
        conn,
    )
    if screener.empty:
        return {"asof": asof, "snapshot": snapshot, "eligible": 0, "candidates": 0, "not_evaluable": 0, "logged_eod": 0}
    screener["_code"] = screener["_raw_code"].map(canonical_code_for_db)
    screener["_excluded"] = [
        _p4_im_is_non_stock(c, m, n)
        for c, m, n in zip(screener["_code"], screener["_market"], screener["_name"])
    ]
    stock_codes = set(screener.loc[~screener["_excluded"], "_code"].dropna().astype(str))

    # 全銘柄を1SQL。raw aliasごとの直近30観測だけを取り、既存共通dedupeでlogical code×営業日へ統合する。
    hist = pd.read_sql_query("""
        WITH ranked AS (
            SELECT rowid AS _rowid, コード, 日付, 始値, 高値, 安値, 終値, 出来高,
                   ROW_NUMBER() OVER (
                       PARTITION BY コード
                       ORDER BY date(日付) DESC, rowid DESC
                   ) AS _rn
            FROM price_history
            WHERE date(日付) <= date(?)
        )
        SELECT _rowid, コード, 日付, 始値, 高値, 安値, 終値, 出来高
        FROM ranked
        WHERE _rn <= ?
        ORDER BY 日付, _rowid
    """, conn, params=(asof, _P4_IM_HISTORY_ROWS))

    if not hist.empty:
        hist = _dedupe_price_history_df(hist)
        hist["コード"] = hist["コード"].map(canonical_code_for_db)
        hist = hist[hist["コード"].isin(stock_codes)].copy()
        hist["日付"] = pd.to_datetime(hist["日付"], errors="coerce").dt.strftime("%Y-%m-%d")
        for c in ("始値", "高値", "安値", "終値", "出来高"):
            hist[c] = pd.to_numeric(hist[c], errors="coerce")
        hist = hist.dropna(subset=["コード", "日付"]).sort_values(["コード", "日付", "_rowid"], kind="stable")
        hist = hist.drop_duplicates(["コード", "日付"], keep="last")
    groups = {c: g.sort_values("日付", kind="stable") for c, g in hist.groupby("コード", sort=False)} if not hist.empty else {}

    results = {}
    for _, sr in screener.sort_values("_srowid").drop_duplicates("_code", keep="first").iterrows():
        code = str(sr["_code"] or "")
        if not code:
            continue
        base = {
            "candidate": None, "score": None, "grade": None,
            "reason": "", "vol_ratio20": None, "turnover_ratio20": None,
            "ret1": None, "close_pos": None, "range_exp": None,
            "breakout20": None, "close_breakout20": None,
            "pre5": None, "pre20": None, "turnover_oku": None,
            "low_tag": None, "state": "NO_HISTORY", "price": None,
        }
        if bool(sr["_excluded"]):
            base.update(state="EXCLUDED_NON_STOCK", reason="指数/ETF等のため対象外")
            results[code] = base
            continue
        g = groups.get(code)
        if g is None or g.empty:
            base.update(state="NO_HISTORY", reason="価格履歴なし")
            results[code] = base
            continue
        cur = g[g["日付"] == asof]
        if cur.empty:
            base.update(state="STALE", reason="基準日の価格行なし")
            results[code] = base
            continue
        r = cur.iloc[-1]
        before = g[g["日付"] < asof]
        prior20 = before.tail(20)
        if len(prior20) < 20:
            base.update(state="HISTORY_SHORT", reason=f"直前履歴不足 {len(prior20)}/20")
            results[code] = base
            continue

        close = _p4_im_num(r.get("終値")); volume = _p4_im_num(r.get("出来高"))
        prev_close = _p4_im_num(prior20.iloc[-1].get("終値"))
        pv = pd.to_numeric(prior20["出来高"], errors="coerce")
        pv = pv[np.isfinite(pv) & (pv >= 0)]
        if close is None or close <= 0 or volume is None or volume < 0 or prev_close is None or prev_close <= 0:
            base.update(state="INVALID", reason="終値/出来高/前日終値が不正")
            results[code] = base
            continue
        if len(pv) < 20 or float(pv.mean()) <= 0:
            base.update(state="INVALID_VOLUME_BASE", reason="直前20日出来高が計算不能")
            results[code] = base
            continue

        vol_ratio20 = float(volume / float(pv.mean()))
        ret1 = float((close / prev_close - 1.0) * 100.0)
        turnover = float(close * volume)
        turnover_oku = turnover / 1e8
        prior_turn = pd.to_numeric(prior20["終値"], errors="coerce") * pd.to_numeric(prior20["出来高"], errors="coerce")
        prior_turn = prior_turn[np.isfinite(prior_turn) & (prior_turn >= 0)]
        turnover_ratio20 = turnover / float(prior_turn.mean()) if len(prior_turn) >= 20 and float(prior_turn.mean()) > 0 else None

        high = _p4_im_num(r.get("高値")); low = _p4_im_num(r.get("安値"))
        close_pos = None
        if high is not None and low is not None and high >= low:
            close_pos = 50.0 if high == low else float(np.clip((close - low) / (high - low) * 100.0, 0.0, 100.0))

        range_exp = None
        if len(before) >= 21:
            tr_block = pd.concat([before.tail(21), cur.tail(1)], ignore_index=True)
            highs = pd.to_numeric(tr_block["高値"], errors="coerce")
            lows = pd.to_numeric(tr_block["安値"], errors="coerce")
            closes = pd.to_numeric(tr_block["終値"], errors="coerce")
            prevs = closes.shift(1)
            tr = pd.concat([(highs-lows).abs(), (highs-prevs).abs(), (lows-prevs).abs()], axis=1).max(axis=1)
            prev_tr20 = tr.iloc[1:-1].tail(20)
            if len(prev_tr20) == 20 and prev_tr20.notna().all() and np.isfinite(tr.iloc[-1]) and float(prev_tr20.mean()) > 0:
                range_exp = float(tr.iloc[-1] / float(prev_tr20.mean()))

        prior_high = pd.to_numeric(prior20["高値"], errors="coerce")
        max_prior_high = float(prior_high.max()) if prior_high.notna().any() else None
        breakout20 = int(high is not None and max_prior_high is not None and high > max_prior_high)
        close_breakout20 = int(max_prior_high is not None and close > max_prior_high)
        pre5 = _p4_im_pct(prev_close, before.iloc[-6].get("終値")) if len(before) >= 6 else None
        pre20 = _p4_im_pct(prev_close, before.iloc[-21].get("終値")) if len(before) >= 21 else None
        candidate = int(vol_ratio20 >= _P4_IM_VOL_GATE and ret1 >= _P4_IM_RET_GATE_PCT)
        score = _p4_im_score(vol_ratio20, ret1, close_pos, range_exp, breakout20,
                             close_breakout20, pre5, pre20, turnover_oku)
        grade = _p4_im_grade(score)
        low_tag = int(close <= _P4_IM_LOW_PRICE_MAX)
        detail = [f"出来高×{vol_ratio20:.2f}", f"前日比{ret1:+.2f}%"]
        if turnover_ratio20 is not None: detail.append(f"代金×{turnover_ratio20:.2f}")
        if close_pos is not None: detail.append(f"終値位置{close_pos:.0f}%")
        if close_breakout20: detail.append("20日高値を終値突破")
        elif breakout20: detail.append("20日高値更新")
        if low_tag: detail.append("低位株タグ")
        base.update(
            candidate=candidate, score=score, grade=grade, reason=" / ".join(detail),
            vol_ratio20=round(vol_ratio20, 3),
            turnover_ratio20=round(turnover_ratio20, 3) if turnover_ratio20 is not None else None,
            ret1=round(ret1, 3), close_pos=round(close_pos, 1) if close_pos is not None else None,
            range_exp=round(range_exp, 3) if range_exp is not None else None,
            breakout20=breakout20, close_breakout20=close_breakout20,
            pre5=round(pre5, 3) if pre5 is not None else None,
            pre20=round(pre20, 3) if pre20 is not None else None,
            turnover_oku=round(turnover_oku, 3), low_tag=low_tag,
            state="OK", price=close,
        )
        results[code] = base

    update_sql = """
        UPDATE screener SET
          INITIAL_MOMENTUM=?, INITIAL_MOMENTUM_SCORE=?, INITIAL_MOMENTUM_GRADE=?,
          INITIAL_MOMENTUM_REASON=?, INITIAL_MOMENTUM_SNAPSHOT=?,
          "初動出来高倍率20"=?, "初動代金倍率20"=?, "初動騰落率"=?, "初動終値位置"=?,
          "初動レンジ拡大率"=?, "初動20日高値更新"=?, "初動20日終値ブレイク"=?,
          "初動事前5日騰落率"=?, "初動事前20日騰落率"=?, "初動売買代金億"=?,
          "初動低位株タグ"=?, "初動データ状態"=?, "初動計算基準日"=?
        WHERE rowid=?
    """
    update_rows = []
    for _, sr in screener.iterrows():
        rr = results.get(str(sr["_code"] or ""))
        if rr is None:
            vals = [None, None, None, "コード正規化不能", snapshot, None, None, None, None, None, None, None, None, None, None, None, "INVALID_CODE", asof]
        else:
            vals = [
                rr["candidate"], rr["score"], rr["grade"], rr["reason"], snapshot,
                rr["vol_ratio20"], rr["turnover_ratio20"], rr["ret1"], rr["close_pos"], rr["range_exp"],
                rr["breakout20"], rr["close_breakout20"], rr["pre5"], rr["pre20"], rr["turnover_oku"],
                rr["low_tag"], rr["state"], asof,
            ]
        update_rows.append(tuple(vals + [int(sr["_srowid"])]))

    log_rows = []
    now_iso = _now_jst().isoformat(timespec="seconds")
    if snapshot == "EOD":
        for code, rr in results.items():
            if rr["state"] == "OK" and rr["candidate"] == 1:
                log_rows.append((
                    code, asof, "INITIAL_MOMENTUM", snapshot,
                    rr["score"], rr["grade"], rr["price"], rr["ret1"], rr["vol_ratio20"],
                    rr["turnover_ratio20"], rr["turnover_oku"], rr["close_pos"], rr["range_exp"],
                    rr["breakout20"], rr["close_breakout20"], rr["pre5"], rr["pre20"], rr["low_tag"],
                    rr["reason"], now_iso,
                ))

    sp = f"p4_im_apply_{time.time_ns()}"
    try:
        conn.execute(f"SAVEPOINT {sp}")
        if update_rows:
            conn.executemany(update_sql, update_rows)
        if log_rows:
            conn.executemany("""
                INSERT INTO candidate_signal_log(
                  コード, シグナル日, 候補種別, snapshot_kind, score, grade, price, ret1_pct,
                  volume_ratio20, turnover_ratio20, turnover_oku, close_position_pct, range_expansion,
                  breakout20, close_breakout20, pre5_ret_pct, pre20_ret_pct, low_price_tag, reason, created_at
                ) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                ON CONFLICT(コード, シグナル日, 候補種別) DO UPDATE SET
                  snapshot_kind=excluded.snapshot_kind, score=excluded.score, grade=excluded.grade,
                  price=excluded.price, ret1_pct=excluded.ret1_pct, volume_ratio20=excluded.volume_ratio20,
                  turnover_ratio20=excluded.turnover_ratio20, turnover_oku=excluded.turnover_oku,
                  close_position_pct=excluded.close_position_pct, range_expansion=excluded.range_expansion,
                  breakout20=excluded.breakout20, close_breakout20=excluded.close_breakout20,
                  pre5_ret_pct=excluded.pre5_ret_pct, pre20_ret_pct=excluded.pre20_ret_pct,
                  low_price_tag=excluded.low_price_tag, reason=excluded.reason, created_at=excluded.created_at
            """, log_rows)
        conn.execute(f"RELEASE SAVEPOINT {sp}")
    except Exception:
        try:
            conn.execute(f"ROLLBACK TO SAVEPOINT {sp}")
            conn.execute(f"RELEASE SAVEPOINT {sp}")
        except Exception:
            pass
        raise

    eligible = sum(1 for rr in results.values() if rr["state"] == "OK")
    candidates = sum(1 for rr in results.values() if rr["state"] == "OK" and rr["candidate"] == 1)
    not_evaluable = sum(1 for rr in results.values() if rr["state"] != "OK")
    low_candidates = sum(1 for rr in results.values() if rr["state"] == "OK" and rr["candidate"] == 1 and rr["low_tag"] == 1)
    summary = {
        "asof": asof, "snapshot": snapshot, "eligible": eligible, "candidates": candidates,
        "low_price_candidates": low_candidates, "not_evaluable": not_evaluable, "logged_eod": len(log_rows),
    }
    print(
        f"[P4-1][INITIAL_MOMENTUM] asof={asof} snapshot={snapshot} eligible={eligible} "
        f"candidates={candidates} low_price={low_candidates} not_evaluable={not_evaluable} logged_eod={len(log_rows)}",
        flush=True,
    )
    return summary

# ==============================================================================
# /P4-1 INITIAL_MOMENTUM Candidate Engine
# ==============================================================================


# ===== メイン処理 =====
def main():
    # P3-41: external producerはTask Scheduler/system_jobs.pyが担当。
    # 本体はDBを読むconsumer/計算・表示エンジンとして動く。
    t0 = time.time()
    print("=== 開始 ===")
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    if EXTERNAL_JOBS_REQUIRED and _auto_run_mode() in ("MIDDAY", "EOD"):
        _ready, _why = _external_live_materials_ready(45)
        if not _ready:
            print(f"[external-jobs] live_materials not ready: {_why} → stale publishを避けて今回runをskip")
            return
        print(f"[external-jobs] live_materials ready: {_why}")

    # (1) DB open & スキーマ保証
    conn = _get_db_conn()
    try:
        # ★ 追加：既存DB内の表記ゆれ（TOPIX）を強制的に ^TOPX に修正する
        # P1-379: autocommitで3テーブルの一部だけ移行済みになるのを防ぐ。
        _topix_sp = f"sp_topix_alias_{time.time_ns()}"
        conn.execute(f"SAVEPOINT {_topix_sp}")
        try:
            conn.execute("UPDATE screener SET コード = '^TOPX' WHERE コード IN ('^TOPIX', 'TOPIX', '998405.T')")
            conn.execute("UPDATE price_history SET コード = '^TOPX' WHERE コード IN ('^TOPIX', 'TOPIX', '998405.T')")
            conn.execute("UPDATE latest_prices SET コード = '^TOPX' WHERE コード IN ('^TOPIX', 'TOPIX', '998405.T')")
            conn.execute(f"RELEASE SAVEPOINT {_topix_sp}")
        except Exception as e:
            try:
                conn.execute(f"ROLLBACK TO SAVEPOINT {_topix_sp}")
                conn.execute(f"RELEASE SAVEPOINT {_topix_sp}")
            except Exception:
                pass
            # logical canonical helpers already tolerate old aliases; partial physical migration is worse.
            print("[fix_code][WARN] TOPIX alias migration rolled back:", e)

        ensure_runlog_schema(conn)

        # P1-548: screenerのlogical alias整理をCSV利用有無に依存させない。
        # 旧DBに 7203 / 7203.0 や 285A / 285a が共存したまま USE_CSV=False で起動すると、
        # export/ランキング母集団へ同一銘柄が2行入り得る。全体cleanupを起動時に1回行う。
        _screener_alias_changed = _cleanup_screener_logical_duplicates(conn)
        if _screener_alias_changed:
            print(f"[startup] screener logical aliases normalized: {_screener_alias_changed}")

        # P3-41: 株探ファンダ/TDnet/テーマ・信用は外部producerで更新済み。
        # 本体はfinance_notes等を同期して利用するだけ。

        # (3) CSV取り込み（コード・銘柄名・市場・登録日のみ）
        if USE_CSV:
            try:
                # CSVマスタ取り込み【同日スキップ】
                _timed_daily_once("phase_csv_import", phase_csv_import, conn)
            except Exception as e:
                # P1-343: CSVマスタは今回の銘柄ユニバースそのもの。失敗をWARNで握りつぶすと
                # 古いscreenerのまま「今回生成」のHTMLを公開してしまうためfail-fast。
                print("[csv-import][FATAL]", e)
                raise RuntimeError("CSV master import failed; refusing to publish a stale universe") from e

        # (4) 上場廃止/空売り無しの反映
        try:
            # 上場廃止や不要レコードの整理【同日スキップ】
            _timed_daily_once("phase_delist_cleanup", phase_delist_cleanup, conn, also_clean_notes=True)
        except Exception as e:
            # P1-344: 上場廃止マスタの反映失敗後に旧ユニバースを公開しない。
            print("[delist][FATAL]", e)
            raise RuntimeError("delist cleanup failed; refusing to publish an unvalidated universe") from e

        # P1-459: fetch_allで更新されたfinance_notesを、季節進捗・EPS等を使う
        # 後続計算より先にscreenerへ同期する。旧実装はHTML直前だったため、
        # update_seasonal_progressが前回のscreener.進捗率を使う一方、表示だけ
        # 今回の進捗率へ更新されるsnapshot不整合が起こり得た。
        _timed("sync_finance_comments_precalc", phase_sync_finance_comments, conn)

        try:
            _timed("phase_mark_karauri_nashi_db", phase_mark_karauri_nashi, conn)
        except Exception as e:
            _cleared = _invalidate_karauri_nashi_snapshot(conn)
            print(f"[karauri-flag][WARN] DB snapshot反映失敗: {e}; stale『なし』cleared={_cleared}")

        # (5) 実行モード決定
        RUN = _auto_run_mode()
        print(f"[AUTO_MODE={AUTO_MODE}] mode={RUN}")

        # (6) 処理対象銘柄
        # P1-214: main phaseへ渡す銘柄集合もcanonical化・重複除去。
        codes = list(dict.fromkeys(canonical_code_for_db(r[0]) for r in conn.execute("SELECT コード FROM screener").fetchall() if canonical_code_for_db(r[0])))
        if TEST_MODE:
            codes = codes[:TEST_LIMIT]
            print(f"[TEST] 対象 {len(codes)} 銘柄に制限")

        try:
            # P1-451/P1-598: MIDDAY/EODではテーマ取得を先に確定。PREOPENは外部取得せず、
            # DBにある直近テーマ情報だけで前営業日price snapshotの初動スコアを再構築する。
            if RUN in ("MIDDAY", "EOD"):
                _timed_daily_once("kabutan_theme_snapshot", phase_refresh_theme_snapshot, conn)

            if RUN == "MIDDAY":
                # ===== MIDDAYモード =====
                _timed("yahoo_intraday_snapshot", phase_yahoo_intraday_snapshot, conn)
                # P1-535: MIDDAY quote取得後、当日price_historyへ本当に到達したことを確認してから派生計算する。
                _require_current_price_history_snapshot(conn, RUN)
                # P1-573/P2-93: 右肩持続は当日価格も使うが、60日trendを10分ごとに
                # 全3500銘柄再計算する必要はない。右肩早期/初動は毎runのまま、持続版だけ30分間隔にする。
                _timed_interval_once(
                    "compute_right_up_persistent_midday", 30 * 60,
                    compute_right_up_persistent, conn, replace_log_day=True
                )
                # P1-574: 右肩早期は当日高値/出来高を直接使うため、MIDDAY runごとに再計算。
                # signals_logは同一営業日のsnapshotを置換してログ増殖を防ぐ。
                _timed("compute_right_up_early_triggers_midday", compute_right_up_early_triggers, conn, replace_log_day=True)
                # P2-93: 信用残履歴と登録日/営業日数は10分で変化しない。
                # 現在価依存分も最大30分の鮮度を保ち、新規シグナル系はこの後に毎run計算する。
                _timed_interval_once("update_margin_metrics_midday", 30 * 60, phase_update_margin_metrics, conn)
                _timed_interval_once("derive_update_midday", 30 * 60, phase_derive_update, conn)
                # ★ ここに追加！他のスコア計算よりも前にRSと地合いをDBへ書き込む
                _timed("update_market_metrics", phase_update_market_metrics, conn)
                _timed("signal_detection", phase_signal_detection, conn)
                # P1-111: 新規初動を検出した同じ実行内で基準価格/出来高を保存する。
                _timed("snapshot_shodou_baseline", phase_snapshot_shodou_baseline, conn)
                _timed("update_shodou_multipliers", phase_update_shodou_multipliers, conn)
                _timed("apply_shodou_score", apply_shodou_score, conn)
                _timed("update_since_dates", phase_update_since_dates, conn)

            elif RUN == "PREOPEN":
                # P1-385: 朝の起動では当日EOD日次markerを消費しない。
                # 外部取得は行わず、既存の確定履歴/基礎データだけで表示snapshotを再構築する。
                print("[PREOPEN] 9:00前のため外部価格取得/EOD日次markerはスキップ。前営業日の確定履歴で表示を再同期します")
                # P1-535: PREOPENは前営業日の確定履歴が無ければ古い派生値を再公開しない。
                _require_current_price_history_snapshot(conn, RUN)
                # P1-593〜P1-598/P2-94: 価格由来snapshot一式は前営業日履歴から
                # 決定的に再構築する。PREOPEN中は価格が変わらないため、同日・同buildで1回だけ実行。
                # finance/material同期とHTML出力は後段で引き続き毎run行う。
                _timed_daily_once("preopen_price_rebuild_v1", _phase_preopen_price_rebuild, conn, codes)

            else:
                # ===== EODモード =====
                # P3-42: 重いEOD確定処理はsystem_jobs.py -> eod_finalize.pyへ分離。
                # ここでは外部jobが確定させた当日price_historyを検証し、表示用の軽い共通処理へ進む。
                _require_current_price_history_snapshot(conn, RUN)
                print("[EOD] external eod_finalize already completed; heavy daily phases skipped in scanner")

            # P1-652: finance_notes同期後の採用営業利益を、価格snapshotが確定したこの地点で毎run再同期。
            # MIDDAYは当日marketCap、PREOPENは前営業日、EODは確定値と同じ時点へ揃える。
            # stale-after-earnings判定も関数内でfail-closedに効くため、旧営業利益の復活を許さない。
            _timed("refresh_operating_income_current", update_operating_income_and_ratio, conn)
                
            # (6.5) シグナル緩和・再判定【毎回】
            _timed("relax_rejudge_signals", relax_rejudge_signals, conn)

            # P3-41: 旧19時フルEODバッチは廃止。
            # EOD確定は15:30以降の通常EODフェーズで実データ到着を検証する。

            # P1-458/P3-41: 最新price_historyの最新price_historyをlatest_pricesへ同期してから、
            # ATR・since-signal・支持抵抗を毎回最終計算する。空売りCSVの件数/日次markerに依存させない。
            if RUN == "MIDDAY":
                _phase_common_price_derivatives(conn)
            else:
                # P2-94: PREOPEN/EODの価格snapshotは同日中不変。確定後の3phaseは
                # modeごと1回にし、finance/material変化時のHTML再出力は妨げない。
                _timed_daily_once(
                    f"{RUN.lower()}_price_derivatives_v1",
                    _phase_common_price_derivatives, conn, RUN
                )
        
            _timed("P4-1:initial_momentum", phase_initial_momentum_p4_1, conn)

            # P1-459: finance_notes同期は計算前へ移動済み。

            # P3-41: シンデン計算はsystem_jobs.pyの外部producerが担当。
            # screenerに保存済みの最新結果を読み、exporter側のfreshness判定でstale時のみmaskする。

            # (7.2) チャート生成
            try:
                _chart_codes_now = _timed(
                    "charts60_prepare",
                    _run_or_reuse_charts60,
                    conn, str(_SCRIPT_DIR / "charts60_make.py"), RUN
                )
                globals()["_CHARTS60_CURRENT_RUN_OK"] = True
                globals()["_CHARTS60_CURRENT_RUN_CODES"] = set(_chart_codes_now or set())
                print(f"[charts60] current-run generated codes={len(globals()['_CHARTS60_CURRENT_RUN_CODES'])}")
            except Exception as e:
                globals()["_CHARTS60_CURRENT_RUN_OK"] = False
                globals()["_CHARTS60_CURRENT_RUN_CODES"] = set()
                print(f"[charts60][WARN] {e}; stale chart links will be masked")

            # P4-2: generated artifact housekeeping. Daily-once keeps scan overhead negligible.
            # Current-run charts are explicitly protected from retention cleanup.
            try:
                _timed_daily_once(
                    "generated_artifact_housekeeping_v1",
                    _housekeeping_generated_artifacts,
                    set(globals().get("_CHARTS60_CURRENT_RUN_CODES") or set()),
                )
            except Exception as e:
                # Cleanup must never block screening/dashboard publication.
                print(f"[HOUSEKEEPING][WARN] generated artifact housekeeping failed: {e}")

            # (8) ダッシュボード出力
            html_path = os.path.join(OUTPUT_DIR, "index.html")
            try:
                _timed("export_html_dashboard", phase_export_html_dashboard_offline, conn, html_path)
            except Exception as e:
                print(f"[HTML-EXPORT][FATAL] HTML生成中に致命的なエラーが発生しました: {e}")
                raise
                

            # (9) ローカル表示
            #ok = False
            #try:
            #    ok = open_html_locally(
            #        r"H:\desctop\株攻略\1-スクリーニング自動化プログラム\main\output_data\index.html",
            #        cool_min=0,
            #        force=True
            #    )
            #except Exception as e:
            #    print("[open-html][WARN]", e)
            #
            #print("opened:", ok)

        finally:
            # サブ後片付けが必要ならここ（今回は pass）
            pass

    finally:
        # ← ここでの wait/stop/close はやらない（エントリポイントで1回だけ実行）
        pass

    print(f"実行時間： {_fmt_hms(time.time() - t0)}")
    print("=== 終了 ===")


# P1-390: スクリーナー本体全体をOSレベルで排他。
# 個別phase lockだけでは、fetch_all終了後のDB計算/HTML生成が別起動と重なり得る。
def _acquire_main_process_lock():
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    lock_path = os.path.join(OUTPUT_DIR, ".screening_main_process.lock")
    fh = open(lock_path, "a+b")
    try:
        fh.seek(0, os.SEEK_END)
        if fh.tell() == 0:
            fh.write(b"0")
            fh.flush()
        fh.seek(0)
        if os.name == "nt":
            import msvcrt
            try:
                msvcrt.locking(fh.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as e:
                raise RuntimeError("another screening process is already running") from e
            return (fh, "nt")
        else:
            import fcntl
            try:
                fcntl.flock(fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as e:
                raise RuntimeError("another screening process is already running") from e
            return (fh, "posix")
    except Exception:
        try: fh.close()
        except Exception: pass
        raise

def _release_main_process_lock(token):
    if not token:
        return
    fh, kind = token
    try:
        fh.seek(0)
        if kind == "nt":
            import msvcrt
            msvcrt.locking(fh.fileno(), msvcrt.LK_UNLCK, 1)
        else:
            import fcntl
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
    finally:
        try: fh.close()
        except Exception: pass

# ===== エントリーポイント =====



#

def v5_collect_data(conn, latest_table=_V5_LATEST_TABLE):
    # latest_prices に付与した V5 指標列を JSON 化して返す。
    # HTML は書かず、 phase_export_html_dashboard_offline で統合埋め込みする。
    # 依存: _v5_ensure_cols, _v5_update_latest, _v5_q, _V5_LATEST_TABLE
    _v5_ensure_cols(conn, latest_table)
    _v5_update_latest(conn, latest_table)
    rows = _v5_q(conn, f'''
        SELECT コード,
               Res_HH, Res_Zone, Res_Zone_Touches, Res_Zone_Last,
               Res_Round, Res_Round_Step, Res_Round_Near,
               Res_Line_Today, Res_Line_R2, Res_Nearest,
               Sup_LL, Sup_Zone, Sup_Zone_Touches, Sup_Zone_Last,
               Sup_Round, Sup_Round_Step, Sup_Round_Near,
               Sup_Line_Today, Sup_Line_R2, Sup_Nearest
        FROM {latest_table}
        ORDER BY コード
    ''')
    keys = ["コード","Res_HH","Res_Zone","Res_Zone_Touches","Res_Zone_Last",
            "Res_Round","Res_Round_Step","Res_Round_Near",
            "Res_Line_Today","Res_Line_R2","Res_Nearest",
            "Sup_LL","Sup_Zone","Sup_Zone_Touches","Sup_Zone_Last",
            "Sup_Round","Sup_Round_Step","Sup_Round_Near",
            "Sup_Line_Today","Sup_Line_R2","Sup_Nearest"]
    out = []
    for r in rows:
        d = {k: (None if isinstance(v, float) and (v != v) else v) for k, v in zip(keys, r)}
        out.append(d)
    return out





# === Light EOD Addons (3 functions) ===

# == Unified V5 data collector (HTML-free) ===


try:
    __has_main_guard = True
except Exception:
    __has_main_guard = False
# ==== [/INJECTED:MAIN_PHASE] ====
# ==== APPENDED PRE-EARNINGS FIX (TOP-LEVEL) ====


# ===== Single-File Plan A postprocess helpers (keep UI, inline DATA, kill only data.js) =====



# ===== End Plan A helpers =====

# P1-501: main呼出しは全top-level関数定義の後ろに置く。
# v5_collect_data等が旧位置ではmain実行時に未定義になり得た。
if __name__ == "__main__":
    _main_lock_token = None
    _shared_writer_token = None
    try:
        _shared_writer_token = _acquire_shared_writer_lock_with_wait()
        if _shared_writer_token is None:
            raise SystemExit(0)
        _main_lock_token = _acquire_main_process_lock()
        main()
    finally:
        try:
            _close_db_conn_safely()
        except Exception as _e:
            print("[db][WARN] _close_db_conn_safely:", _e)
        try:
            _release_main_process_lock(_main_lock_token)
        except Exception as _e:
            print("[main-lock][WARN] release:", _e)
        try:
            _release_shared_writer_lock(_shared_writer_token)
        except Exception as _e:
            print("[shared-writer-lock][WARN] release:", _e)
