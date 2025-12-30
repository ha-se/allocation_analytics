import streamlit as st
import pandas as pd
import pydeck as pdk
from snowflake.snowpark.context import get_active_session

# ページ設定
st.set_page_config(layout="wide", page_title="再配置データ高度分析")

st.title("🔍 再配置データ 高度スクリーニング＆分析")

# ---------------------------------------------------------
# 管理者設定: Git API Integration
# ---------------------------------------------------------
with st.expander("⚙️ 管理者設定: Git API Integration", expanded=False):
    st.markdown("### Git API Integrationの作成")
    st.info("GitHubリポジトリとSnowflakeを連携するためのAPI Integrationを作成します。\n"
            "※この操作にはACCOUNTADMINロールまたはCREATE INTEGRATION権限が必要です。")
    
    if st.button("🔗 Git API Integration を作成", type="primary"):
        try:
            session = get_active_session()
            sql = """
            CREATE OR REPLACE API INTEGRATION git_api_integration
              API_PROVIDER = git_https_api
              API_ALLOWED_PREFIXES = ('https://github.com/ha-se')
              ENABLED = TRUE;
            """
            session.sql(sql).collect()
            st.success("✅ API Integration 'git_api_integration' の作成に成功しました！")
        except Exception as e:
            st.error(f"❌ エラーが発生しました: {str(e)}")
    
    st.markdown("---")

# ---------------------------------------------------------
# 1. データ取得関数（メインデータ ＆ マスターデータ）
# ---------------------------------------------------------
@st.cache_data
def load_all_data():
    session = get_active_session()
    
    # (1) メインデータの取得
    df_main = session.table("DEMO_DB.SALES_SCHEMA.REALLOCATION_DATA").to_pandas()
    
    # (2) 除外用マスターデータの取得
    try:
        df_master = session.table("DEMO_DB.SALES_SCHEMA.REALLOCATION_MASTER").to_pandas()
    except:
        df_master = pd.DataFrame() # テーブルがない場合の保険
    
    # --- データ型変換（メインデータ） ---
    # 日付
    df_main["作成日時"] = pd.to_datetime(df_main["作成日時"], errors='coerce')
    
    # 数値（距離・IDなど）
    df_main["再配置距離(km)"] = pd.to_numeric(df_main["再配置距離(km)"], errors='coerce')
    df_main["Start Port Id"] = pd.to_numeric(df_main["Start Port Id"], errors='coerce')
    df_main["Return Port Id"] = pd.to_numeric(df_main["Return Port Id"], errors='coerce')

    # 地図用（緯度経度）
    df_main = df_main.rename(columns={"緯度_再配置先": "lat", "経度_再配置先": "lon"})
    df_main["lat"] = pd.to_numeric(df_main["lat"], errors='coerce')
    df_main["lon"] = pd.to_numeric(df_main["lon"], errors='coerce')
    
    # --- データ型変換（マスターデータ） ---
    if not df_master.empty and 'ST_ID' in df_master.columns:
        exclude_ids = df_master['ST_ID'].dropna().astype(int).tolist()
    else:
        exclude_ids = []

    # ========================================================
    # ★追加機能: フィルター用の「市区町村+行政区」列を作成
    # ========================================================
    # (A) 回収元
    if "回収元_市区町村+行政区" in df_main.columns:
        if "回収元_市区町村+行政区" in df_main.columns:
            # 2つの列を結合（例: 横浜市 + 中区 = 横浜市中区）※欠損値は空文字扱い
            df_main["_filter_回収元_詳細"] = df_main["回収元_市区町村+行政区"].fillna("") + df_main["回収元_市区町村+行政区"].fillna("")
        else:
            df_main["_filter_回収元_詳細"] = df_main["回収元_市区町村+行政区"]
    
    # (B) 再配置先
    if "再配置先_市区町村+行政区" in df_main.columns:
        if "再配置先_市区町村+行政区" in df_main.columns:
            df_main["_filter_再配置先_詳細"] = df_main["再配置先_市区町村+行政区"].fillna("") + df_main["再配置先_市区町村+行政区"].fillna("")
        else:
            df_main["_filter_再配置先_詳細"] = df_main["再配置先_市区町村+行政区"]

    return df_main, exclude_ids

# データの読み込み実行
raw_df, exclude_ids = load_all_data()


# ---------------------------------------------------------
# 2. クリーニング処理（自動スクリーニング）
# ---------------------------------------------------------
st.sidebar.header("🧹 自動クリーニング設定")
apply_cleaning = st.sidebar.checkbox("マスタ条件で除外処理を行う", value=True)

# ---------------------------------------------------------
# 2.5 色分け用CSV読み込み
# ---------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.header("🎨 地図色分け設定")
st.sidebar.caption("collection.csvとallocation.csvをアップロードすると、該当するデータを青色で表示します")

collection_file = st.sidebar.file_uploader("collection.csv", type=['csv'])
allocation_file = st.sidebar.file_uploader("allocation.csv", type=['csv'])

# CSVファイルが両方アップロードされている場合、St.IDのリストを取得
collection_st_ids = set()
allocation_st_ids = set()

if collection_file is not None:
    collection_df = pd.read_csv(collection_file)
    if 'St.ID' in collection_df.columns:
        collection_st_ids = set(collection_df['St.ID'].dropna().astype(str))
    st.sidebar.success(f"Collection: {len(collection_st_ids)} 件のSt.ID読込")

if allocation_file is not None:
    allocation_df = pd.read_csv(allocation_file)
    if 'St.ID' in allocation_df.columns:
        allocation_st_ids = set(allocation_df['St.ID'].dropna().astype(str))
    st.sidebar.success(f"Allocation: {len(allocation_st_ids)} 件のSt.ID読込")

# collectionから回収してallocationに再配置しているSt.IDの集合
matched_st_ids = collection_st_ids & allocation_st_ids
if matched_st_ids:
    st.sidebar.info(f"🔵 一致: {len(matched_st_ids)} 件（青色で表示）")

if apply_cleaning:
    count_before = len(raw_df)
    
    # (A) 一都三県（東京都・神奈川県・千葉県・埼玉県）のデータのみに限定
    # ※一都三県以外のデータは移動距離の計算精度に問題があるため除外
    target_prefectures = ['埼玉県', '千葉県', '神奈川県', '東京都']
    processed_df = raw_df.copy()
    
    # 回収元都道府県で一都三県のみを残す
    if '回収元都道府県' in processed_df.columns:
        processed_df = processed_df[processed_df['回収元都道府県'].isin(target_prefectures)]
    
    # 再配置先都道府県でも一都三県のみを残す
    if '再配置先都道府県' in processed_df.columns:
        processed_df = processed_df[processed_df['再配置先都道府県'].isin(target_prefectures)]

    # (B) 再配置マスターにあるST-IDを除外
    processed_df = processed_df[~processed_df['Start Port Id'].isin(exclude_ids)]
    processed_df = processed_df[~processed_df['Return Port Id'].isin(exclude_ids)]

    # (C) 再配置_FLAGの「同じST」「NA」を除外
    exclude_flags = ['同じST', 'NA']
    if '再配置_FLAG' in processed_df.columns:
        processed_df = processed_df[~processed_df['再配置_FLAG'].isin(exclude_flags)]
        processed_df = processed_df.dropna(subset=['再配置_FLAG'])
    
    count_after = len(processed_df)
    excluded_count = count_before - count_after
    st.sidebar.caption(f"✅ 一都三県外除外: {excluded_count} 件 / 残件数: {count_after} 件")

else:
    processed_df = raw_df.copy()


# ---------------------------------------------------------
# 3. サイドバー検索条件（ユーザー操作による絞り込み）
# ---------------------------------------------------------
st.sidebar.markdown("---")
st.sidebar.header("🛠 検索条件フィルター")

if processed_df.empty:
    st.error("表示できるデータがありません")
    st.stop()

# (A) 日付
if "作成日時" in processed_df.columns and processed_df["作成日時"].notnull().any():
    min_date = processed_df["作成日時"].min().date()
    max_date = processed_df["作成日時"].max().date()
    date_range = st.sidebar.date_input("日付範囲", value=(min_date, max_date), min_value=min_date, max_value=max_date)
else:
    st.stop()

# (B) 都道府県 (再配置先)
all_prefs = processed_df["再配置先都道府県"].unique()
selected_prefs = st.sidebar.multiselect("再配置先 都道府県", all_prefs, default=all_prefs)

# --- ★追加: 市区町村+行政区フィルター ---

# (C) 回収元_市区町村+行政区
if "_filter_回収元_詳細" in processed_df.columns:
    all_start_cities = sorted(processed_df["_filter_回収元_詳細"].dropna().unique())
    selected_start_cities = st.sidebar.multiselect("回収元 市区町村・行政区", all_start_cities, default=all_start_cities)
else:
    selected_start_cities = []
    st.sidebar.caption("※回収元市区町村データなし")

# (D) 再配置先_市区町村+行政区
if "_filter_再配置先_詳細" in processed_df.columns:
    all_end_cities = sorted(processed_df["_filter_再配置先_詳細"].dropna().unique())
    selected_end_cities = st.sidebar.multiselect("再配置先 市区町村・行政区", all_end_cities, default=all_end_cities)
else:
    selected_end_cities = []
    st.sidebar.caption("※再配置先_市区町村+行政区データなし")

# ----------------------------------------

# (E) 距離
max_dist = float(processed_df["再配置距離(km)"].max()) if not processed_df.empty else 100.0
dist_range = st.sidebar.slider("再配置距離 (km)", 0.0, max_dist, (0.0, max_dist))

# (F) 表示名
all_names = processed_df["表示名"].unique()
selected_names = st.sidebar.multiselect("表示名 (PT企業)", all_names, default=all_names)

# (G) 自転車所有企業
if "自転車所有企業" in processed_df.columns:
    all_owners = processed_df["自転車所有企業"].unique()
    selected_owners = st.sidebar.multiselect("自転車所有企業", all_owners, default=all_owners)
else:
    selected_owners = []

# (H) バイクカテゴリ
if "バイクカテゴリ" in processed_df.columns:
    all_categories = processed_df["バイクカテゴリ"].unique()
    selected_categories = st.sidebar.multiselect("バイクカテゴリ", all_categories, default=all_categories)
else:
    selected_categories = []


# ---------------------------------------------------------
# 4. 最終絞り込み実行
# ---------------------------------------------------------
if len(date_range) != 2:
    st.stop()

# 基本フィルター
final_df = processed_df[
    (processed_df["作成日時"].dt.date >= date_range[0]) &
    (processed_df["作成日時"].dt.date <= date_range[1]) &
    (processed_df["再配置先都道府県"].isin(selected_prefs)) &
    (processed_df["再配置距離(km)"] >= dist_range[0]) &
    (processed_df["再配置距離(km)"] <= dist_range[1]) &
    (processed_df["表示名"].isin(selected_names))
]

# ★追加フィルターの適用
if "_filter_回収元_詳細" in processed_df.columns:
    final_df = final_df[final_df["_filter_回収元_詳細"].isin(selected_start_cities)]

if "_filter_再配置先_詳細" in processed_df.columns:
    final_df = final_df[final_df["_filter_再配置先_詳細"].isin(selected_end_cities)]

# その他のフィルター
if "自転車所有企業" in processed_df.columns:
    final_df = final_df[final_df["自転車所有企業"].isin(selected_owners)]

if "バイクカテゴリ" in processed_df.columns:
    final_df = final_df[final_df["バイクカテゴリ"].isin(selected_categories)]


# ---------------------------------------------------------
# 5. 結果表示
# ---------------------------------------------------------
col1, col2, col3 = st.columns(3)
col1.metric("該当件数", f"{len(final_df)} 件")
mean_dist = final_df['再配置距離(km)'].mean() if not final_df.empty else 0
col2.metric("平均移動距離", f"{mean_dist:.2f} km")
max_dist_val = final_df['再配置距離(km)'].max() if not final_df.empty else 0
col3.metric("最大移動距離", f"{max_dist_val:.2f} km")

tab1, tab2 = st.tabs(["🗺️ 地図で確認", "📋 データ一覧＆ダウンロード"])

with tab1:
    if not final_df.empty:
        # 地図データの準備
        map_data = final_df.dropna(subset=["lat", "lon"]).copy()
        
        # 色分け判定: Return Port Idがmatched_st_idsに含まれる場合は青、それ以外は赤
        if matched_st_ids and 'Return Port Id' in map_data.columns:
            map_data['color'] = map_data['Return Port Id'].astype(str).apply(
                lambda x: [0, 0, 255, 200] if x in matched_st_ids else [255, 0, 0, 200]
            )
            st.caption("🔴 赤: 通常の再配置 | 🔵 青: collection→allocationの再配置")
        else:
            # デフォルトは全て赤
            map_data['color'] = [[255, 0, 0, 200]] * len(map_data)
        
        # pydeckで地図表示
        view_state = pdk.ViewState(
            latitude=map_data['lat'].mean(),
            longitude=map_data['lon'].mean(),
            zoom=10,
            pitch=0
        )
        
        layer = pdk.Layer(
            "ScatterplotLayer",
            data=map_data,
            get_position=["lon", "lat"],
            get_color="color",
            get_radius=100,
            pickable=True,
            auto_highlight=True,
        )
        
        tooltip = {
            "html": "<b>表示名:</b> {表示名}<br/>"
                    "<b>再配置先:</b> {再配置先都道府県}<br/>"
                    "<b>距離:</b> {再配置距離(km)} km<br/>"
                    "<b>St.ID:</b> {Start Port Id} → {Return Port Id}",
            "style": {"backgroundColor": "steelblue", "color": "white"}
        }
        
        deck = pdk.Deck(
            layers=[layer],
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style="mapbox://styles/mapbox/light-v9"
        )
        
        st.pydeck_chart(deck)
    else:
        st.warning("条件に一致するデータがありません")

with tab2:
    if not final_df.empty:
        # ダウンロード用に一時列（_filter_...）は削除しても良いですが、確認用にあえて残しています
        csv = final_df.to_csv(index=False).encode('utf-8_sig')
        st.download_button(
            label="📥 CSVダウンロード",
            data=csv,
            file_name="filtered_reallocation_data.csv",
            mime="text/csv",
        )
    st.dataframe(final_df, use_container_width=True)