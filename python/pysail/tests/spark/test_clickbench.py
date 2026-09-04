from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

_CLICKBENCH_SCHEMA = pa.schema(
    [
        pa.field("WatchID", pa.int64(), nullable=False),
        pa.field("JavaEnable", pa.int16(), nullable=False),
        pa.field("Title", pa.string(), nullable=False),
        pa.field("GoodEvent", pa.int16(), nullable=False),
        pa.field("EventTime", pa.int64(), nullable=False),
        pa.field("EventDate", pa.uint16(), nullable=False),
        pa.field("CounterID", pa.int32(), nullable=False),
        pa.field("ClientIP", pa.int32(), nullable=False),
        pa.field("RegionID", pa.int32(), nullable=False),
        pa.field("UserID", pa.int64(), nullable=False),
        pa.field("CounterClass", pa.int16(), nullable=False),
        pa.field("OS", pa.int16(), nullable=False),
        pa.field("UserAgent", pa.int16(), nullable=False),
        pa.field("URL", pa.string(), nullable=False),
        pa.field("Referer", pa.string(), nullable=False),
        pa.field("IsRefresh", pa.int16(), nullable=False),
        pa.field("RefererCategoryID", pa.int16(), nullable=False),
        pa.field("RefererRegionID", pa.int32(), nullable=False),
        pa.field("URLCategoryID", pa.int16(), nullable=False),
        pa.field("URLRegionID", pa.int32(), nullable=False),
        pa.field("ResolutionWidth", pa.int16(), nullable=False),
        pa.field("ResolutionHeight", pa.int16(), nullable=False),
        pa.field("ResolutionDepth", pa.int16(), nullable=False),
        pa.field("FlashMajor", pa.int16(), nullable=False),
        pa.field("FlashMinor", pa.int16(), nullable=False),
        pa.field("FlashMinor2", pa.string(), nullable=False),
        pa.field("NetMajor", pa.int16(), nullable=False),
        pa.field("NetMinor", pa.int16(), nullable=False),
        pa.field("UserAgentMajor", pa.int16(), nullable=False),
        pa.field("UserAgentMinor", pa.string(), nullable=False),
        pa.field("CookieEnable", pa.int16(), nullable=False),
        pa.field("JavascriptEnable", pa.int16(), nullable=False),
        pa.field("IsMobile", pa.int16(), nullable=False),
        pa.field("MobilePhone", pa.int16(), nullable=False),
        pa.field("MobilePhoneModel", pa.string(), nullable=False),
        pa.field("Params", pa.string(), nullable=False),
        pa.field("IPNetworkID", pa.int32(), nullable=False),
        pa.field("TraficSourceID", pa.int16(), nullable=False),
        pa.field("SearchEngineID", pa.int16(), nullable=False),
        pa.field("SearchPhrase", pa.string(), nullable=False),
        pa.field("AdvEngineID", pa.int16(), nullable=False),
        pa.field("IsArtifical", pa.int16(), nullable=False),
        pa.field("WindowClientWidth", pa.int16(), nullable=False),
        pa.field("WindowClientHeight", pa.int16(), nullable=False),
        pa.field("ClientTimeZone", pa.int16(), nullable=False),
        pa.field("ClientEventTime", pa.int64(), nullable=False),
        pa.field("SilverlightVersion1", pa.int16(), nullable=False),
        pa.field("SilverlightVersion2", pa.int16(), nullable=False),
        pa.field("SilverlightVersion3", pa.int32(), nullable=False),
        pa.field("SilverlightVersion4", pa.int16(), nullable=False),
        pa.field("PageCharset", pa.string(), nullable=False),
        pa.field("CodeVersion", pa.int32(), nullable=False),
        pa.field("IsLink", pa.int16(), nullable=False),
        pa.field("IsDownload", pa.int16(), nullable=False),
        pa.field("IsNotBounce", pa.int16(), nullable=False),
        pa.field("FUniqID", pa.int64(), nullable=False),
        pa.field("OriginalURL", pa.string(), nullable=False),
        pa.field("HID", pa.int32(), nullable=False),
        pa.field("IsOldCounter", pa.int16(), nullable=False),
        pa.field("IsEvent", pa.int16(), nullable=False),
        pa.field("IsParameter", pa.int16(), nullable=False),
        pa.field("DontCountHits", pa.int16(), nullable=False),
        pa.field("WithHash", pa.int16(), nullable=False),
        pa.field("HitColor", pa.string(), nullable=False),
        pa.field("LocalEventTime", pa.int64(), nullable=False),
        pa.field("Age", pa.int16(), nullable=False),
        pa.field("Sex", pa.int16(), nullable=False),
        pa.field("Income", pa.int16(), nullable=False),
        pa.field("Interests", pa.int16(), nullable=False),
        pa.field("Robotness", pa.int16(), nullable=False),
        pa.field("RemoteIP", pa.int32(), nullable=False),
        pa.field("WindowName", pa.int32(), nullable=False),
        pa.field("OpenerName", pa.int32(), nullable=False),
        pa.field("HistoryLength", pa.int16(), nullable=False),
        pa.field("BrowserLanguage", pa.string(), nullable=False),
        pa.field("BrowserCountry", pa.string(), nullable=False),
        pa.field("SocialNetwork", pa.string(), nullable=False),
        pa.field("SocialAction", pa.string(), nullable=False),
        pa.field("HTTPError", pa.int16(), nullable=False),
        pa.field("SendTiming", pa.int32(), nullable=False),
        pa.field("DNSTiming", pa.int32(), nullable=False),
        pa.field("ConnectTiming", pa.int32(), nullable=False),
        pa.field("ResponseStartTiming", pa.int32(), nullable=False),
        pa.field("ResponseEndTiming", pa.int32(), nullable=False),
        pa.field("FetchTiming", pa.int32(), nullable=False),
        pa.field("SocialSourceNetworkID", pa.int16(), nullable=False),
        pa.field("SocialSourcePage", pa.string(), nullable=False),
        pa.field("ParamPrice", pa.int64(), nullable=False),
        pa.field("ParamOrderID", pa.string(), nullable=False),
        pa.field("ParamCurrency", pa.string(), nullable=False),
        pa.field("ParamCurrencyID", pa.int16(), nullable=False),
        pa.field("OpenstatServiceName", pa.string(), nullable=False),
        pa.field("OpenstatCampaignID", pa.string(), nullable=False),
        pa.field("OpenstatAdID", pa.string(), nullable=False),
        pa.field("OpenstatSourceID", pa.string(), nullable=False),
        pa.field("UTMSource", pa.string(), nullable=False),
        pa.field("UTMMedium", pa.string(), nullable=False),
        pa.field("UTMCampaign", pa.string(), nullable=False),
        pa.field("UTMContent", pa.string(), nullable=False),
        pa.field("UTMTerm", pa.string(), nullable=False),
        pa.field("FromTag", pa.string(), nullable=False),
        pa.field("HasGCLID", pa.int16(), nullable=False),
        pa.field("RefererHash", pa.int64(), nullable=False),
        pa.field("URLHash", pa.int64(), nullable=False),
        pa.field("CLID", pa.int32(), nullable=False),
    ]
)

# Keep the fixture larger than one execution batch and align its files with the
# test parallelism so the plans retain multi-partition operators.
_CLICKBENCH_SEED_ROW_COUNT = 10_000
_CLICKBENCH_DATA_FILE_COUNT = 4
# Repeat these predicate-relevant values in every file so Parquet statistics do
# not prune ClickBench filters before the distributed plan is constructed.
_CLICKBENCH_SEED_PATTERNS = {
    "WatchID": (1_000_000, 1_000_001, 1_000_002, 1_000_003),
    "Title": ("Google Search", "Example article", "Google News", ""),
    "EventTime": (1_372_550_400, 1_372_636_800, 1_373_760_000, 1_375_315_200),
    "EventDate": (15_888, 15_900, 15_901, 15_917),
    "CounterID": (7, 62, 62, 91),
    "ClientIP": (134_744_072, 167_772_161, 167_772_162, 2_130_706_433),
    "RegionID": (1, 2, 1, 3),
    "UserID": (435_090_932_899_640_449, 101, 102, 101),
    "AdvEngineID": (0, 1, 2, 0),
    "URL": (
        "https://www.google.com/search?q=sail",
        "https://example.com/google/result",
        "https://news.example.org/article",
        "",
    ),
    "Referer": (
        "https://www.google.com/search?q=sail",
        "https://example.com/home",
        "https://news.example.org/",
        "",
    ),
    "IsRefresh": (0, 0, 1, 0),
    "ResolutionWidth": (375, 1_366, 1_920, 2_560),
    "ResolutionHeight": (667, 768, 1_080, 1_440),
    "MobilePhone": (0, 1, 2, 0),
    "MobilePhoneModel": ("", "iPhone", "Pixel", ""),
    "TraficSourceID": (-1, 6, 1, 6),
    "SearchEngineID": (0, 1, 2, 0),
    "SearchPhrase": ("", "sail query", "rust datafusion", "clickbench"),
    "WindowClientWidth": (375, 1_280, 1_920, 2_560),
    "WindowClientHeight": (667, 720, 1_080, 1_440),
    "IsLink": (1, 0, 1, 1),
    "IsDownload": (0, 0, 1, 0),
    "DontCountHits": (0, 0, 1, 0),
    "RefererHash": (3_594_120_000_172_545_465, 1, 3_594_120_000_172_545_465, 2),
    "URLHash": (2_868_770_270_353_813_622, 1, 2, 2_868_770_270_353_813_622),
}


def _clickbench_seed_table():
    columns = []
    for field in _CLICKBENCH_SCHEMA:
        pattern = _CLICKBENCH_SEED_PATTERNS.get(field.name)
        if pattern is None:
            pattern = ("seed",) if pa.types.is_string(field.type) else (1,)
        values = [pattern[row % len(pattern)] for row in range(_CLICKBENCH_SEED_ROW_COUNT)]
        columns.append(pa.array(values, type=field.type))
    return pa.Table.from_arrays(columns, schema=_CLICKBENCH_SCHEMA)


@pytest.fixture(scope="module", autouse=True)
def data(spark, tmp_path_factory):
    tmp_dir = tmp_path_factory.mktemp("clickbench")
    table = _clickbench_seed_table()
    rows_per_file = _CLICKBENCH_SEED_ROW_COUNT // _CLICKBENCH_DATA_FILE_COUNT
    for file_index in range(_CLICKBENCH_DATA_FILE_COUNT):
        file_table = table.slice(file_index * rows_per_file, rows_per_file)
        pq.write_table(file_table, tmp_dir / f"hits-{file_index:02}.parquet")

    df = spark.read.parquet(str(tmp_dir))
    df = df.withColumn("EventTime", F.col("EventTime").cast("timestamp"))
    df = df.withColumn("EventDate", F.col("EventDate").cast("int").cast("date"))
    df.createOrReplaceTempView("hits")
    yield
    spark.catalog.dropTempView("hits")


@pytest.fixture(scope="module")
def queries():
    path = Path(__file__).parent.parent.parent / "data" / "clickbench" / "queries.sql"
    with open(path) as f:
        yield [x.strip() for x in f]


# ClickBench query ID is zero-based.
@pytest.mark.parametrize("q", list(range(43)), ids=[f"{x:02}" for x in range(43)])
@pytest.mark.skipif(is_jvm_spark(), reason="different plans in JVM Spark")
@pytest.mark.yamlsnapshot(group="plan")
def test_clickbench_query_plan(spark, q, queries, snapshot):
    sql = queries[q]
    plan = normalize_plan_text(spark.sql(sql)._explain_string())  # noqa: SLF001
    assert plan == snapshot
