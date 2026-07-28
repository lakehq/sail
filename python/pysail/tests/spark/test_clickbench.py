from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pyspark.sql.functions as F  # noqa: N812
import pytest

from pysail.testing.spark.steps.plan import normalize_plan_text
from pysail.testing.spark.utils.common import is_jvm_spark

_CLICKBENCH_SCHEMA = pa.schema(
    [
        pa.field("WatchID", pa.int64(), nullable=True),
        pa.field("JavaEnable", pa.int16(), nullable=True),
        pa.field("Title", pa.binary(), nullable=True),
        pa.field("GoodEvent", pa.int16(), nullable=True),
        pa.field("EventTime", pa.int64(), nullable=True),
        pa.field("EventDate", pa.uint16(), nullable=True),
        pa.field("CounterID", pa.int32(), nullable=True),
        pa.field("ClientIP", pa.int32(), nullable=True),
        pa.field("RegionID", pa.int32(), nullable=True),
        pa.field("UserID", pa.int64(), nullable=True),
        pa.field("CounterClass", pa.int16(), nullable=True),
        pa.field("OS", pa.int16(), nullable=True),
        pa.field("UserAgent", pa.int16(), nullable=True),
        pa.field("URL", pa.binary(), nullable=True),
        pa.field("Referer", pa.binary(), nullable=True),
        pa.field("IsRefresh", pa.int16(), nullable=True),
        pa.field("RefererCategoryID", pa.int16(), nullable=True),
        pa.field("RefererRegionID", pa.int32(), nullable=True),
        pa.field("URLCategoryID", pa.int16(), nullable=True),
        pa.field("URLRegionID", pa.int32(), nullable=True),
        pa.field("ResolutionWidth", pa.int16(), nullable=True),
        pa.field("ResolutionHeight", pa.int16(), nullable=True),
        pa.field("ResolutionDepth", pa.int16(), nullable=True),
        pa.field("FlashMajor", pa.int16(), nullable=True),
        pa.field("FlashMinor", pa.int16(), nullable=True),
        pa.field("FlashMinor2", pa.binary(), nullable=True),
        pa.field("NetMajor", pa.int16(), nullable=True),
        pa.field("NetMinor", pa.int16(), nullable=True),
        pa.field("UserAgentMajor", pa.int16(), nullable=True),
        pa.field("UserAgentMinor", pa.binary(), nullable=True),
        pa.field("CookieEnable", pa.int16(), nullable=True),
        pa.field("JavascriptEnable", pa.int16(), nullable=True),
        pa.field("IsMobile", pa.int16(), nullable=True),
        pa.field("MobilePhone", pa.int16(), nullable=True),
        pa.field("MobilePhoneModel", pa.binary(), nullable=True),
        pa.field("Params", pa.binary(), nullable=True),
        pa.field("IPNetworkID", pa.int32(), nullable=True),
        pa.field("TraficSourceID", pa.int16(), nullable=True),
        pa.field("SearchEngineID", pa.int16(), nullable=True),
        pa.field("SearchPhrase", pa.binary(), nullable=True),
        pa.field("AdvEngineID", pa.int16(), nullable=True),
        pa.field("IsArtifical", pa.int16(), nullable=True),
        pa.field("WindowClientWidth", pa.int16(), nullable=True),
        pa.field("WindowClientHeight", pa.int16(), nullable=True),
        pa.field("ClientTimeZone", pa.int16(), nullable=True),
        pa.field("ClientEventTime", pa.int64(), nullable=True),
        pa.field("SilverlightVersion1", pa.int16(), nullable=True),
        pa.field("SilverlightVersion2", pa.int16(), nullable=True),
        pa.field("SilverlightVersion3", pa.int32(), nullable=True),
        pa.field("SilverlightVersion4", pa.int16(), nullable=True),
        pa.field("PageCharset", pa.binary(), nullable=True),
        pa.field("CodeVersion", pa.int32(), nullable=True),
        pa.field("IsLink", pa.int16(), nullable=True),
        pa.field("IsDownload", pa.int16(), nullable=True),
        pa.field("IsNotBounce", pa.int16(), nullable=True),
        pa.field("FUniqID", pa.int64(), nullable=True),
        pa.field("OriginalURL", pa.binary(), nullable=True),
        pa.field("HID", pa.int32(), nullable=True),
        pa.field("IsOldCounter", pa.int16(), nullable=True),
        pa.field("IsEvent", pa.int16(), nullable=True),
        pa.field("IsParameter", pa.int16(), nullable=True),
        pa.field("DontCountHits", pa.int16(), nullable=True),
        pa.field("WithHash", pa.int16(), nullable=True),
        pa.field("HitColor", pa.binary(), nullable=True),
        pa.field("LocalEventTime", pa.int64(), nullable=True),
        pa.field("Age", pa.int16(), nullable=True),
        pa.field("Sex", pa.int16(), nullable=True),
        pa.field("Income", pa.int16(), nullable=True),
        pa.field("Interests", pa.int16(), nullable=True),
        pa.field("Robotness", pa.int16(), nullable=True),
        pa.field("RemoteIP", pa.int32(), nullable=True),
        pa.field("WindowName", pa.int32(), nullable=True),
        pa.field("OpenerName", pa.int32(), nullable=True),
        pa.field("HistoryLength", pa.int16(), nullable=True),
        pa.field("BrowserLanguage", pa.binary(), nullable=True),
        pa.field("BrowserCountry", pa.binary(), nullable=True),
        pa.field("SocialNetwork", pa.binary(), nullable=True),
        pa.field("SocialAction", pa.binary(), nullable=True),
        pa.field("HTTPError", pa.int16(), nullable=True),
        pa.field("SendTiming", pa.int32(), nullable=True),
        pa.field("DNSTiming", pa.int32(), nullable=True),
        pa.field("ConnectTiming", pa.int32(), nullable=True),
        pa.field("ResponseStartTiming", pa.int32(), nullable=True),
        pa.field("ResponseEndTiming", pa.int32(), nullable=True),
        pa.field("FetchTiming", pa.int32(), nullable=True),
        pa.field("SocialSourceNetworkID", pa.int16(), nullable=True),
        pa.field("SocialSourcePage", pa.binary(), nullable=True),
        pa.field("ParamPrice", pa.int64(), nullable=True),
        pa.field("ParamOrderID", pa.binary(), nullable=True),
        pa.field("ParamCurrency", pa.binary(), nullable=True),
        pa.field("ParamCurrencyID", pa.int16(), nullable=True),
        pa.field("OpenstatServiceName", pa.binary(), nullable=True),
        pa.field("OpenstatCampaignID", pa.binary(), nullable=True),
        pa.field("OpenstatAdID", pa.binary(), nullable=True),
        pa.field("OpenstatSourceID", pa.binary(), nullable=True),
        pa.field("UTMSource", pa.binary(), nullable=True),
        pa.field("UTMMedium", pa.binary(), nullable=True),
        pa.field("UTMCampaign", pa.binary(), nullable=True),
        pa.field("UTMContent", pa.binary(), nullable=True),
        pa.field("UTMTerm", pa.binary(), nullable=True),
        pa.field("FromTag", pa.binary(), nullable=True),
        pa.field("HasGCLID", pa.int16(), nullable=True),
        pa.field("RefererHash", pa.int64(), nullable=True),
        pa.field("URLHash", pa.int64(), nullable=True),
        pa.field("CLID", pa.int32(), nullable=True),
    ]
)


@pytest.fixture(scope="module", autouse=True)
def data(spark, tmp_path_factory):
    # Create an empty dataset with the ClickBench schema.
    # Note that when querying this empty dataset, the physical plan may be different
    # from those when querying the dataset with actual data. For example, partial aggregation
    # may be absent due to the small dataset size.
    # But the physical plan in the snapshot would at least ensure certain aspects of
    # query planning and optimization (e.g., filter pushdown) are present.
    tmp_dir = tmp_path_factory.mktemp("clickbench")
    data = [pa.array([], type=f.type) for f in _CLICKBENCH_SCHEMA]
    table = pa.Table.from_arrays(data, schema=_CLICKBENCH_SCHEMA)
    data_path = str(tmp_dir / "hits.parquet")
    pq.write_table(table, data_path)
    df = spark.read.parquet(data_path)
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
