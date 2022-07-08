import pytest
from pyspark.sql import SparkSession, DataFrame
from main import non_working_time
from datetime import datetime, timedelta
from pyspark.sql.types import (
    DateType,
    StructField,
    StructType,
    TimestampType,
    BooleanType,
    ByteType,
)


@pytest.fixture(scope="session")
def spark_session(request):
    spark_session = (
        SparkSession.builder.master("local[*]").appName("pytest").getOrCreate()
    )

    request.addfinalizer(lambda: spark_session.sparkContext.stop())
    return spark_session


pytestmark = pytest.mark.usefixtures("spark_session")


def test_non_working_time(spark_session: SparkSession):
    events = spark_session.createDataFrame(
        data=[
            (1, now := datetime(2022, 7, 10, 8, 0, 0), now + timedelta(days=3, hours=2))
        ],
        schema=StructType(
            fields=[
                StructField(name="Id", dataType=ByteType()),
                StructField(name="StartTime", dataType=TimestampType()),
                StructField(name="EndTime", dataType=TimestampType()),
            ]
        ),
    )
    working_days = spark_session.createDataFrame(
        data=[
            (datetime(2022, 7, 10), False),
            (datetime(2022, 7, 11), True),
            (datetime(2022, 7, 12), False),
            (datetime(2022, 7, 13), False),
        ],
        schema=StructType(
            fields=[
                StructField(name="day", dataType=DateType()),
                StructField(name="IsWorkingDay", dataType=BooleanType()),
            ]
        ),
    )
    result: DataFrame = non_working_time(events, working_days)
    result.show()
    assert (
        result.collect()[0]["sum(non_working_duration)"] == int(timedelta(days=2, hours=2).total_seconds())
    )
