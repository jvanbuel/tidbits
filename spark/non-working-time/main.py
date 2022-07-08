from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as psf
from functools import partial


def add_days_inbetween(events: DataFrame, working_days: DataFrame):
    return (
        events.withColumn("StartDate", psf.to_date("StartTime"))
        .withColumn("EndDate", psf.to_date("EndTime"))
        .join(
            other=working_days,
            on=psf.col("day").between(psf.col("StartDate"), psf.col("EndDate")),
        )
    )


def add_non_working_flag(events_with_working_days: DataFrame) -> DataFrame:
    return events_with_working_days.withColumn(
        "non_working_day_flag", psf.when(psf.col("IsWorkingDay"), 0).otherwise(1)
    )


def calculate_non_working_time_per_day(enriched_events: DataFrame) -> DataFrame:
    return enriched_events.withColumn(
        "non_working_duration",
        psf.when(
            (psf.col("day") == psf.col("StartDate"))
            & (
                psf.col("day") == psf.col("EndDate")
            ),  # Event happened within a single day
            (
                psf.unix_timestamp(psf.col("EndTime"))
                - psf.unix_timestamp(psf.col("StartTime"))
            )
            * psf.col("non_working_day_flag"),
        )
        .when(
            psf.col("day") == psf.col("StartDate"),  # First day of multi-day event
            (
                psf.unix_timestamp(psf.date_add(psf.col("StartDate"), 1))
                - psf.unix_timestamp(psf.col("StartTime"))
            )
            * psf.col("non_working_day_flag"),
        )
        .when(
            psf.col("day") == psf.col("EndDate"),  # Last day of multi-day event
            (
                psf.unix_timestamp(psf.col("EndTime"))
                - psf.unix_timestamp(psf.col("EndDate"))
            )
            * psf.col("non_working_day_flag"),
        )
        .otherwise(
            24 * 60 * 60 * psf.col("non_working_day_flag")
        ),  # Day inbetween start and end date of multi-day event
    )


def aggregate_non_working_time(
    events: DataFrame,
    events_with_working_time_per_day: DataFrame,
) -> DataFrame:
    return events.join(
        other=events_with_working_time_per_day.groupBy(psf.col("Id")).agg(
            psf.sum(psf.col("non_working_duration"))
        ),
        on="Id",
    )


def non_working_time(events: DataFrame, working_days: DataFrame) -> DataFrame:
    return aggregate_non_working_time(
        events=events,
        events_with_working_time_per_day=events.transform(
            partial(add_days_inbetween, working_days=working_days)
        )
        .transform(add_non_working_flag)
        .transform(calculate_non_working_time_per_day),
    )
