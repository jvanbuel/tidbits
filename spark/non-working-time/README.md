# Non-working time

This tidbit is an example of some common (Py)Spark operations. 

It calculates the non-working duration of a DataFrame of `events`. An *event* is defined as a record with an `Id`, `StartTime`, and `EndTime` attribute. The non-working days are contained in a calendar of `working_days` with a datetime attribute `day` and a boolean attribute `IsWorkingDay`.

```python
from pyspark.sql import DataFrame

def  non_working_time(events: DataFrame, working_days: DataFrame) -> DataFrame
```

 An event can span multiple days, and some of these days can be non-working days. 

In `test_non_working_time.py` you can find a functional test of the implementation.