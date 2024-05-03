from lib.Utils import get_spark_session 
import pytest

@pytest.fixture



def spark():
  "This is a spark Session"
  spark_session = get_spark_session("LOCAL")
  yield  spark_session
  spark_session.stop()

@pytest.fixture

def test_results(spark):
  "Gives the expected results"
  results_schema='state string, count int'
  return spark.read \
         .format("csv") \
         .schema(results_schema) \
         .load("data/test_results/results.csv")