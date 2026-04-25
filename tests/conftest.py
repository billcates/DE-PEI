import pytest

from pyspark.sql import SparkSession

import os
import pyspark

os.environ["SPARK_HOME"] = os.path.dirname(pyspark.__file__)

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local[1]").appName("order summary").getOrCreate()