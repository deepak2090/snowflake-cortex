
import toml
import snowflake.connector
from snowflake.snowpark import Session
import os
os.system("pwd")
os.system("ls -a ../../.snowflake/connections.toml")

config = toml.load("/Users/deepakdas/PycharmProjects/snowflake_ml/snowflake-cortex/.snowflake/connections.toml")
#print(config)
sf_config = config["connections"]["test_conn"]

from snowflake.snowpark.functions import sproc, udf, udtf, call_udf, col, lit
from snowflake.snowpark.types import IntegerType, StructType, StructField
from snowflake.snowpark import Session
#from snowflake.ml.utils.connection_params import SnowflakeLoginOptions
connection_parameters = {
        "account": sf_config["account"],
        "user": sf_config["user"],
        "password": sf_config["password"],
        "warehouse": sf_config["warehouse"],
        "database": sf_config["database"],
        "schema": sf_config["schema"]
    }
session = None
if not session:
    session = Session.builder.configs(connection_parameters).create()

session.query_tag = "func-gen"
breakpoint()
add_one = sproc(
  lambda session, x: session.sql(f"select {x} + 1").collect()[0][0],
  input_types=[IntegerType()], return_type=IntegerType(),
  packages=["snowflake-snowpark-python==1.13.0"])

ret = add_one(1)

breakpoint()
add_two = sproc(
  lambda session, x: session.sql(f"select {x} + 2").collect()[0][0],
  name="add_two_proc", replace=True,
  input_types=[IntegerType()], return_type=IntegerType(),
  packages=["snowflake-snowpark-python"])

ret = session.call("add_two_proc", 1)

breakpoint()
@sproc(
  name="add_three", replace=True,
  is_permanent=True, stage_location="@int_stage",
  packages=["snowflake-snowpark-python"])
def add_three(session: Session, x: int) -> int:
  return session.sql(f"select {x} + 3").collect()[0][0]

# alternative
# session.sproc.register(
#   func=add_three, name="add_three", replace=True,
#   is_permanent=True, stage_location="@int_stage",
#   packages=["snowflake-snowpark-python"])

ret = session.sql("call add_three(1)").collect()[0][0]
print(f"add_three: {ret}")

breakpoint()

add_five = udf(lambda x: x+5,
  input_types=[IntegerType()], return_type=IntegerType())

df = session.create_dataframe([[1]]).to_df("a")
ret = df.select(add_five(col("a"))).collect()[0][0]
print(f"add_five: {ret}")

breakpoint()

add_six = udf(lambda x: x+6,
  name="add_six_proc", replace=True,
  input_types=[IntegerType()], return_type=IntegerType())

ret = session.sql("select add_six_proc(1)").collect()[0][0]
print(f"add_six: {ret}")

breakpoint()

@udf(
    name="add_seven", replace=True,
    is_permanent=True, stage_location="@int_stage")
def add_seven(x: int) -> int:
    return x+7

df = session.create_dataframe([[1]], schema=["a"])
ret = df.select(call_udf("add_seven", col("a"))).collect()[0][0]
print(f"add_seven: {ret}")

breakpoint()

class GetTwo:
  def process(self, n):
    yield(1, )
    yield(n, )

get_two = udtf(GetTwo, 
  output_schema=StructType([StructField("number", IntegerType())]),
  input_types=[IntegerType()])

# SELECT * FROM ( TABLE ("TEST"."PUBLIC".SNOWPARK_TEMP_TABLE_FUNCTION_2PR5R5RI4E(3 :: INT) ))
ret = session.table_function(get_two(lit(3))).collect()
print(f"get_two: {ret}")
session.close()




