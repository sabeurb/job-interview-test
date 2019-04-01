# Job interview tests
Exercises used to evaluate candidates.


# Available exercises
## Spark
### Java
Use project sparkjava.

Context : Compute metrics based on CO2 emissions by country dataset.

Tooling used :
* Maven
* JUnit

Goal : use different parts of Spark API : RDD and SQL

Expected work : 
* Implement missing code in following classes
  * Co2SparkSqlApp : SQL version of the processing
  * Co2SparkRDDApp : RDD version of the processing
* All tests should pass

Bonus : implement an additional step of processing to compute the top 10 countries that reduced their emissions between 1990s (1990 to 1999) and 2000s (2000 to 2009).

### Scala
Use project sparkscala.

Context : Compute metrics based on CO2 emissions by country dataset.

Tooling used :
* SBT
* ScalaTest

Goal : use different parts of Spark API : RDD and SQL

Expected work : 
* Implement missing code in following classes
  * Co2SparkSqlApp : SQL version of the processing
  * Co2SparkRDDApp : RDD version of the processing
* All tests should pass

Bonus : implement an additional step of processing to compute the top 10 countries that reduced their emissions between 1990s (1990 to 1999) and 2000s (2000 to 2009).