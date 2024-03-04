### How to build the package
 1. `sbt clean package`
 2. `mkdir jars`
 3. `cp target/scala-2.12/main-scala-assignment_03_2.12-1.0.jar jars/`

To run the Scala code use:

 * `spark-submit --class assignment_03 jars/main-scala-assignment_03_2.12-1.0.jar data/flights/departuredelays.csv`
