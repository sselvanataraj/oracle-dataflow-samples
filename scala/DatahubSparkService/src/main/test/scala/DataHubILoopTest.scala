import oracle.datahub.spark.prod.DataHubIMain

object DataHubILoopTest {
  def main(args: Array[String]): Unit = {
    val dh = new DataHubIMain
    val _intp = dh.startRepl()
    _intp.interpret("""println("====Starting inner interpreter====")""")
    _intp.interpret("""println(sc.version)""")
    _intp.interpret("""println(sqlContext.sparkContext.sparkUser)""")
    _intp.interpret("""val df = sqlContext.read.csv("/Users/siselvan/github/oracle-dataflow-samples/java/csv_to_parquet/src/main/resources/people.csv")""")
    _intp.interpret("""df.createOrReplaceTempView("table")""")
    _intp.interpret("""sqlContext.sql("SELECT * FROM table").show()""")
    _intp.interpret("""println("stopping spark context")""")
    _intp.interpret("""sc.stop()"""")
    _intp.interpret("""println("====Stopping inner interpreter====")""")
    _intp.interpret(""":q""")
  }
}
