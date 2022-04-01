import com.amazonaws.services.glue.util.JsonOptions
import com.amazonaws.services.glue.{DynamicFrame, GlueContext}
import com.amazonaws.services.glue.util.GlueArgParser

object GlueApp {
 val glueContext: GlueContext = new GlueContext(sc)
 val sc: SparkContext = new SparkContext()
 val spark = glueContext.getSparkSession
  def main(sysArgs: Array[String]) {
    val args = GlueArgParser.getResolvedOptions(sysArgs, Array("env"))
    
    // Catálogo de datos: bases de datos y tablas
    val dbName = s"db-ejemplo"
    val tblCSV = s"transacciones" //El nombre de la tabla con la ubicación del S3
    val tblDYNAMO = s"transactions" //El nombre de la tabla con la ubicación de dynamo
    
    // Directorio final donde se guardará nuestro archivo dentro de un bucket S3
    val baseOutputDir = s"s3://${args("env")}-trx-ejemplo/"
    val transactionDir= s"$baseOutputDir/transaction/"
    
    // Read data into a dynamic frame
    val trx_dyn: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tblDYNAMO ).getDynamicFrame()
    val trx_csv: DynamicFrame = glueContext.getCatalogSource(database = dbName, tableName = tblCSV ).getDynamicFrame()
    
    //Casting estricto de datos
    val trx_dyn_resolve= trx_dyn.resolveChoice(specs = Seq(("monto", "cast:double")))
    val trx_csv_resolve= trx_csv.resolveChoice(specs = Seq(("monto", "cast:double")))
    
    // Spark SQL on a Spark dataframe
    val dynDf = trx_dyn_resolve.toDF()
    dynDf .createOrReplaceTempView("dynamoTable")
    val csvDf = trx_csv_resolve.toDF()
    csvDf .createOrReplaceTempView("csvTable")
    
    // SQL Query
    val dynSqlDf = spark.sql("SELECT T1.id,T1.monto,T1.cliente,T1.estado FROM dynamoTable T1 LEFT JOIN csvTable T2 ON (T1.id=T2.id) WHERE T2.idIS NOT NULL AND (T1.monto=T2.monto AND T1.cliente=T2.cliente AND T1.estado = T2.estado)")
    
    //Compact al run-part files into one
    val dynFile = DynamicFrame(dynSqlDf, glueContext).withName("dyn_dyf").coalesce(1)
    
    // Save file into S3
    glueContext.getSinkWithFormat(connectionType = "s3", options = JsonOptions(Map("path" -> transactionDir)), format = "csv").writeDynamicFrame(dynFile)
  }
}
