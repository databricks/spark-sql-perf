// Databricks notebook source
// Multi TPC- H and DS generator and database importer using spark-sql-perf, typically to generate parquet files in S3/blobstore objects
val benchmarks = Seq("TPCDS", "TPCH") // Options: TCPDS", "TPCH"
val scaleFactors = Seq("1", "10", "100", "1000", "10000") // "1", "10", "100", "1000", "10000" list of scale factors to generate and import

val baseLocation = s"s3a://mybucket" // S3 bucket, blob, or local root path
val baseDatagenFolder = "/tmp"  // usually /tmp if enough space is available for datagen files

// Output file formats
val fileFormat = "parquet" // only parquet was tested
val shuffle = true // If true, partitions will be coalesced into a single file during generation up to spark.sql.files.maxRecordsPerFile (if set)
val overwrite = false //if to delete existing files (doesn't check if results are complete on no-overwrite)

// Generate stats for CBO
val createTableStats = true
val createColumnStats = true

val workers: Int = spark.conf.get("spark.databricks.clusterUsageTags.clusterTargetWorkers").toInt //number of nodes, assumes one executor per node
val cores: Int = Runtime.getRuntime.availableProcessors.toInt //number of CPU-cores

val dbSuffix = "" // set only if creating multiple DBs or source file folders with different settings, use a leading _
val TPCDSUseLegacyOptions = false // set to generate file/DB naming and table options compatible with older results

// COMMAND ----------

// Imports, fail fast if we are missing any library

// For datagens
import java.io._
import scala.sys.process._

// spark-sql-perf
import com.databricks.spark.sql.perf._
import com.databricks.spark.sql.perf.tpch._
import com.databricks.spark.sql.perf.tpcds._

// Spark/Hadoop config
import org.apache.spark.deploy.SparkHadoopUtil

// COMMAND ----------

// Set Spark config to produce same and comparable source files across systems 
// do not change unless you want to derive from default source file composition, in that case also set a DB suffix 
spark.sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

// Prevent very large files. 20 million records creates between 500 and 1500MB files in TPCH
spark.sqlContext.setConf("spark.sql.files.maxRecordsPerFile", "20000000")  // This also regulates the file coalesce

// COMMAND ----------

// Checks that we have the correct number of worker nodes to start the data generation
// Make sure you have set the workers variable correctly, as the datagens binaries need to be present in all nodes
val targetWorkers: Int = workers
def numWorkers: Int = sc.getExecutorMemoryStatus.size - 1
def waitForWorkers(requiredWorkers: Int, tries: Int) : Unit = {
  for (i <- 0 to (tries-1)) {
    if (numWorkers == requiredWorkers) {
      println(s"Waited ${i}s. for $numWorkers workers to be ready")
      return
    }
    if (i % 60 == 0) println(s"Waiting ${i}s. for workers to be ready, got only $numWorkers workers")
    Thread sleep 1000
  }
  throw new Exception(s"Timed out waiting for workers to be ready after ${tries}s.")
}
waitForWorkers(targetWorkers, 3600) //wait up to an hour

// COMMAND ----------

// Time command helper
def time[R](block: => R): R = {  
    val t0 = System.currentTimeMillis() //nanoTime()
    val result = block    // call-by-name
    val t1 = System.currentTimeMillis() //nanoTime()
    println("Elapsed time: " + (t1 - t0) + "ms")
    result
}

// COMMAND ----------

// FOR INSTALLING TPCH DBGEN (with the stdout patch)
def installDBGEN(url: String = "https://github.com/databricks/tpch-dbgen.git", useStdout: Boolean = true, baseFolder: String = "/tmp")(i: java.lang.Long): String = {
  // check if we want the revision which makes dbgen output to stdout
  val checkoutRevision: String = if (useStdout) "git checkout 0469309147b42abac8857fa61b4cf69a6d3128a8 -- bm_utils.c" else ""
  Seq("mkdir", "-p", baseFolder).!
  val pw = new PrintWriter(new File(s"${baseFolder}/dbgen_$i.sh" ))
  pw.write(s"""
rm -rf ${baseFolder}/dbgen
rm -rf ${baseFolder}/dbgen_install_$i
mkdir ${baseFolder}/dbgen_install_$i
cd ${baseFolder}/dbgen_install_$i
git clone '$url'
cd tpch-dbgen
$checkoutRevision
make
ln -sf ${baseFolder}/dbgen_install_$i/tpch-dbgen ${baseFolder}/dbgen || echo "ln -sf failed"
test -e ${baseFolder}/dbgen/dbgen
echo "OK"
  """)
  pw.close
  Seq("chmod", "+x", s"${baseFolder}/dbgen_$i.sh").!
  Seq(s"${baseFolder}/dbgen_$i.sh").!!
}

// COMMAND ----------

// FOR INSTALLING TPCDS DSDGEN (with the stdout patch)
// Note: it assumes Debian/Ubuntu host, edit package manager if not
def installDSDGEN(url: String = "https://github.com/databricks/tpcds-kit.git", useStdout: Boolean = true, baseFolder: String = "/tmp")(i: java.lang.Long): String = {
  Seq("mkdir", "-p", baseFolder).!
  val pw = new PrintWriter(new File(s"${baseFolder}/dsdgen_$i.sh" ))
  pw.write(s"""
sudo apt-get update
sudo apt-get -y --force-yes install gcc make flex bison byacc git
rm -rf ${baseFolder}/dsdgen
rm -rf ${baseFolder}/dsdgen_install_$i
mkdir ${baseFolder}/dsdgen_install_$i
cd ${baseFolder}/dsdgen_install_$i
git clone '$url'
cd tpcds-kit/tools
make -f Makefile.suite
ln -sf ${baseFolder}/dsdgen_install_$i/tpcds-kit/tools ${baseFolder}/dsdgen || echo "ln -sf failed"
${baseFolder}/dsdgen/dsdgen -h
test -e ${baseFolder}/dsdgen/dsdgen
echo "OK"
  """)
  pw.close
  Seq("chmod", "+x", s"${baseFolder}/dsdgen_$i.sh").!
  Seq(s"${baseFolder}/dsdgen_$i.sh").!!
}

// COMMAND ----------

// install (build) the data generators in all nodes
val res = spark.range(0, workers, 1, workers).map(worker => benchmarks.map{
    case "TPCDS" => s"TPCDS worker $worker\n" + installDSDGEN(baseFolder = baseDatagenFolder)(worker)
    case "TPCH" => s"TPCH worker $worker\n" + installDBGEN(baseFolder = baseDatagenFolder)(worker)
  }).collect()

// COMMAND ----------

// Set the benchmark name, tables, and location for each benchmark
// returns (dbname, tables, location) 
def getBenchmarkData(benchmark: String, scaleFactor: String) = benchmark match {  
  
  case "TPCH" => (
    s"tpch_sf${scaleFactor}_${fileFormat}${dbSuffix}",
    new TPCHTables(spark.sqlContext, dbgenDir = s"${baseDatagenFolder}/dbgen", scaleFactor = scaleFactor, useDoubleForDecimal = false, useStringForDate = false, generatorParams = Nil),
    s"$baseLocation/tpch/sf${scaleFactor}_${fileFormat}")  
  
  case "TPCDS" if !TPCDSUseLegacyOptions => (
    s"tpcds_sf${scaleFactor}_${fileFormat}${dbSuffix}",
    new TPCDSTables(spark.sqlContext, dsdgenDir = s"${baseDatagenFolder}/dsdgen", scaleFactor = scaleFactor, useDoubleForDecimal = false, useStringForDate = false),
    s"$baseLocation/tpcds-2.4/sf${scaleFactor}_${fileFormat}")
  
  case "TPCDS" if TPCDSUseLegacyOptions => (
    s"tpcds_sf${scaleFactor}_nodecimal_nodate_withnulls${dbSuffix}",
    new TPCDSTables(spark.sqlContext, s"${baseDatagenFolder}/dsdgen", scaleFactor = scaleFactor, useDoubleForDecimal = true, useStringForDate = true),
    s"$baseLocation/tpcds/sf$scaleFactor-$fileFormat/useDecimal=false,useDate=false,filterNull=false")
}

// COMMAND ----------

// Data generation
def isPartitioned (tables: Tables, tableName: String) : Boolean = 
  util.Try(tables.tables.find(_.name == tableName).get.partitionColumns.nonEmpty).getOrElse(false)

def loadData(tables: Tables, location: String, scaleFactor: String) = {
  val tableNames = tables.tables.map(_.name)
  tableNames.foreach { tableName =>
  // generate data
    time {
      tables.genData(
        location = location, 
        format = fileFormat, 
        overwrite = overwrite, 
        partitionTables = true, 
        // if to coallesce into a single file (only one writter for non partitioned tables = slow) 
        clusterByPartitionColumns = shuffle, //if (isPartitioned(tables, tableName)) false else true, 
        filterOutNullPartitionValues = false, 
        tableFilter = tableName,
        // this controlls parallelism on datagen and number of writers (# of files for non-partitioned)
        // in general we want many writers to S3, and smaller tasks for large scale factors to avoid OOM and shuffle errors    
        numPartitions = if (scaleFactor.toInt <= 100 || !isPartitioned(tables, tableName)) (workers * cores) else (workers * cores * 4))
    }
  }
}

// COMMAND ----------

// Create the DB, import data, create
def createExternal(location: String, dbname: String, tables: Tables) = {
  tables.createExternalTables(location, fileFormat, dbname, overwrite = overwrite, discoverPartitions = true)
}

def loadDB(dbname: String, tables: Tables, location: String) = {
  val tableNames = tables.tables.map(_.name)
  time { 
    println(s"Creating external tables at $location")
    createExternal(location, dbname, tables) 
  }
  // Show table information and attempt to vacuum
  tableNames.foreach { tableName =>
    println(s"Table $tableName has " + util.Try(sql(s"SHOW PARTITIONS $tableName").count() + " partitions").getOrElse(s"no partitions"))
    util.Try(sql(s"VACUUM $tableName RETAIN 0.0. HOURS"))getOrElse(println(s"Cannot VACUUM $tableName"))
    sql(s"DESCRIBE EXTENDED $tableName").show(999, false)
    println
  }
}

// COMMAND ----------

def setScaleConfig(scaleFactor: String): Unit = {
  // Avoid OOM when shuffling large scale fators
  // and errors like 2GB shuffle limit at 10TB like: Most recent failure reason: org.apache.spark.shuffle.FetchFailedException: Too large frame: 9640891355
  // For 10TB 16x4core nodes were needed with the config below, 8x for 1TB and below. 
  // About 24hrs. for SF 1 to 10,000.
  if (scaleFactor.toInt >= 10000) {    
    spark.conf.set("spark.sql.shuffle.partitions", "20000")
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.1")
  } 
  else if (scaleFactor.toInt >= 1000) {
    spark.conf.set("spark.sql.shuffle.partitions", "2001") //one above 2000 to use HighlyCompressedMapStatus
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.3")    
  }
  else { 
    spark.conf.set("spark.sql.shuffle.partitions", "200") //default
    SparkHadoopUtil.get.conf.set("parquet.memory.pool.ratio", "0.5")
  }
}

// COMMAND ----------

// Generate the data, import the tables, generate stats for selected benchmarks and scale factors
scaleFactors.foreach { scaleFactor => {
  
  // First set some config settings affecting OOMs/performance
  setScaleConfig(scaleFactor)
  
  benchmarks.foreach{ benchmark => {
    val (dbname, tables, location) = getBenchmarkData(benchmark, scaleFactor)
    // Start the actual loading
    time {
      println(s"Generating data for $benchmark SF $scaleFactor at $location")
      loadData(tables = tables, location = location, scaleFactor = scaleFactor)
    }
    time {
      println(s"\nImporting data for $benchmark into DB $dbname from $location")
      loadDB(dbname = dbname, tables = tables, location = location)
    }
    if (createTableStats) time { 
      println(s"\nGenerating table statistics for DB $dbname (with analyzeColumns=$createColumnStats)")
      tables.analyzeTables(dbname, analyzeColumns = createColumnStats)
    }
  }}
}}


// COMMAND ----------

// Print table structure for manual validation
scaleFactors.foreach { scaleFactor =>
  benchmarks.foreach{ benchmark => {
    val (dbname, tables, location) = getBenchmarkData(benchmark, scaleFactor)
    sql(s"use $dbname")
    time {
      sql(s"show tables").select("tableName").collect().foreach{ tableName =>        
        val name: String = tableName.toString().drop(1).dropRight(1)
        println(s"Printing table information for $benchmark SF $scaleFactor table $name")
        val count = sql(s"select count(*) as ${name}_count from $name").collect()(0)(0)
        println(s"Table $name has " + util.Try(sql(s"SHOW PARTITIONS $name").count() + " partitions").getOrElse(s"no partitions") + s" and $count rows.")
        sql(s"describe extended $name").show(999, false)
      } 
    }
    println
  }}
}
