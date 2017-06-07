// http://spark.apache.org/docs/latest/programming-guide.html
package edu.nps.deep.be_scan_spark;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.ExecutionException;
import java.util.Date;
import java.util.Iterator;
import java.text.SimpleDateFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

public final class BEScanSpark{

  // ************************************************************
  // SplitFileInputFormat implements createRecordReader which returns
  // BEScanRecordReader which stores features instead of returning them.
  // ************************************************************
  public static class SplitFileInputFormat
        extends org.apache.hadoop.mapreduce.lib.input.FileInputFormat<
                         Long, String> {

    // createRecordReader returns EmailReader
    @Override
    public org.apache.hadoop.mapreduce.RecordReader<Long, String>
           createRecordReader(
                 org.apache.hadoop.mapreduce.InputSplit split,
                 org.apache.hadoop.mapreduce.TaskAttemptContext context)
                       throws IOException, InterruptedException {

      BEScanRecordReader reader = new BEScanRecordReader();
      reader.initialize(split, context);
      return reader;
    }
  }

  // ************************************************************
  // Main
  // ************************************************************

  public static void main(String[] args) {

    if (args.length != 2) {
      System.err.println("Usage: BEScanSpark <directory holding .so files> <input path>");
      System.exit(1);
    }

    // set up the Spark Configuration
    SparkConf sparkConfiguration = new SparkConf();
    sparkConfiguration.setAppName("Spark Byte Count App");
    sparkConfiguration.set("log4j.logger.org.apache.spark.rpc.akka.ErrorMonitor", "FATAL");
    sparkConfiguration.set("log4j.logger.org.apache.spark.scheduler.DAGScheduler", "TRACE");
    sparkConfiguration.set("yarn.log-aggregation-enable", "true");
    sparkConfiguration.set("fs.hdfs.impl.disable.cache", "true");
    sparkConfiguration.set("spark.app.id", "BEScanSpark App");
//    sparkConfiguration.set("spark.executor.extrajavaoptions", "-XX:+UseConcMarkSweepGC");
    sparkConfiguration.set("spark.dynamicAllocation.maxExecutors", "600");

    sparkConfiguration.set("spark.default.parallelism", "1");

    sparkConfiguration.set("spark.driver.maxResultSize", "100g"); // default 1g

    sparkConfiguration.set("spark.yarn.executor.memoryOverhead", "4000"); // default 1g

    // set up the Spark context
    JavaSparkContext sparkContext = new JavaSparkContext(sparkConfiguration);

    // make .so libraries available on each node
    sparkContext.addFile(args[0] + "/" + "libbe_scan.so");
    sparkContext.addFile(args[0] + "/" + "libbe_scan_jni.so");

    try {

      // get the hadoop job
      Job hadoopJob = Job.getInstance(sparkContext.hadoopConfiguration(),
                    "BEScanSpark job");

      // get the file system
      FileSystem fileSystem =
                       FileSystem.get(sparkContext.hadoopConfiguration());

      // get the input path
      Path inputPath = new Path(args[1]);

      // iterate over files under the input path
      RemoteIterator<LocatedFileStatus> fileStatusListIterator =
                                       fileSystem.listFiles(inputPath, true);
      int i = 0;
      long totalBytes = 0;
      while (fileStatusListIterator.hasNext()) {

        // get file status for this file
        LocatedFileStatus locatedFileStatus = fileStatusListIterator.next();

//        // restrict number of files to process else comment this out
//        if (++i > 2) {
//          break;
//        }

        // show file being added
        System.out.println("adding " + locatedFileStatus.getLen() +
                  " bytes at path " + locatedFileStatus.getPath().toString());

        // add this file to the job
        FileInputFormat.addInputPath(hadoopJob, locatedFileStatus.getPath());
        totalBytes += locatedFileStatus.getLen();

//        // stop after some amount
//        if (totalBytes > 3510000000000L) {
//          break;
//        }
      }

      // Transformation: create the pairRDD for all the files and splits
      JavaPairRDD<Long, String> pairRDD = sparkContext.newAPIHadoopRDD(
               hadoopJob.getConfiguration(),         // configuration
               SplitFileInputFormat.class,           // F
               Long.class,                           // K
               String.class);                        // V

      // Capture all the feature strings
      // perform the write action
      pairRDD.count();

      // show the total bytes processed
      System.out.println("total bytes processed: " + totalBytes);

      // Done
      System.out.println("Done.");

    }  catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}

