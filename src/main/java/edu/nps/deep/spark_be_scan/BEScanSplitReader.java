// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.spark_be_scan;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.conf.Configuration;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.SparkFiles;
import scala.Tuple2;

import edu.nps.deep.be_scan.Artifact;

/**
 * Scans and provides artifacts.
 */
public final class BEScanSplitReader
                         extends org.apache.hadoop.mapreduce.RecordReader<
                         String, NullWritable> {

  // state
  private Path path;
  private FileSystem fileSystem;
  private String filename;
  private long fileSize;
  private long splitStart; // offset from start of file
  private int splitSize;
  private boolean isDone = false;
  private String printableArtifacts; // formatted artifacts with \n between

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context)
                                throws IOException, InterruptedException {

    // configuration
    final Configuration configuration = context.getConfiguration();

    // path
    path = ((FileSplit)split).getPath();

    // hadoop filesystem
    fileSystem = path.getFileSystem(configuration);

    // get the filename string for reporting artifacts
    filename = ((FileSplit)split).getPath().toString();

    // fileSize
    fileSize = fileSystem.getFileStatus(path).getLen();

    // splitStart
    splitStart = ((FileSplit)split).getStart();

    // splitSize
    splitSize = (int)((FileSplit)split).getLength();
    if (splitSize != ((FileSplit)split).getLength()) {
      throw new IOException("split size error");
    }
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (isDone) {
      throw new IOException("Error in BufferReader.next: usage error: buffer already processed");
    }

    // mark done even if this fails
    isDone = true;

    try {
      // open the HDFS binary file
      FSDataInputStream in = fileSystem.open(path);

      // move to split
      in.seek(splitStart);

      // read the buffer from the split
      byte[] buffer = new byte[splitSize];
      org.apache.hadoop.io.IOUtils.readFully(in, buffer, 0, splitSize);
      in.close();

      // scan the split
      ScanBuffer scanBuffer = new ScanBuffer();
      printableArtifacts = scanBuffer.scan(filename, splitStart, buffer);

      // maybe help GC
      buffer = new byte[0];

    } catch (Exception e) {
      printableArtifacts = "Error in BufferReader initialization file " +
                         filename + ", splitStart " + splitStart;
      System.out.println(printableArtifacts);
    }
    return true;
  }

  @Override
  public String getCurrentKey() throws IOException, InterruptedException {
    return printableArtifacts;
  }

  @Override
  public NullWritable getCurrentValue()
                              throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public float getProgress() throws IOException {
    return isDone ? 1.0f : 0.0f;
  }

  @Override
  public void close() throws IOException {
    // no action
  }
}

