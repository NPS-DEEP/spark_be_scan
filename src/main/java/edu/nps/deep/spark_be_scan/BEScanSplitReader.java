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
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.io.NullWritable;

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
  private final Path path;
  private final FileSystem fileSystem;
  private final String filename;
  private final long fileSize;
  private final long splitStart; // offset from start of file
  private final long splitSize;
  private bool isDone = false;
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
    splitStart = ((FileSplit)inputSplit).getStart();

    // splitSize
    splitSize = ((FileSplit)inputSplit).getLength();
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
      org.apache.hadoop.io.IOUtils.readFully(in, buffer, 0, count);
      close(in);

      // scan the split
      ScanBuffer scanBuffer = new ScanBuffer();
      printableArtifacts = scanBuffer.scan(filename, offset, buffer);

      // maybe help GC
      buffer = "";

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




















/*
  private void scanAsNeeded() throws IOException {

    // scan buffers until we get artifacts
    while (reader.hasNext() && scanner.empty()) {

      // read next
      BufferReader.BufferRecord record = reader.next();

      // scan next
      String success = scanner.scan(record.offset,
                                    previous_buffer, record.buffer);
      if (!success.equals("")) {
        throw new IOException("Error: " + success);
      }

      // move current buffer to previous buffer
      previous_buffer = record.buffer;
    }
  }
*/

