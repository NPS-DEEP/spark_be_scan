// based loosely on Spark examples and
// http://spark.apache.org/docs/latest/programming-guide.html

package edu.nps.deep.spark_be_scan;

import java.lang.StringBuilder;
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
public final class ScanBuffer {

  // static scan engine
  private static final edu.nps.deep.be_scan.ScanEngine scanEngine;

  // class state
  private static final int MAX_RECURSION_DEPTH = 7;
  private edu.nps.deep.be_scan.Uncompressor uncompressor;
  private String filename;
  private StringBuilder stringBuilder;


  // pre-loaded libraries
  static {
    System.load(SparkFiles.get("libstdc++.so"));
    System.load(SparkFiles.get("libicudata.so"));
    System.load(SparkFiles.get("libicuuc.so"));
    System.load(SparkFiles.get("liblightgrep.so"));
    System.load(SparkFiles.get("liblightgrep_wrapper.so"));
    System.load(SparkFiles.get("libbe_scan.so"));
    System.load(SparkFiles.get("libbe_scan_jni.so"));

    scanEngine = new edu.nps.deep.be_scan.ScanEngine("email");
  }

  public ScanBuffer() {
  }

  // from https://stackoverflow.com/questions/415953/how-can-i-generate-an-md5-hash
  private static String convertByteToHex(byte[] byteData) {

    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < byteData.length; i++) {
      sb.append(Integer.toString((byteData[i] & 0xff) + 0x100, 16).substring(1));
    }

    return sb.toString();
  }

  private static void setCompressionText(edu.nps.deep.be_scan.Artifact artifact,
                           edu.nps.deep.be_scan.Uncompressed uncompressed) {

    if (uncompressed.getStatus != "") {
      // uncompression failure so put failure into artifact text
      artifact.setArtifact(uncompressed.getStatus());
    } else {
      // valid so put MD5 into artifact text
      MessageDigest md = MessageDigest.getInstance("MD5");
      md.update(uncompressed.getBuffer());
      byte[] digest = md.digest();
      artifact.setArtifact(convertByteToHex(digest));
    }
  }

  // recurse
  private void recurse(String filename,
                       byte[] uncompressedBuffer,
                       String recursion_prefix,
                       long depth) {

    // open a scanner
    edu.nsp.deep.be_scan.Artifacts artifacts = new
                                    edu.nsp.deep.be_scan.Artifacts();
    scanner = be_scan.Scanner(scan_engine, artifacts);
    scanner.scan_setup(filename, recursion_prefix);

    // scan
    String status = scanner.scanFinal(0, "", uncompressedBuffer);
    if (status != "") {
      return ("Error in scan_stream: " + status);
    }

    // consume recursed artifacts
    while (!(artifacts.empty())) {
      edu.nps.deep.be_scan.Artifact artifact = artifacts.get();

      // prepare for zip or gzip
      if (artifact.getArtifactClass() == "zip" ||
                        artifact.getArtifactClass() == "gzip") {

        // uncompress
        uncompressed = uncompressor.uncompress(uncompressedBuffer,
                          artifact.offset);

        // no error and nothing uncompressed so disregard this artifact
        if (!uncompressed.getStatus() && !uncompressed.getBuffer()) {
          continue;
        }

        // set artifact text for this uncompression
        setCompressionText(artifact, uncompressed);

        // skip popular useless uncompressed data
        if (artifact.artifact == "8da7a0b0144fc58332b03e90aaf7ba25") {
          continue;
        }
      }

      // prepare for other artifact types as needed
      // none.

      // add the artifact
      stringBuilder.append(artifact.toString());
      stringBuilder.append("\n");

      // manage recursion
      if ((artifact.getArtifactClass() == "zip" ||
                        artifact.getArtifactClass() == "gzip") &&
                        depth <= MAX_RECURSION_DEPTH) {

        // calculate next recursion prefix
        String nextRecursionPrefix = artifact.getRecursionPrefix + "-" +
                    Long.toString(artifact.getOffset) + "-" + 
                    artifact.getArtifactClass.toUpperCase();

        // recurse
        recurse(uncompressed.buffer, nextRecursionPrefix, depth + 1);
      }
    }
  }

  public String scan(String filename, long offset, byte[] buffer) {

    // class variables
    uncompressor = new edu.nps.deep.be_scan.Uncompressor();
    StringBuilder stringBuilder = new StringBuilder();
    this.filename = filename;

    // scanner
    edu.nps.deep.be_scan.Artifacts artifacts =
                                  new edu.nps.deep.be_scan.Artifacts();
    edu.nps.deep.be_scan.Scanner scanner =
        new edu.nps.deep.be_scan.Scanner(scanEngine, artifacts);
    scanner.scanSetup(filename, "");
 
    // scan
    String status = scanner.scanFinal(offset, "", buffer);
    if (status != "") {
      return ("Error in scan_stream: " + status);
    }

    // consume
    StringBuilder sb = new StringBuilder();
    while (!artifacts.empty()) {
      edu.nps.deep.be_scan.Artifact artifact = artifacts.get();

      // prepare for zip or gzip
      if (artifact.getArtifactClass() == "zip" ||
                        artifact.getArtifactClass() == "gzip") {

        // uncompress
        uncompressed = uncompressor.uncompress(buffer,
                          artifact.offset - offset);

        // no error and nothing uncompressed so disregard this artifact
        if (!uncompressed.getStatus() && !uncompressed.getBuffer()) {
          continue;
        }

        // set artifact text for this uncompression
        setCompressionText(artifact, uncompressed);

        // skip popular useless uncompressed data
        if (artifact.artifact == "8da7a0b0144fc58332b03e90aaf7ba25") {
          continue;
        }
      }

      // prepare for other artifact types as needed
      // none.

      // add the artifact
      stringBuilder.append(artifact.toString());
      stringBuilder.append("\n");

      // manage recursion
      if (artifact.getArtifactClass() == "zip" ||
                    artifact.getArtifactClass() == "gzip") {

        // calculate recursion prefix for first recursion depth
        String nextRecursionPrefix = Long.toString(artifact.getOffset() +
                   "-" + artifact.getArtifactClass.toUpperCase());

        // recurse
        recurse(uncompressed.buffer, nextRecursionPrefix, 1);
      }
    }
    return stringBuilder.toString();
  }
}

