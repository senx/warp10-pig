package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class TestBucketize {

  public static String fill() {
    StringBuffer warpscript = new StringBuffer();

    warpscript.append("bucketizer.sum ")
        .append("0 ")
        .append("3600000000 ")
        .append("20 ")
        .append("5 ->LIST ")
        .append("BUCKETIZE ");

    return warpscript.toString();
  }

}
