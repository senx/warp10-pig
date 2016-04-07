package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class TestReduceWithLabels {

  public static String fill() {
    StringBuffer warpscript = new StringBuffer();

    warpscript.append("'app' ")
        .append("1 ->LIST ")
        .append("reducer.max ")
        .append("3 ->LIST ")
        .append("REDUCE ");

    return warpscript.toString();
  }

}
