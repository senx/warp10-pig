package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class TestMapperMean {

  public static String fill() {
    StringBuffer warpscript = new StringBuffer();

      warpscript.append("mapper.mean ");
      warpscript.append("0 ");
      warpscript.append("28175 ");
      warpscript.append("1 ");
      warpscript.append("5 ->LIST ");
      warpscript.append("MAP ");

    warpscript.append("TODOUBLE ");

    return warpscript.toString();
  }

}
