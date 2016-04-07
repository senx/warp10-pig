package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class TestFor {

  public static String fill() {
    StringBuffer warpscript = new StringBuffer();

    warpscript.append("VALUES FLATTEN LIST-> ")
        .append("'size' STORE ")
        .append("0 'sum' STORE ")
        .append("1 $size <% DROP $sum + 'sum' STORE %> FOR ")
        .append("$sum $size TODOUBLE / ");

    return warpscript.toString();
  }

}
