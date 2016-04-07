package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class TestFilterByLabels {

  public static String fill() {
    StringBuffer warpscript = new StringBuffer();

      //
      warpscript.append("[] ");
      warpscript.append("'producer' '=3aacec85-c9bd-40f8-b0f0-95c9d2bde285' ");
      warpscript.append("2 ->MAP ");
      warpscript.append("filter.bylabels ");
      warpscript.append("3 ->LIST ");
      warpscript.append("FILTER ");


    return warpscript.toString();
  }

}
