package io.warp10.pig.warpscript;

/**
 * Created by dav on 28/10/14.
 */
public class Chunk {

  /**
   * Chunk GTS
   * @param chunkLabelName
   * @param chunkwidth
   * @return
   */
  public static String fill(final String chunkLabelName, Long chunkwidth) {
    StringBuffer warpscript = new StringBuffer();

    //
    // 5: lastchunk : depends of the most recent chunk. 0 otherwise
    // 4: chunkwidth : width of each chunk in time units
    // 3: chunkcount : could be set to 0
    // 2: chunklabel : Use an id not already used in the GTS - chunkId
    // 1: keepempty : false if we want delete empty chunk - Warning : some function like test if null during a reduce could fail
    //

    warpscript.append("0 ")
        // 24 hours : 8640000000
        .append(chunkwidth + " ")
        .append("0 ")
        .append("'").append(chunkLabelName).append("' ")
        .append("true ")
        .append("CHUNK ");

    return warpscript.toString();

  }

}
