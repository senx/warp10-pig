package io.warp10.pig.warpscript;

import java.util.List;

/**
 * Created by dav on 28/10/14.
 */
public class Apply {

  /**
   *
   * @param opName
   * @param labels
   * @return
   */
  public static String fill(String opName, List<String> labels) {
    StringBuffer warpscript = new StringBuffer();

    if ((null != labels) || (labels.size() > 0)) {
      // ex : 'app' 'foo'

      int nbLabels = labels.size();

      StringBuffer labelsStr = new StringBuffer();
      for (String label : labels) {
        labelsStr.append("'" + label + "' ");
      }
      warpscript.append(labelsStr.toString())
          .append(nbLabels + " ->LIST ");
    } else {
      warpscript.append("[] ");
    }
        // Ex : op.add
    warpscript.append(opName + " ")
        .append("3 ->LIST ")
        .append("APPLY ");

    return warpscript.toString();
  }

}
