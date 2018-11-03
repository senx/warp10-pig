//
//   Copyright 2018  SenX S.A.S.
//
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.
//
package io.warp10.pig.utils;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSSplitter;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.script.WarpScriptException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import org.apache.pig.data.Tuple;
import org.apache.pig.data.TupleFactory;

import com.google.common.base.Charsets;

public class WarpScriptUtils {

  //
  // Chunk label name
  //
  public static final String chunkIdLabelName = "chunkId";

  protected WarpScriptUtils() { }

  /**
   * Parse Warpscript file and return its content as String
   * @param warpscriptName name of the script to parse
   * @return String
   */
  public static String parseScript(String warpscriptName) throws IOException, WarpScriptException {

    //
    // Load the Warpscript file
    //
    StringBuffer scriptSB = new StringBuffer();
    InputStream fis = null;
    try {

      fis = WarpScriptUtils.class.getClassLoader().getResourceAsStream(warpscriptName);
      
      if (null == fis) {
        throw new IOException("Path '" + warpscriptName + "' not found, you may have forgotten to REGISTER your script.");
      }
      
      BufferedReader br = new BufferedReader(new InputStreamReader(fis, Charsets.UTF_8));

      while (true) {
        String line = br.readLine();
        if (null == line) {
          break;
        }
        scriptSB.append(line).append("\n");
      }
    } catch (IOException ioe) {
      throw new IOException("Warpscript file not found.", ioe);
    } finally {
      if (null != fis) {
        fis.close();
      }
    }

    return scriptSB.toString();

  }

  /**
   * Return a Tuple that represents the stack
   *
   * @param stack (List)
   * @return tuple - stack: (top, level1, ...)
   *
   */
  public static Tuple stackToPig(List<Object> stack) throws IOException {

    Tuple stackAstuple = TupleFactory.getInstance().newTuple(stack.size());

    //
    // Take object in reverse order to really represents the Stack
    //

    for (int level = 0; level < stack.size(); level++) {
      //
      // Cast object to Pig type
      //

      Object pigObj = PigUtils.toPig(stack.get(level));

      stackAstuple.set(level, pigObj);
    }

    return stackAstuple;

  }

  /**
   * chunk a GTSWrapper instance
   *
   * @param gtsWrapper
   * @return a list of chunks (GTSWrapper instances)
   * @throws java.io.IOException
   */
  public static List<GTSWrapper> chunk(GTSWrapper gtsWrapper, long clipFrom, long clipTo, long chunkwidth) throws IOException {

    //
    // Chunk this wrapper
    //

    GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

    long tsboundary = 0L;
    String fileprefix = "warp10-pig";
    long maxsize = Long.MAX_VALUE;
    long maxgts = Long.MAX_VALUE;
    float lwmratio = 0.0F;

    List<GTSWrapper> gtsWrappers = new ArrayList<GTSWrapper>();

    try {

      List<GTSWrapper> chunkWrappers = GTSSplitter
          .chunk(decoder, clipFrom, clipTo, tsboundary,
              chunkwidth, chunkIdLabelName, fileprefix, maxsize, maxgts,
              lwmratio);

      return chunkWrappers;

    } catch(WarpScriptException ee) {
      throw new IOException(ee);
    }
  }

}
