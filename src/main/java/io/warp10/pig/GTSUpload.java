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
package io.warp10.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.RateLimiter;
import io.warp10.continuum.gts.GTSWrapperHelper;
import io.warp10.continuum.store.Constants;
import io.warp10.crypto.OrderPreservingBase64;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.DataBag;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.logicalLayer.schema.Schema;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

import io.warp10.continuum.gts.GTSDecoder;
import io.warp10.continuum.gts.GTSHelper;
import io.warp10.continuum.store.thrift.data.GTSWrapper;
import io.warp10.continuum.store.thrift.data.Metadata;

/**
 * Upload a GTS
 */
public class GTSUpload extends EvalFunc<Long> {

  private static final String usageStr = "bytearray(GTSWrapper) OR chararray(Input Format) OR bag:{(bytearray)} OR bag:{(chararray)}";

  //
  // Limit data upload
  //

  private RateLimiter rateLimiter = null;

  private String params = null;

  public GTSUpload() { }
  
  public GTSUpload(String... args) {
    this.params = args[0];
  }

  /**
   * Dump a GTS (toString)
   *
   * @param input Tuple containing the GTSWrapper or String (Warp10 InputFormat)
   * @return the number of datapoints uploaded
   * @throws java.io.IOException
   */
  @Override
  public Long exec(Tuple input) throws IOException {

    //
    // HttpURLConnection to Warp (update)
    //

    HttpURLConnection conn = null;

    //
    // Number of datapoints uploaded
    //

    long count = 0L;

    //
    // GTSWrapper
    //

    List<DataByteArray> serialized = new ArrayList<>();

    //
    // We can provided GTS String representation directly (InputFormat)
    //

    List<String> gtsAsString = new ArrayList<>();

    String params = this.params;

    if (2 == input.size()) {
      params = input.get(0).toString();
    }

    if ((0 == input.size()) || (input.size() > 2)) {
      throw new IOException("Invalid input, should be a tuple containing a GTS or parameters and GTS.");
    }

    //
    // Extract parameters
    //

    String endpoint = null;
    String token = null;
    boolean gzip = false;
    String header = null;
    String rateLimit = null;

    String[] tokens = params.split(" ");

    int i = 0;

    while (i < tokens.length) {
      if ("-t".equals(tokens[i])) {
        i++;
        token = tokens[i];
      }
      if ("-u".equals(tokens[i])) {
        i++;
        endpoint = tokens[i];
      }
      if ("-c".equals(tokens[i])) {
        gzip = true;
      }
      if ("-H".equals(tokens[i])) {
        i++;
        header = tokens[i];
      }
      /**
       * rate limit (double): datapoints/second
       */
      if ("-l".equals(tokens[i])) {
        i++;
        /**
         * Ignore it if RateLimiter has been set
         */
        if (null == this.rateLimiter) {
          this.rateLimiter = RateLimiter.create(Double.valueOf(tokens[i]));
        }
      }
      i++;
    }

    try {
      conn = (HttpURLConnection) new URL(endpoint).openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestProperty(null == header ? Constants.HTTP_HEADER_TOKEN_DEFAULT : header, token);
      conn.setChunkedStreamingMode(65536);

      if (gzip) {
        conn.setRequestProperty("Content-Type", "application/gzip");
      }

      conn.connect();

      OutputStream out = conn.getOutputStream();

      if (gzip) {
        out = new GZIPOutputStream(out);
      }

      PrintWriter pw = new PrintWriter(out);

      Object value = input.get(input.size() - 1);

      if (DataType.BYTEARRAY == DataType.findType(value)) {
        count += uploadGtsWrapper((DataByteArray) value, pw);
      } else if (DataType.CHARARRAY == DataType.findType(value)) {

        String valueStr = (String) value;

        //
        // If the String contains a '/' this is Input Format
        // Otherwise this a GTSWrapper wrapped as OrderPreservingBase64 (no '/')
        //

        if (valueStr.contains("/")) {
          pw.println(valueStr);
          count++;
        } else {
          byte[] bytes = OrderPreservingBase64.decode(valueStr.getBytes(Charsets.US_ASCII));
          DataByteArray wrapperEncoded = new DataByteArray(bytes);
          count += uploadGtsWrapper(wrapperEncoded, pw);
        }

      } else if (DataType.BAG == DataType.findType(input.get(input.size() - 1))) {
        DataBag inbag = (DataBag)input.get(0);
        Iterator<Tuple> iterMain = inbag.iterator();

        while (iterMain.hasNext()) {
          reporter.progress();

          //
          // One tuple with one field (bytearray or chararray)
          //

          Tuple inTuple = iterMain.next();
          Object inValue = inTuple.get(0);

          if (DataType.BYTEARRAY == DataType.findType(inValue)) {
            count += uploadGtsWrapper((DataByteArray) inValue,pw);
          } else if (DataType.CHARARRAY == DataType.findType(inValue)) {

            String inValueStr = (String) inValue;

            if (inValueStr.contains("/")) {
              // Input Format
              pw.println(inValueStr);
              count++;
            } else {
              byte[] bytes = OrderPreservingBase64.decode(inValueStr.getBytes(Charsets.US_ASCII));
              DataByteArray wrapperEncoded = new DataByteArray(bytes);
              count += uploadGtsWrapper(wrapperEncoded, pw);
            }

          } else {
            throw new IOException("Invalid input: " + usageStr);
          }
        }
      } else {
        throw new IOException("Invalid input: " + usageStr);
      }

      pw.close();
      
      int respcode = conn.getResponseCode();
      
      if (HttpURLConnection.HTTP_OK != respcode) {
        throw new IOException("HTTP code: " + respcode + " - " + conn.getResponseMessage());
      }
    } finally {
      if (null != conn) { conn.disconnect(); }
    }
    
    return count;
  }

  /**
   * Upload a GTSWrapper through the PrintWriter (onto HTTP OutputStream)
   * Returns the number of datapoints uploaded
   *
   * @param wrapperEncoded
   * @param printWriter
   * @return long
   * @throws IOException
   */
  protected long uploadGtsWrapper(DataByteArray wrapperEncoded, PrintWriter printWriter) throws IOException {
    long count = 0L;

    TDeserializer deserializer = new TDeserializer(
        new TCompactProtocol.Factory());

    GTSWrapper gtsWrapper = new GTSWrapper();

    try {
      deserializer.deserialize(gtsWrapper, (wrapperEncoded.get()));
    } catch (TException te) {
      throw new IOException(te);
    }

    Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());

    GTSDecoder decoder = GTSWrapperHelper
        .fromGTSWrapperToGTSDecoder(gtsWrapper);

    StringBuilder metasb = new StringBuilder();
    GTSHelper.metadataToString(metasb, metadataChunk.getName(),
        metadataChunk.getLabels());

    boolean first = true;

    while (decoder.next()) {
      reporter.progress();

      if (null != rateLimiter) {
        this.rateLimiter.acquire(1);
      }

      if (!first) {
        printWriter.print("=");
        printWriter.println(GTSHelper.tickToString(null, decoder.getTimestamp(),
            decoder.getLocation(), decoder.getElevation(),
            decoder.getValue()));
      } else {
        printWriter.println(GTSHelper.tickToString(metasb, decoder.getTimestamp(),
            decoder.getLocation(), decoder.getElevation(),
            decoder.getValue()));
        first = false;
      }
      count++;
    }
    return count;
  }

  @Override
  public Schema outputSchema(Schema input) {

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("count", DataType.LONG);

    return new Schema(fieldSchema);
  }
}