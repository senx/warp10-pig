package io.warp10.pig;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.zip.GZIPOutputStream;

import io.warp10.continuum.gts.GTSWrapperHelper;
import org.apache.pig.EvalFunc;
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

  private String params = null;
  
  public GTSUpload() {  }
  
  public GTSUpload(String... args) {
    this.params = args[0];
  }

  /**
   * Dump a GTS (toString)
   *
   * @param input Tuple containing the GTSWrapper to dump
   * @return a Bag of String representation of each datapoint
   * @throws java.io.IOException
   */
  @Override
  public Long exec(Tuple input) throws IOException {

    DataByteArray serialized = null;
    String params = this.params;

    reporter.progress();

    if (1 == input.size()) {
      serialized = (DataByteArray) input.get(0);      
    } else if (2 == input.size()) {
      params = input.get(0).toString();
      serialized = (DataByteArray) input.get(1);
    }

    //
    // Extract parameters
    //
    
    String[] tokens = params.split(" ");
    
    int i = 0;
    
    String endpoint = null;
    String token = null;
    boolean gzip = false;
    
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
      i++;
    }
    
    HttpURLConnection conn = null;
    
    long count = 0L;
    
    try {
      conn = (HttpURLConnection) new URL(endpoint).openConnection();
      conn.setRequestMethod("POST");
      conn.setDoOutput(true);
      conn.setDoInput(true);
      conn.setRequestProperty("X-CityzenData-Token", token);
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
      
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());

      GTSWrapper gtsWrapper = new GTSWrapper();

      try {
        deserializer.deserialize(gtsWrapper, (serialized.get()));
      } catch (TException te) {
        throw new IOException(te);
      }

      Metadata metadataChunk = new Metadata(gtsWrapper.getMetadata());

      GTSDecoder decoder = GTSWrapperHelper.fromGTSWrapperToGTSDecoder(gtsWrapper);

      StringBuilder metasb = new StringBuilder();
      GTSHelper.metadataToString(metasb, metadataChunk.getName(), metadataChunk.getLabels());
    
      boolean first = true;
      
      while(decoder.next()) {
        reporter.progress();
        
        if (!first) {
          pw.print("=");
          pw.println(GTSHelper.tickToString(null, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue()));
        } else {
          pw.println(GTSHelper.tickToString(metasb, decoder.getTimestamp(), decoder.getLocation(), decoder.getElevation(), decoder.getValue()));
          first = false;
        }
        count++;
      }

      pw.close();
      
      int respcode = conn.getResponseCode();
      
      if (HttpURLConnection.HTTP_OK != respcode) {
        throw new IOException(conn.getResponseMessage());
      }
    } finally {
      if (null != conn) { conn.disconnect(); }
    }
    
    return count;
  }

  @Override
  public Schema outputSchema(Schema input) {

    Schema.FieldSchema fieldSchema = new Schema.FieldSchema("count", DataType.LONG);

    return new Schema(fieldSchema);
  }
}