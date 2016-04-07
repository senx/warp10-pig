package io.warp10.pig;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.protocol.TCompactProtocol;
import org.junit.Test;

import java.io.IOException;

public class TestSFRead {

  @Test
  public void testRead() throws IOException {

    //String file = "/mnt/czd/workspace/pig-arinc/MERGED_DAR.DAT_256.seq";
    String file  = "/mnt/czd/data/debs.seq";

    long nano = System.nanoTime();
    int gts = 0;
    long bytes = 0L;

    Configuration conf = new Configuration();

    BytesWritable key = new BytesWritable();
    BytesWritable value = new BytesWritable();

    TDeserializer deserializer = new TDeserializer(
        new TCompactProtocol.Factory());

    SequenceFile.Reader.Option optPath = SequenceFile.Reader
        .file(new Path(file));

    SequenceFile.Reader reader = null;

    //try {
      reader = new SequenceFile.Reader(conf, optPath);

      while (reader.next(key, value)) {
        gts++;
        //GTSWrapper wrapper = new GTSWrapper();
        //deserializer.deserialize(wrapper, key.copyBytes());
        //wrapper.setEncoded(value.copyBytes());
        //bytes += value.getLength() + key.getLength();
      }
    //} catch (TException te) {
    //  throw new IOException(te);
    //}
    reader.close();

    nano = System.nanoTime() - nano;

    System.out.println("Loaded " + gts + " GTS (" + bytes + " bytes) in " + (nano / 1000000.0D) + " ms.");
  }

}
