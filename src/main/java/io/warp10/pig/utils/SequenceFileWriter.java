package io.warp10.pig.utils;

import java.io.IOException;
import java.util.List;

import io.warp10.continuum.store.thrift.data.GTSWrapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;

import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.pig.StoreFunc;
import org.apache.pig.data.DataByteArray;
import org.apache.pig.data.Tuple;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;

public class SequenceFileWriter extends StoreFunc {

  private String storeLocation;
  private RecordWriter writer;
  private Job job;

  private Class<? extends WritableComparable> keyClass;
  private Class<? extends Writable> valueClass;


  @SuppressWarnings({ "unchecked", "rawtypes" })
  public SequenceFileWriter(String... args)
      throws IOException {
    try {
      this.keyClass = (Class<? extends WritableComparable>) Class.forName(args[0]);
      this.valueClass = (Class<? extends Writable>) Class.forName(args[1]);
    }
    catch (Exception e) {
      throw new IOException("Invalid key/value type", e);
    }
  }


  @Override
  public OutputFormat getOutputFormat() throws IOException {
    return new SequenceFileOutputFormat()
    {
      public RecordWriter getRecordWriter( TaskAttemptContext context )
          throws IOException, InterruptedException
      {
        Configuration conf = context.getConfiguration();

        CompressionCodec codec = new DefaultCodec();
        SequenceFile.CompressionType compressionType = SequenceFile.CompressionType.BLOCK;

        // get the path of the temporary output file
        Path file = getDefaultWorkFile(context, "");

        final SequenceFile.Writer out = SequenceFile.createWriter(conf,
            SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK, new DefaultCodec()),
            SequenceFile.Writer.keyClass(BytesWritable.class),
            SequenceFile.Writer.valueClass(BytesWritable.class),
            SequenceFile.Writer.file(file));

        return new RecordWriter() {

          public void write( Object key, Object value)
              throws IOException {
            out.append(key, value);
          }

          public void close(TaskAttemptContext context) throws IOException {
            out.close();
          }
        };
      }
    };
  }

  @Override
  public void setStoreLocation(String location, Job job) throws IOException {
    this.storeLocation = location;
    this.job = job;
    this.job.setOutputKeyClass(keyClass);
    this.job.setOutputValueClass(valueClass);
    FileOutputFormat.setOutputPath(job, new Path(location));
    FileOutputFormat.setCompressOutput(this.job, true);
    FileOutputFormat.setOutputCompressorClass(this.job, org.apache.hadoop.io.compress.DefaultCodec.class);
  }

  @Override
  public void prepareToWrite(RecordWriter writer) throws IOException {
    this.writer = writer;
  }

  /**
   *
   * @param input : tuple with one field (encoded : GTSWrapper)
   * @throws IOException
   */
  @Override
  public void putNext(Tuple input) throws IOException {
    try {

      if (input.size() != 1) {
        throw new IOException("Invalid input, expecting Tuple: (gtswrapper)");
      }

      if (!(input.get(0) instanceof DataByteArray)) {
        throw new IOException("Invalid input, expecting Tuple: (gtswrapper)");
      }

      DataByteArray data = (DataByteArray) input.get(0);

      //
      // Deserialize
      //

      GTSWrapper wrapper = new GTSWrapper();

      TDeserializer deser = new TDeserializer(new TCompactProtocol.Factory());

      try {
        deser.deserialize(wrapper, data.get());
      } catch (TException te) {
        throw new IOException(te);
      }

      List<DataByteArray> kv = GTSWrapperPigHelper.gtsWrapperToSF(wrapper);

      writer.write(new BytesWritable(kv.get(0).get()), new BytesWritable(kv.get(1).get()));
    } catch (InterruptedException e) {
      throw new IOException(e);
    }

  }
}