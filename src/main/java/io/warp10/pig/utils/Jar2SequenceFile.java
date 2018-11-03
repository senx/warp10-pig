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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.InputStream;
import java.net.URLEncoder;
import java.util.Enumeration;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Converts a Jar file into a Hadoop SequenceFile
 */
public class Jar2SequenceFile extends Configured implements Tool {
  
  @Override
  public int run(String[] args) throws Exception {
    //
    // Open JarFile
    //
    
    File jar = new File(args[0]);
    JarFile jarfile = new JarFile(jar);
    
    //
    // Create SequenceFile
    //
    
    //
    // Open output SequenceFile
    //
    
    Configuration conf = getConf();
    
    //
    // Open output file
    //
    
    FSDataOutputStream out = null;
    
    if ("-".equals(args[1])) {
      out = new FSDataOutputStream(System.out, null);
    }

    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
      SequenceFile.Writer.compression(CompressionType.BLOCK, new DefaultCodec()),
      SequenceFile.Writer.keyClass(BytesWritable.class),
      SequenceFile.Writer.valueClass(BytesWritable.class),
      null == out ? SequenceFile.Writer.file(new Path(args[args.length - 1])) : SequenceFile.Writer.stream(out));

    Enumeration<JarEntry> entries = jarfile.entries();
    
    byte[] buf = new byte[1024 * 1024];
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    
    long bytes = 0;
    int count = 0;
    
    long gnano = System.nanoTime();
    
    while(entries.hasMoreElements()) {
      long nano = System.nanoTime();
      JarEntry entry = entries.nextElement();
      
      // Skip directories
      if (entry.isDirectory()) {
        continue;
      }
      
      InputStream in = jarfile.getInputStream(entry);
      
      count++;
      
      String key = URLEncoder.encode(jar.getName(), "UTF-8").replaceAll("\\+", "%2B") + " " + entry.getName(); 
      System.out.print("Converting " + key + " ... ");

      baos.reset();
            
      long size = 0;
      
      while(true) {
        int len = in.read(buf);
        
        if (len < 0) {
          break;
        }     
        
        baos.write(buf, 0, len);
        size += len;
      }
            
      in.close();
      
      
      bytes += size;
      
      writer.append(new BytesWritable(key.getBytes("UTF-8")), new BytesWritable(baos.toByteArray()));
      nano = System.nanoTime() - nano;
      System.out.println(size + " bytes in " + (nano / 1000000.0D) + " ms");
    }
    
    jarfile.close();
    writer.close();
    
    gnano = System.nanoTime() - gnano;
    
    System.out.println("TOTAL: " + count + " entries (" + bytes + " bytes) in " + (gnano / 1000000.0D) + " ms.");
    return 0;
  }
  
  public static void main(String[] args) throws Exception {
    if (args.length < 2) {
      throw new RuntimeException("Usage: Jar2SequenceFile [options] jarfile seqfile");
    }
    
    int exitCode = ToolRunner.run(new Jar2SequenceFile(), args);
  }
}
