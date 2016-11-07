package io.warp10.pig;

import io.warp10.WarpConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.util.Map.Entry;

import org.apache.pig.impl.util.UDFContext;

public class PigWarpConfig {
    
  static {
    if (!WarpConfig.isPropertiesSet()) {
      
      String conf = UDFContext.getUDFContext().getClientSystemProps().getProperty(WarpConfig.WARP10_CONFIG);
      
      InputStream config = null;
      
      if (null != conf) {
        config = PigWarpConfig.class.getClassLoader().getResourceAsStream(conf);
      }
      
      if (null != config) {
        InputStreamReader reader = new InputStreamReader(config);
        try {
          WarpConfig.safeSetProperties(reader);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      } else {
        try {
          for (Entry<Object,Object> entry: UDFContext.getUDFContext().getClientSystemProps().entrySet()) {
            System.setProperty(entry.getKey().toString(), entry.getValue().toString());
          }
          WarpConfig.safeSetProperties((Reader) null);
        } catch (IOException ioe) {
          throw new RuntimeException(ioe);
        }
      }
    }
  }
  
  public static boolean ensureConfig() {
    return WarpConfig.isPropertiesSet();
  }
}
