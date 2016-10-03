package io.warp10.pig;

import io.warp10.WarpConfig;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

public class PigWarpConfig {
    
  static {
    if (!WarpConfig.isPropertiesSet()) {
      String conf = System.getProperty(WarpConfig.WARP10_CONFIG);
      
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
