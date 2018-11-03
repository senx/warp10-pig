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

import io.warp10.WarpConfig;
import io.warp10.script.WarpScriptLib;

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
    
    //
    // Register out Pig extension
    //
    
    WarpScriptLib.register(new PigWarpScriptExtension());
  }
  
  public static boolean ensureConfig() {
    return WarpConfig.isPropertiesSet();
  }
}
