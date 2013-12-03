/*
 * Copyright 2013 NGDATA nv
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.ngdata.hbaseindexer;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import com.google.common.collect.Maps;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.junit.Test;

public class ConfigureUtilTest {
    
    static class ConfigurableObject implements Configurable {

        byte[] params;
        
        @Override
        public void configure(byte[] config) {
            this.params = config;
        }
        
    }

    @Test
    public void testConfigure_ConfigurableObject() {
        ConfigurableObject configurable = new ConfigurableObject();
        Map<String, String> params = Maps.newHashMap();
        params.put("key", "value");

        ConfigureUtil.configure(configurable, ConfigureUtil.mapToJson(params));
        
        assertEquals(params, configurable.params);
    }
    
    
    @Test
    public void testConfigure_NotConfigurableObject() {
        Object o = new Object();
        Map<String, String> params = Maps.newHashMap();
        params.put("key", "value");
        
        ConfigureUtil.configure(o, ConfigureUtil.mapToJson(params));
        
        // Nothing to check
    }

}
