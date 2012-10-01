/**
 * Copyright (C) 2009-2012 enStratus Networks Inc
 *
 * ====================================================================
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 * ====================================================================
 */

package org.dasein.cloud.cloudstack;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class Param implements Comparable<Param> {
    private String key   = null;
    private String value = null;
    
    public Param(@Nonnull String key, @Nullable String value) {
        this.key = key;
        this.value = value;
    }
    
    public int compareTo(@Nullable Param other) {
        if( other == null ) {
            return 1;
        }
        else if( other == this ) {
            return 0;
        }
        else {
            return getKey().toLowerCase().compareTo(other.getKey().toLowerCase());
        }
    }
    
    public @Nonnull String getKey() {
        return key;
    }
    
    public @Nullable String getValue() {
        return value;
    }
}
