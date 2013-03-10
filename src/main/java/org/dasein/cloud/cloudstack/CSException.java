/**
 * Copyright (C) 2009-2013 enstratius, Inc.
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

import org.dasein.cloud.CloudErrorType;
import org.dasein.cloud.CloudException;

import javax.annotation.Nonnull;

public class CSException extends CloudException {
    public CSException(@Nonnull Throwable exception) {
        super(CloudErrorType.GENERAL, 593, "593", exception.getMessage());
    }

    public CSException(@Nonnull CSMethod.ParsedError e) {
        super(CloudErrorType.GENERAL, e.code, String.valueOf(e.code), e.message);
    }
    
    public CSException(@Nonnull CloudErrorType type, @Nonnull CSMethod.ParsedError e) {
        super(type, e.code, String.valueOf(e.code), e.message);        
    }
}
