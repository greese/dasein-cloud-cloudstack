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

package org.dasein.cloud.cloudstack.identity;

import org.dasein.cloud.cloudstack.CSCloud;
import org.dasein.cloud.identity.AbstractIdentityServices;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Implements the identity services for CSCloud.
 * @author George Reese (george.reese@imaginary.com)
 * @since 2012.02
 * @version 2012.02
 */
public class CSIdentityServices extends AbstractIdentityServices {
    private CSCloud provider;
    
    public CSIdentityServices(@Nonnull CSCloud provider) { this.provider = provider; }
    
    public @Nullable Keypair getShellKeySupport() {
        return new Keypair(provider);
    }
}
