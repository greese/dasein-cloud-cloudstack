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

/**
 * Represents meaningful differences in CloudStack versions. Minor changes that don't impact Dasein functionality
 * are not represented.
 */
public enum CSVersion {
    CS21, CS22, CS3;

    public boolean greaterThan(@Nonnull CSVersion control) {
        return (ordinal() > control.ordinal());
    }
}
