/**
 * Copyright (C) 2009-2014 Dell, Inc.
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

import java.util.ArrayList;
import java.util.List;

/**
 * Description
 * <p>Created by stas: 24/09/2014 11:52</p>
 *
 * @author Stas Maksimov
 * @version 2014.08 initial version
 * @since 2014.08
 */
public final class Iterables {

    public static <T> List<T> toList(Iterable<T> iterable) {
        if( iterable == null ) {
            return new ArrayList<T>();
        }
        if( iterable instanceof List ) {
            return (List<T>) iterable;
        } else {
            List<T> list = new ArrayList<T>();
            for( T item : iterable) {
                list.add(item);
            }
            return list;
        }
    }

}
