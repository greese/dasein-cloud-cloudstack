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

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Description
 * <p>Created by stas: 24/09/2014 11:42</p>
 *
 * @author Stas Maksimov
 * @version 2014.08 initial version
 * @since 2014.08
 */
public class Tuple<K, V> implements Comparable<Tuple<K, V>> {
    private K key;
    private V value;

    public Tuple( @Nonnull K key, @Nullable V value ) {
        this.key = key;
        this.value = value;
    }

    public @Nonnull K getKey() {
        return key;
    }

    public @Nullable V getValue() {
        return value;
    }

    public void setValue(@Nullable V value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return key.toString() + ":" + value.toString();
    }

    @Override
    public int compareTo( Tuple<K, V> other ) {
        if( other == null ) {
            return 1;
        }
        else if( other == this ) {
            return 0;
        }
        else {
            if( getKey() instanceof String && other.getKey() instanceof String ) {
                return ( ( String ) getKey() ).compareToIgnoreCase(( String ) other.getKey());
            }
            else if( getKey() instanceof Comparable ) {
                return ( ( Comparable ) getKey() ).compareTo(other.getKey());
            }
            else {
                if( getKey().equals(other.getKey()) ) {
                    return 0;
                }
                else {
                    return 1; // not really, but nothing else I can do
                }
            }
        }
    }
}