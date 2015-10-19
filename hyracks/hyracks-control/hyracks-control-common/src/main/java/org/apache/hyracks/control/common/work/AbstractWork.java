/*
 * Copyright 2009-2013 by The Regents of the University of California
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * you may obtain a copy of the License from
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.uci.ics.hyracks.control.common.work;

import java.util.logging.Level;

public abstract class AbstractWork implements Runnable {
    public Level logLevel() {
        return Level.INFO;
    }

    public String getName() {
        final String className = getClass().getName();
        final int endIndex = className.endsWith("Work") ? className.length() - 4 : className.length();
        return className.substring(className.lastIndexOf('.') + 1, endIndex);
    }

    @Override
    public String toString() {
        return getName();
    }
}
