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
package org.apache.hyracks.algebricks.common.utils;

public class Quintuple<T1, T2, T3, T4, T5> {
    public T1 first;
    public T2 second;
    public T3 third;
    public T4 fourth;
    public T5 fifth;

    public Quintuple(T1 first, T2 second, T3 third, T4 fourth, T5 fifth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
        this.fifth = fifth;
    }

    @Override
    public String toString() {
        return first + "," + second + ", " + third + ", " + fourth + ", " + fifth;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 149 + second.hashCode() * 71 + third.hashCode() * 31 + fourth.hashCode() * 17
                + fifth.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Quintuple<?, ?, ?, ?, ?>))
            return false;
        Quintuple<?, ?, ?, ?, ?> quinTuple = (Quintuple<?, ?, ?, ?, ?>) o;
        return first.equals(quinTuple.first) && second.equals(quinTuple.second) && third.equals(quinTuple.third)
                && fourth.equals(quinTuple.fourth) && fifth.equals(quinTuple.fifth);
    }

}
