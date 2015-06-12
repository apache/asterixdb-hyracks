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
package edu.uci.ics.hyracks.algebricks.common.utils;

public class Quadruple<T1, T2, T3, T4> {
    public T1 first;
    public T2 second;
    public T3 third;
    public T4 fourth;

    public Quadruple(T1 first, T2 second, T3 third, T4 fourth) {
        this.first = first;
        this.second = second;
        this.third = third;
        this.fourth = fourth;
    }

    @Override
    public String toString() {
        return first + "," + second + ", " + third + ", " + fourth;
    }

    @Override
    public int hashCode() {
        return first.hashCode() * 71 + second.hashCode() * 31 + third.hashCode() * 17 + fourth.hashCode();
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof Quadruple<?, ?, ?, ?>))
            return false;
        Quadruple<?, ?, ?, ?> quadRuple = (Quadruple<?, ?, ?, ?>) o;
        return first.equals(quadRuple.first) && second.equals(quadRuple.second) && third.equals(quadRuple.third)
                && fourth.equals(quadRuple.fourth);
    }

}
