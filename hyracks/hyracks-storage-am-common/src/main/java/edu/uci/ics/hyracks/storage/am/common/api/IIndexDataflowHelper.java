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
package edu.uci.ics.hyracks.storage.am.common.api;

import edu.uci.ics.hyracks.api.context.IHyracksTaskContext;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.api.io.FileReference;

public interface IIndexDataflowHelper {
    public void create() throws HyracksDataException;

    public void close() throws HyracksDataException;

    public void open() throws HyracksDataException;

    public void destroy() throws HyracksDataException;

    public IIndex getIndexInstance();

    public FileReference getFileReference();

    public long getResourceID() throws HyracksDataException;

    public IHyracksTaskContext getTaskContext();

    public boolean needKeyDuplicateCheck();
}
