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

import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;
import edu.uci.ics.hyracks.dataflow.common.data.accessors.ITupleReference;

/**
 * This operation callback allows for arbitrary actions to be taken while traversing
 * an index structure. The {@link ISearchOperationCallback} will be called on
 * all search operations for ordered indexes only.
 *
 * @author zheilbron
 */
public interface ISearchOperationCallback {

    /**
     * During an index search operation, this method will be called on tuples as they are
     * passed by with a search cursor. This call will be invoked while a leaf page is latched
     * and pinned. If the call returns false, then the page will be unlatched and unpinned
     * and {@link #reconcile(ITupleReference)} will be called with the tuple that was not proceeded
     * on.
     *
     * @param tuple
     *            the tuple that is being passed over by the search cursor
     * @return true to proceed otherwise false to unlatch and unpin, leading to reconciliation
     */
    public boolean proceed(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is only called on a tuple that was not 'proceeded' on
     * (see {@link #proceed(ITupleReference)}). This method allows an opportunity to reconcile
     * by performing any necessary actions before resuming the search (e.g. a try-lock may have
     * failed in the proceed call, and now in reconcile we should take a full (blocking) lock).
     *
     * @param tuple
     *            the tuple that failed to proceed
     */
    public void reconcile(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is called on a tuple that was reconciled on, but not found after
     * re-traversing. This method allows an opportunity to cancel some action that
     * was taken in {@link #reconcile(ITupleReference))}.
     *
     * @param tuple
     *            the tuple that was previously reconciled or proceeded
     */
    public void cancelReconcile(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is called on a tuple that was proceeded on to cancel any actions
     * that were taken in proceed(). This method allows an opportunity to cancel some
     * action that was taken in {@link #proceed(ITupleReference))}.
     *
     * @param tuple
     *            the tuple that was previously reconciled or proceeded
     */
    public void cancelProceed(ITupleReference tuple) throws HyracksDataException;

    /**
     * This method is only called on a tuple that was reconciled on, and found after
     * retraversing. This method allows an opportunity to do some subsequent action that was
     * taken in {@link #reconcile(ITupleReference))}.
     *
     * @param tuple
     *            the tuple that was previously reconciled
     */
    public void complete(ITupleReference tuple) throws HyracksDataException;
}
