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
package edu.uci.ics.hyracks.dataflow.common.data.partition;

import java.util.Random;

import edu.uci.ics.hyracks.api.comm.IFrameTupleAccessor;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputer;
import edu.uci.ics.hyracks.api.dataflow.value.ITuplePartitionComputerFactory;
import edu.uci.ics.hyracks.api.exceptions.HyracksDataException;

public class RandomPartitionComputerFactory implements
		ITuplePartitionComputerFactory {

	private static final long serialVersionUID = 1L;

	private final int domainCardinality;

	public RandomPartitionComputerFactory(int domainCardinality) {
		this.domainCardinality = domainCardinality;
	}

	@Override
	public ITuplePartitionComputer createPartitioner() {
		return new ITuplePartitionComputer() {

			private final Random random = new Random();

			@Override
			public int partition(IFrameTupleAccessor accessor, int tIndex,
					int nParts) throws HyracksDataException {
				return random.nextInt(domainCardinality);
			}
		};
	}

}
