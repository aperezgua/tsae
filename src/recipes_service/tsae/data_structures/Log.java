/*
* Copyright (c) Joan-Manuel Marques 2013. All rights reserved.
* DO NOT ALTER OR REMOVE COPYRIGHT NOTICES OR THIS FILE HEADER.
*
* This file is part of the practical assignment of Distributed Systems course.
*
* This code is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This code is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.
*
* You should have received a copy of the GNU General Public License
* along with this code.  If not, see <http://www.gnu.org/licenses/>.
*/

package recipes_service.tsae.data_structures;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias December 2012
 *
 */
public class Log implements Serializable {
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;

	/**
	 * Object to synchronized updates and reads
	 */
	private Object lock = new Serializable() {
		private static final long serialVersionUID = 6453722610536757895L;

	};

	/**
	 * This class implements a log, that stores the operations received by a client. They are stored in a
	 * ConcurrentHashMap (a hash table), that stores a list of operations for each member of the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log = new ConcurrentHashMap<String, List<Operation>>();

	public Log(List<String> participants) {
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext();) {
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * Constructor privado para clonar
	 * 
	 * @param log
	 */
	private Log(ConcurrentHashMap<String, List<Operation>> log) {
		this.log = new ConcurrentHashMap<String, List<Operation>>();
		for (String key : log.keySet()) {
			this.log.put(key, new ArrayList<Operation>(log.get(key)));
		}
	}

	/**
	 * inserts an operation into the log. Operations are inserted in order. If the last operation for the user is not
	 * the previous operation than the one being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public boolean add(Operation op) {
		synchronized (lock) {
			lsim.log(Level.TRACE, "Inserting into Log the operation: " + op);
			if (op != null) {
				// Sync by hostID

				List<Operation> operations = log.get(op.getTimestamp().getHostid());
				if (operations == null) {
					operations = new ArrayList<Operation>();
					log.put(op.getTimestamp().getHostid(), operations);
				}
				Operation lastOperation = null;
				if (!operations.isEmpty()) {
					lastOperation = operations.get(operations.size() - 1);
				}
				if (lastOperation == null || lastOperation.getTimestamp().compare(op.getTimestamp()) < 0) {
					operations.add(op);
					return true;
				}

			}
		}
		return false;
	}

	/**
	 * Checks the received summary (sum) and determines the operations contained in the log that have not been seen by
	 * the proprietary of the summary. Returns them in an ordered list.
	 * 
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum) {

		List<Operation> operations = new ArrayList<Operation>();
		synchronized (lock) {
			for (String key : getSortedKeys()) {

				Timestamp lastTimestamp = sum.getLast(key);
				for (Operation op : log.get(key)) {
					if (lastTimestamp == null || op.getTimestamp().compare(lastTimestamp) > 0) {
						operations.add(op);
					}
				}

			}
		}
		//
		lsim.log(Level.TRACE, "[Log.listNewer] [" + Thread.currentThread().getName() + "]: " + operations);
		return operations;
	}

	/**
	 * Removes from the log the operations that have been acknowledged by all the members of the group, according to the
	 * provided ackSummary.
	 * 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack) {
		synchronized (lock) {
			TimestampVector ackVector = ack.minTimestampVector();

			Enumeration<String> keys = log.keys();
			while (keys.hasMoreElements()) {
				String key = keys.nextElement();
				// sync block by String host

				Timestamp timestamp = ackVector.getLast(key);

				List<Operation> operationsToremove = new ArrayList<Operation>();
				List<Operation> logOperations = this.log.get(key);
				for (Operation op : logOperations) {
					if (op.getTimestamp().compare(timestamp) <= 0) {
						operationsToremove.add(op);
					}
				}
				logOperations.removeAll(operationsToremove);
			}
		}
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		Log other = (Log) obj;
		return this.log.equals(other.log);
	}

	public Log clone() {
		synchronized (lock) {
			return new Log(log);
		}
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name = "";
		for (String key : getSortedKeys()) {
			List<Operation> sublog = log.get(key);
			for (ListIterator<Operation> en2 = sublog.listIterator(); en2.hasNext();) {
				name += en2.next().toString() + "\n";
			}
		}

		return name;
	}

	/**
	 * Method to return log keys sorted by name
	 * 
	 * @return
	 */
	private List<String> getSortedKeys() {
		List<String> sortedKeys = new ArrayList<String>();
		for (Enumeration<String> en = log.keys(); en.hasMoreElements();) {
			sortedKeys.add(en.nextElement());
		}
		Collections.sort(sortedKeys);
		return sortedKeys;
	}
}