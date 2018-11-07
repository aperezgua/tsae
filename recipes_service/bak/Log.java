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
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

//LSim logging system imports sgeag@2017
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;
import recipes_service.data.Operation;

/**
 * @author Joan-Manuel Marques, Daniel Lázaro Iglesias
 * December 2012
 *
 */
public class Log implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -4864990265268259700L;
	/**
	 * This class implements a log, that stores the operations
	 * received  by a client.
	 * They are stored in a ConcurrentHashMap (a hash table),
	 * that stores a list of operations for each member of 
	 * the group.
	 */
	private ConcurrentHashMap<String, List<Operation>> log= new ConcurrentHashMap<String, List<Operation>>();  

	/**
	 * Semaforo que bloquea la inserción de una operación en el log hasta
	 * que termina para solucionar la concurrencia de varios procesos 
	 */
	private final Semaphore available = new Semaphore(1, true);
	
	public Log(List<String> participants){
		// create an empty log
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			log.put(it.next(), new Vector<Operation>());
		}
	}

	/**
	 * inserts an operation into the log. Operations are 
	 * inserted in order. If the last operation for 
	 * the user is not the previous operation than the one 
	 * being inserted, the insertion will fail.
	 * 
	 * @param op
	 * @return true if op is inserted, false otherwise.
	 */
	public boolean add(Operation op){
		lsim.log(Level.TRACE, "Inserting into Log the operation: "+op);
		
		try {
			// Bloqueamos el acceso a la función add por la concurrencia
			available.acquire();

			
			Timestamp timestampOperation = op.getTimestamp();
			String hostId = timestampOperation.getHostid();
			
			// Recuperamos las operaciones que tenemos en el log del servidor 
			List<Operation> hostOperations = log.get(hostId);
			
			// Recuperamos el número de operaciones que tenemos de ese servidor
			int sizeHostOperation = hostOperations.size();
			
			// Si no hay ninguna operación de este servidor, le añadimos la recibida
			if(sizeHostOperation == 0)
			{
				log.get(hostId).add(op);
			}
			else
			{
				// Recuperamos la última operación registrada en nuestro log de dicho servidor
				Operation lastOperation = hostOperations.get(sizeHostOperation - 1);
				
				// Comparamos los timestamp de la operación recibida y de la última de ese host
				// Si es posterior a la última se añade a su log de operaciones
				if (timestampOperation.compare(lastOperation.getTimestamp()) > 0) 
				{
					log.get(hostId).add(op);
				}
				else
				{
					return false;
				}
			}

		} 
		catch (InterruptedException e) 
		{
			return false;
		} 
		finally 
		{
			// Liberamos el acceso a la función add
			available.release();
		}

		return true;
	}
	
	/**
	 * Checks the received summary (sum) and determines the operations
	 * contained in the log that have not been seen by
	 * the proprietary of the summary.
	 * Returns them in an ordered list.
	 * @param sum
	 * @return list of operations
	 */
	public List<Operation> listNewer(TimestampVector sum){
		lsim.log(Level.TRACE, "listNewer sum: "+ sum);
		List<Operation> result = new ArrayList<Operation>();

		// Recorremos los ids de los hosts que tenemos en el log
		for (String hostId : log.keySet()) 
		{
			// Recuperamos las operaciones que tenemos en el log de cada uno de los host
			List<Operation> hostOperations = log.get(hostId);
			
			// Recuperamos el último timestamp de cada host de los timestamps recibidos
			Timestamp lastHostTimestamp = null;
			if(sum!=null)lastHostTimestamp = sum.getLast(hostId);
							
			for (Operation operation : hostOperations) 
			{
				Timestamp operationTimestamp = operation.getTimestamp(); 
				// Si el último timestamp recibido es menor o igual a cada operación del log
				// añadimos esa operación para devolver ya que está pendiente
				if (lastHostTimestamp == null || operationTimestamp.compare(lastHostTimestamp) < 1) 
				{
					result.add(operation);
				}
			}
		}

		return result;
	}
	
	/**
	 * Removes from the log the operations that have
	 * been acknowledged by all the members
	 * of the group, according to the provided
	 * ackSummary. 
	 * @param ack: ackSummary.
	 */
	public void purgeLog(TimestampMatrix ack){
	}

	/**
	 * equals
	 */
	@Override
	public boolean equals(Object obj) {
		
		// Si el objeto es nulo no son iguales
		if(obj == null)
			return false;
		
		// Si el objeto que recibe no es del tipo Log no son iguales
		if(obj.getClass()!=Log.class)
			return false;		
		
		Log received = (Log)obj;
		
		// Si los dos hashmap son nulos son iguales		
		if(log == null && received.log == null)
			return true;
		
		// Si los dos hashmap no son nulos y uno de ellos si, son diferentes
		if(received.log == null)
			return false;
		
		if(log == null)
			return false;
				
		// Si no tienen el mismo tamaño son diferentes
		if (log.size() != received.log.size())
			return false;		

		// Recorremos nuestros identificadores de nodo
		for (String hostId : log.keySet()) 
		{
			// En el momento que haya un nodo diferente al nodo en el vector recibido, ya no son iguales
			List<Operation> ownOperations = log.get(hostId);
			List<Operation> receivedOperations = received.log.get(hostId);			
			if(!ownOperations.equals(receivedOperations))
				return false;
		}
		
		return true;
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String name="";
		for(Enumeration<List<Operation>> en=log.elements();
		en.hasMoreElements(); ){
		List<Operation> sublog=en.nextElement();
		for(ListIterator<Operation> en2=sublog.listIterator(); en2.hasNext();){
			name+=en2.next().toString()+"\n";
		}
	}
		
		return name;
	}
}