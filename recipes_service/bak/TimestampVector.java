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
import java.util.concurrent.ConcurrentHashMap;

import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;

/**
 * @author Joan-Manuel Marques
 * December 2012
 *
 */
public class TimestampVector implements Serializable{
	// Needed for the logging system sgeag@2017
	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	private static final long serialVersionUID = -765026247959198886L;
	/**
	 * This class stores a summary of the timestamps seen by a node.
	 * For each node, stores the timestamp of the last received operation.
	 */
	
	private ConcurrentHashMap<String, Timestamp> timestampVector= new ConcurrentHashMap<String, Timestamp>();
	
	public TimestampVector (List<String> participants){
		// create and empty TimestampVector
		for (Iterator<String> it = participants.iterator(); it.hasNext(); ){
			String id = it.next();
			// when sequence number of timestamp < 0 it means that the timestamp is the null timestamp
			timestampVector.put(id, new Timestamp(id, Timestamp.NULL_TIMESTAMP_SEQ_NUMBER));
		}
	}

	/**
	 * Updates the timestamp vector with a new timestamp. 
	 * @param timestamp
	 */
	public void updateTimestamp(Timestamp timestamp)
	{
		lsim.log(Level.TRACE, "Updating the TimestampVectorInserting with the timestamp: "+timestamp);

		timestampVector.replace(timestamp.getHostid(), timestamp);
	
	}
	
	/**
	 * merge in another vector, taking the elementwise maximum
	 * @param tsVector (a timestamp vector)
	 */
	public void updateMax(TimestampVector tsVector)
	{
		lsim.log(Level.TRACE, "UpdateMax with the timestampVector: " + tsVector.timestampVector);
		// Recorremos nuestros identificadores de nodo
		for (String nodeId : timestampVector.keySet()) 
		{			
			Timestamp timestampOwn = timestampVector.get(nodeId);
			lsim.log(Level.TRACE, "UpdateMax timestampOwn: " + timestampOwn);
			Timestamp timestampReceived = tsVector.timestampVector.get(nodeId);
			lsim.log(Level.TRACE, "UpdateMax timestampReceived: " + timestampReceived);
			//Si el nuestro es más antiguo al recibido, reemplazamos el nuestro por el recibido
			if(timestampOwn.compare(timestampReceived) < 0) 
			{			
				lsim.log(Level.TRACE, "UpdateMax Replace with received");
				timestampVector.replace(nodeId, timestampReceived);			
			}
		}
	}
	
	/**
	 * 
	 * @param node
	 * @return the last timestamp issued by node that has been
	 * received.
	 */
	public Timestamp getLast(String node)
	{		
		return timestampVector.get(node);
	}
	
	/**
	 * merges local timestamp vector with tsVector timestamp vector taking
	 * the smallest timestamp for each node.
	 * After merging, local node will have the smallest timestamp for each node.
	 *  @param tsVector (timestamp vector)
	 */
	public void mergeMin(TimestampVector tsVector)
	{
		// Recorremos nuestros identificadores de nodo
		for (String nodeId : timestampVector.keySet()) 
		{
			Timestamp timestampLastOwn = timestampVector.get(nodeId);			
			Timestamp timestampLastReceived = tsVector.timestampVector.get(nodeId);
			
			if(timestampLastOwn.compare(timestampLastReceived) > 0) 
			{
				timestampVector.put(nodeId, timestampLastReceived);
			}

		}
	}
	
	/**
	 * clone
	 */
	public TimestampVector clone(){
		
		// Recupermaos los id de servidores participantes
		List<String> participants = new ArrayList<String>(timestampVector.keySet());
		// Creamos un vector de timestamps con el mismo número de participantes
		TimestampVector clonedVector = new TimestampVector(participants);

		// Recorremos los participantes y recuperamos los nodos que ponemos en el vector resultante
		for (String nodeId : participants) 
		{
			Timestamp timestampNode = timestampVector.get(nodeId);
			clonedVector.timestampVector.put(nodeId, timestampNode);
		}
		
		return clonedVector;
	}
	
	/**
	 * equals
	 */
	public boolean equals(Object obj){							
		//lsim.log(Level.TRACE, "Equals");				
		
		// Si el objeto es nulo no son iguales
		if(obj == null)
			return false;
		
		//lsim.log(Level.TRACE, "obj != null");
		
		// Si el objeto que recibe no es del tipo TimestampVector no son iguales
		if(obj.getClass()!=TimestampVector.class)
			return false;		
		
		//lsim.log(Level.TRACE, "obj.getClass()==TimestampVector.class");
		
		TimestampVector received = (TimestampVector)obj;
		
		// Si los dos vectores son nulos son iguales		
		if(timestampVector == null && received.timestampVector == null)
			return true;
		
		//lsim.log(Level.TRACE, "timestampVector != null || received.timestampVector != null");
		
		// Si los dos vectores no son nulos y uno de ellos si, son diferentes
		if(received.timestampVector == null)
			return false;
		
		//lsim.log(Level.TRACE, "received.timestampVector != null");
		
		if(timestampVector == null)
			return false;
		
		//lsim.log(Level.TRACE, "timestampVector != null");
		
		// Si no tienen el mismo tamaño son diferentes
		if (timestampVector.size() != received.timestampVector.size())
			return false;		
		
		//lsim.log(Level.TRACE, "timestampVector.size() == received.timestampVector.size()");
		
		// Recorremos nuestros identificadores de nodo
		for (String nodeId : timestampVector.keySet()) 
		{
			// En el momento que haya un nodo diferente al nodo en el vector recibido, ya no son iguales
			Timestamp ownNode = timestampVector.get(nodeId);
			Timestamp receivedNode = timestampVector.get(nodeId);
			if(!ownNode.equals(receivedNode))
				return false;
		}
		
		//lsim.log(Level.TRACE, "Equals == true");
		
		return true;
	}

	/**
	 * toString
	 */
	@Override
	public synchronized String toString() {
		String all="";
		if(timestampVector==null){
			return all;
		}
		for(Enumeration<String> en=timestampVector.keys(); en.hasMoreElements();){
			String name=en.nextElement();
			if(timestampVector.get(name)!=null)
				all+=timestampVector.get(name)+"\n";
		}
		return all;
	}
}