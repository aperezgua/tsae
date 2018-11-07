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

package recipes_service.tsae.sessions;

import java.io.IOException;
import java.net.Socket;
import java.util.List;

import communication.ObjectInputStream_DS;
import communication.ObjectOutputStream_DS;
import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
//LSim logging system imports sgeag@2017
import lsim.worker.LSimWorker;
import recipes_service.ServerData;
import recipes_service.communication.Message;
import recipes_service.communication.MessageAErequest;
import recipes_service.communication.MessageEndTSAE;
import recipes_service.communication.MessageOperation;
import recipes_service.communication.MsgType;
import recipes_service.data.AddOperation;
import recipes_service.data.Operation;
import recipes_service.data.Recipe;
import recipes_service.data.RemoveOperation;
import recipes_service.tsae.data_structures.TimestampMatrix;
import recipes_service.tsae.data_structures.TimestampVector;

/**
 * @author Joan-Manuel Marques December 2012
 *
 */
public class TSAESessionPartnerSide extends Thread {
	// Needed for the logging system sgeag@2017
	private LSimWorker lsim = LSimFactory.getWorkerInstance();

	private Socket socket = null;
	private ServerData serverData = null;

	public TSAESessionPartnerSide(Socket socket, ServerData serverData) {
		super("TSAEPartnerSideThread");
		this.socket = socket;
		this.serverData = serverData;
	}

	public void run() {

		Message msg = null;

		int current_session_number = -1;
		try {
			ObjectOutputStream_DS out = new ObjectOutputStream_DS(socket.getOutputStream());
			ObjectInputStream_DS in = new ObjectInputStream_DS(socket.getInputStream());

			// receive originator's summary and ack
			msg = (Message) in.readObject();
			current_session_number = msg.getSessionNumber();
			lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] TSAE session");
			lsim.log(Level.TRACE,
					"[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
			if (msg.type() == MsgType.AE_REQUEST) {
				// send operations
				TimestampVector originatorSummary = ((MessageAErequest) msg).getSummary();
				TimestampMatrix originatorAck = ((MessageAErequest) msg).getAck();

				List<Operation> operationsToSend = serverData.getLog().listNewer(originatorSummary);

				for (Operation operation : operationsToSend) {
					msg = new MessageOperation(operation);
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					lsim.log(Level.TRACE,
							"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);
				}

				// send to originator: local's summary and ack
				TimestampVector localSummary = null;
				TimestampMatrix localAck = null;

				// Send to partner: local's summary and ack
				// ...
				// Clone to prevent concurrent modification
				localSummary = serverData.getSummary().clone();
				localAck = serverData.getAck().clone();
				localAck.update(serverData.getId(), localSummary);

				// ...

				msg = new MessageAErequest(localSummary, localAck);
				msg.setSessionNumber(current_session_number);
				out.writeObject(msg);
				lsim.log(Level.TRACE,
						"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);

				// receive operations
				msg = (Message) in.readObject();
				lsim.log(Level.TRACE,
						"[TSAESessionPartnerSide] [session: " + current_session_number + "] received message: " + msg);
				while (msg.type() == MsgType.OPERATION) {

					Operation operation = ((MessageOperation) msg).getOperation();

					if(serverData.getLog().add(operation)) {
						switch (operation.getType()) {
						case ADD:
							Recipe recipe = ((AddOperation) operation).getRecipe();
							serverData.getRecipes().add(recipe);
							break;
						case REMOVE:
							String recipeTitle = ((RemoveOperation) operation).getRecipeTitle();
							serverData.getRecipes().remove(recipeTitle);
							break;
						}
					}

					//serverData.getLog().add(operation);
					// ...
					msg = (Message) in.readObject();
					lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number
							+ "] received message: " + msg);
				}

				// receive message to inform about the ending of the TSAE session
				if (msg.type() == MsgType.END_TSAE) {
					// send and "end of TSAE session" message
					msg = new MessageEndTSAE();
					msg.setSessionNumber(current_session_number);
					out.writeObject(msg);
					lsim.log(Level.TRACE,
							"[TSAESessionPartnerSide] [session: " + current_session_number + "] sent message: " + msg);
					// ...
					if (originatorSummary != null) {
						serverData.getSummary().updateMax(originatorSummary);
						serverData.getAck().updateMax(originatorAck);
						serverData.getLog().purgeLog(serverData.getAck());

						// ...

					}
				}
			}
			socket.close();
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			lsim.log(Level.FATAL,
					"[TSAESessionPartnerSide] [session: " + current_session_number + "]" + e.getMessage());
			e.printStackTrace();
			System.exit(1);
		} catch (IOException e) {
		}

		lsim.log(Level.TRACE, "[TSAESessionPartnerSide] [session: " + current_session_number + "] End TSAE session");
	}
}