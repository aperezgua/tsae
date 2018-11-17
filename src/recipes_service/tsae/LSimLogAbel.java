package recipes_service.tsae;

import edu.uoc.dpcs.lsim.LSimFactory;
import edu.uoc.dpcs.lsim.logger.LoggerManager.Level;
import lsim.worker.LSimWorker;

public class LSimLogAbel {

	private transient LSimWorker lsim = LSimFactory.getWorkerInstance();

	public void log(Level level, String message) {
		try {
			lsim.log(level, message);
		} catch (Exception e) {
			System.out.println("Message: " + message);
		}
	}

}
