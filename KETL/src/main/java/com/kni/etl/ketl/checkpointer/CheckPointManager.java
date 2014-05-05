package com.kni.etl.ketl.checkpointer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.LinkedList;

import com.kni.etl.ketl.ETLStep;

public class CheckPointManager {
	public LinkedList<Integer> successfullySavedBatches = new LinkedList<Integer>();
	private ETLStep step;

	public CheckPointManager(ETLStep step) {
		this.step = step;
	}

	public File getCheckPointAdminFile() {
		File adminFile = new File(getPathName(step));
		return adminFile.exists() ? adminFile : null;
	}

	public boolean writeCheckPointAdminDataToFile() throws IOException {
		String pathname = getPathName(step);
		File file = new File(pathname);
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(successfullySavedBatches);
		return true;
	}

	private String getPathName(ETLStep step) {
		String pathname = step.getJobID() + step.getName() + step.getJobExecutionID() + ".admin";
		return pathname;
	}

	public Integer getLastSuccessFullBatch() {
		return successfullySavedBatches.element();
	}

	public void putSuccessFulBatch(Integer batchId) {
		successfullySavedBatches.add(batchId);
	}

	public LinkedList<Integer> readFromFile() throws IOException, ClassNotFoundException {
		File adminFile = new File(getPathName(step));
		if (!adminFile.exists())
			return null;
		else {
			ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(adminFile));
			return successfullySavedBatches = (LinkedList<Integer>) objectInputStream.readObject();
		}
	}

	public void logSuccessfulBatchInfo(int res) {
		// TODO Auto-generated method stub

	}
}
