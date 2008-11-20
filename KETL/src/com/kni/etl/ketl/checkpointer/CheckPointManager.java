package com.kni.etl.ketl.checkpointer;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

import com.kni.etl.ketl.ETLStep;

public class CheckPointManager {
	public Map<ETLStep, SortedSet<Integer>> stepBatchMap = new HashMap<ETLStep, SortedSet<Integer>>();
	private ETLStep step;

	public CheckPointManager(ETLStep step) {
		this.step = step;
	}

	public File getCheckPointAdminFile(){
		File adminFile = new File(getPathName(step));
		return adminFile.exists()?adminFile:null;	
	}
	
	public boolean writeCheckPointAdminDataToFile() throws IOException{
		String pathname = getPathName(step);
		File file = new File(pathname);
		ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file));
		oos.writeObject(stepBatchMap);
		return true;
	}
	private String getPathName(ETLStep step) {
		String pathname = step.getJobID() + step.getName() + step.getJobExecutionID() + ".admin";
		return pathname;
	}
	
	public Integer getLastSuccessFullBatch(){
		return stepBatchMap.get(step).last();
	}
	
	public void putSuccessFulBatch(Integer batchId){
		SortedSet<Integer> stepBatches = stepBatchMap.get(step);
		if(stepBatches==null)
			stepBatches = new TreeSet<Integer>();
		stepBatches.add(batchId);	
	}
	public Map<ETLStep, SortedSet<Integer>> readFromFile() throws IOException, ClassNotFoundException{
		File adminFile = new File(getPathName(step));
		if(!adminFile.exists())
			return null;
		else {
			ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(adminFile));
			return stepBatchMap = (HashMap<ETLStep, SortedSet<Integer>>)objectInputStream.readObject();
		}
	}
}
