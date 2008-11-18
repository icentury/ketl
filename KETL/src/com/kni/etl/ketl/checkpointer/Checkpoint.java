package com.kni.etl.ketl.checkpointer;

import java.io.IOException;

import com.kni.etl.ketl.exceptions.KETLException;

public interface Checkpoint {

	
	boolean checkpointEnabled();

	void readCheckpoint() throws InterruptedException, IOException, ClassNotFoundException;

	// loads the checkpoint and waits until all other parallel step threads have also loaded
	// if the source failes the checkpoint was never reached and does not exist
	void loadCheckpoint() throws InterruptedException, IOException, KETLException;

	boolean checkpointExists();

}
