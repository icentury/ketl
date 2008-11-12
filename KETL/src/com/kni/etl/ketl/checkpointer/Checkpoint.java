package com.kni.etl.ketl.checkpointer;

public interface Checkpoint {

	
	boolean checkpointEnabled();

	void readCheckpoint();

	// loads the checkpoint and waits until all other parallel step threads have also loaded
	// if the source failes the checkpoint was never reached and does not exist
	void loadCheckpoint() throws InterruptedException;

	boolean checkpointExists();

}
