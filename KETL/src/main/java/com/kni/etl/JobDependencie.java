package com.kni.etl;

class JobDependencie {
  String name;
  boolean allowDuplicates = false;
  boolean critical = true;
  public int pathPriority = 100;

}
