package com.kni.etl.ketl.annotations;


public @interface Parameter {

    boolean required() default false;

    String datatype() default "STRING";

}
