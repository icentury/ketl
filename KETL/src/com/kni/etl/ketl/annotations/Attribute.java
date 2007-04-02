package com.kni.etl.ketl.annotations;


public @interface Attribute {

    boolean required() default false;

    String datatype() default "STRING";

}
