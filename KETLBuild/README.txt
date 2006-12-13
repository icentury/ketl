Basic Install
-------------
These basic install steps assume the KETL server will be running on
the same machine as the KETL metadata database.


Requirements
------------
1. Java 1.5 SDK
2. A relational database for the storage of the KETL metadata. KETL
   has been tested against PostgreSQL (8.x), Oracle (9.1 or greater), 
   and MySql (5.1 or greater using InnoDB for storage). In theory,
   any relational database that supports row-level locking will work. 
   However, the above three have been tested, and have scripts provided.

   If the decision is made to use either Oracle or MySQL as repositories 
   for the KETL metadata, it will be necessary to place the driver files
   into the KETL directories. please refer to "Non-Supplied Optional
   Items" for more information.
 
   IMPORTANT NOTE: This is JUST the KETL metadata repository, and NOT
   a restriction of either data sources or targets

3. Thin client drivers for the appropiate metadata database.  Due to
   licensing restrictions, it is only possible to include the client
   drivers for PostgreSQL.  If the decision is made to use either
   Oracle or MySQL as repositories for the KETL metadata, it will be
   necessary to place the driver files into the KETL directories. Please
   refer to "Non-Supplied Optional Items" for more information.
 
4. Support for procedural language in the SQL engine of the metadata 
   repository enabled. This would be pgSQL (PostgreSQL), PL/SQL (Oracle),
   and is enabled by default in MySQL 5.0 and above.

Non-Supplied Optional Items That Should Be Added To The Classpath
-----------------------------------------------------------------
1. Complex Transformations - sun.tools.jar 

   If complex data transformations are needed, a copy (or link) of 
   sun.tools.jar in the classpath ($KETLDIR/lib) is also required. This
   will allow dynamic compilation of the transformations.  This class 
   library is usually bundled with the Java SDK installation.

2. MySQL JDBC Connectivity - mysql-connector-java-5.0.3-bin.jar

http://dev.mysql.com/downloads/connector/j/5.0.html


3. Oracle JDBC connectivity - ojdbc14.jar


 http://www.oracle.com/technology/software/tech/java/sqlj_jdbc/index.html



Optional Items That Are Included in the KETL Open Source Distibution
--------------------------------------------------------------------
The follows class libraries have liscensing that allows for the inclusion
into the KETL Open Source bundle.  Becuase these libraries are constantly
being updated and improved, it is reccomended that the sources of the 
class libraries be checked for the most recent versions. Copies of the 
licensing agreements for these class libraries can be found in 
$KETLDIR/license. If these libraries are updated, it will be necessary 
to replace the copies in $KETLDIR/lib.

1. Enhanced Logging - log4j-1.x.x.jar, commons-logging-1.x.x

   These class libraries allow for more detailed levels of system and
   error logging.  Please refer to 
   http://logging.apache.org/log4j/docs/download.html

2. Web Services/SOAP Protocols - axis.jar, jaxrpc.jar, saaj.jar

   This class library contains lightweight protocol for exchanging
   structured information in a decentralized, distributed environment.
   (i.e. the internet). It is an XML based protocol that consists of
   three parts: an envelope that defines a framework for describing what
   is in a message and how to process it, a set of encoding rules for
   expressing instances of application-defined datatypes, and a convention
   for representing remote procedure calls and responses.  Please refer to 
   http://www.apache.org/dyn/closer.cgi/ws/axis .

3. XML Parsing - saxon8.jar, saxon8-dom.jar, saxon8-xpath.jar

   These class libraries are implementation of XSLT 2.0 and XPath 2.0, and
   XQuery 1.0 . These provide all of the languages except schema-aware
   processing.  Please refer to http://saxon.sourceforge.net .

4) PostgreSQL thin JDBC drivers - postgresql.jar

   This class library will allow connection to the PostgreSQL database. It
   should be noted that in addition to providing connectivity to PostgreSQL
   as the repostory for the KETL metadata, it will also provide the ability 
   to use PostgreSQL as either a target or a source for data flows.  Please
   refer to http://jdbc.postgresql.org/download.html . 


Install Steps
-------------
1. set the following environment variables:

   The root directory of the KETL code tree (KETLDIR):

	export KETLDIR=/usr/local/KETL  # (Bourne shell or ksh) or
        setenv KETLDIR /usr/local/KETL  # (csh or tcsh )

   Default search path ($path or PATH )to include the KETL binaries
   and control scripts

        PATH=$PATH:$KETLDIR/bin         # (Bourne shell or ksh) or
        set path=($path $KETLDIR/bin)   # (csh or tcsh)

2. Create an the initial metadata repository in relational database:

   i.  Login as an existing user with create user priv's
   ii. Run the SQL script $KETL/setup/KETL_MD_Schema_PG.ddl or KETL_MD_Schema_Oracle.ddl

3. Modify the $KETLDIR/xml/KETLServers.xml file to have the machine name of the server which
   will run the KETL program if it is not the localhost.

4. Modify $KETLDIR/conf/Extra.Libraries to include the paths of the appropiate thin client 
   drivers, if not postgreSQL

5. Run quickstart and check the logs in $KETLDIR/log for any errors.


