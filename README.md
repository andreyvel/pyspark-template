# pyspark-template

### Configure project
- Create new python environment, for example ./venv
- Activate ./venv and check python version: 
  > ubuntu$ source ./venv/bin/activate<br>
  > (venv)$ pip --version<br>
  > pip 22.3.1 from ./pyspark-template/venv/lib/python3.10/site-packages/pip (python 3.10)
- Install pyspark package
  > (venv)$ pip install pyspark==3.2.3
- Check JAVA_HOME environment variable
  > (venv)$ echo $JAVA_HOME
- Should be used compatible java version for pyspark 3.2  
  JAVA_HOME can be configured as environment variable or configured in python code.<br>
  For linux OS, add line to ~/.profile file
  > export JAVA_HOME=/opt/jdk-11.0.2<br>

  and activate it: source ~/.profile
  
 
<br>
if JAVA_HOME missed or is not configured, you get error like this:

> py4j.protocol.Py4JJavaError: An error occurred while calling None.org.apache.spark.api.java.JavaSparkContext.
> java.lang.IllegalAccessError: class org.apache.spark.storage.StorageUtils$ (in unnamed module @0x3e07d849) 
> cannot access class sun.nio.ch.DirectBuffer (in module java.base) because module java.base does not export sun.nio.ch to unnamed module @0x3e07d849
