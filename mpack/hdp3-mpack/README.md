# Mpack for Dataplane Profilers

#### Generate mpack  
>```mvn clean install```

#### Install mpack


1.  Copy the mpack.tar.gz to a  folder on ambari server host (like /tmp)
2.  Run the following command
    ambari-server install-mpack --mpack=/tmp/dpprofiler-mpack-1.0.0.tar.gz --verbose
3.  Restart Ambari server
    ambari-server restart
3.  Logon to ambari from a browser and click add service
4.  In the addservice wizard, you will see dpprofiler as an available component
    Select it
5.  Install the service by following through the options


`[root@s ~]# ambari-server install-mpack --mpack=/root/dpprofiler-ambari-mpack-1.0.0.tar.gz --verbose`

````
Using python  /usr/bin/python
Installing management pack
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Installing management pack /root/dpprofiler-ambari-mpack-1.0.0.tar.gz
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Download management pack to temp location /var/lib/ambari-server/data/tmp/dpprofiler-ambari-mpack-1.0.0.tar.gz
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Expand management pack at temp location /var/lib/ambari-server/data/tmp/dpprofiler-ambari-mpack-1.0.0/
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Stage management pack dpprofiler.mpack-1.0.0 to staging location /var/lib/ambari-server/resources/mpacks/dpprofiler.mpack-1.0.0
INFO: Processing artifact DPPROFILER-common-services of type service-definitions in /var/lib/ambari-server/resources/mpacks/dpprofiler.mpack-1.0.0/common-services
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Symlink: /var/lib/ambari-server/resources/common-services/DPPROFILER/1.0.0
INFO: Processing artifact DPPROFILER-addon-services of type stack-addon-service-definitions in /var/lib/ambari-server/resources/mpacks/dpprofiler.mpack-1.0.0/addon-services
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
INFO: Management pack dpprofiler.mpack-1.0.0 successfully installed!
INFO: Loading properties from /etc/ambari-server/conf/ambari.properties
Ambari Server 'install-mpack' completed successfully.
````

#### Uninstall mpack

`ambari-server uninstall-mpack --mpack-name=dpprofiler.mpack`


#####NOTE
Livy server should be present on cluster. 
DP Profiler has dependency on Spark2. But we need to select livy when installing spark2.