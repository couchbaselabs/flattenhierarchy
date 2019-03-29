# flattenhierarchy

usage: flattenhierarchy.py -u  -p -i  
                          -b BUCKET  ** source bucketname
                          -t TYPECOL  ** the type column for the bucket
                          -s SOURCETYPE   ** the type value for the source data
                          -l HIERARCHYLEVELCOUNT ** number of level. Choose a max value
                          -n NODE   ** the id column for the node. e.g. employee_id
                           [-d NODENAME]  ** the optional name column for the node. e.g. employee_name
                           -m PARENT . ** the parentid column name. e.g. manager_id
