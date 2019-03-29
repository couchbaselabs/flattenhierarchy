# installation
# pip3 install Faker
# pip3 install faker-schema
# faker https://pypi.org/project/Faker/
# for json schema based, https://pypi.org/project/faker-schema/
# reference dimension data https://datahub.io/collections/reference-data#list-of-countries

import argparse
import json
import random
from couchbase.cluster import Cluster
from couchbase.cluster import PasswordAuthenticator
from couchbase.exceptions import NotFoundError
from couchbase.bucket import Bucket
from couchbase.n1ql import N1QLQuery
import time
import random
from datetime import datetime
from datetime import timedelta
import decimal



def flatten_hierarchy(cb,bucketname,type_col,src_doc_type,num_hier_level,node_id_col,node_name_col, parent_id_col):
    """
    A generic function to flatten a self referencing hierarchy as described in this couchbase blog.
    https://blog.couchbase.com/n1ql-query-with-self-referencing-hierarchy/ 

    The function reads from a couchbase $bucketname with a specfic $src_doc_type value, then generate 
    two additional datasets. 1) scr_doc_type "_hier" and 2) src_doc_type "_hier_level". 

    Parameters:

    cb - cb handle
    bucketname - name of the bucket to read from
    type_col - the type column name. Typicall 'type' is used in CB bucket.
    src_doc_type - the type value of the source documents, e.g. 'employee'
    num_hier_level - the number of hierarchy levels that the function will generate. 
    node_id_col - the field name of the node id. e.g. 'employee_id'
    node_name_col - the field name of the node name. e.g. 'employee_name'
    parent_id_col - the field name for the parent node. e.g.'manager_id'

    """
    
    gen_doc_type = '_'+src_doc_type+'_hier'
    if (num_hier_level > 1):
        qstr_ins = 'INSERT into '+bucketname+' (KEY UUID(), VALUE ndoc) '
        qstr_sel = 'SELECT { "'+type_col+'":"'+gen_doc_type+'"'
        
        for i in range(1,num_hier_level+1):
            qstr_sel += ',"id'+str(i)+'":l'+str(i)+'.'+node_id_col
            if(len(node_name_col)>0):
                qstr_sel += ',"name'+str(i)+'":l'+str(i)+'.'+node_name_col

        qstr_sel += '} ndoc'
        
        #qstr_sel_one =  ' SELECT '+node_id_col+','+parent_id_col+' FROM '+bucketname+' WHERE type="'+src_doc_type+'"'
        qstr_sel_one = bucketname
        for i in range(1,num_hier_level+1):
            if (i==1):
                qstr_sel += ' FROM ('+qstr_sel_one+') l'+str(i)
            else:
                qstr_sel += ' LEFT OUTER JOIN ('+qstr_sel_one
                qstr_sel += ') l'+str(i)+' ON l'+str(i-1)+'.'+parent_id_col+' = l'+str(i)+'.'+node_id_col+' AND l'+str(i)+'.'+type_col+'="'+src_doc_type+'"'
        qstr_sel += ' WHERE l1.'+type_col+'="'+src_doc_type+'"'
 

        
        try:
            print(qstr_ins+qstr_sel)
            #q = N1QLQuery(qstring)
            rows = cb.n1ql_query(qstr_ins+qstr_sel).execute()
        except Exception as e:
            print("query error",e)
        # generate connect by

    if (num_hier_level > 1):
        qstr_ins = 'INSERT into '+bucketname+' (KEY UUID(), VALUE ndoc) '
        qstr_sel = 'SELECT { "id":ll.child,"parent":ll.parent,"level":ll.level ,"'+type_col+'":"'+gen_doc_type+'_level" } ndoc FROM ('

        for i in range(1,num_hier_level):
            if (i>1):
                qstr_sel += 'UNION ALL '
            qstr_sel += 'SELECT id'+str(i+1)+' parent, id1 child,'+str(i)+' level FROM '+bucketname+' WHERE type="'+gen_doc_type+'" and id'+str(i+1)+' IS NOT NULL '
                
        qstr_sel += ') ll'
        try:
            print(qstr_ins+qstr_sel)
            rows = cb.n1ql_query(qstr_ins+qstr_sel).execute()
        except Exception as e:
            print("query error",e)

    return


def main():

    parser = argparse.ArgumentParser(description='python3 flattenhierarchy')
    parser.add_argument("-u", "--user", help="cb user login name", required=True)
    parser.add_argument("-p", "--password", help="cb user login password", required=True)
    parser.add_argument("-i", "--ip", help="IP address of a cb node", required=True)
    parser.add_argument("-b", "--bucket", help="bucket of the source/target data", required=True)
    parser.add_argument("-t", "--typecol", help="the type column in the bucket", required=True)
    parser.add_argument("-s", "--sourcetype", help="the type value for the source documents", required=True)
    parser.add_argument("-l", "--hierarchylevelcount", help="the maximum mnumber of hierarchy levels in the source documents", required=True)
    parser.add_argument("-n", "--node", help="the node id column name of the source document", required=True)
    parser.add_argument("-d", "--nodename", help="the node name column name of the source document",default='', required=False)
    parser.add_argument("-m", "--parent", help="the parent node id column name of the source document", required=True)


    # parse input arguments
    args = parser.parse_args()

    # construct the full url for the query end point & connect 
    urlcbq = "http://" + args.ip + ":8093"

 
    # connect to cb cluster
    connstr = "couchbase://" + args.ip 
    cluster = Cluster(connstr)
    authenticator = PasswordAuthenticator(args.user, args.password)
    cluster.authenticate(authenticator)
    cb = cluster.open_bucket(args.bucket)
    ttls = [0, 0, 0]
        
    
    try:
        flatten_hierarchy(cb,args.bucket,args.typecol,args.sourcetype,int(args.hierarchylevelcount),args.node,args.nodename,args.parent)
    except:
        print("main:unexpected error")

if __name__ == "__main__":
    main()

