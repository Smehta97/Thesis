import json, glob, os, getopt
from pymongo import MongoClient, ASCENDING
import sys
import time
import multiprocessing as mp 

def makeEntries(f_name, f_date):
    
    # Set up each worker with db: ttl_test, collection
    client = MongoClient('212.201.49.38', 27017)
    db = client['ttl']
    collection = db['cname_A_data']

    newcount = 0
    updatecount = 0

    # Open JSON file
    with open(f_name) as f:
        raw = json.load(f)
        print("[INFO]: Opening:", f_name)

    for entry in raw[2:]:
        if(entry['_cdn'] == 1 and entry['query_type'] == 'A' and entry['response_ttl'] != None and entry['response_type'] == 'CNAME'):
            # INFO: All response types will be CNAME by filtering algo.
            url_id = entry['response_name']
            cursor_count = collection.find({'response': url_id}).count()
            
            #TODO: ADD ID FOR REFERENCE TO OPENINTEL
            if(cursor_count == 1):
                data = {
                    'date': entry['timestamp'],
                    'ttl': entry['response_ttl'],
                    'q_name': entry['query_name'],
                    'ip4': entry['ip4_address'],
                    'ip6': entry['ip6_address'],
                    'oi_id': entry['_id'],
                    'f_date': f_date
                }
                collection.update_one({"response": url_id}, {"$push": {'dates': data}})
                updatecount+=1

            elif(cursor_count == 0):
                schema = {
                    'response': entry['response_name'],
                    'r_type': entry['response_type'],
                    'q_type': entry['query_type'],
                    'c_name': entry['cname_name'],
                    'dates':[
                        {
                        'date': entry['timestamp'],
                        'ttl': entry['response_ttl'],
                        'q_name': entry['query_name'],
                        'ip4': entry['ip4_address'],
                        'ip6': entry['ip6_address'],
                        'oi_id': entry['_id'],
                        'f_date': f_date
                        }
                    ]
                }
                
                try:
                    collection.insert_one(schema)
                    newcount+=1
                except Exception as e:
                    print("Failed to save db", e)

            else:
                print("Duplicates found! {}").format(cursor_count)
                f.close()
                break
                    # TODO: push results to json or Mongo.
    
    f.close()
    print("[INFO]: Closing", f_name, "[Stats]:", newcount, updatecount)

def main():
     # Set up mongo with db: ttl_test, collection: AAAA_data
    client = MongoClient('212.201.49.38', 27017)
    db = client['ttl']
    collection = db['cname_A_data']
    
    # Create index for key: 'response'
    index_name = "resp_ind"
    if(index_name not in collection.index_information()):
        collection.create_index([('response', ASCENDING)], unique=True, name='resp_ind')
        print("[INFO] Creating index:", collection.index_information())
    
    #fetch options
    try:
        opts, args = getopt.getopt(sys.argv[1:], "cdef:p:")
    except getopt.GetoptError as err:
        print(str(err))
        sys.exit(1)

    parsedate = ""
    num_processor = 1

    for opt, val in opts:
        if opt == "-h":
        	print("ttl.py -e exit -c continue -f <dirname> -d drop database")
        elif opt == "-c":
            print("[INFO]: Continuing")
            break
        elif opt == "-e":
            print("[INFO]: Exiting")
            sys.exit()
        elif opt == "-f":
            parsedate = str(val)
        elif opt == "-d":
            print("[INFO]: Dropping collection")
            collection.drop()
            sys.exit()
        elif opt == "-p":
            num_processor = int(val)
            print("[INFO]: Using", num_processor, "cores")

    before = time.time()

    os.chdir("serverdata/" + parsedate)

    arglist = []
    for f_name in glob.glob("*.json"):
        print("[--MP] Pooling:", f_name)
        #result = makeEntries(f_name, collection, parsedate)
        arglist.append((f_name, parsedate))
    pool = mp.Pool(processes=num_processor, maxtasksperchild=1)
    pool.starmap(makeEntries, arglist)
        

    after = time.time()

    print("Processing time (s):", after-before)

if __name__ == "__main__":
    main()
