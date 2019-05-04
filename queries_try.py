from pymongo import MongoClient
import pprint

def average_list(arr):
    return sum(arr) / len(arr)

def graph(arr):
    x_vals = []
    y_vals = []

    markers = [30, 60, 120, 240, 300, 600, 900, 1400, 1800, 3600,
            7200, 14400, 21600, 28800, 36000, 43200, 86400,
            172800, 604800, 691200]

    for i in arr:
        if(i['_id'] != None and i['count'] != None):
            # if(i['count'] > 200):
                x_vals.append(i['_id'])
                y_vals.append(i['count'])

    zipped = zip(x_vals, y_vals)
    zips = sorted(zipped, key=lambda x:x[0])
    
    # Partition the TTLs into major categories.
    prev = 0
    sum_dict = {}
    for m in markers:
        sum = 0
        for x in zips:
            if(x[0] > prev and x[0] <= m):
                sum+= x[1]
        sum_dict[m] = sum
        prev = m

    # sort dict for plot
    lists = sorted(sum_dict.items())
    x, y = zip(*lists)

    totalvals = 0
    for val in y:
        totalvals += val

    print("Total:", totalvals, "items")
    print(y)

def main():
    client = MongoClient('212.201.49.38', 27017)
    db = client['ttl']
    collection = db['cname_A_data']

    # Uncomment to print DB stats
    #pprint.pprint(db.command('dbstats'))

    amazon = "amazonaws|cloudflare"
    akamai = "akamai|edgekey|akadns|edgesuite"
    wixdns = "wixdns"
    google = "googleusercontent|googlehosted"
    tencent = "dnsv1|qcloud|tencdns"
    azure = "azure"
    squarespace = "squarespace"
    incapsula = "incapdns"
    rackcdn = "rackcdn"

    # Pipe to get average TTL of each response name.
    pipe1 = [
        {"$match": {"response": {"$regex": akamai}}},
        {"$unwind": "$dates"},
        {"$group":{
            "_id": "$response",
            "avg": {"$avg": "$dates.ttl"}
        }}]

    # Pipe to get the count of each unique TTL value used.
    pipe2 = [
        {"$match": {"response": {"$regex": "cloudflare"}}},
        {"$unwind": "$dates"},
        {"$group":{
            "_id": "$dates.ttl",
            "count": {"$sum": 1}
        }}]

    domains = [akamai, wixdns, amazon, google, tencent, azure, squarespace, incapsula, rackcdn]
    dates = ["20180501", "20180515", "20180601", "20180615", "20180701", "20180715"]
    for d in domains:
        print("Data for:", d, "----------------------------")
        for dt in dates:
            print("+Date:", dt)
            #try googleusercontent, amazonaws, wixdns
            pipe = [
                {"$unwind": "$dates"},
                {"$match": {"$and":[{"c_name": {"$regex": d}},{"dates.f_date": dt}]}},
                {"$group":{
                    "_id": "$dates.ttl",
                    "count": {"$sum": 1}
                }}]

            call = collection.aggregate(pipe)
            objs = list(call)
            # pprint.pprint(objs)
            graph(objs)

    # for pipe1
    # to_avg = []
    # # [to_avg.append(o['avg']) for o in objs if o['avg] != None]
    # print("Total avg:", average_list(to_avg), "Items:", len(to_avg))

if __name__ == "__main__":
    main()    