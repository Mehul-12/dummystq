from pymongo import MongoClient

client = MongoClient("mongodb://localhost:27017/")
db = client["smartshark"]

def q_20(): 
    path1 = "DPrRcD"
    path2 = "DPrPrR"
    path3 = "DPrPrD"
    path4 = "DFPrFD"
    client = MongoClient("mongodb://localhost:27017/")
    db = client["smartshark"]
    project_id = db["project"].find({"name": "kafka"})
    pid = project_id[0]['_id']
    dictionary = dict()
    file_to_dev = dict()
    pr_to_dev = dict()
    dictionary[path1] = dict()
    dictionary[path2] = dict()
    dictionary[path3] = dict()
    dictionary[path4] = dict()  
    pull_request_files = db["pull_request_file"]
    pull_request_data = db["pull_request"]
    pull_request_review = db["pull_request_review"]

    files = db.pull_request_file.aggregate([
        {
            "$group": {
                "_id": "$path", "pull_request_id": {"$addToSet": "$pull_request_id"}
            }
        }

    ])

    projects = db.pull_request_event.aggregate([
        {
            "$match": {
                "$or": [{"event_type": "closed"}, {"event_type": "merged"}]

            }
        },
        {
            "$group": {
                "_id": {
                    "pull_request_id": "$pull_request_id",
                    "event_type": "$event_type"
                },
                "close_time": {"$last": "$created_at"}
            }
        },
        {
            "$group": {
                "_id": "$_id.pull_request_id", "status": {"$sum": 1.0}, "close_time": {"$last": "$close_time"}
            }
        },
        # insert filter for approved reject status here

        {
            "$lookup": {
                "from": "pull_request",
                "localField": "_id",
                "foreignField": "_id",
                "as": "pull_request"

            }
        },
        {"$unwind": "$pull_request"},
        {"$project": {
            "status": "$status",
            "close_time": "$close_time",
            "pull_request_system_id": "$pull_request.pull_request_system_id",
            "create_time": "$pull_request.created_at",
            "pr_dev_id": "$pull_request.creator_id",
            # "state":"$pull_request.state"    uncomment if add extra filter

        }},

        {
            "$lookup": {
                "from": "pull_request_system",
                "localField": "pull_request_system_id",
                "foreignField": "_id",
                "as": "pull_request_system"

            }
        },
        {"$unwind": "$pull_request_system"},
        {"$project": {
            "status": "$status",
            "close_time": "$close_time",
            "pull_request_system_id": "$pull_request_system_id",
            "create_time": "$create_time",
            "pr_dev_id": "$pr_dev_id",
            "project_id": "$pull_request_system.project_id"
            # "state":"$pull_request.state"    uncomment if add extra filter

        }},
        # {
        #     "$match": {
        #         "project_id": pid
        #
        #     }
        # },

        {
            "$lookup": {
                "from": "pull_request_comment",
                "localField": "_id",
                "foreignField": "pull_request_id",
                "as": "pull_request_comment"

            }
        },
        # {"$unwind": "$pull_request_comment"},
        # {"$project": {
        #     "status":"$status",
        #     "close_time":"$close_time",
        #     "pull_request_system_id":"$pull_request_system_id",
        #     "create_time":"$create_time",
        #     "pr_dev_id":"$pr_dev_id",

        #     # "state":"$pull_request.state"    uncomment if add extra filter
        #     "comment_dev_id":"$pull_request_comment.author_id"

        # }},

        {
            "$lookup": {
                "from": "pull_request_review",
                "localField": "_id",
                "foreignField": "pull_request_id",
                "as": "pull_request_review"

            }
        },
        # {"$unwind": "$pull_request_review"},
        # {"$project": {
        #     "status":"$status",
        #     "close_time":"$close_time",
        #     "pull_request_system_id":"$pull_request_system_id",
        #     "create_time":"$create_time",
        #     "pr_dev_id":"$pr_dev_id",

        #     # "state":"$pull_request.state"    uncomment if add extra filter
        #     "pull_request_comment":"$pull_request_comment",
        #     "reviewer_id":"$pull_request_review.creator_id"

        # }},

        {
            "$lookup": {
                "from": "pull_request_file",
                "localField": "_id",
                "foreignField": "pull_request_id",
                "as": "pull_request_file"

            }
        },
        # {"$unwind": "$pull_request_file"},
        # {"$project": {
        #     "status":"$status",
        #     "close_time":"$close_time",
        #     "pull_request_system_id":"$pull_request_system_id",
        #     "create_time":"$create_time",
        #     "pr_dev_id":"$pr_dev_id",

        #     # "state":"$pull_request.state"    uncomment if add extra filter
        #     "pull_request_comment":"$pull_request_comment",
        #     "reviewer_id":"reviewer_id",
        #     "file_path":"$pull_request_file.path"

        # }},
        {
            "$limit": 100
        }
    ])
    pr_ids = set()
    entries = list(projects)
    
    for req in pull_request_data.find({}):
        pr_to_dev[req["_id"]] = req["creator_id"]

    added = list(pull_request_files.find({"status": "added"}))
    for file_row in added:
        file = file_row["path"]
        pid = file_row["pull_request_id"]
        dev = pr_to_dev[pid]
        file_to_dev[file] = dev

    
    for entry in entries:
        
        pr_ids.add(entry["_id"])
        # unique pr id
        for comment in entry["pull_request_comment"]:
            dev_id = entry["pr_dev_id"]
            dev2_id = comment["author_id"]
            if dictionary["DPrRcD"].get(dev_id) == None:
                dictionary["DPrRcD"][dev_id] = dict()
            if dictionary["DPrRcD"][dev_id].get(dev2_id) == None:
                dictionary["DPrRcD"][dev_id][dev2_id] = 0
            dictionary["DPrRcD"][dev_id][dev2_id] += 1

    
    for entry in entries:
        
        pr_ids.add(entry["_id"])
        for review in entry["pull_request_review"]:
            prrid = review["_id"]
            review_comments = db.pull_request_review_comment.find(
                {"pull_request_review_id": prrid, "creator_id": {"$exists": True}})
            for review_comment in review_comments:
                dev_id = entry["pr_dev_id"]
                dev2_id = review_comment["creator_id"]
                if dictionary["DPrPrD"].get(dev_id) == None:
                    dictionary["DPrPrD"][dev_id] = dict()
                if dictionary["DPrPrD"][dev_id].get(dev2_id) == None:
                    dictionary["DPrPrD"][dev_id][dev2_id] = 0
                dictionary["DPrPrD"][dev_id][dev2_id] += 1

    
    for entry in entries:
       
        pr_ids.add(entry["_id"])

        for review in entry["pull_request_review"]:
            dev_id = entry["pr_dev_id"]
            dev2_id = review["creator_id"]
            if dictionary["DPrPrR"].get(dev_id) == None:
                dictionary["DPrPrR"][dev_id] = dict()
            if dictionary["DPrPrR"][dev_id].get(dev2_id) == None:
                dictionary["DPrPrR"][dev_id][dev2_id] = 0
            dictionary["DPrPrR"][dev_id][dev2_id] += 1
            
    
   
    for entry in entries:
        pr_ids.add(entry["_id"])

        for file1 in entry["pull_request_file"]:
            for file2 in entry["pull_request_file"]:
                if file1["path"] == file2["path"]:
                    continue
                if file_to_dev.get(file1["path"]) == None:
                    continue
                if file_to_dev.get(file2["path"]) == None:
                    continue
                dev_id = file_to_dev[file1["path"]]
                dev2_id =file_to_dev[file2["path"]]
                if dictionary["DFPrFD"].get(dev_id) == None:
                    dictionary["DFPrFD"][dev_id] = dict()
                if dictionary["DFPrFD"][dev_id].get(dev2_id) == None:
                    dictionary["DFPrFD"][dev_id][dev2_id] = 0
                dictionary["DFPrFD"][dev_id][dev2_id] += 1
           
    return dictionary