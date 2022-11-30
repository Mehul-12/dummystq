from pymongo import MongoClient
from datetime import datetime
import datetime as dt
import matplotlib.pyplot as plt
import pprint
import copy
import numpy as np


class Stq:
    path1 = "DPrRcD"
    path2 = "DPrPrR"
    path3 = "DPrPrD"
    path4 = "DFPrFD"
    path5 = "DFPrPrrR"
    path6 = "DPrFPrD"
    path7 = "DPrFPrPrrR"
    client = MongoClient("mongodb://localhost:27017/")
    db = client["smartshark"]
    # project_id = db["project"].find({"name": "kafka"})
    # pid = project_id[0]['_id']
    time = dt.timedelta(days=0, seconds=0)
    count = 0
    dictionary = dict()
    file_to_dev = dict()
    pr_to_dev = dict()
    dictionary[path1] = dict()
    dictionary[path2] = dict()
    dictionary[path3] = dict()
    dictionary[path4] = dict()  
    dictionary[path5] = dict()
    dictionary[path6] = dict()
    dictionary[path7] = dict()
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
            "$match": {
                "status": {
                    "$eq": 2
                }

            }
        },

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
    # for entry in entries:
    #     print(entry["status"])
    for entry in entries:
        approved_time = entry["close_time"]
        req_time = entry["create_time"]
        time = time + approved_time - req_time
        count = count + 1

    mean = time / count
    print(mean)

    for req in pull_request_data.find({}):
        pr_to_dev[req["_id"]] = req["creator_id"]

    added = list(pull_request_files.find({"status": "added"}))
    for file_row in added:
        file = file_row["path"]
        pid = file_row["pull_request_id"]
        dev = pr_to_dev[pid]
        file_to_dev[file] = dev

    def metaPath_DPrRcD(self):
        for entry in self.entries:
            approved_time = entry["close_time"]
            req_time = entry["create_time"]
            if approved_time - req_time < self.mean:
                continue
            self.pr_ids.add(entry["_id"])
            # unique pr id
            for comment in entry["pull_request_comment"]:
                dev_id = entry["pr_dev_id"]
                dev2_id = comment["author_id"]
                if self.dictionary["DPrRcD"].get(dev_id) == None:
                    self.dictionary["DPrRcD"][dev_id] = dict()
                if self.dictionary["DPrRcD"][dev_id].get(dev2_id) == None:
                    self.dictionary["DPrRcD"][dev_id][dev2_id] = 0
                self.dictionary["DPrRcD"][dev_id][dev2_id] += 1

    def metaPath_DPrPrD(self):
        for entry in self.entries:
            approved_time = entry["close_time"]
            req_time = entry["create_time"]
            if approved_time - req_time < self.mean:
                continue
            self.pr_ids.add(entry["_id"])
            for review in entry["pull_request_review"]:
                prrid = review["_id"]
                review_comments = self.db.pull_request_review_comment.find(
                    {"pull_request_review_id": prrid, "creator_id": {"$exists": True}})
                for review_comment in review_comments:
                    dev_id = entry["pr_dev_id"]
                    dev2_id = review_comment["creator_id"]
                    if self.dictionary["DPrPrD"].get(dev_id) == None:
                        self.dictionary["DPrPrD"][dev_id] = dict()
                    if self.dictionary["DPrPrD"][dev_id].get(dev2_id) == None:
                        self.dictionary["DPrPrD"][dev_id][dev2_id] = 0
                    self.dictionary["DPrPrD"][dev_id][dev2_id] += 1

    def metaPath_DPrPrR(self):
        for entry in self.entries:
            approved_time = entry["close_time"]
            req_time = entry["create_time"]
            if approved_time - req_time < self.mean:
                continue
            self.pr_ids.add(entry["_id"])

            for review in entry["pull_request_review"]:
                dev_id = entry["pr_dev_id"]
                dev2_id = review["creator_id"]
                if self.dictionary["DPrPrR"].get(dev_id) == None:
                    self.dictionary["DPrPrR"][dev_id] = dict()
                if self.dictionary["DPrPrR"][dev_id].get(dev2_id) == None:
                    self.dictionary["DPrPrR"][dev_id][dev2_id] = 0
                self.dictionary["DPrPrR"][dev_id][dev2_id] += 1

    def metaPath_DFPrPrrR(self):
        for entry in self.entries:
            approved_time = entry["close_time"]
            req_time = entry["create_time"]
            if approved_time - req_time < self.mean:
                continue
            self.pr_ids.add(entry["_id"])

            for review in entry["pull_request_review"]:
                for file in entry["pull_request_file"]:
                    if self.file_to_dev.get(file["path"]) == None:
                        continue
                    dev_id = self.file_to_dev[file["path"]]
                    dev2_id = review["creator_id"]
                    if dev_id == dev2_id:
                        continue
                    if self.dictionary["DFPrPrrR"].get(dev_id) == None:
                        self.dictionary["DFPrPrrR"][dev_id] = dict()
                    if self.dictionary["DFPrPrrR"][dev_id].get(dev2_id) == None:
                        self.dictionary["DFPrPrrR"][dev_id][dev2_id] = 0
                    self.dictionary["DFPrPrrR"][dev_id][dev2_id] += 1

    def metaPath_DFPrFD(self):
        for entry in self.entries:
            approved_time = entry["close_time"]
            req_time = entry["create_time"]
            if approved_time - req_time < self.mean:
                continue
            self.pr_ids.add(entry["_id"])

            for file1 in entry["pull_request_file"]:
                for file2 in entry["pull_request_file"]:
                    if file1["path"] == file2["path"]:
                        continue
                    if self.file_to_dev.get(file1["path"]) == None:
                        continue
                    if self.file_to_dev.get(file2["path"]) == None:
                        continue
                    dev_id = self.file_to_dev[file1["path"]]
                    dev2_id = self.file_to_dev[file2["path"]]
                    if self.dictionary["DFPrFD"].get(dev_id) == None:
                        self.dictionary["DFPrFD"][dev_id] = dict()
                    if self.dictionary["DFPrFD"][dev_id].get(dev2_id) == None:
                        self.dictionary["DFPrFD"][dev_id][dev2_id] = 0
                    self.dictionary["DFPrFD"][dev_id][dev2_id] += 1

    def metaPath_DPrFPrD(self):
        for file in self.files:
            for pr in file["pull_request_id"]:
                if pr not in self.pr_ids:
                    continue
                for pr2 in file["pull_request_id"]:
                    if pr2 not in self.pr_ids:
                        continue
                    dev_id = self.pr_to_dev[pr]
                    dev2_id = self.pr_to_dev[pr2]
                    if dev_id == dev2_id:
                        continue
                    if self.dictionary["DPrFPrD"].get(dev_id) == None:
                        self.dictionary["DPrFPrD"][dev_id] = dict()
                    if self.dictionary["DPrFPrD"][dev_id].get(dev2_id) == None:
                        self.dictionary["DPrFPrD"][dev_id][dev2_id] = 0
                    self.dictionary["DPrFPrD"][dev_id][dev2_id] += 1

    def metaPath_DPrFPrPrrR(self):
        for file in self.files:
            for pr in file["pull_request_id"]:
                if pr not in self.pr_ids:
                    continue
                for pr2 in file["pull_request_id"]:
                    if pr2 not in self.pr_ids:
                        continue
                    dev_id = self.pr_to_dev[pr]
                    pr_revs = self.pull_request_review.find({"pull_request_id": pr2})
                    for pr_rev in pr_revs:
                        rev_id = pr_rev["creator_id"]
                        if dev_id == rev_id:
                            continue
                        if self.dictionary["DPrFPrPrrR"].get(dev_id) == None:
                            self.dictionary["DPrFPrPrrR"][dev_id] = dict()
                        if self.dictionary["DPrFPrPrrR"][dev_id].get(rev_id) == None:
                            self.dictionary["DPrFPrPrrR"][dev_id][rev_id] = 0
                        self.dictionary["DPrFPrPrrR"][dev_id][rev_id] += 1

    def cal_R2(self):

        paths = set()
        source = set()
        dest = set()
        for path in self.dictionary:
            paths.add(path)
            for src in self.dictionary[path]:
                source.add(src)
                for dst in self.dictionary[path][src]:
                    dest.add(dst)

        # pprint.pprint(dictionary)
        F = copy.deepcopy(self.dictionary)
        R = copy.deepcopy(self.dictionary)
        T = copy.deepcopy(self.dictionary)

        for path in paths:
            for src in source:
                for dst in dest:
                    if self.dictionary[path].get(src) == None:
                        continue
                    if self.dictionary[path][src].get(dst) == None:
                        continue
                    sum1 = 0
                    for src2 in source:
                        if self.dictionary[path].get(src2) == None:
                            continue
                        if self.dictionary[path][src2].get(dst) == None:
                            continue
                        sum1 += self.dictionary[path][src2][dst]

                    sum2 = 0
                    for dst2 in dest:
                        if self.dictionary[path].get(src) == None:
                            continue
                        if self.dictionary[path][src].get(dst2) == None:
                            continue
                        sum2 += self.dictionary[path][src][dst2]

                    sum3 = 0
                    for path2 in paths:
                        if self.dictionary[path2].get(src) == None:
                            continue
                        if self.dictionary[path2][src].get(dst) == None:
                            continue
                        sum3 += self.dictionary[path2][src][dst]

                    F[path][src][dst] = F[path][src][dst] / sum1
                    R[path][src][dst] = R[path][src][dst] / sum2
                    T[path][src][dst] = R[path][src][dst] / sum3

        n = len(dest)
        m = len(source)
        l = len(paths)

        x0 = np.empty(m)
        x0.fill(1 / m)
        y0 = np.empty(l)
        y0.fill(1 / l)
        z0 = np.empty(n)
        z0.fill(1 / n)
        x1 = np.empty(m)
        x1.fill(1 / m)
        y1 = np.empty(l)
        y1.fill(1 / l)
        z1 = np.empty(n)
        z1.fill(1 / n)

        E = 0.1
        t = 0
        while True:
            t += 1
            i = 0
            for src in source:
                sum = 0
                j = 0
                for path in paths:
                    k = 0
                    for dst in dest:
                        if F[path].get(src) == None:
                            continue
                        if F[path][src].get(dst) == None:
                            continue
                        sum += F[path][src][dst] * y0[j] * z0[k]
                        k += 1
                    j += 1
                x1[i] = sum
                i += 1

            j = 0
            for path in paths:
                sum = 0
                i = 0
                for src in source:
                    k = 0
                    for dst in dest:
                        if R[path].get(src) == None:
                            continue
                        if R[path][src].get(dst) == None:
                            continue
                        sum += R[path][src][dst] * x0[i] * z0[k]
                        k += 1
                    i += 1
                y1[j] = sum
                j += 1

            k = 0
            for dst in dest:
                sum = 0
                i = 0
                for src in source:
                    j = 0
                    for path in paths:
                        if T[path].get(src) == None:
                            continue
                        if T[path][src].get(dst) == None:
                            continue
                        sum += T[path][src][dst] * x0[i] * y0[j]
                        j += 1
                    i += 1
                z1[k] = sum
                k += 1

            x_diff = np.linalg.norm(np.subtract(x1, x0))
            y_diff = np.linalg.norm(np.subtract(y1, y0))
            z_diff = np.linalg.norm(np.subtract(z1, z0))
            if x_diff + y_diff + z_diff < E:
                break
            x0 = np.copy(x1)
            y0 = np.copy(y1)
            z0 = np.copy(z1)

        print('time:', t)
        src_list = []
        path_list = []
        dest_list = []
        i = 0
        for src in source:
            # print(src," : ",x1[i])
            src_list.append((src, x1[i]))
            i += 1
        j = 0
        for path in paths:
            # print(path," : ",y1[j])
            path_list.append((path, y1[j]))
            j += 1
        k = 0
        for dst in dest:
            # print(dst," : ",z1[k])
            dest_list.append((dst, z1[k]))
            k += 1

        print("\n")
        src_list.sort(key=lambda x: x[1], reverse=True)
        path_list.sort(key=lambda x: x[1], reverse=True)
        dest_list.sort(key=lambda x: x[1], reverse=True)

        pprint.pprint(src_list)
        pprint.pprint(path_list)
        pprint.pprint(dest_list)


lab1 = Stq()
lab1.metaPath_DPrRcD()
lab1.metaPath_DPrPrD()
lab1.metaPath_DPrPrR()
lab1.metaPath_DFPrPrrR()
lab1.metaPath_DFPrFD()
lab1.metaPath_DPrFPrD()
lab1.metaPath_DPrFPrPrrR()
lab1.cal_R2()
