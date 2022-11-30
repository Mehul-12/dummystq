from pymongo import MongoClient
import copy
import numpy as np
import pprint


def compute(dt):
    paths = set()
    source = set()
    dest = set()
    for path in dt:
        paths.add(path)
        for src in dt[path]:
            source.add(src)
            for dst in dt[path][src]:
                dest.add(dst)

    # pprint.pprint(dictionary)
    F = copy.deepcopy(dt)
    R = copy.deepcopy(dt)
    T = copy.deepcopy(dt)

    for path in paths:
        for src in source:
            for dst in dest:
                if dt[path].get(src) == None:
                    continue
                if dt[path][src].get(dst) == None:
                    continue
                sum1 = 0
                for src2 in source:
                    if dt[path].get(src2) == None:
                        continue
                    if dt[path][src2].get(dst) == None:
                        continue
                    sum1 += dt[path][src2][dst]

                sum2 = 0
                for dst2 in dest:
                    if dt[path].get(src) == None:
                        continue
                    if dt[path][src].get(dst2) == None:
                        continue
                    sum2 += dt[path][src][dst2]

                sum3 = 0
                for path2 in paths:
                    if dt[path2].get(src) == None:
                        continue
                    if dt[path2][src].get(dst) == None:
                        continue
                    sum3 += dt[path2][src][dst]

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

    src_list.sort(key=lambda x: x[1], reverse=True)
    path_list.sort(key=lambda x: x[1], reverse=True)
    dest_list.sort(key=lambda x: x[1], reverse=True)

    pprint.pprint(src_list)
    pprint.pprint(path_list)
    pprint.pprint(dest_list)    