from itertools import combinations
import time

if __name__ == "__main__":
    start = time.time()
    impressionVector = ["domain","city","state","weekday","hour","daypart","size","isp"]
    dimensions = len(impressionVector)
    r = range(dimensions)
    ruleSet=[]
    for i in range(dimensions + 1):
        for c in combinations(r, i):
            result = ["*"] * dimensions
            for j in c:
                result[j] = impressionVector[j]
            rule = '|'.join(str(n) for n in result)    
            ruleSet.append(rule)

    timeTaken = time.time() - start
    print timeTaken*1000