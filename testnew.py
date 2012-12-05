from collections import defaultdict

def autovivify(levels=1, final=dict):
    return (defaultdict(final) if levels < 2 else defaultdict(lambda: autovivify(levels - 1, final)))
            
words = autovivify(5, int)

words["sam"][2012][5][25]["hello"] += 1
words["sue"][2012][5][24]["today"] += 1