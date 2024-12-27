import pandas as pd

# predefined
data_1 = {"source": [1,1,1,2,2],
     "target": [6,7,8,9,10]}

# monopartite
data_2 = {"source": [1,2,3,4,5],
     "target": [3,4,5,6,7]}

# bipartite
data_3 = {"source": [1,1,1,2,2],
     "target": [6,7,8,9,6]}


loc = "tests/data/"
pd.DataFrame(data=data_1).to_csv(loc+"predefined.csv", index=False)
pd.DataFrame(data=data_2).to_csv(loc+"monopartite.csv", index=False)
pd.DataFrame(data=data_3).to_csv(loc+"bipartite.csv", index=False)

