import random, string
import pandas as pd
from sklearn.datasets import make_regression
import snowflake.snowpark as snowpark

# 13 min for 100M rows w/ SO-Medium
def main(session: snowpark.Session): 
    X, _ = make_regression(n_samples=100000000, n_features=2, noise=0.1, random_state=0)
    X = pd.DataFrame(X, columns=["N1", "N2"])
    
    cat_features = {}
    for c in ["C1", "C2"]:
        cat_features[c] = ["".join(random.choices(string.ascii_uppercase, k=2))
            for _ in range(X.shape[0])]
    X = X.assign(**cat_features)
    
    df = session.create_dataframe(X)
    df.write.mode("overwrite").save_as_table("REGRESSION_DATASET")
    return df