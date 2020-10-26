import pandas as pd
from pandasgui import show


Objects_df = pd.read_csv('Objects.csv')
# print(Objects_df)
gui = show(Objects_df)