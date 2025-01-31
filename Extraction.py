import pandas as pd

#Extraction of Data 
def run_extraction():
  try:
    data = pd.read_csv(r'zipco_transaction.csv')
    print("Data Extracted Successfully")
  except Exception as e:
    print("Data Extraction Failed")
    print(e)
