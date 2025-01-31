import pandas as pd

def run_transformation():
    # Load data
  data = pd.read_csv(r'zipco_transaction.csv')
      
  #Removing duplicates
  data.drop_duplicates(inplace=True)

  # handling missing values (filling missing numerical values with mean or median)   
  numerical_columns = data.select_dtypes(include=['float64','int64']).columns
  for column in numerical_columns:
      data.fillna({column: data[column].mean()}, inplace=True)


  # handling missing values (filling missing string values with unknown)
  string_columns = data.select_dtypes(include=['object']).columns
  for column in string_columns:
      data.fillna({column: 'unknown'}, inplace=True)

  # cleaning data column : assigning the right data type
  data['Date'] = pd.to_datetime(data['Date'])

  #Create the Product Table
  products = data[['ProductName']].drop_duplicates().reset_index(drop=True)
  products.index.name = 'ProductID'
  products = products.reset_index()

  products.head(20)

  #Create the Customer Table
  customers = data[['CustomerName','CustomerAddress',     
   'Customer_PhoneNumber','CustomerEmail']].drop_duplicates().reset_index(drop=True)
  customers.index.name = 'CustomerID'
  customers = customers.reset_index()

  customers.head()

  #staff table
  staff = data[['Staff_Name','Staff_Email',]].drop_duplicates().reset_index(drop=True)
  staff.index.name = 'StaffID'
  staff = staff.reset_index()

  staff.head() 


  #Create the Transaction Table
  transaction = data.merge(products, on='ProductName', how='left') \
                    .merge(customers, on=['CustomerName','CustomerAddress', 'Customer_PhoneNumber','CustomerEmail'], how='left')\
                    .merge(staff, on=['Staff_Name','Staff_Email'], how='left')

  transaction.index.name = 'TransactionID'
  transaction = transaction.reset_index()[['Date','TransactionID','ProductID','Quantity','UnitPrice','StoreLocation','PaymentType','PromotionApplied',
      'Weather','Temperature','StaffPerformanceRating','CustomerFeedback','DeliveryTime_min','OrderType','CustomerID','StaffID','DayOfWeek','TotalSales']]

  transaction.head()
  

  # save data as csv files
  data.to_csv('clean_data.csv', index=False)
  products.to_csv('products.csv', index=False)
  customers.to_csv('customers.csv', index=False)
  staff.to_csv('staff.csv', index=False)
  transaction.to_csv('transaction.csv', index=False)

  print("Data Cleaning and Transformation Completed Successfully")
