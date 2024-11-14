import os
import re
import sqlite3
import pandas as pd
from flask import Flask, request, jsonify
from flask_cors import CORS
from langchain_nvidia_ai_endpoints import ChatNVIDIA
from langchain_core.messages import HumanMessage, SystemMessage

app = Flask(__name__)
CORS(app) 

# Initialize environment variables and the model
os.environ["LANGCHAIN_TRACING_V2"] = "true"
os.environ["LANGCHAIN_API_KEY"] = "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"
os.environ["NVIDIA_API_KEY"] = "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" 

model = ChatNVIDIA(model="meta/llama3-70b-instruct")

# Database connection
database = "northwind.db"
connection = sqlite3.connect(database, check_same_thread=False)

# Directory containing the CSV files
csv_directory = r"C:\Users\LENOVO\Downloads\Northwind+Traders\Northwind Traders"

def load_csv_to_db(csv_directory):
    for filename in os.listdir(csv_directory):
        if filename.endswith(".csv"):
            table_name = filename.split('.')[0]
            csv_path = os.path.join(csv_directory, filename)
            
            # Attempt to read CSV file with UTF-8 encoding, fallback to ISO-8859-1 if it fails
            try:
                df = pd.read_csv(csv_path, encoding='utf-8')
            except UnicodeDecodeError:
                print(f"UTF-8 encoding failed for {filename}. Retrying with ISO-8859-1 encoding.")
                df = pd.read_csv(csv_path, encoding='ISO-8859-1')
                
            # Save DataFrame to SQLite table, replacing if exists
            df.to_sql(table_name, connection, if_exists='replace', index=False)
            print(f"Loaded {filename} into table '{table_name}'.")

# Load CSV files on startup
load_csv_to_db(csv_directory)

@app.route('/generate_query', methods=['POST'])
def generate_query():
    data = request.get_json()
    user_command = data.get('query', '')

    # Set the prompt to guide the model to use the correct tables
    messages = [
SystemMessage(content="""
Your task is to generate SQL queries for an SQLite database with these tables and columns:

- **orders**:
  - orderID (INTEGER): Unique identifier for each order
  - customerID (INTEGER): The customer who placed the order
  - employeeID (INTEGER): The employee who processed the order
  - orderDate (TEXT): The date when the order was placed
  - requiredDate (TEXT): The date when the customer requested the order to be delivered
  - shippedDate (TEXT): The date when the order was shipped
  - shipperID (INTEGER): The ID of the shipping company used for the order
  - freight (REAL): The shipping cost for the order (USD)

- **order_details**:
  - orderID (INTEGER): The ID of the order this detail belongs to
  - productID (INTEGER): The ID of the product being ordered
  - unitPrice (REAL): The price per unit of the product at the time the order was placed (USD - discount not included)
  - quantity (INTEGER): The number of units being ordered
  - discount (REAL): The discount percentage applied to the price per unit

- **customers**:
  - customerID (INTEGER): Unique identifier for each customer
  - companyName (TEXT): The name of the customer's company
  - contactName (TEXT): The name of the primary contact for the customer
  - contactTitle (TEXT): The job title of the primary contact for the customer
  - city (TEXT): The city where the customer is located
  - country (TEXT): The country where the customer is located

- **products**:
  - productID (INTEGER): Unique identifier for each product
  - productName (TEXT): The name of the product
  - quantityPerUnit (TEXT): The quantity of the product per package
  - unitPrice (REAL): The current price per unit of the product (USD)
  - discontinued (INTEGER): Indicates with a 1 if the product has been discontinued
  - categoryID (INTEGER): The ID of the category the product belongs to

- **categories**:
  - categoryID (INTEGER): Unique identifier for each product category
  - categoryName (TEXT): The name of the category
  - description (TEXT): A description of the category and its products

- **employees**:
  - employeeID (INTEGER): Unique identifier for each employee
  - employeeName (TEXT): Full name of the employee
  - title (TEXT): The employee's job title
  - city (TEXT): The city where the employee works
  - country (TEXT): The country where the employee works
  - reportsTo (INTEGER): The ID of the employee's manager

- **shippers**:
  - shipperID (INTEGER): Unique identifier for each shipper
  - companyName (TEXT): The name of the company that provides shipping services

Ensure that generated queries strictly use these column names without creating new ones. Also, maintain the exact case sensitivity of each column and table name.
"""),
        HumanMessage(content=user_command),
    ]

    # Invoke the model with the messages
    ai_message = model.invoke(messages)
    
    # Extract the SQL query
    sql_query_match = re.search(r'```(.*?)```', ai_message.content, re.DOTALL)
    if not sql_query_match:
        return jsonify({"success": False, "message": "Failed to generate SQL query."})

    sql_query = sql_query_match.group(1).strip()
    # sql_query  = "SELECT SUM(freight) AS total_shipping_cost FROM orders JOIN customers ON orders.customerID = customers.customerID WHERE customers.customerID = 42;"

    
    
    print("Generated SQL Query:", sql_query)

    # Execute the SQL query and return results as a JSON table
    try:
        df = pd.read_sql_query(sql_query, connection)
        result_data = df.to_dict(orient='records')  # Convert the DataFrame to a list of dicts
        return jsonify({"success": True, "sql_query": sql_query, "result": result_data})
    except Exception as e:
        return jsonify({"success": False, "message": f"Error executing query: {str(e)}"})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
