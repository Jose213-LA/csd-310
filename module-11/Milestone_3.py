# Name: Jose Flores, Laura Makokha
# Date: 5/10/2025
# Assignment_11.1: Bacchus Winery Project Milestone #3



import mysql.connector
from dotenv import dotenv_values

# Load credentials
secrets = dotenv_values(".env")

# DB Config
config = {
    "user": secrets["USER"],
    "password": secrets["PASSWORD"],
    "host": secrets["HOST"],
    "database": secrets["DATABASE"],
    "raise_on_warnings": True
}

def connect_to_db():
    try:
        return mysql.connector.connect(**config)
    except mysql.connector.Error as err:
        print("DB Connection Error:", err)
        return None

def run_report(cursor, query, title):
    print(f"\n--- {title} ---")
    cursor.execute(query)
    results = cursor.fetchall()
    for row in results:
        print(row)
    print(f"\n{len(results)} rows returned.\n")

def main():
    db = connect_to_db()
    if db:
        cursor = db.cursor(dictionary=True)

        # Report 1: Late Supply Deliveries
        query1 = """
        SELECT sd.Delivery_ID, s.Supply_Name, sup.Supplier_Name,
               sd.Planned_Delivery_Date, sd.Actual_Delivery_Date,
               DATEDIFF(sd.Actual_Delivery_Date, sd.Planned_Delivery_Date) AS Days_Late
        FROM Supply_Deliveries sd
        JOIN Supplies s ON sd.Supply_ID = s.Supply_ID
        JOIN Suppliers sup ON sd.Supplier_ID = sup.Supplier_ID
        WHERE DATEDIFF(sd.Actual_Delivery_Date, sd.Planned_Delivery_Date) > 0;
        """
        run_report(cursor, query1, "Late Supply Deliveries Report")

        # Report 2: Wine Sales by Distributor
        query2 = """
        SELECT d.Distributor_Name, w.Wine_Name, wd.Quantity_Sold, wd.Sales_Date
        FROM Wine_Distribution wd
        JOIN Wine w ON wd.Wine_ID = w.Wine_ID
        JOIN Distributors d ON wd.Distributor_ID = d.Distributor_ID
        ORDER BY w.Wine_Name, wd.Sales_Date;
        """
        run_report(cursor, query2, "Wine Sales by Type and Distributor")

        # Report 3: Employee Hours by Quarter
        query3 = """
        SELECT e.Employee_Name, e.Department, eh.Quarter, eh.Hours_Worked
        FROM Employee_Hours eh
        JOIN Employee e ON eh.Employee_ID = e.Employee_ID
        ORDER BY e.Employee_Name, eh.Quarter;
        """
        run_report(cursor, query3, "Employee Hours by Quarter")

        cursor.close()
        db.close()

if __name__ == "__main__":
    main()
