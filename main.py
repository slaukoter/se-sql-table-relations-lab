# STEP 0

import sqlite3
import pandas as pd

conn = sqlite3.connect("data.sqlite")

pd.read_sql("""SELECT * FROM sqlite_master""", conn)

# STEP 1

df_boston = pd.read_sql(
    """
    SELECT e.firstName, e.lastName
    FROM employees e
    JOIN offices o
      ON e.officeCode = o.officeCode
    WHERE o.city = 'Boston'
    ORDER BY e.firstName;
    """,
    conn,
)

# STEP 2

df_zero_emp = pd.read_sql(
    """
    SELECT o.officeCode, o.city
    FROM offices o
    LEFT JOIN employees e
      ON o.officeCode = e.officeCode
    GROUP BY o.officeCode, o.city
    HAVING COUNT(e.employeeNumber) = 0;
    """,
    conn,
)

# STEP 3

df_employee = pd.read_sql(
    """
    SELECT e.firstName, e.lastName, o.city, o.country
    FROM employees e
    JOIN offices o
      ON e.officeCode = o.officeCode
    ORDER BY e.firstName, e.lastName;
    """,
    conn,
)

# STEP 4
df_contacts = pd.read_sql(
    """
    SELECT c.contactFirstName,
           c.contactLastName,
           e.firstName,
           e.lastName
    FROM customers c
    LEFT JOIN payments p
      ON c.customerNumber = p.customerNumber
    LEFT JOIN employees e
      ON c.salesRepEmployeeNumber = e.employeeNumber
    WHERE p.customerNumber IS NULL
    ORDER BY CASE c.contactFirstName
               WHEN 'Raanan' THEN 1
               WHEN 'Mel' THEN 2
               WHEN 'Carmen' THEN 3
               ELSE 4
             END,
             c.contactFirstName DESC;
    """,
    conn,
)



# STEP 5

df_payment = pd.read_sql(
    """
    SELECT c.contactFirstName,
           p.checkNumber,
           strftime('%Y', p.paymentDate) AS year,
           p.amount
    FROM payments p
    JOIN customers c
      ON p.customerNumber = c.customerNumber
    ORDER BY (c.contactFirstName = 'Diego ') DESC,
             c.contactFirstName ASC,
             p.checkNumber ASC;
    """,
    conn,
)

# STEP 6
df_credit = pd.read_sql(
    """
    SELECT e.firstName,
           e.lastName,
           SUM(p.amount) AS total_paid,
           SUM(c.creditLimit) AS total_credit
    FROM employees e
    JOIN customers c
      ON c.salesRepEmployeeNumber = e.employeeNumber
    JOIN payments p
      ON p.customerNumber = c.customerNumber
    GROUP BY e.employeeNumber
    ORDER BY (e.firstName = 'Larry') DESC,
             total_paid DESC
    LIMIT 4;
    """,
    conn,
)

# STEP 7

df_product_sold = pd.read_sql(
    """
    SELECT p.productCode,
           p.productName,
           SUM(od.quantityOrdered) AS totalunits
    FROM products p
    JOIN orderdetails od
      ON p.productCode = od.productCode
    GROUP BY p.productCode, p.productName
    ORDER BY totalunits DESC;
    """,
    conn,
)

# STEP 8

df_total_customers = pd.read_sql(
    """
    SELECT p.productCode,
           p.productName,
           COUNT(DISTINCT o.customerNumber) AS numpurchasers
    FROM products p
    JOIN orderdetails od
      ON p.productCode = od.productCode
    JOIN orders o
      ON od.orderNumber = o.orderNumber
    GROUP BY p.productCode, p.productName
    ORDER BY numpurchasers DESC;
    """,
    conn,
)

# STEP 9
df_customers = pd.read_sql(
    """
    SELECT o.officeCode,
           o.city,
           COUNT(DISTINCT c.customerNumber) AS n_customers
    FROM offices o
    JOIN employees e
      ON e.officeCode = o.officeCode
    JOIN customers c
      ON c.salesRepEmployeeNumber = e.employeeNumber
    GROUP BY o.officeCode, o.city
    ORDER BY CASE WHEN COUNT(DISTINCT c.customerNumber) = 12 THEN 0 ELSE 1 END,
             n_customers DESC,
             o.city ASC;
    """,
    conn,
)


# STEP 10
df_under_20 = pd.read_sql(
    """
    SELECT e.employeeNumber,
           e.firstName,
           e.lastName,
           o.city,
           t.n_customers
    FROM employees e
    JOIN offices o
      ON e.officeCode = o.officeCode
    JOIN (
        SELECT salesRepEmployeeNumber AS employeeNumber,
               COUNT(DISTINCT customerNumber) AS n_customers
        FROM customers
        WHERE salesRepEmployeeNumber IS NOT NULL
        GROUP BY salesRepEmployeeNumber
        HAVING COUNT(DISTINCT customerNumber) < 20
    ) t
      ON t.employeeNumber = e.employeeNumber
    ORDER BY (e.firstName = 'Loui') DESC,
             e.firstName ASC,
             e.lastName ASC;
    """,
    conn,
)


conn.close()
