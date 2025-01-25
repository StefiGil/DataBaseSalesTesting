package storeProcedureTesting;

import java.sql.*;

import org.testng.Assert;
import org.apache.commons.lang3.StringUtils;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;



public class SPTesting {
    Connection con=null;
    Statement stmt = null;
    ResultSet rs;
    private CallableStatement cStmt;
    private ResultSet rs1;
    private ResultSet rs2;

    @BeforeClass
    void setup() throws SQLException {
        con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "root");
    }

    @AfterClass
    void tearDown() throws SQLException {
        con.close();
    }

    // TC001: Check stored procedures are exists in database.
    @Test
    void test_storedProceduresExists() throws SQLException {
        stmt = con.createStatement();
        rs = stmt.executeQuery("SHOW PROCEDURE STATUS WHERE Name='SelectAllCustomers'");
        rs.next();
        Assert.assertEquals(rs.getString("Name"), "SelectAllCustomers");
    }

    // TC002: Test to verify that the stored procedure "SelectAllCustomers"
    // returns the same results as the SQL query "SELECT * FROM customers".
    @Test
    void test_SelectAllCustomers() throws SQLException {
        cStmt = con.prepareCall("{CALL SelectAllCustomers()}");
        rs1 = cStmt.executeQuery(); // resultSet1
        Statement stmt = con.createStatement();
        rs2 = stmt.executeQuery("select * from customers");
        Assert.assertEquals(compareResultSets(rs1, rs2), true);
    }

    // T003: Test to verify that the stored procedure "SelectAllCustomersByCity"
    // with the input "Singapore" returns the same results as the SQL query
    // "SELECT * FROM Customers WHERE city = 'Singapore'".
    @Test
    void test_SelectAllCustomersByCity() throws SQLException {
        cStmt = con.prepareCall("{call SelectAllCustomersByCity(?)}");
        cStmt.setString(1, "Singapore");
        rs1 = cStmt.executeQuery(); // resultSet1
        Statement stmt = con.createStatement();
        rs2 = stmt.executeQuery("SELECT * FROM Customers WHERE city = 'Singapore'");
        Assert.assertEquals(compareResultSets(rs1, rs2), true);
    }

    // T004: This test verifies that the stored procedure 'SelectAllCustomersByCity' retrieves the correct results
    // by comparing its output with an equivalent SQL query on the 'Customers' table.
    @Test
    void test_SelectAllCustomersByCityAndPincode() throws SQLException {
        cStmt = con.prepareCall("{call SelectAllCustomersByCityAndPin(?,?)}");
        cStmt.setString(1, "Singapore");
        cStmt.setString(2, "079903");
        rs1 = cStmt.executeQuery(); // resultSet1
        Statement stmt = con.createStatement();
        rs2 = stmt.executeQuery("SELECT * FROM Customers WHERE City = 'Singapore' and postalCode='079903'");
        Assert.assertEquals(compareResultSets(rs1, rs2), true);
    }

    // Compares two ResultSets (resultSet1 and resultSet2) to ensure they have the same data.
    // It iterates through all rows and compares each column's value for equality.
    public boolean compareResultSets(ResultSet resultSet1, ResultSet resultSet2) throws SQLException {
        while (resultSet1.next()) {
            resultSet2.next(); // Advances both ResultSets to the next row
            int count = resultSet1.getMetaData().getColumnCount(); // Retrieves the number of columns in the row
            for (int i = 1; i <= count; i++) { // Loops through each column
                // Compares the values of the columns in the current row
                if (!StringUtils.equals(resultSet1.getString(i), resultSet2.getString(i))) {
                    return false; // Returns false if any column value does not match
                }
            }
        }
        return true; // Returns true if all rows and columns match
    }

    //T005: Check stored procedure "get_order_by_cust" by passing input parameter 'custID'
    @Test
    void test_get_order_by_cust() throws SQLException {
        cStmt = con.prepareCall("{call get_order_by_cust(?,?,?,?,?)}");
        cStmt.setInt(1, 141);

        cStmt.registerOutParameter(2, Types.INTEGER);
        cStmt.registerOutParameter(3, Types.INTEGER);
        cStmt.registerOutParameter(4, Types.INTEGER);
        cStmt.registerOutParameter(5, Types.INTEGER);

        cStmt.executeQuery();

        int shipped = cStmt.getInt(2);
        int canceled = cStmt.getInt(3);
        int resolved = cStmt.getInt(4);
        int disputed = cStmt.getInt(5);

        //System.out.println(shipped + " " + canceled + " " + resolved + " " + disputed);

        Statement stmt = con.createStatement();
        rs = stmt.executeQuery("SELECT \n" +
                "    (SELECT COUNT(*) AS 'shipped' FROM orders WHERE customerNumber = 141 AND status = 'Shipped') AS Shipped,\n" +
                "    (SELECT COUNT(*) AS 'canceled' FROM orders WHERE customerNumber = 141 AND status = 'Canceled') AS Canceled,\n" +
                "    (SELECT COUNT(*) AS 'resolved' FROM orders WHERE customerNumber = 141 AND status = 'Resolved') AS Resolved,\n" +
                "    (SELECT COUNT(*) AS 'disputed' FROM orders WHERE customerNumber = 141 AND status = 'Disputed') AS Disputed;\n");
        rs.next();

        int exp_shipped = rs.getInt("shipped");
        int exp_canceled = rs.getInt("canceled");
        int exp_resolved = rs.getInt("resolved");
        int exp_disputed = rs.getInt("disputed");

        if (shipped == exp_shipped && canceled == exp_canceled && resolved == exp_resolved && disputed == exp_disputed) {
            Assert.assertTrue(true);
        } else {
            Assert.assertTrue(false);
        }
    }

    //T006: Check Stored procedure "GetCustomerShipping" by passing input parameter custID
    @Test
    void test_GetCustomerShipping() throws SQLException {
        cStmt = con.prepareCall("{call GetCustomerShipping(?,?)}");
        cStmt.setInt(1, 121);
        cStmt.registerOutParameter(2, Types.VARCHAR);
        cStmt.executeQuery();
        String shippingTime = cStmt.getString(2);

        Statement stmt = con.createStatement();
        rs = stmt.executeQuery("SELECT \n" +
                "    country, \n" +
                "    CASE \n" +
                "        WHEN country = 'USA' THEN '2-day Shipping'\n" +
                "        WHEN country = 'Canada' THEN '3-day Shipping'\n" +
                "        ELSE '5-day Shipping'\n" +
                "    END AS ShippingTime \n" +
                "FROM customers \n" +
                "WHERE customerNumber = 121");
        rs.next();
        String exp_shippingTime = rs.getString("ShippingTime");

        Assert.assertEquals(shippingTime, exp_shippingTime);
    }
}
