package storeFunctionsTesting;

import org.apache.commons.lang3.StringUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.sql.*;

public class SFTesting {

    Connection con=null;
    Statement stmt;
    ResultSet rs;

    @BeforeClass
    void setup() throws SQLException {
        con = DriverManager.getConnection("jdbc:mysql://localhost:3306/classicmodels", "root", "root");
    }

    @AfterClass
    void tearDown() throws SQLException {
        con.close();
    }

    @Test
    void test_storedFunctionExists() throws SQLException {
        ResultSet rs = con.createStatement().executeQuery("SHOW FUNCTION STATUS WHERE Name='CustomerLevel'");
        rs.next();
        Assert.assertEquals(rs.getString("Name"), "CustomerLevel");
    }

    @Test
    void test_CustomerLevel_with_SQLStatement() throws SQLException {
        ResultSet rs1 = con.createStatement().executeQuery("SELECT customerName, customerLevel(creditLimit) FROM customers");
        ResultSet rs2 = con.createStatement().executeQuery(
                "SELECT customerName,\n" +
                "CASE\n" +
                "    WHEN creditLimit > 50000 THEN 'PLATINUM'\n" +
                "    WHEN creditLimit >= 10000 AND creditLimit <= 50000 THEN 'GOLD'\n" +
                "    WHEN creditLimit < 10000 THEN 'SILVER'\n" +
                "END AS customerLevel\n" +
                "FROM customers;\n");
        Assert.assertEquals(compareResultSets(rs1, rs2), true);
    }

    @Test
    void test_CustomerLevel_with_StoredProcedure() throws SQLException {
        CallableStatement cStmt = con.prepareCall("{CALL GetCustomerLevel(?,?)}");
        cStmt.setInt(1, 131);
        cStmt.registerOutParameter(2, Types.VARCHAR);
        cStmt.executeQuery();

        String custlevel = cStmt.getString(2);

        ResultSet rs = con.createStatement().executeQuery("SELECT customerName,\n" +
                "CASE\n" +
                "    WHEN creditLimit > 50000 THEN 'PLATINUM'\n" +
                "    WHEN creditLimit >= 10000 AND creditLimit < 50000 THEN 'GOLD'\n" +
                "    WHEN creditLimit < 10000 THEN 'SILVER'\n" +
                "END AS customerLevel\n" +
                "FROM customers\n" +
                "WHERE customerNumber = 131;\n");
        rs.next();
        String exp_custlevel = rs.getString("customerlevel");

        Assert.assertEquals(custlevel, exp_custlevel);
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
}
