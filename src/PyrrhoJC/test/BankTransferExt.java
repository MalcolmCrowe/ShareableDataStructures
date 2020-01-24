/* DBTechNet Concurrency Lab 15.5.2008 Martti Laiho
Bank Transfer Extended using Java/JDBC
Updates ..
4.2 2018-04-25 ML adopted the transaction outcome protocol
# compile
cd $HOME/AppDev/JDBC
javac BankTransferExt.java
# sample test using Pyrrho
cd $HOME/AppDev/JDBC
export CLASSPATH=.
export driver="PyrrhoJC"
export URL="testdb"
export user=dbtech
export password=dbtech
export fromAcct=202
export toAcct=100
export amount=100
export custno=303
export sleep=0
export timeout=10
export maxRetries=2
java BankTransferExt
************************************************************************/
import java.io.*;
import org.pyrrhodb.*;
import java.util.Date;

public class BankTransferExt {

    public static class Outcomes {

        int rc; // return code
        String msg; // error message

        Outcomes(int code, String errMsg) {
            rc = code;
            msg = errMsg;
        }
    }

    public static void main(String args[]) throws Exception {
        System.out.println("BankTransferExt version 4.2");
        Connection conn = null;
        boolean sqlServer = false;
        int counter = 0;
        int rc = 0;
        String driver = "PyrrhoJC"; //System.getenv("driver");
        String URL = "testdb"; //System.getenv("URL");
        String user = ""; //System.getenv("user");
        String password = ""; //System.getenv("password");
        int fromAcct = 202; //Integer.parseInt(args[1]); //System.getenv("fromAcct"));
        int toAcct = 100; //Integer.parseInt(args[2]); //System.getenv("toAcct"));
        int amount = 100; //Integer.parseInt(args[3]); //System.getenv("amount"));
        int custno = 303; //Integer.parseInt(args[4]); //System.getenv("custno"));
        int sleep = 0; //Integer.parseInt(System.getenv("sleep"));
        int timeout = 0; //Integer.parseInt(System.getenv("timeout"));
        int maxRetries = 0;// Integer.parseInt(System.getenv("maxRetries"));
// SQL Server's explicit transactions will require special treatment
        if (URL.length()>18 && URL.substring(5, 14).equals("sqlserver")) {
            sqlServer = true;
        }
// register the JDBC driver and open connection
        try {
           // Class.forName(driver);
            conn = DriverManager.getConnection(URL, user, password);
            conn.setAutoCommit(false);
            conn.setTransactionIsolation(
                    Connection.TRANSACTION_SERIALIZABLE);
            PreparedStatement pstmt = conn.prepareStatement(
                    "SELECT name FROM Customers WHERE custno=? ");
// setting the parameter values
            pstmt.setInt(1, custno); // Who ?
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                String name = rs.getString(1);
                System.out.println("Welcome " + name);
            } else {
                throw new Exception("Unknown customer " + custno + " !");
            }
            rs.close();
            pstmt.close();
        } catch (SQLException ex) {
            System.out.println("URL: " + URL);
            System.out.println("** Connection failure: " + ex.getMessage()
                    + "\n SQLSTATE: " + ex.getSQLState()
                    + " SQLcode: " + ex.getErrorCode());
            System.exit(-1);
        }
        String msg = " ";
        long startTime = System.currentTimeMillis();
        counter = -1;
        do {
// Retry wrapper block of TransaferTransaction -------------
            if (counter++ > 0) {
                System.out.println("retry #" + counter);
                if (sqlServer) {
                    conn.close();
                    System.out.println("Connection closed");
                    conn = DriverManager.getConnection(URL, user, password);
                    conn.setAutoCommit(true);
                }
            }
            Outcomes oc = TransferTransaction(conn, fromAcct, toAcct, amount,
                    custno, sqlServer, sleep, timeout);
            rc = oc.rc;
            msg = oc.msg;
            if (rc == 1) {
                long pause = (long) (Math.random() * 1000); // max 1 sec.
// just for testing:
                System.out.println("Waiting for " + pause + " mseconds before rc");
                Thread.sleep(pause);
            }
            if (counter >= maxRetries) {
                rc = 3; // live lock !
            }
        } while (rc == 1 && counter < 10); // max 10 retries
// end of the Retry wrapper block -------------------------------
        conn.close();
        long stopTime = System.currentTimeMillis();
        double totalTime = 0.001 * (double) (stopTime - startTime);
        conn.close();
        System.out.print("\nElapsed time " + String.format("%.4f", totalTime));
        System.out.print(", Return code= " + rc + " Retries used: " + (counter));
        System.out.print("\nerror message: " + msg);
        System.out.println("\n End of Program. ");
    }

    static Outcomes TransferTransaction(Connection conn, int fromAcct, int toAcct,
            int amount, int custno, boolean sqlServer, int sleep, int timeout)
            throws Exception {
        DatabaseMetaData dmd = conn.getMetaData();
        String dbms = dmd.getDatabaseProductName();
//System.out.println("dbms="+dbms.substring(0,3));
        int user = custno;
        int allowed;
        String status = "";
        String SQLState = "*****";
        String errMsg = "";
        String sql = "";
        int rc = 0;
        try {
            errMsg = "";
            rc = 0;
            if (timeout > 0 && dbms.equals("MySQL")) { // includes MariaDB
                sql = "SET innodb_lock_wait_timeout = ? ";
//System.out.println ("MariaDB: "+sql);
                PreparedStatement pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, timeout);
                pstmt.executeUpdate();
                pstmt.close();
            }
// is the fromAcct available?
            sql = "SELECT status FROM Accounts WHERE acctno = ? FOR UPDATE";
            PreparedStatement pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, fromAcct);
            pstmt.setQueryTimeout(60);
            pstmt.setQueryTimeout(timeout);
            ResultSet rs = pstmt.executeQuery();
            if (!rs.next()) {
                throw new Exception("*Missing 'from' account " + fromAcct);
            }
            status = rs.getString(1);
            if (status.equals("C")) {
                throw new Exception("* Account " + fromAcct + " is closed");
            }
            pstmt.close();
// OK, let's UPDATE if the user is the owner
            sql = "UPDATE Accounts SET balance = balance + ? "
                    + "WHERE acctno = ? AND custno = ?";
            PreparedStatement pstmt1 = conn.prepareStatement(sql);
// setting the parameter values
            pstmt1.setInt(1, -amount); // how much money to withdraw
            pstmt1.setInt(2, fromAcct); // from which account
            pstmt1.setInt(3, custno); // whose account
            int count1 = pstmt1.executeUpdate();
// System.out.println("count1 for owner is "+ count1);
            if (count1 != 1) { // so this user is not the owner
// Has the user permission to use the fromAcct ?
// locking first the Grants row to make sure that
// we will read the current row also in case of MVCC
                sql = "SELECT acctno FROM Grants WHERE custno = ? FOR UPDATE";
                pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, custno); // the user
                pstmt.setQueryTimeout(timeout);
                pstmt.executeQuery();
// ** for Pyrrho we must close the resultset before creating another
// ** PreparedStatement will do this if we close it at least
                pstmt.close();
// then checking if the account use is granted
// ** for Pyrrho we can't have extra brackets after FROM 
// ** because ( introduces a subquery
                sql = "SELECT A.custno FROM Customers C "
                        + "JOIN Grants G ON (C.custno=G.custno) "
                        + "JOIN Accounts A ON (G.acctno=A.acctno) "
                        + "WHERE A.acctno = ? AND C.custno = ? AND G.granted='Y'";
                pstmt = conn.prepareStatement(sql);
                pstmt.setInt(1, fromAcct);
                pstmt.setInt(2, custno); // the user
                rs = pstmt.executeQuery();
                if (rs.next()) {
                    allowed = rs.getInt(1);
                    pstmt1.setInt(3, allowed); // whose account
// System.out.println("owner of fromAcct is "+ allowed);
                    count1 = pstmt1.executeUpdate();
// System.out.println("count1 for user is "+ count1);
                    if (count1 != 1) {
                        throw new Exception("* Missing account " + fromAcct);
                    }
                } else {
// System.out.println("..else of rs.next() ");
                    throw new Exception("* Missing permission to use account "
                            + fromAcct);
                }
                rs.close();
                pstmt.close();
            }
            if (sleep > 0) {
// ************* pause just for concurrency testing ******
                System.out.print("\nTime for concurrency test "
                        + sleep + " seconds ..");
                Thread.sleep(1000 * sleep);
                System.out.print("\n..transaction continues!\n");
// ********************************************************
            }
// is the toAcct available?
            pstmt = conn.prepareStatement(
                    "SELECT status FROM Accounts WHERE acctno = ? FOR UPDATE");
            pstmt.setInt(1, toAcct);
            pstmt.setQueryTimeout(60);
            pstmt.setQueryTimeout(timeout);
            rs = pstmt.executeQuery();
            if (!rs.next()) {
                throw new Exception("* Missing 'to' account " + toAcct);
            }
            status = rs.getString(1);
            if (status.equals("C")) {
                throw new Exception("* Account " + toAcct + " is closed");
            }
            pstmt.close();
// OK, let's UPDATE:
            sql = "UPDATE Accounts SET balance = balance + ? WHERE acctno = ? ";
            pstmt1 = conn.prepareStatement(sql);
            pstmt1.setInt(1, amount); // how much money to add
            pstmt1.setInt(2, toAcct); // to which account
            int count2 = pstmt1.executeUpdate();
            if (count2 != 1) {
                throw new Exception("* Account " + toAcct + " is missing!");
            }
// tracing the Transfer into TransferHistory
            Date date = new Date();
            long t = date.getTime();
            Date sqlDate = new Date(t);
            Timestamp sqlTimestamp = new Timestamp(t);
// System.out.println("sqlDate=" + sqlDate);
// System.out.println("sqlTimestamp=" + sqlTimestamp);
            sql
                    = "INSERT INTO TransferHistory "
                    + "(fromAcct,toAcct,amount,custno,onDate,atTime) "
                    + "VALUES (?, ?, ?, ?, {d '" + sqlDate + "'},{ts '" + sqlTimestamp + "'})";
            pstmt = conn.prepareStatement(sql);
            pstmt.setInt(1, fromAcct);
            pstmt.setInt(2, toAcct);
            pstmt.setInt(3, amount);
            pstmt.setInt(4, custno);
            int count3 = pstmt.executeUpdate();
            if (count3 != 1) {
                throw new Exception("* INSERT into TransferHistory failed!");
            }
// COMMIT
            System.out.print("committing ..");
            conn.commit(); // end of transaction
            pstmt1.close();
        } catch (SQLException ex) {
            rc = -1;
            try {
                errMsg = "SQLException:";
                while (ex != null) {
                    SQLState = ex.getSQLState();
                    errMsg = errMsg + " SQLSTATE=" + SQLState + ", " + ex.getMessage();
// is it a concurrency conflict?
                    System.out.println("dbms=" + dbms + " SQLState=" + SQLState);
                    if (dbms.equals("Oracle")) {
                        if (SQLState.equals("61000")) {
// Oracle ORA-00060: deadlock detected
                            conn.rollback(); // explicit rollback needed for Oracle
                            rc = 1;
                        } else if (SQLState.equals("72000")) { // timeout
                            conn.rollback();
                            rc = 2;
                        }
                    } else if (SQLState.equals("40001")) { // DB2, SQL Server, MariaDB, ...
                        rc = 1;
                    } else if (dbms.substring(0, 3).equals("DB2") && SQLState.equals("08001")) {
                        rc = 2; // timeout expired
                    } else if (dbms.equals("MySQL") && errMsg.indexOf("timeout") > 0) { // MariaDB
                        conn.rollback(); // explicit rollback
                        rc = 2; // Lock wait timeout exceeded
                    } else if (dbms.equals("PostgreSQL")) {
                        if (SQLState.equals("40P01")) { // PostgreSQL
                            conn.rollback(); // explicit rollback needed
                            rc = 1;
                        } else if (SQLState.equals("57014")) { // PostgreSQL
                            conn.rollback(); // explicit rollback needed
                            rc = 2;
                        }
                    }
                    ex = ex.getNextException();
                }
// println for testing purposes
                System.out.println("\n** " + errMsg);
            } catch (Exception e) {
// In case of possible problems in SQLException handling
                System.out.println(sql + "\nSQLException handling error: " + e);
                conn.rollback(); // Current transaction is rolled back
                rc = -1; // This is reserved for potential exception handling
            }
            if (rc == -1) {
                conn.rollback();
            }
        } // SQLException
        catch (Exception e) {
            errMsg = e.getMessage();
// System.out.println(errMsg);
            conn.rollback(); // Current transaction is rolled back also in this case
            rc = -1; // This is reserved for potential other exception handling
        } // other exceptions
        finally {
            return new Outcomes(rc, errMsg);
        }
    }
}
