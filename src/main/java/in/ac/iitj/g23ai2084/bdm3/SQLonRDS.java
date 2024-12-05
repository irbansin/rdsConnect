package in.ac.iitj.g23ai2084.bdm3;
import io.github.cdimascio.dotenv.Dotenv;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLonRDS {
    Dotenv dotenv = Dotenv.load();

    private Connection con;
    private final String url = dotenv.get("DB_URL");
    private final String uid = dotenv.get("DB_USERNAME");
    private final String pw = dotenv.get("DB_PASSWORD");

    public static void main(String[] args) {
        SQLonRDS q = new SQLonRDS();
        try {
            q.connect();
            q.create(); // TODO: Implement as per requirements
            q.insert(); // TODO: Implement as per requirements
            q.queryOne(); // TODO: Implement as per requirements
            q.queryTwo(); // TODO: Implement as per requirements
            q.queryThree(); // TODO: Implement as per requirements
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void connect() throws SQLException, ClassNotFoundException {
        // Load the PostgreSQL JDBC Driver
        Class.forName("org.postgresql.Driver");
        String dbName = "postgres";
        // Replace these with your Aurora DB details
        String jdbcUrl = "jdbc:postgresql://" + url + dbName; // Update dbName
        String username = uid; // Replace with your username
        String password = pw; // Replace with your password

        System.out.println("Connecting to AWS Aurora database...");
        System.out.println(jdbcUrl);
        // Establish the connection
        con = DriverManager.getConnection(jdbcUrl, username, password);

        System.out.println("Connection Successful");
    }

    public void close() throws SQLException {
        if (con != null) {
            con.close();
            System.out.println("Connection Closed");
        }
    }

    public void drop() throws SQLException {
        String dropCompany = "DROP TABLE IF EXISTS company";
        String dropStockPrice = "DROP TABLE IF EXISTS stockprice";

        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(dropStockPrice);
            stmt.executeUpdate(dropCompany);
            System.out.println("Tables dropped successfully");
        }
    }

    public void create() throws SQLException {
        String createCompany = """
                    CREATE TABLE IF NOT EXISTS company (
                        id INT PRIMARY KEY,
                        name VARCHAR(50),
                        ticker CHAR(10),
                        annualRevenue DECIMAL(15,2),
                        numEmployees INT
                    )
                """;
        String createStockPrice = """
                    CREATE TABLE IF NOT EXISTS stockprice (
                        companyId INT,
                        priceDate DATE,
                        openPrice DECIMAL(10,2),
                        highPrice DECIMAL(10,2),
                        lowPrice DECIMAL(10,2),
                        closePrice DECIMAL(10,2),
                        volume INT,
                        PRIMARY KEY (companyId, priceDate),
                        FOREIGN KEY (companyId) REFERENCES company(id)
                    )
                """;

        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(createCompany);
            stmt.executeUpdate(createStockPrice);
            System.out.println("Tables created successfully");
        }
    }

    public void insert() throws SQLException {
        String insertCompany = """
                    INSERT INTO company (id, name, ticker, annualRevenue, numEmployees)
                    VALUES
                    (1, 'Apple', 'AAPL', 387540000000.00, 154000),
                    (2, 'GameStop', 'GME', 611000000.00, 12000),
                    (3, 'Handy Repair', NULL, 2000000, 50),
                    (4, 'Microsoft', 'MSFT', 198270000000.00, 221000),
                    (5, 'StartUp', NULL, 50000, 3)
                """;

        String insertStockPrice = """
                    INSERT INTO stockprice (companyId, priceDate, openPrice, highPrice, lowPrice, closePrice, volume)
                    VALUES
                    (1, '2022-08-15', 171.52, 173.39, 171.35, 173.19, 54091700),
                    (1, '2022-08-16', 172.78, 173.71, 171.66, 173.03, 56377100),
                    (1, '2022-08-17', 172.77, 176.15, 172.57, 174.55, 79542000),
                    (1, '2022-08-18', 173.75, 174.90, 173.12, 174.15, 62290100),
                    (1, '2022-08-19', 173.03, 173.74, 171.31, 171.52, 70211500),
                    (1, '2022-08-22', 169.69, 169.86, 167.14, 167.57, 69026800),
                    (1, '2022-08-23', 167.08, 168.71, 166.65, 167.23, 54147100),
                    (1, '2022-08-24', 167.32, 168.11, 166.25, 167.53, 53841500),
                    (1, '2022-08-25', 168.78, 170.14, 168.35, 170.03, 51218200),
                    (1, '2022-08-26', 170.57, 171.05, 163.56, 163.62, 78823500),
                    (1, '2022-08-29', 161.15, 162.90, 159.82, 161.38, 73314000),
                    (1, '2022-08-30', 162.13, 162.56, 157.72, 158.91, 77906200),
                    (2, '2022-08-15', 39.75, 40.39, 38.81, 39.68, 5243100),
                    (2, '2022-08-16', 39.17, 45.53, 38.60, 42.19, 23602800),
                    (2, '2022-08-17', 42.18, 44.36, 40.41, 40.52, 9766400),
                    (2, '2022-08-18', 39.27, 40.07, 37.34, 37.93, 8145400),
                    (2, '2022-08-19', 35.18, 37.19, 34.67, 36.49, 9525600),
                    (2, '2022-08-22', 34.31, 36.20, 34.20, 34.50, 5798600),
                    (2, '2022-08-23', 34.70, 34.99, 33.45, 33.53, 4836300),
                    (2, '2022-08-24', 34.00, 34.94, 32.44, 32.50, 5620300),
                    (2, '2022-08-25', 32.84, 32.89, 31.50, 31.96, 4726300),
                    (2, '2022-08-26', 31.50, 32.38, 30.63, 30.94, 4289500),
                    (2, '2022-08-29', 30.48, 32.75, 30.38, 31.55, 4292700),
                    (2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200),
                    (4, '2022-08-15', 291.00, 294.18, 290.11, 293.47, 18085700),
                    (4, '2022-08-16', 291.99, 294.04, 290.42, 292.71, 18102900),
                    (4, '2022-08-17', 289.74, 293.35, 289.47, 291.32, 18253400),
                    (4, '2022-08-18', 290.19, 291.91, 289.08, 290.17, 17186200),
                    (4, '2022-08-19', 288.90, 289.25, 285.56, 286.15, 20557200),
                    (4, '2022-08-22', 282.08, 282.46, 277.22, 277.75, 25061100),
                    (4, '2022-08-23', 276.44, 278.86, 275.40, 276.44, 17527400),
                    (4, '2022-08-24', 275.41, 277.23, 275.11, 275.79, 18137000),
                    (4, '2022-08-25', 277.33, 279.02, 274.52, 278.85, 16583400),
                    (4, '2022-08-26', 279.08, 280.34, 267.98, 268.09, 27532500),
                    (4, '2022-08-29', 265.85, 267.40, 263.85, 265.23, 20338500),
                    (4, '2022-08-30', 266.67, 267.05, 260.66, 262.97, 22767100)
                """;

        try (Statement stmt = con.createStatement()) {
            stmt.executeUpdate(insertCompany);
            stmt.executeUpdate(insertStockPrice);
            System.out.println("Records inserted successfully");
        }
    }

    public void delete() throws SQLException {
        String deleteStockPrices = """
                    DELETE FROM stockprice
                    WHERE priceDate < '2022-08-20' OR companyId = 2
                """;

        try (Statement stmt = con.createStatement()) {
            int rowsDeleted = stmt.executeUpdate(deleteStockPrices);
            System.out.println(rowsDeleted + " records deleted");
        }
    }

    public ResultSet queryOne() throws SQLException {
        String query = """
                    SELECT name, annualRevenue, numEmployees
                    FROM company
                    WHERE numEmployees > 10000 OR annualRevenue < 1000000
                    ORDER BY name ASC
                """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
    }

    public ResultSet queryTwo() throws SQLException {
        String query = """
                    SELECT c.name, c.ticker,
                           MIN(sp.lowPrice) AS lowestPrice,
                           MAX(sp.highPrice) AS highestPrice,
                           AVG(sp.closePrice) AS avgClosingPrice,
                           AVG(sp.volume) AS avgVolume
                    FROM company c
                    JOIN stockprice sp ON c.id = sp.companyId
                    WHERE sp.priceDate BETWEEN '2022-08-22' AND '2022-08-26'
                    GROUP BY c.name, c.ticker
                    ORDER BY avgVolume DESC
                """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
    }

    public ResultSet queryThree() throws SQLException {
        String query = """
                    SELECT c.name, c.ticker, sp.closePrice
                    FROM company c
                    LEFT JOIN stockprice sp ON c.id = sp.companyId
                    WHERE sp.priceDate = '2022-08-30'
                      AND (sp.closePrice <= 1.10 * (
                            SELECT AVG(sp1.closePrice)
                            FROM stockprice sp1
                            WHERE sp1.companyId = sp.companyId
                              AND sp1.priceDate BETWEEN '2022-08-15' AND '2022-08-19'
                          ) OR c.ticker IS NULL)
                    ORDER BY c.name ASC
                """;
        Statement stmt = con.createStatement();
        return stmt.executeQuery(query);
    }

    public static String resultSetToString(ResultSet rst, int maxrows) throws SQLException {
        StringBuilder buf = new StringBuilder(5000);
        int rowCount = 0;

        if (rst == null) {
            return "ERROR: No ResultSet";
        }

        ResultSetMetaData meta = rst.getMetaData();
        buf.append("Total columns: ").append(meta.getColumnCount()).append('\n');

        // Append column names
        if (meta.getColumnCount() > 0) {
            buf.append(meta.getColumnName(1));
        }
        for (int j = 2; j <= meta.getColumnCount(); j++) {
            buf.append(", ").append(meta.getColumnName(j));
        }
        buf.append('\n');

        // Append rows
        while (rst.next()) {
            if (rowCount < maxrows) {
                for (int j = 0; j < meta.getColumnCount(); j++) {
                    Object obj = rst.getObject(j + 1);
                    buf.append(obj);
                    if (j != meta.getColumnCount() - 1) {
                        buf.append(", ");
                    }
                }
                buf.append('\n');
            }
            rowCount++;
        }
        buf.append("Total results: ").append(rowCount);
        return buf.toString();
    }

    public static String resultSetMetaDataToString(ResultSetMetaData meta) throws SQLException {
        StringBuilder buf = new StringBuilder(5000);
        buf.append(meta.getColumnName(1)).append(" (")
                .append(meta.getColumnLabel(1)).append(", ")
                .append(meta.getColumnType(1)).append("-")
                .append(meta.getColumnTypeName(1)).append(", ")
                .append(meta.getColumnDisplaySize(1)).append(", ")
                .append(meta.getPrecision(1)).append(", ")
                .append(meta.getScale(1)).append(")");

        for (int j = 2; j <= meta.getColumnCount(); j++) {
            buf.append(", ").append(meta.getColumnName(j)).append(" (")
                    .append(meta.getColumnLabel(j)).append(", ")
                    .append(meta.getColumnType(j)).append("-")
                    .append(meta.getColumnTypeName(j)).append(", ")
                    .append(meta.getColumnDisplaySize(j)).append(", ")
                    .append(meta.getPrecision(j)).append(", ")
                    .append(meta.getScale(j)).append(")");
        }
        return buf.toString();
    }
}
