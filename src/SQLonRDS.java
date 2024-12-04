import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

public class SQLonRDS {
    private Connection con;
    private final String url = "iitjdb.cpmqoqquutp0.us-east-1.rds.amazonaws.com:3306";
    private final String uid = "admin";
    private final String pw = "iitj@12345";

    public static void main(String[] args) {
        SQLonRDS q = new SQLonRDS();
        try {
            q.connect();
            q.create(); // TODO: Implement as per requirements
            q.drop();   // TODO: Implement as per requirements
            q.insert(); // TODO: Implement as per requirements
            q.queryOne();   // TODO: Implement as per requirements
            q.queryTwo();   // TODO: Implement as per requirements
            q.queryThree(); // TODO: Implement as per requirements
            q.close();
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public void connect() throws SQLException, ClassNotFoundException {
        // Load MySQL JDBC Driver
        Class.forName("com.mysql.cj.jdbc.Driver");
        String jdbcUrl = "jdbc:mysql://" + url + "/mydb?user=" + uid + "&password=" + pw;
        System.out.println("Connecting to database...");
        con = DriverManager.getConnection(jdbcUrl);
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
            CREATE TABLE company (
                id INT PRIMARY KEY,
                name VARCHAR(50),
                ticker CHAR(10),
                annualRevenue DECIMAL(15,2),
                numEmployees INT
            )
        """;
        String createStockPrice = """
            CREATE TABLE stockprice (
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
            ...
            -- Add all stock price records here
            (2, '2022-08-30', 31.62, 31.87, 29.42, 29.84, 5060200)
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
