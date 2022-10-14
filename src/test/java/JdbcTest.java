import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.postgresql.util.PGobject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Random;

@Testcontainers
public class JdbcTest {
    @Container
    private static final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13.4");

    private final DataSource dataSource;

    private static final Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    public JdbcTest() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl() + "?maxResultBuffer=10M");
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        hikariConfig.setMaximumPoolSize(20);
        dataSource = new HikariDataSource(hikariConfig);
    }


    @Test
    void runTest() throws Exception {
        String data = "{\"test\": \"" + randomString(1_000_000) + "\"}";

        createTable();
        
        for (int i = 0; i < 100; i++) {
            insert( i , data);
            logger.info("inserted {}", i);
        }
        doSelect();
    }

    private void createTable() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.executeUpdate("CREATE TABLE queue (id INTEGER PRIMARY KEY, status JSONB)");
        }
    }


    protected void insert(int i, String data) throws SQLException {
        PGobject jsonObject = new PGobject();
        jsonObject.setType("json");
        jsonObject.setValue(data);

        try (Connection connection = dataSource.getConnection(); PreparedStatement statement = connection.prepareStatement("insert into queue values(?, ?)")) {
            statement.setInt(1, i);
            statement.setObject(2, jsonObject);
            statement.executeUpdate();
        }
    }

    protected void doSelect() throws SQLException {
        try (Connection connection = dataSource.getConnection(); Statement statement = connection.createStatement()) {
            statement.execute("SELECT id, status FROM queue");
            try (ResultSet rs = statement.getResultSet()) {
                while (rs.next()) ;
            }
        }
    }
    private String randomString(int targetStringLength) {
        int leftLimit = 48; // numeral '0'
        int rightLimit = 122; // letter 'z'
        Random random = new Random();

        return random.ints(leftLimit, rightLimit + 1)
            .filter(i -> (i <= 57 || i >= 65) && (i <= 90 || i >= 97))
            .limit(targetStringLength)
            .collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
            .toString();
    }
}
