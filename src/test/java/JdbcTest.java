import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.IntStream;

@Testcontainers
public class JdbcTest {
    @Container
    private static final PostgreSQLContainer<?> container = new PostgreSQLContainer<>("postgres:13.4");

    private final DataSource dataSource;

    private final ExecutorService executorService = Executors.newFixedThreadPool(20);

    private static final Logger logger = LoggerFactory.getLogger(JdbcTest.class);

    public JdbcTest() {
        HikariConfig hikariConfig = new HikariConfig();
        hikariConfig.setJdbcUrl(container.getJdbcUrl());
        hikariConfig.setUsername(container.getUsername());
        hikariConfig.setPassword(container.getPassword());
        hikariConfig.setDriverClassName(container.getDriverClassName());
        hikariConfig.setMaximumPoolSize(20);
        dataSource = new HikariDataSource(hikariConfig);
    }


    @Test
    void runTest() throws Exception {
        performQuery("CREATE TABLE queue (id INTEGER PRIMARY KEY, status CHAR(10))");
        performQuery("INSERT INTO queue VALUES (1, 'PENDING')");
        performQuery("INSERT INTO queue VALUES (2, 'PENDING')");
        performQuery("INSERT INTO queue VALUES (3, 'PENDING')");

        List<Callable<Integer>> tasks = IntStream.range(0, 20).mapToObj(i -> (Callable<Integer>) this::runSelectForUpdate).toList();

        executorService.invokeAll(tasks).forEach(future -> {
            try {
                logger.info("Updated " + future.get());
            } catch (Exception e) {
                logger.info("Exception " + e.getMessage());
            }
        });
    }

    private int runSelectForUpdate() throws Exception {
        int updatedRows = 0;
        try (Connection connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            try (Statement statement = connection.createStatement()) {
                statement.execute("SELECT id, status FROM queue WHERE status = 'PENDING' LIMIT 2 FOR UPDATE NOWAIT");
            }
            Thread.sleep(100);
            try (Statement statement = connection.createStatement()) {
                updatedRows = statement.executeUpdate("UPDATE queue SET status = 'PROCESSED' WHERE status = 'PENDING'");
            }
            connection.commit();
        }
        return updatedRows;
    }

    protected void performQuery(String sql) throws SQLException {
        try (Statement statement = dataSource.getConnection().createStatement()) {
            statement.execute(sql);
        }
    }
}
