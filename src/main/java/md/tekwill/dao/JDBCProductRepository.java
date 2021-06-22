package md.tekwill.dao;

import md.tekwill.entity.product.Drink;
import md.tekwill.entity.product.Food;
import md.tekwill.entity.product.FoodCategory;
import md.tekwill.entity.product.Product;
import org.postgresql.ds.PGSimpleDataSource;
import org.postgresql.ds.common.BaseDataSource;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.List;

public class JDBCProductRepository implements ProductRepository {

    private static final String CONNECTION_URL = "jdbc:postgresql://localhost:5432/product-manager";
    private static final String USERNAME = "postgres";
    private static final String PASSWORD = "password";

    private final BaseDataSource dataSource;

    public JDBCProductRepository() {
        this.dataSource = new PGSimpleDataSource();
        this.dataSource.setUrl(CONNECTION_URL);
        this.dataSource.setUser(USERNAME);
        this.dataSource.setPassword(PASSWORD);
    }

    @Override
    public void save(Product product) {
        String insertSQL = "INSERT INTO product(name, price, best_before, category, volume) VALUES (?, ?, ?, ?, ?)";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(insertSQL, Statement.RETURN_GENERATED_KEYS)) {

            preparedStatement.setString(1, product.getName());
            preparedStatement.setDouble(2, product.getPrice());
            preparedStatement.setDate(3, Date.valueOf(product.getBestBefore()));


            if (product instanceof Food) {
                Food food = (Food) product;
                preparedStatement.setString(4, food.getCategory().name()); // preparedStatement.setDouble(5, 0.0);
                preparedStatement.setNull(5, 8);
            }

            if (product instanceof Drink) {
                Drink drink = (Drink) product;
                preparedStatement.setString(4, null);// preparedStatement.setNull(4, 12);
                preparedStatement.setDouble(5, drink.getVolume());
            }

            int row = preparedStatement.executeUpdate();
            System.out.println("Inserted " + row + " row(s)!");

            ResultSet generatedKeys = preparedStatement.getGeneratedKeys();
            while (generatedKeys.next()) {
                System.out.println("Created task with " + generatedKeys.getInt(1) + " id ");
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }
    }

    @Override
    public List<Product> findAll() {
        return null;
    }

    @Override
    public Product findById(int id) {
        return null;
    }

    @Override
    public Product findByName(String nameToFind) {
        Product product = null;
        String selectSQL = "SELECT id, name, price, best_before, category, volume FROM product where name = ?";

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(selectSQL)) {
            preparedStatement.setString(1, nameToFind);

            ResultSet resultSet = preparedStatement.executeQuery();

            while (resultSet.next()) {
                int id = resultSet.getInt("id");
                String name = resultSet.getString("name");
                double price = resultSet.getDouble("price");
                LocalDate bestBefore = resultSet.getDate("best_before").toLocalDate();
                String category = resultSet.getString("category");
                double volume = resultSet.getDouble("volume");

                if (category != null) {
                    product = new Food(name, price, bestBefore, FoodCategory.valueOf(category));
                }

                if (volume != 0.0) {
                    product = new Drink(name, price, bestBefore, volume);
                }

                if (product != null) {
                    product.setId(id);
                }
            }

        } catch (SQLException sqlException) {
            sqlException.printStackTrace();
        }

        return product;
    }

    @Override
    public void update(int id, double volume) {

    }

    @Override
    public void update(int id, FoodCategory category) {

    }

    @Override
    public void delete(int id) {

    }
}
