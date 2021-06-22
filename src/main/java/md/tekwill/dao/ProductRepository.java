package md.tekwill.dao;

import md.tekwill.entity.product.FoodCategory;
import md.tekwill.entity.product.Product;

import java.util.List;

public interface ProductRepository {

    void save(Product product);

    List<Product> findAll();

    Product findById(int id);

    Product findByName(String name);

    void update(int id, double volume);

    void update(int id, FoodCategory category);

    void delete(int id);
}
