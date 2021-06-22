package md.tekwill.dao;

import md.tekwill.entity.product.Drink;
import md.tekwill.entity.product.Food;
import md.tekwill.entity.product.FoodCategory;
import md.tekwill.entity.product.Product;
import md.tekwill.exceptions.ProductExistsException;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

public class InMemoryProductRepository implements ProductRepository {

    private final List<Product> productList = new ArrayList<>();

    public InMemoryProductRepository() {
        Food sushi = new Food("Sushi", 5.99, LocalDate.now().plusDays(7), FoodCategory.ANIMAL_SOURCE);
        sushi.setId(1);
        Food avocado = new Food("Avocado", 1.99, LocalDate.now().plusDays(14), FoodCategory.FRUIT);
        avocado.setId(2);
        Food bread = new Food("Bread", 0.99, LocalDate.now().plusDays(2), FoodCategory.GRAIN);
        bread.setId(3);
        Drink tea = new Drink("Tea", 2.49, LocalDate.now().plusDays(1), 1.5);
        tea.setId(4);
        Drink beer = new Drink("Beer", 3.49, LocalDate.now().plusMonths(1), 0.5);
        beer.setId(5);
        Drink juice = new Drink("Juice", 1.49, LocalDate.now().plusMonths(1), 2.0);
        juice.setId(6);
        Drink coffee = new Drink("Coffee", 19.99, LocalDate.now().minusDays(2), 1.0);
        coffee.setId(7);

        productList.add(sushi);
        productList.add(avocado);
        productList.add(bread);
        productList.add(tea);
        productList.add(beer);
        productList.add(juice);
        productList.add(coffee);
    }

    @Override
    public void save(Product product) {
        if (findByName(product.getName()) != null) {
            throw new ProductExistsException("Product with name " + product.getName() + " already exists!");
        }
        product.setId(getMaxId() + 1);
        productList.add(product);
    }

    private int getMaxId() {
        int id = 0;
        for (Product product : productList) {
            if (product.getId() > id) {
                id = product.getId();
            }
        }
        return id;
    }

    @Override
    public List<Product> findAll() {
        return new ArrayList<>(productList);
    }

    @Override
    public Product findById(int id) {
        Product product = null;
        for (Product p : productList) {
            if (id == p.getId()) {
                product = p;
            }
        }
        return product;
    }

    @Override
    public Product findByName(String name) {
        Product product = null;
        for (Product p : productList) {
            if (p.getName().equals(name)) {
                product = p;
            }
        }
        return product;
    }

    @Override
    public void update(int id, double volume) {
        for (Product p : productList) {
            if (id == p.getId()) {
                ((Drink) p).setVolume(volume);
            }
        }
    }

    @Override
    public void update(int id, FoodCategory category) {
        for (Product p : productList) {
            if (id == p.getId()) {
                ((Food) p).setCategory(category);
            }
        }
    }

    @Override
    public void delete(int id) {
        Product product = findById(id);
        productList.remove(product);
    }
}
