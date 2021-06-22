package md.tekwill;

import md.tekwill.app.ProductManagement;
import md.tekwill.app.ProductManagementImpl;
import md.tekwill.dao.InMemoryProductRepository;
import md.tekwill.dao.JDBCProductRepository;
import md.tekwill.dao.ProductRepository;
import md.tekwill.service.ProductService;
import md.tekwill.service.ProductServiceImpl;

import java.util.Scanner;

public class ProductManagementApp {

    public static void main(String[] args) {
//        ProductRepository productRepository = new InMemoryProductRepository();
        ProductRepository productRepository = new JDBCProductRepository();
        ProductService productService = new ProductServiceImpl(productRepository);
        ProductManagement productManagement = new ProductManagementImpl(productService,
                                                                        new ShoppingCart(),
                                                                        new Scanner(System.in));
        productManagement.run();
    }

}
