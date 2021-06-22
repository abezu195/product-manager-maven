package md.tekwill.app;

import md.tekwill.ShoppingCart;
import md.tekwill.entity.product.Product;
import md.tekwill.service.ProductService;

import java.util.Scanner;

class ProductManagementUserMenu {

    private final ProductService productService;
    private final ShoppingCart cart;
    private final Scanner scanner;

    public ProductManagementUserMenu(ProductService productService, ShoppingCart cart, Scanner scanner) {
        this.productService = productService;
        this.cart = cart;
        this.scanner = scanner;
    }

    public void showMenu() {
        boolean exitOptionNotSelected;
        do {
            System.out.println("\n\nAvailable options:" +
                                       "\n==============USER OPTIONS==============" +
                                       "\n[1] View all products" +
                                       "\n[2] View shopping cart" +
                                       "\n[3] Add product to shopping cart" +
                                       "\n[4] Print bill" +
                                       "\n==========================================" +
                                       "\n[0] Exit" +
                                       "\n==========================================");
            System.out.print(">> ");
            exitOptionNotSelected = handleUserChoice(scanner.nextInt());
        } while (exitOptionNotSelected);
    }

    private boolean handleUserChoice(int userChoice) {
        scanner.nextLine();
        switch (userChoice) {
            case 1:
                viewAllNonExpiredProducts();
                return true;
            case 2:
                viewShoppingCart();
                return true;
            case 3:
                addProductToShoppingCart();
                return true;
            case 4:
                printBill();
                return true;
            case 0:
                System.out.println("BYE!");
                return false;
            default:
                System.out.println("Unknown option selected!");
                return true;
        }
    }

    private void viewAllNonExpiredProducts() {
        System.out.println("--- ALL AVAILABLE PRODUCTS ---");
        for (Product product : productService.getAllNonExpired()) {
            System.out.println(product.getPrintText());
        }
    }

    private void viewShoppingCart() {
        System.out.println("--- SHOPPING CART CONTENT ---");
        if (cart.getProductList().isEmpty()) {
            System.out.println("Empty!");
        } else {
            for (Product product : cart.getProductList()) {
                System.out.println(product.getPrintText());
            }
        }
    }

    private void addProductToShoppingCart() {
        System.out.println("Input the id of the item to add to cart: \n>> ");
        try {
            int id = scanner.nextInt();
            scanner.nextLine();
            Product product = productService.getById(id);
            cart.addProduct(product);
            System.out.println("Product with ID " + id + " successfully added!");
        } catch (RuntimeException ex) {
            scanner.nextLine();
            System.out.println("Error: " + ex.getMessage());
        }
    }

    private void printBill() {
        if (cart.getProductList().isEmpty()) {
            System.out.println("No products yet!");
        } else {
            System.out.println("---------------------------------------------");
            System.out.println("You want to purchase the following products: ");
            for (Product product : cart.getProductList()) {
                System.out.println(product.getPrintText());
            }
            System.out.println("Full price: " + cart.getPriceWithoutDiscount());
            System.out.println("Discount: " + cart.getSavedMoney());
            System.out.println("Total to pay: " + cart.getPrice());
            System.out.println("---------------------------------------------");
        }
    }
}
