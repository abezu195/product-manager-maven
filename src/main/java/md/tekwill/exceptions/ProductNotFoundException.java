package md.tekwill.exceptions;

public class ProductNotFoundException extends ProductManagementRuntimeException {

    public ProductNotFoundException(String message) {
        super(message);
    }
}
