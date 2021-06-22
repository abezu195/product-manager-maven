package md.tekwill.exceptions;

public class ProductExistsException extends ProductManagementRuntimeException {
    public ProductExistsException(String message) {
        super(message);
    }
}
