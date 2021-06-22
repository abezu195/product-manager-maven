package md.tekwill.entity.product;

import java.time.LocalDate;
import java.time.Period;
import java.util.Objects;

public class Food extends Product {

    private static final double DISCOUNT = 0.8;

    private FoodCategory category;

    public Food(String name, double price, LocalDate bestBefore, FoodCategory category) {
        super(name, price, bestBefore);
        this.category = category;
    }

    public FoodCategory getCategory() {
        return category;
    }

    public void setCategory(FoodCategory category) {
        this.category = category;
    }

    @Override
    public double getPriceOnBill() {
        if (Period.between(LocalDate.now(), getBestBefore()).getDays() <= 3) {
            return getPrice() * DISCOUNT;
        }
        return getPrice();
    }

    @Override
    public String getPrintText() {
        return "[" + id + "] FOOD: " + name + " | " + price + " | " + bestBefore + " | " + category;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Food)) return false;
        if (!super.equals(o)) return false;
        Food food = (Food) o;
        return Objects.equals(category, food.category);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), category);
    }

    @Override
    public String toString() {
        return "Food{" +
                "category='" + category + '\'' +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", bestBefore=" + bestBefore +
                '}';
    }
}
