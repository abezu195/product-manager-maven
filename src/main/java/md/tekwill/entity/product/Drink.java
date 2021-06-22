package md.tekwill.entity.product;

import java.time.LocalDate;
import java.util.Objects;

public class Drink extends Product {

    private static final double DISCOUNT = 0.5;

    private double volume;

    public Drink(String name, double price, LocalDate bestBefore, double volume) {
        super(name, price, bestBefore);
        this.volume = volume;
    }

    public double getVolume() {
        return volume;
    }

    public void setVolume(double volume) {
        this.volume = volume;
    }

    @Override
    public String getPrintText() {
        return "[" + id + "] DRINK: " + name + " | " + price + " | " + bestBefore + " | " + volume ;
    }

    @Override
    public double getPriceOnBill() {
        LocalDate today = LocalDate.now();
        LocalDate tomorrow = today.plusDays(1);
        if (today.equals(getBestBefore()) || tomorrow.equals(getBestBefore()))
            return getPrice() * DISCOUNT;
        return getPrice();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Drink)) return false;
        if (!super.equals(o)) return false;
        Drink drink = (Drink) o;
        return Double.compare(drink.volume, volume) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), volume);
    }

    @Override
    public String toString() {
        return "Drink{" +
                "volume=" + volume +
                ", id=" + id +
                ", name='" + name + '\'' +
                ", price=" + price +
                ", bestBefore=" + bestBefore +
                '}';
    }
}
