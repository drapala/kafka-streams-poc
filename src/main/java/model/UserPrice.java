package model;

public class UserPrice {
    private String status;
    private Long price;

    // Default constructor for serialization
    public UserPrice() {
    }

    public UserPrice(String status, Long price) {
        this.status = status;
        this.price = price;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    public Long getPrice() {
        return price;
    }

    public void setPrice(Long price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return "UserPrice{" +
                "status='" + status + '\'' +
                ", price=" + price +
                '}';
    }
}