package model;

public class EnrichedTeamUser {
    private Long teamId;
    private Long userId;
    private String courier;
    private String status;
    private Long price;

    // Default constructor for serialization
    public EnrichedTeamUser() {
    }

    public EnrichedTeamUser(Long teamId, Long userId, String courier, String status, Long price) {
        this.teamId = teamId;
        this.userId = userId;
        this.courier = courier;
        this.status = status;
        this.price = price;
    }

    public Long getTeamId() {
        return teamId;
    }

    public void setTeamId(Long teamId) {
        this.teamId = teamId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getCourier() {
        return courier;
    }

    public void setCourier(String courier) {
        this.courier = courier;
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
        return "EnrichedTeamUser{" +
                "teamId=" + teamId +
                ", userId=" + userId +
                ", courier='" + courier + '\'' +
                ", status='" + status + '\'' +
                ", price=" + price +
                '}';
    }
}