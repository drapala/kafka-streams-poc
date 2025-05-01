package model;

public class TeamUser {
    private Long teamId;
    private Long userId;
    private String courier;

    // Default constructor for serialization
    public TeamUser() {
    }

    public TeamUser(Long teamId, Long userId, String courier) {
        this.teamId = teamId;
        this.userId = userId;
        this.courier = courier;
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

    @Override
    public String toString() {
        return "TeamUser{" +
                "teamId=" + teamId +
                ", userId=" + userId +
                ", courier='" + courier + '\'' +
                '}';
    }
}