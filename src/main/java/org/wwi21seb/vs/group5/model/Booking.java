package org.wwi21seb.vs.group5.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Date;
import java.util.UUID;

public class Booking {

    @JsonProperty("booking_id")
    private UUID id;

    @JsonProperty("room_id")
    private UUID roomId;

    @JsonProperty("start_date")
    private Date startDate;

    @JsonProperty("end_date")
    private Date endDate;

    @JsonProperty("total_price")
    private Double totalPrice;

    public Booking(UUID id, UUID roomId, Date startDate, Date endDate, Double totalPrice) {
        this.id = id;
        this.roomId = roomId;
        this.startDate = startDate;
        this.endDate = endDate;
        this.totalPrice = totalPrice;
    }

    public UUID getId() {
        return id;
    }

    public UUID getRoomId() {
        return roomId;
    }

    public Date getStartDate() {
        return startDate;
    }

    public Date getEndDate() {
        return endDate;
    }

    public Double getTotalPrice() {
        return totalPrice;
    }
}
