package org.wwi21seb.vs.group5.model;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.UUID;

public class Room {

    @JsonProperty("room_id")
    private UUID id;

    @JsonProperty("type")
    private String type;

    @JsonProperty("capacity")
    private int capacity;

    @JsonProperty("price_per_night")
    private double pricePerNight;

    public Room(UUID id, String type, int capacity, double pricePerNight) {
        this.id = id;
        this.type = type;
        this.capacity = capacity;
        this.pricePerNight = pricePerNight;
    }

    public UUID getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    public int getCapacity() {
        return capacity;
    }

    public double getPricePerNight() {
        return pricePerNight;
    }
}
