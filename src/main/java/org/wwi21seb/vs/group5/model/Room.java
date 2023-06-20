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

}
