package org.wwi21seb.vs.group5.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.Request.HotelReservationRequest;
import org.wwi21seb.vs.group5.TwoPhaseCommit.TransactionContext;
import org.wwi21seb.vs.group5.TwoPhaseCommit.TransactionManager;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.communication.DatabaseConnection;
import org.wwi21seb.vs.group5.model.Booking;
import org.wwi21seb.vs.group5.model.Room;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class BookingDAO {

    private final ObjectMapper mapper;
    private final DateTimeFormatter dateFormatter;

    public BookingDAO() {
        this.mapper = new ObjectMapper();
        this.dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    }

    private String serializeBookings(List<Booking> bookings) throws JsonProcessingException {
        return mapper.writeValueAsString(bookings);
    }

    private String serializeRooms(List<Room> availableRooms) throws JsonProcessingException {
        return mapper.writeValueAsString(availableRooms);
    }

    public boolean reserveRoom(Object payload) {
        PreparedStatement stmt = null;
        HotelReservationRequest hotelReservationRequest = mapper.convertValue(payload, HotelReservationRequest.class);

        try (Connection conn = DatabaseConnection.getConnection(false)) {
            UUID bookingId = UUID.randomUUID();

            // SELECT ROOM TO GET DAILY PRICE
            stmt = conn.prepareStatement("SELECT * FROM rooms WHERE room_id = ?");
            stmt.setObject(1, hotelReservationRequest.roomID());
            stmt.executeQuery();

            ResultSet resultSet = stmt.getResultSet();
            resultSet.next();
            double dailyPrice = resultSet.getDouble("daily_price");

            LocalDate startDate = LocalDate.parse(hotelReservationRequest.startDate(), dateFormatter);
            LocalDate endDate = LocalDate.parse(hotelReservationRequest.endDate(), dateFormatter);
            double totalPrice = dailyPrice * (startDate.until(endDate).getDays() + 1);

            stmt = conn.prepareStatement("INSERT INTO bookings (booking_id, room_id, start_date, end_date, total_price) VALUES (?, ?, ?, ?, ?)");
            stmt.setObject(1, bookingId);
            stmt.setObject(2, hotelReservationRequest.roomID());
            stmt.setDate(3, java.sql.Date.valueOf(hotelReservationRequest.startDate()));
            stmt.setDate(4, java.sql.Date.valueOf(hotelReservationRequest.endDate()));
            stmt.setDouble(5, totalPrice);
            stmt.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return false;
    }

    public String getBookings() {
        PreparedStatement stmt = null;
        List<Booking> bookings = new ArrayList<>();

        try (Connection conn = DatabaseConnection.getConnection(true)) {
            stmt = conn.prepareStatement("SELECT * FROM bookings");
            stmt.executeQuery();

            ResultSet resultSet = stmt.getResultSet();
            while (resultSet.next()) {
                Booking booking = new Booking(
                        resultSet.getObject("booking_id", java.util.UUID.class),
                        resultSet.getObject("room_id", java.util.UUID.class),
                        resultSet.getDate("start_date"),
                        resultSet.getDate("end_date"),
                        resultSet.getDouble("total_price")
                );

                bookings.add(booking);
            }

            return serializeBookings(bookings);
        } catch (Exception e) {
            System.out.println("Error while getting bookings: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

    public String getAvailableRooms(String payload) {
        PreparedStatement stmt = null;
        List<Room> availableRooms = new ArrayList<>();

        try (Connection conn = DatabaseConnection.getConnection(true)) {
            AvailabilityRequest availabilityRequest = mapper.readValue(payload, AvailabilityRequest.class);

            LocalDate startDate = LocalDate.parse(availabilityRequest.getStartDate(), dateFormatter);
            LocalDate endDate = LocalDate.parse(availabilityRequest.getEndDate(), dateFormatter);

            stmt = conn.prepareStatement("SELECT * FROM rooms WHERE capacity >= ? AND room_id NOT IN (SELECT room_id FROM bookings WHERE start_date BETWEEN ? AND ? OR end_date BETWEEN ? AND ?)");
            stmt.setInt(1, availabilityRequest.getNumberOfPersons());
            stmt.setDate(2, java.sql.Date.valueOf(startDate));
            stmt.setDate(3, java.sql.Date.valueOf(endDate));
            stmt.setDate(4, java.sql.Date.valueOf(startDate));
            stmt.setDate(5, java.sql.Date.valueOf(endDate));
            stmt.executeQuery();

            ResultSet resultSet = stmt.getResultSet();
            while (resultSet.next()) {
                Room booking = new Room(
                        resultSet.getObject("room_id", java.util.UUID.class),
                        resultSet.getString("type"),
                        resultSet.getInt("capacity"),
                        resultSet.getDouble("price_per_night")
                );

                availableRooms.add(booking);
            }

            return serializeRooms(availableRooms);
        } catch (Exception e) {
            System.out.println("Error while getting available rooms: " + e.getMessage());
            e.printStackTrace();
            throw new RuntimeException();
        }
    }

}
