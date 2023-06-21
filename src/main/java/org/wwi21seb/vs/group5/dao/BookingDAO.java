package org.wwi21seb.vs.group5.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.Request.HotelReservationRequest;
import org.wwi21seb.vs.group5.Request.PrepareResult;
import org.wwi21seb.vs.group5.communication.DatabaseConnection;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Room;

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

    public String reserveRoom(String payload, UUID transactionID) {
        PreparedStatement stmt = null;
        UUID bookingId = UUID.randomUUID();

        try (Connection conn = DatabaseConnection.getConnection()) {
            HotelReservationRequest hotelReservationRequest = mapper.readValue(payload, HotelReservationRequest.class);

            // SELECT ROOM TO GET DAILY PRICE
            stmt = conn.prepareStatement("SELECT price_per_night FROM rooms WHERE room_id = ?");
            stmt.setObject(1, hotelReservationRequest.getRoomID());
            stmt.executeQuery();

            ResultSet resultSet = stmt.getResultSet();
            resultSet.next();
            double dailyPrice = resultSet.getDouble("price_per_night");

            LocalDate startDate = LocalDate.parse(hotelReservationRequest.getStartDate(), dateFormatter);
            LocalDate endDate = LocalDate.parse(hotelReservationRequest.getEndDate(), dateFormatter);
            double totalPrice = dailyPrice * (startDate.until(endDate).getDays() + 1);

            stmt = conn.prepareStatement("INSERT INTO bookings (booking_id, room_id, start_date, end_date, total_price, is_confirmed) VALUES (?, ?, ?, ?, ?, ?)");
            stmt.setObject(1, bookingId);
            stmt.setObject(2, hotelReservationRequest.getRoomID());
            stmt.setDate(3, java.sql.Date.valueOf(hotelReservationRequest.getStartDate()));
            stmt.setDate(4, java.sql.Date.valueOf(hotelReservationRequest.getEndDate()));
            stmt.setDouble(5, totalPrice);
            stmt.setBoolean(6, false);
            stmt.executeUpdate();
            return mapper.writeValueAsString(new PrepareResult(transactionID, bookingId));
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            return null;
        } catch (JsonProcessingException e) {
            System.out.println("JSON Exception: " + e.getMessage());
            return null;
        }
    }

    public boolean confirmBooking(String payload) {
        PreparedStatement stmt = null;

        try (Connection conn = DatabaseConnection.getConnection()) {
            PrepareResult prepareResult = mapper.readValue(payload, PrepareResult.class);
            stmt = conn.prepareStatement("UPDATE bookings SET is_confirmed = true WHERE booking_id = ?");
            stmt.setObject(1, prepareResult.getResourceId(), java.sql.Types.OTHER);
            stmt.executeUpdate();
        } catch (SQLException  e) {
            System.out.println("SQL Exception: " + e.getMessage());
            return false;
        } catch (JsonProcessingException e) {
            System.out.println("JSON Exception: " + e.getMessage());
            return false;
        }

        return true;
    }

    public boolean abortBooking(Object payload) {
        PreparedStatement stmt = null;
        PrepareResult prepareResult = mapper.convertValue(payload, PrepareResult.class);

        try (Connection conn = DatabaseConnection.getConnection()) {
            stmt = conn.prepareStatement("DELETE FROM bookings WHERE booking_id = ?");
            stmt.setObject(1, prepareResult.getResourceId(), java.sql.Types.OTHER);
            stmt.executeUpdate();
        } catch (SQLException e) {
            System.out.println("SQL Exception: " + e.getMessage());
            return false;
        }

        return false;
    }

    public String getBookings() {
        PreparedStatement stmt = null;
        List<Booking> bookings = new ArrayList<>();

        try (Connection conn = DatabaseConnection.getConnection()) {
            stmt = conn.prepareStatement("SELECT * FROM bookings WHERE is_confirmed = true");
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

        try (Connection conn = DatabaseConnection.getConnection()) {
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
