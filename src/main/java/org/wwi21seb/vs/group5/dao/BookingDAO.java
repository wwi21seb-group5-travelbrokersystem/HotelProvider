package org.wwi21seb.vs.group5.dao;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Logger.LoggerFactory;
import org.wwi21seb.vs.group5.Request.AvailabilityRequest;
import org.wwi21seb.vs.group5.Request.ReservationRequest;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.communication.DatabaseConnection;
import org.wwi21seb.vs.group5.Model.Booking;
import org.wwi21seb.vs.group5.Model.Room;

import java.sql.*;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Logger;

public class BookingDAO {

    private final Logger LOGGER = LoggerFactory.setupLogger(BookingDAO.class.getName());
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

    public UUID reserveRoom(ReservationRequest request, UUID transactionID) {
        PreparedStatement stmt = null;
        UUID bookingId = UUID.randomUUID();

        try (Connection conn = DatabaseConnection.getConnection()) {
            // SELECT ROOM TO GET DAILY PRICE
            stmt = conn.prepareStatement("SELECT price_per_night FROM rooms WHERE room_id = ?");
            stmt.setObject(1, request.getResourceId());
            stmt.executeQuery();

            ResultSet resultSet = stmt.getResultSet();
            resultSet.next();
            double dailyPrice = resultSet.getDouble("price_per_night");

            // CHECK IF ROOM IS AVAILABLE
            stmt = conn.prepareStatement("SELECT * FROM bookings WHERE room_id = ? AND ((start_date <= ? AND end_date >= ?) OR (start_date <= ? AND end_date >= ?))");
            stmt.setObject(1, request.getResourceId());
            stmt.setDate(2, Date.valueOf(request.getStartDate()));
            stmt.setDate(3, Date.valueOf(request.getStartDate()));
            stmt.setDate(4, Date.valueOf(request.getEndDate()));
            stmt.setDate(5, Date.valueOf(request.getEndDate()));
            stmt.executeQuery();

            resultSet = stmt.getResultSet();
            if (resultSet.next()) {
                return null;
            }

            // PREPARE BOOKING
            LocalDate startDate = LocalDate.parse(request.getStartDate(), dateFormatter);
            LocalDate endDate = LocalDate.parse(request.getEndDate(), dateFormatter);
            double totalPrice = dailyPrice * (startDate.until(endDate).getDays() + 1);

            stmt = conn.prepareStatement("INSERT INTO bookings (booking_id, room_id, start_date, end_date, total_price, is_confirmed) VALUES (?, ?, ?, ?, ?, ?)");
            stmt.setObject(1, bookingId);
            stmt.setObject(2, request.getResourceId());
            stmt.setDate(3, Date.valueOf(request.getStartDate()));
            stmt.setDate(4, Date.valueOf(request.getEndDate()));
            stmt.setDouble(5, totalPrice);
            stmt.setBoolean(6, false);
            stmt.executeUpdate();

            stmt.close();
            return bookingId;
        } catch (SQLException e) {
            LOGGER.severe("SQL Exception: " + e.getMessage());
            return null;
        }
    }

    public boolean confirmBooking(UUID bookingId) {
        PreparedStatement stmt = null;


        try (Connection conn = DatabaseConnection.getConnection()) {
            stmt = conn.prepareStatement("UPDATE bookings SET is_confirmed = true WHERE booking_id = ?");
            stmt.setObject(1, bookingId, java.sql.Types.OTHER);
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException  e) {
            LOGGER.severe("SQL Exception: " + e.getMessage());
            return false;
        }

        return true;
    }

    public boolean abortBooking(UUID bookingId) {
        PreparedStatement stmt = null;

        try (Connection conn = DatabaseConnection.getConnection()) {
            stmt = conn.prepareStatement("DELETE FROM bookings WHERE booking_id = ?");
            stmt.setObject(1, bookingId, java.sql.Types.OTHER);
            stmt.executeUpdate();
            stmt.close();
        } catch (SQLException e) {
            LOGGER.severe("SQL Exception: " + e.getMessage());
            return false;
        }

        return true;
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

            stmt.close();
            return serializeBookings(bookings);
        } catch (Exception e) {
            LOGGER.severe("SQL Exception: " + e.getMessage());
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

            stmt.close();
            return serializeRooms(availableRooms);
        } catch (Exception e) {
            LOGGER.severe("Error while getting available rooms: " + e.getMessage());
            throw new RuntimeException();
        }
    }

}
