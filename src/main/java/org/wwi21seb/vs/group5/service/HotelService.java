package org.wwi21seb.vs.group5.service;

import org.wwi21seb.vs.group5.TwoPhaseCommit.Participant;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.dao.BookingDAO;

public class HotelService implements Participant {

    private final BookingDAO bookingDAO;

    public HotelService() {
        this.bookingDAO = new BookingDAO();
    }

    @Override
    public UDPMessage prepare(UDPMessage udpMessage) {
        String prepareResultJsonString = bookingDAO.reserveRoom(udpMessage.getData(), udpMessage.getTransactionId());

        // Create a new UDPMessage with the bookingId as payload
        return new UDPMessage(
                udpMessage.getOperation(),
                udpMessage.getTransactionId(),
                "HOTEL_PROVIDER",
                prepareResultJsonString
        );
    }

    @Override
    public UDPMessage commit(UDPMessage udpMessage) {
        boolean success = bookingDAO.confirmBooking(udpMessage.getData());

        // Create a new UDPMessage with an acknowledgement as payload
        String commitResultJsonString = "{\"success\": " + success + "}";
        return new UDPMessage(
                udpMessage.getOperation(),
                udpMessage.getTransactionId(),
                "HOTEL_PROVIDER",
                commitResultJsonString
        );
    }

    @Override
    public UDPMessage abort(UDPMessage udpMessage) {
        boolean success = bookingDAO.abortBooking(udpMessage.getData());

        // Create a new UDPMessage with an acknowledgement as payload
        String commitResultJsonString = "{\"success\": " + success + "}";
        return new UDPMessage(
                udpMessage.getOperation(),
                udpMessage.getTransactionId(),
                "HOTEL_PROVIDER",
                commitResultJsonString
        );
    }

    public UDPMessage getBookings(UDPMessage parsedMessage) {
        String bookingsString = bookingDAO.getBookings();

        // Create a new UDPMessage with the bookingsString as payload
        return new UDPMessage(
                parsedMessage.getOperation(),
                parsedMessage.getTransactionId(),
                "HOTEL_PROVIDER",
                bookingsString
        );
    }

    public UDPMessage getAvailableRooms(UDPMessage parsedMessage) {
        String availableRoomsString = bookingDAO.getAvailableRooms(parsedMessage.getData());

        // Create a new UDPMessage with the availableRoomsString as payload
        return new UDPMessage(
                parsedMessage.getOperation(),
                parsedMessage.getTransactionId(),
                "HOTEL_PROVIDER",
                availableRoomsString
        );
    }

}
