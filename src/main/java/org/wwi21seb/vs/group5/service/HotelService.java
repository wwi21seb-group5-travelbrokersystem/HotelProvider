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
        return null;
    }

    @Override
    public UDPMessage commit(UDPMessage udpMessage) {
        return null;
    }

    @Override
    public UDPMessage abort(UDPMessage udpMessage) {
        return null;
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
