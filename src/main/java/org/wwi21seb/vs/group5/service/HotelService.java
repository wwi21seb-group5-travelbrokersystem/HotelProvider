package org.wwi21seb.vs.group5.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Logger.LoggerFactory;
import org.wwi21seb.vs.group5.Request.ReservationRequest;
import org.wwi21seb.vs.group5.Request.TransactionResult;
import org.wwi21seb.vs.group5.TwoPhaseCommit.*;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.dao.BookingDAO;

import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HotelService {

    private static final Logger LOGGER = LoggerFactory.setupLogger(HotelService.class.getName());
    private final ConcurrentHashMap<UUID, ParticipantContext> contexts = new ConcurrentHashMap<>();
    private final LogWriter<ParticipantContext> logWriter = new LogWriter<>();
    private static final String HOTEL_PROVIDER = "HotelProvider";
    private final BookingDAO bookingDAO;
    private final ObjectMapper mapper;

    public HotelService() {
        this.bookingDAO = new BookingDAO();
        this.mapper = new ObjectMapper();

        // Restore the state of the service
        // This is done by reading the log file and replaying the transactions
        for (ParticipantContext participantContext : logWriter.readAllLogs()) {
            LOGGER.log(Level.INFO, "Restoring transaction {0}", participantContext.getTransactionId());
            contexts.put(participantContext.getTransactionId(), participantContext);
        }
    }

    public UDPMessage prepare(UDPMessage message) {
        // Parse the data payload of the UDPMessage to a CoordinatorContext
        CoordinatorContext coordinatorContext = null;
        try {
            coordinatorContext = mapper.readValue(message.getData(), CoordinatorContext.class);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Error parsing CoordinatorContext", e);
            throw new RuntimeException(e);
        }

        // Create a new ParticipantContext with the coordinatorContext
        ParticipantContext participantContext = new ParticipantContext(coordinatorContext);
        contexts.put(participantContext.getTransactionId(), participantContext);
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);

        // Get the bookingContext of the hotel provider
        BookingContext bookingContext = participantContext.getParticipants().stream().filter(participant -> participant.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow().getBookingContext();

        ReservationRequest reservationRequest = new ReservationRequest(bookingContext.getResourceId(), bookingContext.getStartDate(), bookingContext.getEndDate(), bookingContext.getNumberOfPersons());

        UUID bookingId = bookingDAO.reserveRoom(reservationRequest, message.getTransactionId());
        TransactionResult transactionResult = null;

        if (bookingId == null) {
            // If the bookingId is null, the reservation failed
            // We need to set our decision to ABORT and send it to the coordinator
            participantContext.setParticipants(participantContext.getParticipants().stream().peek(participant -> {
                if (participant.getName().equals(HOTEL_PROVIDER)) {

                    participant.setVote(Vote.NO);
                }
            }).toList());

            transactionResult = new TransactionResult(false);
        } else {
            participantContext.setParticipants(participantContext.getParticipants().stream().peek(participant -> {
                if (participant.getName().equals(HOTEL_PROVIDER)) {
                    participant.setVote(Vote.YES);
                }
            }).toList());
            participantContext.setBookingIdForParticipant(bookingId, HOTEL_PROVIDER);
            transactionResult = new TransactionResult(true);
        }

        // Update the context in the log
        contexts.put(participantContext.getTransactionId(), participantContext);
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);

        // Create a new UDPMessage with the bookingId as payload
        String transactionResultString = "";

        try {
            transactionResultString = mapper.writeValueAsString(transactionResult);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Could not parse TransactionResult to JSON", e);
            throw new RuntimeException(e);
        }

        return new UDPMessage(message.getOperation(), message.getTransactionId(), HOTEL_PROVIDER, transactionResultString);
    }

    public UDPMessage commit(UDPMessage message) {
        // Get the participantContext from contexts
        ParticipantContext participantContext = contexts.get(message.getTransactionId());
        participantContext.setTransactionState(TransactionState.COMMIT);

        // Get the participant from the participantContext
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        if (participant.isDone()) {
            // Double check if the transaction was already committed previously
            // If so, return a TransactionResult with success = true because
            // the transaction was already committed
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(message, transactionResult);
        }

        boolean success = bookingDAO.confirmBooking(participant.getBookingContext().getBookingId());

        if (success) {
            // Update the participantContext
            participantContext.setParticipants(participantContext.getParticipants().stream().peek(p -> {
                if (p.getName().equals(HOTEL_PROVIDER)) {
                    p.setDone();
                }
            }).toList());
        }

        // Update the context in the log
        contexts.put(participantContext.getTransactionId(), participantContext);
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);

        // Create a new UDPMessage with an acknowledgement as payload
        TransactionResult transactionResult = new TransactionResult(success);
        return getSuccessMessage(message, transactionResult);
    }

    private UDPMessage getSuccessMessage(UDPMessage message, TransactionResult transactionResult) {
        String transactionResultString;

        try {
            transactionResultString = mapper.writeValueAsString(transactionResult);
        } catch (JsonProcessingException e) {
            LOGGER.log(Level.SEVERE, "Could not parse TransactionResult to JSON", e);
            throw new RuntimeException(e);
        }

        return new UDPMessage(message.getOperation(), message.getTransactionId(), HOTEL_PROVIDER, transactionResultString);
    }

    public UDPMessage abort(UDPMessage udpMessage) {
        // Get the participantContext from contexts
        ParticipantContext participantContext = contexts.get(udpMessage.getTransactionId());

        // Get the participant from the participantContext
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        if (participant.isDone()) {
            // Double check if the transaction was already aborted previously
            // If so, return a TransactionResult with success = true because
            // the transaction was already aborted
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(udpMessage, transactionResult);
        }

        boolean success = bookingDAO.abortBooking(participant.getBookingContext().getBookingId());

        if (success) {
            // Update the participantContext
            participantContext.setParticipants(participantContext.getParticipants().stream().peek(p -> {
                if (p.getName().equals(HOTEL_PROVIDER)) {
                    p.setDone();
                }
            }).toList());
        }


        // Update the context in the log
        contexts.put(participantContext.getTransactionId(), participantContext);
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);

        TransactionResult transactionResult = new TransactionResult(success);
        return getSuccessMessage(udpMessage, transactionResult);
    }

    public UDPMessage getBookings(UDPMessage parsedMessage) {
        String bookingsString = bookingDAO.getBookings();

        // Create a new UDPMessage with the bookingsString as payload
        return new UDPMessage(parsedMessage.getOperation(), parsedMessage.getTransactionId(), HOTEL_PROVIDER, bookingsString);
    }

    public UDPMessage getAvailableRooms(UDPMessage parsedMessage) {
        String availableRoomsString = bookingDAO.getAvailableRooms(parsedMessage.getData());

        // Create a new UDPMessage with the availableRoomsString as payload
        return new UDPMessage(parsedMessage.getOperation(), parsedMessage.getTransactionId(), HOTEL_PROVIDER, availableRoomsString);
    }

}
