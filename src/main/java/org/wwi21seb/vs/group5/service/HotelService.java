package org.wwi21seb.vs.group5.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.Logger.LoggerFactory;
import org.wwi21seb.vs.group5.Request.ReservationRequest;
import org.wwi21seb.vs.group5.Request.TransactionResult;
import org.wwi21seb.vs.group5.TwoPhaseCommit.*;
import org.wwi21seb.vs.group5.UDP.Operation;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.dao.BookingDAO;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.SocketException;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

public class HotelService {

    private static final Logger LOGGER = LoggerFactory.setupLogger(HotelService.class.getName());
    private static final String HOTEL_PROVIDER = "HotelProvider";
    private final DatagramSocket socket;
    private final byte[] buffer = new byte[16384];
    private final ConcurrentHashMap<UUID, ParticipantContext> contexts = new ConcurrentHashMap<>();
    private final LogWriter<ParticipantContext> logWriter = new LogWriter<>();
    private final BookingDAO bookingDAO;
    private final ObjectMapper mapper;
    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);

    public HotelService() {
        this.bookingDAO = new BookingDAO();
        this.mapper = new ObjectMapper();

        try {
            socket = new DatagramSocket(5002);
            LOGGER.info("Socket initialized on port 5002!");
        } catch (SocketException e) {
            throw new RuntimeException(e);
        }

        // Restore the state of the service
        // This is done by reading the log file and replaying the transactions
        for (ParticipantContext participantContext : logWriter.readAllLogs()) {
            LOGGER.log(Level.INFO, "Restoring transaction {0}", participantContext.getTransactionId());
            contexts.put(participantContext.getTransactionId(), participantContext);

            // Get participant
            Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();
            UDPMessage response = null;

            switch (participantContext.getTransactionState()) {
                case PREPARE -> {
                    if (participant.getVote().equals(Vote.NO)) {
                        // If the participant voted no, abort the transaction
                        UDPMessage message = new UDPMessage(Operation.ABORT, participantContext.getTransactionId(), participantContext.getCoordinator().getName(), null);
                        response = abort(message);
                    } else {
                        // If the participant voted yes, we need to ask the coordinator
                        // for the result of the transaction. This is because we probably
                        // crashed after voting yes, which is why we didn't receive the
                        // commit/abort message from the coordinator
                        response = new UDPMessage(Operation.RESULT, participantContext.getTransactionId(), participant.getName(), null);

                        // Schedule a timeout task to ask the other participants for their vote
                        // after 10 seconds, since we assume that the coordinator crashed
                        // We don't need to ask the coordinator for the result again, since the
                        // coordinator will send the result to us again after it has recovered
                        askParticipantForDecision(participantContext);
                    }
                }
                case COMMIT -> {
                    // If the transaction was already committed, commit it again
                    // The Coordinator will ignore the commit request if the transaction
                    // was already committed
                    UDPMessage message = new UDPMessage(Operation.COMMIT, participantContext.getTransactionId(), participantContext.getCoordinator().getName(), null);
                    response = commit(message);
                }
                case ABORT -> {
                    // If the transaction was already aborted, abort it again
                    // The Coordinator will ignore the abort request if the transaction
                    // was already aborted
                    UDPMessage message = new UDPMessage(Operation.ABORT, participantContext.getTransactionId(), participantContext.getCoordinator().getName(), null);
                    response = abort(message);
                }
            }

            if (response != null) {
                // Send the response to the coordinator
                LOGGER.log(Level.INFO, "Restored transaction {0} with response {1}", new Object[]{participantContext.getTransactionId(), response.getOperation()});

                try {
                    byte[] responseBytes = mapper.writeValueAsBytes(response);
                    LOGGER.info(participantContext.getCoordinator().toString());
                    DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, participantContext.getCoordinator().getUrl(), participantContext.getCoordinator().getPort());
                    LOGGER.info(String.format("Sending %s message to %s: %s", response.getOperation(), participantContext.getCoordinator().getName(), response.getData()));
                    socket.send(responsePacket);
                } catch (JsonProcessingException e) {
                    LOGGER.log(Level.SEVERE, "Failed to serialize response", e);
                    throw new RuntimeException(e);
                } catch (IOException e) {
                    LOGGER.log(Level.SEVERE, "Failed to send response", e);
                    throw new RuntimeException(e);
                }
            } else {
                LOGGER.log(Level.INFO, "Restored transaction {0}", participantContext.getTransactionId());
            }
        }

        LOGGER.info("Restored all transactions!");
    }

    public void start() {
        try {
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);

            while (true) {
                LOGGER.info("Waiting for message!");
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());

                UDPMessage parsedMessage = mapper.readValue(message, UDPMessage.class);
                UDPMessage response = null;
                LOGGER.info(String.format("Received %s message from %s: %s", parsedMessage.getOperation(), parsedMessage.getSender(), parsedMessage.getData()));

                switch (parsedMessage.getOperation()) {
                    case PREPARE -> response = prepare(parsedMessage);
                    case COMMIT -> response = commit(parsedMessage);
                    case ABORT -> response = abort(parsedMessage);
                    case GET_BOOKINGS -> response = getBookings(parsedMessage);
                    case GET_AVAILABILITY -> response = getAvailableRooms(parsedMessage);
                    case RESULT -> response = sendResult(parsedMessage);
                    default -> LOGGER.severe("Unknown operation received!");
                }

                if (response != null) {
                    InetAddress recipient = null;
                    int port = -1;
                    String recipientName = null;

                    if (parsedMessage.getSender().equals("CarProvider") && (parsedMessage.getOperation().equals(Operation.COMMIT) || parsedMessage.getOperation().equals(Operation.ABORT))) {
                        ParticipantContext participantContext = contexts.get(parsedMessage.getTransactionId());
                        if (participantContext == null) {
                            // This should not happen, but just in case
                            LOGGER.log(Level.SEVERE, "No context found for transaction {0}", parsedMessage.getTransactionId());
                            continue;
                        }

                        Coordinator coordinator = participantContext.getCoordinator();
                        recipient = coordinator.getUrl();
                        port = coordinator.getPort();
                        recipientName = coordinator.getName();
                    } else {
                        recipient = packet.getAddress();
                        port = packet.getPort();
                        recipientName = parsedMessage.getSender();
                    }

                    LOGGER.info(String.format("Sending %s message to %s: %s", response.getOperation(), recipientName, response.getData()));
                    byte[] responseBytes = mapper.writeValueAsBytes(response);
                    DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, recipient, port);
                    socket.send(responsePacket);
                } else {
                    LOGGER.info("No response to send!");
                }

                // System.exit(0);
            }
        } catch (SocketException e) {
            LOGGER.severe("Error while initializing socket!");
            throw new RuntimeException(e);
        } catch (IOException e) {
            LOGGER.severe("Error while receiving message!");
            throw new RuntimeException(e);
        }
    }

    public void scheduleContextDeletion(UUID transactionId) {
        scheduler.schedule(() -> {
            LOGGER.log(Level.INFO, "Deleting transaction {0}", transactionId);
            logWriter.deleteLog(transactionId);
            contexts.remove(transactionId);
        }, 1, TimeUnit.MINUTES);
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
        LOGGER.log(Level.INFO, "Prepare Transaction {0}", participantContext.getTransactionId());

        // Get participant
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        // Get the bookingContext of the hotel provider
        BookingContext bookingContext = participant.getBookingContext();
        ReservationRequest reservationRequest = new ReservationRequest(bookingContext.getResourceId(), bookingContext.getStartDate(), bookingContext.getEndDate(), bookingContext.getNumberOfPersons());
        UUID bookingId = bookingDAO.reserveRoom(reservationRequest, message.getTransactionId());
        TransactionResult transactionResult = null;

        if (bookingId == null) {
            // If the bookingId is null, the reservation failed
            // We need to set our decision to ABORT and send it to the coordinator
            participant.setVote(Vote.NO);
            transactionResult = new TransactionResult(false);
        } else {
            participant.setVote(Vote.YES);
            participantContext.setBookingIdForParticipant(bookingId, HOTEL_PROVIDER);
            transactionResult = new TransactionResult(true);
        }

        // Update the context in the log
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);
        return getSuccessMessage(message, transactionResult);
    }

    public UDPMessage commit(UDPMessage message) {
        // Get the participantContext from contexts
        ParticipantContext participantContext = contexts.get(message.getTransactionId());

        if (participantContext == null) {
            // If the participantContext is null, the transaction is unknown to our service
            // This is because there was a prepare request in which we weren't available
            // To the coordinator, this means that the transaction was aborted which is why
            // we need to return a successful TransactionResult to let the coordinator finish
            // its protocol
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(message, transactionResult);
        }

        participantContext.setTransactionState(TransactionState.COMMIT);
        LOGGER.log(Level.INFO, "Commit for transaction {0}", participantContext.getTransactionId());

        // Get the participant from the participantContext
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        // Cancel the timeout task
        CompletableFuture<Boolean> future = participant.getCommitFuture();
        if (future != null) {
            future.complete(true);
        }

        if (participant.isDone()) {
            // Double check if the transaction was already committed previously
            // If so, return a TransactionResult with success = true because
            // the transaction was already committed
            scheduleContextDeletion(participantContext.getTransactionId());
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(message, transactionResult);
        }

        boolean success = bookingDAO.confirmBooking(participant.getBookingContext().getBookingId());

        if (success) {
            // Update the participantContext
            participant.setDone();
        }

        // Set a timer to delete the context after 5 minutes
        // This is to prevent the contexts map from growing too large
        // After 5 minutes every participant should have finished its protocol
        scheduleContextDeletion(participantContext.getTransactionId());

        // Update the context in the log
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);
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

        if (participantContext == null) {
            // If the participantContext is null, the transaction is unknown to our service
            // This is because there was a prepare request in which we weren't available
            // To the coordinator, this means that the transaction was aborted which is why
            // we need to return a successful TransactionResult to let the coordinator finish
            // its protocol
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(udpMessage, transactionResult);
        }

        participantContext.setTransactionState(TransactionState.ABORT);
        LOGGER.log(Level.INFO, "Abort for transaction {0}", participantContext.getTransactionId());

        // Get the participant from the participantContext
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        // Cancel the timeout task
        CompletableFuture<Boolean> future = participant.getCommitFuture();
        if (future != null) {
            future.complete(true);
        }

        if (participant.isDone()) {
            // Double check if the transaction was already aborted previously
            // If so, return a TransactionResult with success = true because
            // the transaction was already aborted
            scheduleContextDeletion(participantContext.getTransactionId());
            TransactionResult transactionResult = new TransactionResult(true);
            return getSuccessMessage(udpMessage, transactionResult);
        }

        boolean success = bookingDAO.abortBooking(participant.getBookingContext().getBookingId());

        if (success) {
            // Update the participantContext
            participant.setDone();
        }

        // Set a timer to delete the context after 5 minutes
        // This is to prevent the contexts map from growing too large
        // After 5 minutes every participant should have finished its protocol
        scheduleContextDeletion(participantContext.getTransactionId());

        // Update the context in the log
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);
        TransactionResult transactionResult = new TransactionResult(success);
        return getSuccessMessage(udpMessage, transactionResult);
    }

    public UDPMessage sendResult(UDPMessage message) {
        ParticipantContext participantContext = contexts.get(message.getTransactionId());

        if (participantContext == null) {
            // If the participantContext is null, the transaction is unknown to our service
            // This is either because we weren't available in the prepare phase or because
            // we already deleted the context
            return null;
        }

        // Get the transaction state from the participantContext
        TransactionState transactionState = participantContext.getTransactionState();
        UDPMessage udpMessage = null;

        if (transactionState == TransactionState.COMMIT) {
            LOGGER.log(Level.INFO, "Sending commit for transaction {0}", message.getTransactionId());
            udpMessage = new UDPMessage(Operation.COMMIT, message.getTransactionId(), HOTEL_PROVIDER, null);
        } else if (transactionState == TransactionState.ABORT) {
            LOGGER.log(Level.INFO, "Sending abort for transaction {0}", message.getTransactionId());
            udpMessage = new UDPMessage(Operation.ABORT, message.getTransactionId(), HOTEL_PROVIDER, null);
        } else {
            // We can't send a result if we don't know the transaction result
            LOGGER.log(Level.WARNING, "Could not send result for transaction {0} because we don't know the transaction state", message.getTransactionId());
            return null;
        }

        return udpMessage;
    }

    public void askParticipantForDecision(ParticipantContext participantContext) {
        // Get the participant from the participantContext
        Participant participant = participantContext.getParticipants().stream().filter(p -> p.getName().equals(HOTEL_PROVIDER)).findFirst().orElseThrow();

        participant.resetCommitFuture();
        CompletableFuture<Boolean> commitFuture = participant.getCommitFuture();
        // After 10 seconds of no response, we assume the coordinator crashed
        // and ask the other participants for the result of the transaction
        commitFuture.orTimeout(10, TimeUnit.SECONDS).exceptionally(throwable -> {
            LOGGER.log(Level.WARNING, "Coordinator crashed, asking other participants for result of transaction {0}", participantContext.getTransactionId());
            for (Participant p : participantContext.getParticipants()) {
                if (!p.getName().equals(HOTEL_PROVIDER)) {
                    UDPMessage resultRequest = new UDPMessage(Operation.RESULT, participantContext.getTransactionId(), HOTEL_PROVIDER, null);
                    DatagramPacket resultRequestPacket = new DatagramPacket(buffer, buffer.length, p.getUrl(), p.getPort());
                    try {
                        byte[] resultRequestBytes = mapper.writeValueAsBytes(resultRequest);
                        resultRequestPacket.setData(resultRequestBytes);
                        socket.send(resultRequestPacket);
                    } catch (JsonProcessingException e) {
                        LOGGER.log(Level.SEVERE, "Error while serializing message", e);
                    } catch (IOException e) {
                        LOGGER.log(Level.SEVERE, "Error while sending message", e);
                    }

                    // Trigger the askParticipantForDecision method to invoke another timeout
                    // in case the participant also crashed
                    askParticipantForDecision(participantContext);
                }
            }

            return null;
        });

        // Update the context in the log
        logWriter.writeLog(participantContext.getTransactionId(), participantContext);
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
