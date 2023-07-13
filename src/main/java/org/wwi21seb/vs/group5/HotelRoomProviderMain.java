package org.wwi21seb.vs.group5;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.wwi21seb.vs.group5.UDP.UDPMessage;
import org.wwi21seb.vs.group5.service.HotelService;

import java.io.IOException;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.SocketException;

public class HotelRoomProviderMain {

    public static void main(String[] args) {
        System.out.println("Hotel Room Provider: Initializing!");
        ObjectMapper mapper = new ObjectMapper();

        System.out.println("Hotel Room Provider: Initializing services!");
        HotelService hotelService = new HotelService();

        try (DatagramSocket socket = new DatagramSocket(5002)) {
            System.out.printf("Hotel Room Provider: Socket initialized on port %s!%n", socket.getLocalPort());

            byte[] buffer = new byte[16384];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            System.out.println("Hotel Room Provider: Waiting for message...");

            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                System.out.printf("Hotel Room Provider: Received message: %s from %s%n", message, packet.getAddress());

                UDPMessage parsedMessage = mapper.readValue(message, UDPMessage.class);
                UDPMessage response = null;

                switch (parsedMessage.getOperation()) {
                    case PREPARE -> {
                        System.out.printf("Hotel Room Provider - %s: Booking room!%n", parsedMessage.getTransactionId());
                        response = hotelService.prepare(parsedMessage);
                    }
                    case COMMIT -> {
                        System.out.printf("Hotel Room Provider - %s: Committing transaction!", parsedMessage.getTransactionId());
                        response = hotelService.commit(parsedMessage);
                    }
                    case ABORT -> {
                        System.out.printf("Hotel Room Provider - %s: Aborting transaction!%n", parsedMessage.getTransactionId());
                        response = hotelService.abort(parsedMessage);
                    }
                    case GET_BOOKINGS -> {
                        System.out.printf("Hotel Room Provider - %s: Getting bookings!%n", parsedMessage.getTransactionId());
                        response = hotelService.getBookings(parsedMessage);
                    }
                    case GET_AVAILABILITY -> {
                        System.out.printf("Hotel Room Provider - %s: Getting available rooms!%n", parsedMessage.getTransactionId());
                        response = hotelService.getAvailableRooms(parsedMessage);
                    }
                    default -> {
                        System.out.printf("Hotel Room Provider - %s: Unknown operation!%n", parsedMessage.getTransactionId());
                    }
                }

                if (response != null) {
                    System.out.printf("Hotel Room Provider - %s: Sending response to %s:%s: %s%n", parsedMessage.getTransactionId(), packet.getAddress(), packet.getPort(), response);
                    byte[] responseBytes = mapper.writeValueAsBytes(response);
                    DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(responsePacket);
                }

                System.out.println("%nHotel Room Provider: Waiting for message...");
            }
        } catch (SocketException e) {
            System.out.println("Hotel Room Provider: Error while initializing socket!");
            e.printStackTrace();
            throw new RuntimeException(e);
        } catch (IOException e) {
            System.out.println("Hotel Room Provider: Error while receiving message!");
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

}