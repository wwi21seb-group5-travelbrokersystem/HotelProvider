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

            byte[] buffer = new byte[1024];
            DatagramPacket packet = new DatagramPacket(buffer, buffer.length);
            System.out.println("Hotel Room Provider: Waiting for message...");

            while (true) {
                socket.receive(packet);
                String message = new String(packet.getData(), 0, packet.getLength());
                System.out.printf("Hotel Room Provider: Received message: %s%n", message);

                UDPMessage parsedMessage = mapper.readValue(message, UDPMessage.class);
                System.out.printf("Hotel Room Provider: Parsed message: %s%n", parsedMessage);

                UDPMessage response = null;

                switch (parsedMessage.getOperation()) {
                    case PREPARE -> {
                        System.out.println("Hotel Room Provider: Booking room!");
                        response = hotelService.prepare(parsedMessage);
                    }
                    case COMMIT -> {
                        System.out.println("Hotel Room Provider: Committing transaction!");
                        response = hotelService.commit(parsedMessage);
                    }
                    case ABORT -> {
                        System.out.println("Hotel Room Provider: Aborting transaction!");
                        response = hotelService.abort(parsedMessage);
                    }
                    case GET_BOOKINGS -> {
                        System.out.println("Hotel Room Provider: Getting bookings!");
                        response = hotelService.getBookings(parsedMessage);
                    }
                    case GET_AVAILABILITY -> {
                        System.out.println("Hotel Room Provider: Getting available rooms!");
                        response = hotelService.getAvailableRooms(parsedMessage);
                    }
                    default -> {
                        System.out.println("Hotel Room Provider: Unknown operation!");
                    }
                }

                if (response != null) {
                    System.out.printf("Hotel Room Provider: Sending response: %s%n", response);
                    byte[] responseBytes = mapper.writeValueAsBytes(response);
                    DatagramPacket responsePacket = new DatagramPacket(responseBytes, responseBytes.length, packet.getAddress(), packet.getPort());
                    socket.send(responsePacket);
                }

                System.out.println("Hotel Room Provider: Waiting for message...");
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