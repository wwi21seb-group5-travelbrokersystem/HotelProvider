package org.wwi21seb.vs.group5;

import org.wwi21seb.vs.group5.Logger.LoggerFactory;
import org.wwi21seb.vs.group5.service.HotelService;

import java.util.logging.Logger;

public class HotelRoomProviderMain {

    private static final Logger LOGGER = LoggerFactory.setupLogger(HotelRoomProviderMain.class.getName());

    public static void main(String[] args) {
        LOGGER.info("Starting HotelRoomProvider");
        HotelService hotelService = new HotelService();
        hotelService.start();
    }

}