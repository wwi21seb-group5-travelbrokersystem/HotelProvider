CREATE TABLE bookings
(
    booking_id  uuid           NOT NULL,
    start_date  date           NOT NULL,
    end_date    date           NOT NULL,
    room_id     uuid           NOT NULL,
    total_price decimal(10, 2) NOT NULL
);

ALTER TABLE bookings
    ADD CONSTRAINT booking_id
        PRIMARY KEY (booking_id);

CREATE TABLE rooms
(
    type            varchar(25)    NOT NULL,
    capacity        integer        NOT NULL,
    room_id         uuid           NOT NULL,
    price_per_night decimal(10, 2) NOT NULL
);

ALTER TABLE rooms
    ADD CONSTRAINT room_id
        PRIMARY KEY (room_id);

ALTER TABLE bookings
    ADD CONSTRAINT fk_bookings_rooms
        FOREIGN KEY (room_id) REFERENCES rooms (room_id);
