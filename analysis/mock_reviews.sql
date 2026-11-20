-- Synthetic input with the same rating column and table names as the scrapers.
CREATE TABLE HotelReview (rating VARCHAR(225));
CREATE TABLE RestaurantReview (rating VARCHAR(225));
CREATE TABLE AttractionReview (rating VARCHAR(225));
INSERT INTO HotelReview VALUES ('50'), ('40'), ('40'), ('30'), (NULL);
INSERT INTO RestaurantReview VALUES ('50'), ('30'), ('30'), ('20'), ('bad');
INSERT INTO AttractionReview VALUES ('4.5'), ('5.0'), ('5'), ('4.0'), ('50');
