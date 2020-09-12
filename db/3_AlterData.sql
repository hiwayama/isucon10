UPDATE estate SET geo_hash=ST_GeoHash(longitude, latitude, 12),
	w1=GREATEST(door_width, door_height), w2=LEAST(door_width, door_height);
ALTER TABLE estate ADD INDEX i1(w1, w2);
ALTER TABLE estate ADD INDEX i2(latitude, longitude);