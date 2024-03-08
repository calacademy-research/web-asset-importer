# removes images from image server uploaded in last X days
DELETE FROM images WHERE datetime > (now() - interval  40 day );