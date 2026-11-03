-- One database per service that needs persistent storage.
-- Runs automatically only the first time the Postgres volume is created.
CREATE DATABASE quake_correlator;
CREATE DATABASE quake_subscription;
CREATE DATABASE quake_notifier;
