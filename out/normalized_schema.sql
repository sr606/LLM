CREATE TABLE Company (
    CompanyId SERIAL PRIMARY KEY,
    CompanyName VARCHAR(255),
    Website VARCHAR(255)
);

CREATE TABLE Location (
    LocationId SERIAL PRIMARY KEY,
    City VARCHAR(100),
    Country VARCHAR(100)
);

CREATE TABLE Customer (
    CustomerId VARCHAR(50) PRIMARY KEY,
    FirstName VARCHAR(50),
    LastName VARCHAR(50),
    Email VARCHAR(255) UNIQUE,
    SubscriptionDate DATE,
    CompanyId INT,
    LocationId INT,
    FOREIGN KEY (CompanyId) REFERENCES Company(CompanyId),
    FOREIGN KEY (LocationId) REFERENCES Location(LocationId)
);

CREATE TABLE Phone (
    PhoneId SERIAL PRIMARY KEY,
    CustomerId VARCHAR(50),
    PhoneNumber VARCHAR(50),
    PhoneType VARCHAR(20),
    FOREIGN KEY (CustomerId) REFERENCES Customer(CustomerId)
);

